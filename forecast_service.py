"""Demand forecasting + replenishment planner.
History comes from HDFS, merged with live demand (incl. rejected oversells = lost demand) from the central inventory.
Per SKU: backtest ARIMA vs Prophet on a 14-day hold-out, keep the lower-WAPE model, forecast with 80% intervals,
compute safety stock / reorder point / restock qty. --auto publishes RESTOCK events back into Kafka.
Usage: python forecast_service.py [--loop SECONDS] [--auto] [--horizon 7]"""
import argparse, io, math, time, json, uuid, warnings, logging, numpy as np, pandas as pd
from common import *
warnings.filterwarnings("ignore"); logging.getLogger("cmdstanpy").setLevel(logging.ERROR); logging.getLogger("prophet").setLevel(logging.ERROR)
ap = argparse.ArgumentParser(); ap.add_argument("--auto", action="store_true"); ap.add_argument("--loop", type=int, default=0); ap.add_argument("--horizon", type=int, default=7); A = ap.parse_args()

def arima(idx, y, h):
    from statsmodels.tsa.arima.model import ARIMA
    f = ARIMA(y, order=(2, 1, 2)).fit().get_forecast(h); ci = f.conf_int(alpha=.2)
    return np.clip(f.predicted_mean, 0, None), np.clip(ci[:, 0], 0, None), ci[:, 1]
def prophet(idx, y, h):
    from prophet import Prophet
    m = Prophet(daily_seasonality=False, interval_width=.8); m.fit(pd.DataFrame({"ds": idx, "y": y}))
    f = m.predict(m.make_future_dataframe(h)).tail(h)
    return f.yhat.clip(lower=0).values, f.yhat_lower.clip(lower=0).values, f.yhat_upper.values
MODELS = {"arima": arima}
try: import prophet as _p; MODELS["prophet"] = prophet
except Exception: print("Prophet not installed -> ARIMA only")

last_sim = None   # simulated (dataset) clock at the previous run

def run():
    """Per SKU: forecast network demand -> split by each warehouse's demand share -> per-warehouse reorder point and
    order-up-to level. Short warehouses are first topped up by TRANSFERs from surplus warehouses, the rest by SUPPLIER restocks.
    The stream replays history fast, so the review period (simulated days since last run) is added to the protection interval."""
    global last_sim
    conn = db()
    hist = pd.read_csv(io.StringIO(hdfs_get(HIST)), parse_dates=["date"], dtype={"sku": str})
    live = pd.read_sql("select date(ts) date, sku, sum(-delta) qty from events where type='SALE' group by 1,2", conn, parse_dates=["date"])
    if len(live): live = live[live.date < live.date.max()]            # drop the still-filling day
    dd = pd.concat([hist, live]).groupby(["date", "sku"]).qty.sum().reset_index()
    sim = conn.execute("select max(ts) from events where type='SALE'").fetchone()[0]; sim = pd.Timestamp(sim) if sim else None
    cyc = min(30, max(0, (sim - last_sim).days)) if sim is not None and last_sim is not None else 0
    if sim is not None: last_sim = sim
    dem = pd.read_sql("select wh, sku, sum(-delta) q from events where type='SALE' group by 1,2", conn)   # incl. rejected = lost demand
    prod, out = None, []
    for sku, g in dd.groupby("sku"):
        s = g.set_index("date").qty.asfreq("D", fill_value=0); y = s.values.astype(float)
        if len(y) < 45: continue
        scores = {}
        for n, fn in MODELS.items():
            try: scores[n] = np.abs(y[-14:] - fn(s.index[:-14], y[:-14], 14)[0]).sum() / max(y[-14:].sum(), 1)
            except Exception: pass
        if not scores: continue
        H = max(A.horizon, LEAD_DAYS) + cyc; prot = LEAD_DAYS + cyc
        name = min(scores, key=scores.get); yh, lo, hi = MODELS[name](s.index, y, H)
        days = pd.date_range(s.index[-1] + pd.Timedelta(days=1), periods=H)
        conn.execute("delete from forecast where sku=?", (sku,))
        conn.executemany("insert or replace into forecast values(?,?,?,?,?,?)", [(sku, str(t.date()), float(a), float(b), float(c), name) for t, a, b, c in zip(days, yh, lo, hi)])
        dsku = dem[dem.sku == sku].set_index("wh").q.reindex(WAREHOUSES).fillna(0)
        share = dsku / dsku.sum() if dsku.sum() > 0 else pd.Series(WH_SHARE)
        rows = dict(conn.execute("select wh, qty from stock where sku=?", (sku,)).fetchall())     # fresh stock, taken after the slow model fits
        std, pos = y[-28:].std(), {}
        for w in WAREHOUSES:
            sh = float(share[w]); safety = Z * std * math.sqrt(prot * sh)
            pos[w] = dict(cur=int(rows.get(w, init_qty(w, sku))), rop=int(sh * yh[:prot].sum() + safety), target=int(sh * yh.sum() + safety), tin=0, order=0)
        spare = {w: max(0, p["cur"] - int(1.5 * p["target"])) for w, p in pos.items()}              # stock a donor can give away
        moves = []
        for w, p in pos.items():
            if p["cur"] > p["rop"]: continue
            need = max(0, p["target"] - p["cur"])
            for dn in WAREHOUSES:
                if dn == w or need <= 0 or spare[dn] <= 0: continue
                q = min(need, spare[dn]); spare[dn] -= q; need -= q; p["tin"] += q; moves.append((dn, w, q))
            p["order"] = need
        for w, p in pos.items():
            conn.execute("insert or replace into forecast_wh values(?,?,?,?,?,?,?,datetime('now'))", (sku, w, p["cur"], p["rop"], p["target"], p["order"], p["tin"]))
        st, rop, need = (sum(p[k] for p in pos.values()) for k in ("cur", "rop", "order"))
        conn.execute("insert or replace into forecast_summary values(?,?,?,?,?,?,?,datetime('now'))", (sku, name, round(scores[name], 3), float(yh[:max(A.horizon, LEAD_DAYS)].sum()), st, rop, need))
        conn.commit(); print(f"{sku}: model={name} wape={scores[name]:.2f} stock={st} rop={rop} supplier_restock={need} transfers={sum(m[2] for m in moves)} (review {cyc}d)")
        if A.auto:
            from confluent_kafka import Producer; prod = prod or Producer({"bootstrap.servers": BOOTSTRAP})
            ts = str(pd.Timestamp.now())[:19]
            def send(w, ch, typ, delta):
                e = {"event_id": str(uuid.uuid4()), "ts": ts, "wh": w, "sku": sku, "channel": ch, "type": typ, "delta": int(delta)}
                prod.produce(TOPIC, key=f"{w}|{sku}", value=json.dumps(e)); out.append(e)
            for dn, w, q in moves: send(dn, "TRANSFER", "TRANSFER_OUT", -q); send(w, "TRANSFER", "TRANSFER_IN", q)
            for w, p in pos.items():
                if p["order"] > 0: send(w, "SUPPLIER", "RESTOCK", p["order"])
    if prod: prod.flush()
    return out

while True:
    run()
    if not A.loop: break
    time.sleep(A.loop)
