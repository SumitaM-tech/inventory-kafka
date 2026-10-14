"""Online Retail II -> (a) first 80% of time = history, stored on HDFS for forecasting
                       (b) last 20% = live stream replayed by the channel producers."""
import numpy as np, pandas as pd, os
from common import *
os.makedirs("data", exist_ok=True); rng = np.random.default_rng(7)
src = next((f"data/{f}" for f in os.listdir("data") if f.lower().startswith("online_retail") and f.lower().endswith((".xlsx", ".csv"))), None)
if src:
    print("Reading", src, "(xlsx may take 1-3 min)")
    raw = pd.concat(pd.read_excel(src, sheet_name=None).values()) if src.endswith("xlsx") else pd.read_csv(src, encoding="latin1")
    df = raw.rename(columns={"StockCode": "sku", "InvoiceDate": "ts", "Quantity": "qty"}); df["sku"] = df.sku.astype(str)
    df = df[df.sku.str.fullmatch(r"\d{5}[A-Za-z]?") & (df.Price > 0)].copy(); df["ts"] = pd.to_datetime(df.ts)
    df = df[df.sku.isin(df[df.qty > 0].groupby("sku").qty.sum().nlargest(TOP_SKUS).index)]
else:
    print("No dataset in data/ -> synthetic data"); rows = []
    for i in range(TOP_SKUS):
        base = rng.integers(20, 80)
        for d in pd.date_range("2010-01-01", "2011-12-09"):
            rows += [(f"SKU{i:03d}", d + pd.Timedelta(hours=int(rng.integers(8, 18))), int(rng.poisson(base * (1 + .3 * np.sin(d.dayofyear / 58) + .2 * (d.dayofweek < 5)))))]
    df = pd.DataFrame(rows, columns=["sku", "ts", "qty"])
df = df.sort_values("ts")
cap = df.groupby("sku").qty.transform(lambda q: q.abs().quantile(.99)).clip(lower=1)   # clip bulk-order / bulk-cancellation outliers
df["qty"] = df.qty.clip(-cap, cap).round().astype(int); cut = df.ts.quantile(.8)
hist, live = df[df.ts < cut], df[df.ts >= cut].copy()
dd = hist[hist.qty > 0].assign(date=lambda x: x.ts.dt.normalize()).groupby(["date", "sku"]).qty.sum().reset_index()
hdfs_put(HIST, dd.to_csv(index=False))
daily = hist[hist.qty > 0].groupby("sku").qty.sum() / max((hist.ts.max() - hist.ts.min()).days, 1)   # avg units/day per SKU
pd.DataFrame([(w, k, max(50, int(np.ceil(v * 21 * WH_SHARE[w])))) for k, v in daily.items() for w in WAREHOUSES], columns=["wh", "sku", "qty"]).to_csv("data/init_stock.csv", index=False)
live["channel"] = rng.choice(CHANNELS, len(live)); live["wh"] = rng.choice(WAREHOUSES, len(live), p=[WH_SHARE[w] for w in WAREHOUSES]); live["delta"] = -live.qty
live[["ts", "sku", "channel", "wh", "delta"]].to_csv("data/stream_events.csv", index=False)
print(f"history: {len(dd)} sku-days -> HDFS {HIST} | live stream: {len(live)} events | cutoff {cut:%Y-%m-%d}")
