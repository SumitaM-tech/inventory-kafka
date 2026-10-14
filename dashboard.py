import io, sqlite3, time, pandas as pd, plotly.express as px, plotly.graph_objects as go, streamlit as st
from common import *

st.set_page_config("Inventory Control Tower", page_icon="📦", layout="wide")
st.markdown("""<style>
.block-container{padding-top:1.2rem}
div[data-testid=stMetric]{background:linear-gradient(135deg,#1b2433,#121826);border:1px solid #2a3550;border-radius:14px;padding:14px 18px}
div[data-testid=stMetricLabel]{opacity:.75}
.badge{display:inline-block;padding:3px 12px;border-radius:20px;font-size:.8rem;font-weight:600;margin-right:6px}
.live{background:#14532d;color:#86efac}.warn{background:#7c2d12;color:#fdba74}.info{background:#1e3a8a;color:#93c5fd}
</style>""", unsafe_allow_html=True)
TPL = "plotly_dark"; STATUS_COL = {"APPLIED": "#22c55e", "REJECTED_OVERSELL": "#ef4444"}
q = lambda s, p=(): pd.read_sql(s, sqlite3.connect(DB, timeout=30), params=p)
def fig_fmt(f, h=330): f.update_layout(template=TPL, height=h, margin=dict(l=10, r=10, t=45, b=10), legend=dict(orientation="h", y=-.2)); return f

@st.cache_data(ttl=600)
def history(): return pd.read_csv(io.StringIO(hdfs_get(HIST)), parse_dates=["date"], dtype={"sku": str})

# ---------- sidebar ----------
st.sidebar.header("Controls")
whs = st.sidebar.multiselect("Warehouses", WAREHOUSES, WAREHOUSES)
sec = st.sidebar.slider("Refresh (seconds)", 2, 30, 5)
live_on = st.sidebar.toggle("Live auto-refresh", True)
if st.sidebar.button("Refresh now"): st.rerun()

# ---------- data ----------
try:
    stock = q("select * from stock"); stock = stock[stock.wh.isin(whs)]
    k = q("select count(*) n, coalesce(sum(status!='APPLIED'),0) rej, coalesce(sum(case when status!='APPLIED' then -delta end),0) lost, "
          "coalesce(sum(case when type='SALE' and status='APPLIED' then -delta end),0) sold, coalesce(sum(case when type='RESTOCK' then delta end),0) restocked, coalesce(sum(case when type='TRANSFER_IN' then delta end),0) moved from events").iloc[0]
    ch = q("select channel, status, count(*) n, sum(abs(delta)) units from events group by 1,2")
    chtime = q("select strftime('%H:%M',proc_ts) t, channel, count(*) n from events group by 1,2 order by 1 desc limit 120")
    rate = q("select strftime('%H:%M',proc_ts) t, count(*) n from events group by 1 order by 1 desc limit 30")
    recent = q("select ts,wh,sku,channel,type,delta,status,stock_after,consumer from events order by rowid desc limit 100")
    topsku = q("select sku, sum(-delta) units from events where type='SALE' and status='APPLIED' group by 1 order by 2 desc limit 10")
    cons = q("select consumer, count(*) n from events group by 1")
    summ = q("select * from forecast_summary order by restock_qty desc")
    summ_wh = q("select * from forecast_wh"); sw = summ_wh[summ_wh.wh.isin(whs)]
    alerts = q("select ts,wh,sku,kind,msg from alerts order by id desc limit 200")
    assert len(stock)
except Exception:
    st.info("Waiting for the consumers to process the first events..."); time.sleep(3); st.rerun()

synthetic = stock.sku.str.startswith("SKU").any()
risk = int((sw.stock <= sw.reorder_point).sum()) if len(sw) else 0
fill = 100 * (1 - risk / len(sw)) if len(sw) else 0   # share of SKU x warehouse positions above their reorder point

st.title("📦 Inventory Control Tower")
st.markdown(f'<span class="badge live">● LIVE</span><span class="badge {"warn" if synthetic else "info"}">'
            f'{"Synthetic data" if synthetic else "Online Retail II (UCI)"}</span>'
            f'<span class="badge info">Kafka → Consumers → HDFS → ARIMA/Prophet</span>', unsafe_allow_html=True)
st.write("")
c = st.columns(7)
c[0].metric("Units in stock", f"{int(stock.qty.sum()):,}"); c[1].metric("Events processed", f"{int(k.n):,}")
c[2].metric("Units sold", f"{int(k.sold):,}"); c[3].metric("Oversells blocked", f"{int(k.rej):,}", f"{int(k.lost):,} units saved", delta_color="off")
c[4].metric("Supplier restock", f"{int(k.restocked):,}"); c[5].metric("Rebalanced units", f"{int(k.moved):,}"); c[6].metric("Below reorder pt", risk, delta_color="inverse")

t0, t1, t2, t3, t4, t5 = st.tabs(["🏠 Overview", "📊 Live stock", "🔮 Forecast", "🛒 Restock plan", "📡 Channels", "🚨 Alerts & stream"])

with t0:
    a, b, d = st.columns([1.2, 1.4, 1.4])
    g = go.Figure(go.Indicator(mode="gauge+number", value=fill, number={"suffix": "%"}, title={"text": "Positions above reorder point"},
        gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#38bdf8"}, "steps": [{"range": [0, 30], "color": "#7f1d1d"}, {"range": [30, 60], "color": "#78350f"}, {"range": [60, 100], "color": "#14532d"}]}))
    a.plotly_chart(fig_fmt(g), width="stretch")
    whs_tot = stock.groupby("wh").qty.sum().reset_index()
    b.plotly_chart(fig_fmt(px.bar(whs_tot, x="wh", y="qty", color="wh", text_auto=True, title="Stock by warehouse")), width="stretch")
    d.plotly_chart(fig_fmt(px.pie(ch.groupby("channel").n.sum().reset_index(), names="channel", values="n", hole=.55, title="Event share by channel")), width="stretch")
    a, b = st.columns(2)
    a.plotly_chart(fig_fmt(px.bar(topsku.sort_values("units"), x="units", y="sku", orientation="h", title="Top selling SKUs (units)")), width="stretch")
    b.plotly_chart(fig_fmt(px.line(rate.sort_values("t"), x="t", y="n", markers=True, title="Throughput (events / minute)")), width="stretch")

with t1:
    a, b = st.columns([3, 2])
    hm = stock.pivot(index="sku", columns="wh", values="qty")
    a.plotly_chart(fig_fmt(px.imshow(hm, aspect="auto", color_continuous_scale="RdYlGn", text_auto=True, title="Stock heatmap (SKU × warehouse)"), 520), width="stretch")
    tm = stock.assign(root="All")
    b.plotly_chart(fig_fmt(px.treemap(tm, path=["root", "wh", "sku"], values="qty", color="qty", color_continuous_scale="Blues", title="Stock treemap"), 520), width="stretch")
    sku_sel = st.selectbox("Stock level history for SKU", sorted(stock.sku.unique()))
    hist_ev = q("select rowid r, wh, stock_after, type from events where sku=? order by rowid desc limit 400", (sku_sel,)).sort_values("r")
    st.plotly_chart(fig_fmt(px.line(hist_ev, x="r", y="stock_after", color="wh", title=f"Stock after each event · {sku_sel}"), 350), width="stretch")

with t2:
    if len(summ):
        sku = st.selectbox("SKU", summ.sku, key="fc"); f = q("select * from forecast where sku=? order by day", (sku,)); h = history(); h = h[h.sku == sku].tail(60)
        r = summ[summ.sku == sku].iloc[0]; m = st.columns(5)
        m[0].metric("Model", r.model.upper()); m[1].metric("Backtest WAPE", f"{r.wape:.2f}"); m[2].metric("Reorder point", int(r.reorder_point))
        m[3].metric("Current stock", int(r.total_stock)); m[4].metric("Suggested restock", int(r.restock_qty))
        fig = go.Figure([go.Scatter(x=h.date, y=h.qty, name="History (HDFS)", line=dict(color="#60a5fa")), go.Scatter(x=f.day, y=f.hi, line=dict(width=0), showlegend=False),
                         go.Scatter(x=f.day, y=f.lo, fill="tonexty", line=dict(width=0), name="80% interval", fillcolor="rgba(250,204,21,.2)"),
                         go.Scatter(x=f.day, y=f.yhat, name="Forecast", line=dict(dash="dash", color="#facc15", width=3))])
        fig.update_layout(title=f"Daily demand forecast · {sku}"); st.plotly_chart(fig_fmt(fig, 420), width="stretch")
        a, b = st.columns(2)
        a.plotly_chart(fig_fmt(px.bar(summ, x="sku", y="wape", color="model", title="Model accuracy per SKU (lower WAPE is better)")), width="stretch")
        b.plotly_chart(fig_fmt(px.pie(summ, names="model", hole=.5, title="Winning model (ARIMA vs Prophet)")), width="stretch")
    else: st.info("Forecasts appear after the first forecast run (about a minute).")

with t3:
    if len(sw):
        s = sw.copy(); s["cover"] = (s.stock / s.reorder_point.clip(lower=1)).round(2)
        s["status"] = s.apply(lambda x: "🔴 CRITICAL" if x.stock <= x.reorder_point else ("🟠 WATCH" if x.stock <= 1.5 * x.reorder_point else "🟢 OK"), axis=1)
        a, b = st.columns(2)
        a.plotly_chart(fig_fmt(px.imshow(s.pivot(index="sku", columns="wh", values="cover"), aspect="auto", color_continuous_scale="RdYlGn", zmin=0, zmax=2, text_auto=".1f",
                                         title="Coverage = stock ÷ reorder point (below 1 = reorder)"), 480), width="stretch")
        act = s.melt(id_vars=["sku", "wh"], value_vars=["restock_qty", "transfer_in"], var_name="source", value_name="units"); act = act[act.units > 0]
        if len(act): b.plotly_chart(fig_fmt(px.bar(act, x="sku", y="units", color="source", title="Planned actions: supplier restock vs warehouse transfer"), 480), width="stretch")
        else: b.info("All warehouses are above their reorder points.")
        s = s.sort_values("cover")
        st.dataframe(s[["sku", "wh", "status", "stock", "reorder_point", "target", "restock_qty", "transfer_in", "updated"]], width="stretch", hide_index=True,
                     column_config={"restock_qty": st.column_config.ProgressColumn("supplier restock", max_value=max(int(s.restock_qty.max()), 1), format="%d"),
                                    "transfer_in": st.column_config.ProgressColumn("transfer in", max_value=max(int(s.transfer_in.max()), 1), format="%d")})
        st.subheader("Latest restock / transfer events")
        st.dataframe(q("select ts, wh, sku, channel, type, delta, status from events where type in ('RESTOCK','TRANSFER_IN','TRANSFER_OUT') order by rowid desc limit 30"), width="stretch", hide_index=True)
    else: st.info("Restock plan appears after the first forecast run.")

with t4:
    a, b = st.columns(2)
    a.plotly_chart(fig_fmt(px.bar(ch, x="channel", y="n", color="status", color_discrete_map=STATUS_COL, barmode="stack", title="Events by channel and outcome")), width="stretch")
    ov = ch[ch.status != "APPLIED"]
    b.plotly_chart(fig_fmt(px.bar(ov, x="channel", y="units", color="channel", title="Units blocked by oversell, per channel")), width="stretch")
    a, b = st.columns(2)
    a.plotly_chart(fig_fmt(px.area(chtime.sort_values("t"), x="t", y="n", color="channel", title="Channel activity over time")), width="stretch")
    b.plotly_chart(fig_fmt(px.pie(cons, names="consumer", values="n", hole=.5, title="Load across consumers (partition sharing)")), width="stretch")

with t5:
    a, b = st.columns([2, 3])
    kinds = alerts.kind.value_counts().reset_index(); kinds.columns = ["kind", "n"]
    a.plotly_chart(fig_fmt(px.bar(kinds, x="kind", y="n", color="kind", title="Alerts by type")), width="stretch")
    a.subheader("Alerts"); a.dataframe(alerts, width="stretch", hide_index=True, height=360)
    b.subheader("Latest events"); b.dataframe(recent, width="stretch", hide_index=True, height=520)

if live_on: time.sleep(sec); st.rerun()
