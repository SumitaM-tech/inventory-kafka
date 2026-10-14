"""Shared config, schema and HDFS helpers (HDFS via WebHDFS, with a local fallback so the demo never blocks)."""
import os, sqlite3
BOOTSTRAP = "localhost:9092"
TOPIC, DLQ, ALERTS = "stock-events", "stock-events-dlq", "stock-alerts"
DB = "data/inventory.db"; HDFS_URL = "http://localhost:9870"; HDFS_USER = "root"
LAKE = "/inventory/lake"; HIST = "/inventory/history/daily_demand.csv"
CHANNELS = ["WEB", "STORE", "MOBILE", "MARKETPLACE"]; WAREHOUSES = ["WH-NORTH", "WH-SOUTH", "WH-EAST"]
INIT_STOCK, TOP_SKUS, LEAD_DAYS, Z = 1200, 20, 5, 1.65   # Z=1.65 -> 95% service level
WH_SHARE = {"WH-NORTH": .40, "WH-SOUTH": .35, "WH-EAST": .25}   # share of demand each warehouse serves
_init = None
def init_qty(wh, sku):
    """Opening stock per warehouse/SKU (data/init_stock.csv written by prepare_data.py, ~3 weeks of demand)."""
    global _init
    if _init is None:
        try:
            import csv; _init = {(r["wh"], r["sku"]): int(r["qty"]) for r in csv.DictReader(open("data/init_stock.csv"))}
        except Exception: _init = {}
    return _init.get((wh, sku), INIT_STOCK)
SCHEMA = """
create table if not exists stock(wh text, sku text, qty int, primary key(wh, sku));
create table if not exists events(id text primary key, ts text, wh text, sku text, channel text, type text, delta int,
  status text, stock_after int, consumer text, proc_ts text default (datetime('now')));
create table if not exists forecast(sku text, day text, yhat real, lo real, hi real, model text, primary key(sku, day));
create table if not exists forecast_summary(sku text primary key, model text, wape real, forecast_h real, total_stock int,
  reorder_point int, restock_qty int, updated text);
create table if not exists forecast_wh(sku text, wh text, stock int, reorder_point int, target int, restock_qty int, transfer_in int,
  updated text default (datetime('now')), primary key(sku, wh));
create table if not exists alerts(id integer primary key autoincrement, ts text default (datetime('now')), wh text, sku text, kind text, msg text);"""

def db():
    os.makedirs("data", exist_ok=True)
    c = sqlite3.connect(DB, timeout=60); c.execute("pragma journal_mode=wal"); c.executescript(SCHEMA); return c

def _client():
    from hdfs import InsecureClient
    c = InsecureClient(HDFS_URL, user=HDFS_USER); c.status("/"); return c

def hdfs_put(path, text):
    try: _client().write(path, data=text, encoding="utf-8", overwrite=True)
    except Exception as ex:
        print(f"[WARN] HDFS unavailable ({type(ex).__name__}) -> local fallback for {path}")
        p = os.path.join("data/hdfs_fallback", path.lstrip("/")); os.makedirs(os.path.dirname(p), exist_ok=True); open(p, "w").write(text)

def hdfs_get(path):
    try:
        with _client().read(path, encoding="utf-8") as r: return r.read()
    except Exception: return open(os.path.join("data/hdfs_fallback", path.lstrip("/"))).read()
