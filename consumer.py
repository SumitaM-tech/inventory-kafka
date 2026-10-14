"""Warehouse consumer (run several: python consumer.py c1 / c2). Same consumer group -> partitions are shared.
Applies events to central inventory: idempotent, oversell-proof, DLQ for bad messages, low-stock alerts,
and writes raw events to the HDFS data lake partitioned by date (dt=YYYY-MM-DD)."""
import json, sys, time
from confluent_kafka import Consumer, Producer
from common import *
ID = sys.argv[1] if len(sys.argv) > 1 else "c1"
d = db(); p = Producer({"bootstrap.servers": BOOTSTRAP})
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "warehouse-consumers", "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe([TOPIC]); lake, pending, last = {}, 0, time.time()

def alert(wh, sku, kind, msg):
    d.execute("insert into alerts(wh,sku,kind,msg) values(?,?,?,?)", (wh, sku, kind, msg))
    p.produce(ALERTS, json.dumps({"wh": wh, "sku": sku, "kind": kind, "msg": msg}))

def apply(e):
    if not all(k in e for k in ("event_id", "ts", "wh", "sku", "channel", "type", "delta")) or e["wh"] not in WAREHOUSES: raise ValueError("schema violation")
    with d:   # one transaction per event
        if d.execute("select 1 from events where id=?", (e["event_id"],)).fetchone(): return "DUPLICATE"
        r = d.execute("select qty from stock where wh=? and sku=?", (e["wh"], e["sku"])).fetchone()
        cur = r[0] if r else init_qty(e["wh"], e["sku"]); new, status = cur + e["delta"], "APPLIED"
        if new < 0:
            new, status = cur, "REJECTED_OVERSELL"; alert(e["wh"], e["sku"], "OVERSELL_BLOCKED" if e["type"] == "SALE" else "TRANSFER_REJECTED", f"{e['channel']} wanted {-e['delta']}, only {cur}")
        d.execute("insert or replace into stock values(?,?,?)", (e["wh"], e["sku"], new))
        d.execute("insert into events(id,ts,wh,sku,channel,type,delta,status,stock_after,consumer) values(?,?,?,?,?,?,?,?,?,?)",
                  (e["event_id"], e["ts"], e["wh"], e["sku"], e["channel"], e["type"], e["delta"], status, new, ID))
        low = max(20, int(.15 * init_qty(e["wh"], e["sku"])))
        if status == "APPLIED" and cur >= low > new: alert(e["wh"], e["sku"], "LOW_STOCK", f"{new} units left")
    return status

print(f"[{ID}] consuming '{TOPIC}'...")
while True:
    m = c.poll(1.0)
    if m is not None and not m.error():
        try:
            e = json.loads(m.value()); s = apply(e)
            if s != "DUPLICATE": lake.setdefault(e["ts"][:10], []).append(json.dumps({**e, "status": s}))
        except Exception as ex:
            p.produce(DLQ, m.value())
            with d: alert("-", "-", "DLQ", str(ex)[:80])
        pending += 1
    p.poll(0)
    if pending >= 200 or (pending and time.time() - last > 5):
        for day, lines in lake.items(): hdfs_put(f"{LAKE}/dt={day}/{ID}_{int(time.time()*1000)}.jsonl", "\n".join(lines))
        lake.clear(); p.flush(); c.commit(asynchronous=False); pending, last = 0, time.time()
