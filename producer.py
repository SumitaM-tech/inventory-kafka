"""Sales-channel producers (WEB/STORE/MOBILE/MARKETPLACE), idempotent, keyed by warehouse|sku.
Injects ~1% duplicate deliveries and ~0.3% malformed messages to exercise idempotency + the dead-letter queue.
Usage: python producer.py [events_per_sec]"""
import json, random, sys, threading, time, uuid, pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from common import *
RATE = float(sys.argv[1]) if len(sys.argv) > 1 else 30

def ensure_topics():
    for f in AdminClient({"bootstrap.servers": BOOTSTRAP}).create_topics([NewTopic(TOPIC, 6, 1), NewTopic(DLQ, 1, 1), NewTopic(ALERTS, 1, 1)]).values():
        try: f.result()
        except Exception: pass

def channel(ch, df):
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "linger.ms": 20})
    for r in df.itertuples():
        e = {"event_id": str(uuid.uuid4()), "ts": str(r.ts), "wh": r.wh, "sku": r.sku, "channel": ch,
             "type": "SALE" if r.delta < 0 else "RETURN", "delta": int(r.delta)}
        v, k = json.dumps(e), f"{r.wh}|{r.sku}"      # same key -> same partition -> per-SKU ordering
        p.produce(TOPIC, key=k, value=v)
        if random.random() < .01: p.produce(TOPIC, key=k, value=v)
        if random.random() < .003: p.produce(TOPIC, key=k, value='{"broken":')
        p.poll(0); time.sleep(random.expovariate(RATE / len(CHANNELS)))
    p.flush()

if __name__ == "__main__":
    ensure_topics(); ev = pd.read_csv("data/stream_events.csv", dtype={"sku": str}); print("Streaming", len(ev), "events...")
    th = [threading.Thread(target=channel, args=(c, ev[ev.channel == c])) for c in CHANNELS]
    [t.start() for t in th]; [t.join() for t in th]
