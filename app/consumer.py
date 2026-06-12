import json
import logging
import signal
import threading
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, TopicPartition
from psycopg.types.json import Jsonb

from app.config import Settings, connect
from app.validation import validate

COLUMNS = "icao24,position_at,callsign,origin_country,longitude,latitude,baro_altitude,velocity,on_ground,source,payload,polled_at"
PLACEHOLDERS = ",".join(["%s"] * 12)
UPDATE = ",".join(f"{c}=EXCLUDED.{c}" for c in COLUMNS.split(",") if c != "icao24")


def invalid_json_constant(value):
    raise ValueError("non_finite_json_number")


def write_batch(conn, messages, retention=14, lag=None):
    """One atomic sink transaction. Caller may commit Kafka ONLY after return."""
    started = time.monotonic()
    count = inserted = rejected = 0
    latencies = []
    with conn.transaction():
        for message in messages:
            identity = (message.topic(), message.partition(), message.offset())
            fresh = conn.execute("""INSERT INTO processed_messages(topic,partition_id,offset_id)
                VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING 1""", identity).fetchone()
            if not fresh:
                continue
            count += 1
            raw = (message.value() or b"").decode("utf-8", errors="backslashreplace")
            try:
                envelope = json.loads(raw, parse_constant=invalid_json_constant)
                if not isinstance(envelope, dict) or envelope.get("schema_version") != 1:
                    raise ValueError("unsupported_envelope")
                if envelope.get("source") not in {"demo", "live"}:
                    raise ValueError("invalid_source")
                polled_at = datetime.fromisoformat(envelope["polled_at"])
                if polled_at.tzinfo is None:
                    raise ValueError("polled_at_requires_timezone")
                row = validate(envelope["state"], retention_days=retention)
            except (ValueError, KeyError, TypeError, OverflowError) as exc:
                # Fixed reason codes only; do not emit source payloads to logs.
                reason = str(exc) if type(exc) is ValueError else type(exc).__name__
                conn.execute("""INSERT INTO rejected_flight_states
                    (topic,partition_id,offset_id,payload,reject_reason) VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING""", (*identity, raw.replace('\x00', r'\u0000'), reason[:200]))
                rejected += 1
                continue
            conn.execute("SELECT ensure_raw_partition(%s)", (row["position_at"].date(),))
            values = (row["icao24"], row["position_at"], row["callsign"], row["origin_country"],
                      row["longitude"], row["latitude"], row["baro_altitude"], row["velocity"],
                      row["on_ground"], envelope["source"], Jsonb(envelope["state"]), polled_at)
            result = conn.execute(f"INSERT INTO raw_flight_states ({COLUMNS}) VALUES ({PLACEHOLDERS}) ON CONFLICT DO NOTHING", values)
            inserted += result.rowcount
            conn.execute(f"""INSERT INTO flight_current ({COLUMNS}) VALUES ({PLACEHOLDERS})
                ON CONFLICT (icao24) DO UPDATE SET {UPDATE}, ingested_at=clock_timestamp()
                WHERE EXCLUDED.position_at > flight_current.position_at""", values)
            latencies.append((datetime.now(timezone.utc) - row["position_at"]).total_seconds())
        if count:
            conn.execute("""INSERT INTO pipeline_metrics
                (component,batch_size,inserted_rows,rejected_rows,duration_ms,latency_seconds,consumer_lag)
                VALUES ('consumer',%s,%s,%s,%s,%s,%s)""",
                (count, inserted, rejected, (time.monotonic()-started)*1000,
                 sum(latencies)/len(latencies) if latencies else None, lag))
    return {"processed": count, "inserted": inserted, "rejected": rejected}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = Settings()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    consumer = Consumer({"bootstrap.servers": cfg.bootstrap, "group.id": cfg.group,
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
        "enable.auto.offset.store": False, "max.poll.interval.ms": 300000})
    consumer.subscribe([cfg.topic])
    try:
        with connect() as conn:
            # Explicit batch transactions: no outer transaction before offset commits.
            conn.autocommit = True
            while not stop.is_set():
                batch = consumer.consume(num_messages=500, timeout=2)
                if not batch:
                    continue
                for message in batch:
                    if message.error():
                        raise KafkaException(message.error())
                ends = {}
                for message in batch:
                    ends[(message.topic(), message.partition())] = message.offset()+1
                lag = sum(max(0, consumer.get_watermark_offsets(TopicPartition(t,p), timeout=10)[1]-offset)
                          for (t,p), offset in ends.items())
                stats = write_batch(conn, batch, cfg.retention, lag)
                # A failed commit exits; restart replays the committed DB transaction safely.
                consumer.commit(offsets=[TopicPartition(t,p,o) for (t,p),o in ends.items()], asynchronous=False)
                logging.info("batch=%s lag=%s", stats, lag)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
