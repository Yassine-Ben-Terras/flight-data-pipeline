import json
import logging
import signal
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

import requests
from confluent_kafka import Producer

from app.config import Settings, connect
from app.demo import snapshot
from app.opensky import OpenSky, retry_delay

log = logging.getLogger("producer")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = Settings()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    kafka = Producer({"bootstrap.servers": cfg.bootstrap, "enable.idempotence": True,
                      "acks": "all", "delivery.timeout.ms": 120000})
    api = OpenSky(cfg)
    attempt = 0
    log.info("source=%s interval=%ss estimated_credits_per_request=%s", cfg.mode, cfg.interval, cfg.cost)
    while not stop.is_set():
        started = time.monotonic()
        try:
            payload, headers = api.poll() if cfg.mode == "live" else (snapshot(int(time.time())), {})
        except (requests.RequestException, ValueError) as exc:
            response = getattr(exc, "response", None)
            delay = retry_delay(attempt, response.headers if response is not None else {})
            log.warning("poll_failed type=%s retry_seconds=%s", type(exc).__name__, delay)
            attempt += 1
            stop.wait(delay)
            continue
        attempt = 0
        poll_id = str(uuid4())
        polled_at = datetime.now(timezone.utc).isoformat()
        errors = []
        def delivered(error, message):
            if error:
                errors.append(str(error))
        for state in payload.get("states") or []:
            envelope = {"schema_version": 1, "poll_id": poll_id, "polled_at": polled_at,
                        "source": cfg.mode, "state": state}
            key = state[0] if isinstance(state, list) and state and isinstance(state[0], str) else "invalid"
            while True:
                try:
                    kafka.produce(cfg.topic, key=key, value=json.dumps(envelope), on_delivery=delivered)
                    break
                except BufferError:
                    kafka.poll(1)
            kafka.poll(0)
        remaining = kafka.flush(125)
        if remaining or errors:
            # Fail loudly: the source cannot guarantee exact recovery of this snapshot.
            raise RuntimeError("Kafka delivery failed; see broker logs; source snapshot may be incomplete")
        with connect() as conn:
            conn.execute("""INSERT INTO pipeline_metrics
                (component,source,batch_size,duration_ms,credits_estimated,credits_remaining)
                VALUES ('producer',%s,%s,%s,%s,%s)""",
                (cfg.mode, len(payload.get("states") or []), (time.monotonic()-started)*1000,
                 cfg.cost if cfg.mode == "live" else 0, headers.get("X-Rate-Limit-Remaining")))
        log.info("published rows=%s source=%s", len(payload.get("states") or []), cfg.mode)
        delay = max(0, cfg.interval - (time.monotonic() - started))
        if headers.get("X-Rate-Limit-Remaining") == "0":
            delay = max(delay, retry_delay(0, headers))
        stop.wait(delay)


if __name__ == "__main__":
    main()
