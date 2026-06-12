import os
import signal
import threading
from datetime import datetime, timedelta, timezone

from psycopg import sql
from app.config import Settings, connect


def maintain():
    cfg = Settings()
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=cfg.retention)
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(721934)")
        partitions = conn.execute("""SELECT child.relname FROM pg_inherits i
            JOIN pg_class parent ON i.inhparent=parent.oid
            JOIN pg_class child ON i.inhrelid=child.oid
            WHERE parent.oid='raw_flight_states'::regclass""").fetchall()
        for (name,) in partitions:
            day = datetime.strptime(name.removeprefix("raw_flight_states_"), "%Y%m%d").date()
            if day < cutoff:
                conn.execute(sql.SQL("DROP TABLE {} ").format(sql.Identifier(name)))
        conn.execute("DELETE FROM rejected_flight_states WHERE ingested_at < now() - %s * interval '1 day'", (cfg.retention,))
        conn.execute("DELETE FROM processed_messages WHERE processed_at < now() - interval '3 days'")
        conn.execute("DELETE FROM pipeline_metrics WHERE recorded_at < now() - interval '30 days'")
        conn.execute("DELETE FROM flight_current WHERE position_at < now() - %s * interval '1 hour'",
                     (int(os.getenv("CURRENT_RETENTION_HOURS", "24")),))
        conn.execute("SELECT ensure_raw_partition((now() AT TIME ZONE 'UTC')::date + 1)")


def main():
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        maintain()
        stop.wait(3600)


if __name__ == "__main__":
    main()
