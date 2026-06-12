"""Emit a reproducible short-run report; never imply a 24h benchmark."""
import json
from datetime import datetime, timezone
from app.config import Settings, connect


def main():
    cfg = Settings()
    with connect() as conn:
        rows = conn.execute("""SELECT source, count(*), count(distinct icao24),
            min(ingested_at), max(ingested_at),
            percentile_cont(0.5) within group (order by extract(epoch from ingested_at-polled_at)),
            percentile_cont(0.95) within group (order by extract(epoch from ingested_at-polled_at))
            FROM raw_flight_states WHERE ingested_at > now()-interval '24 hours'
            GROUP BY source""").fetchall()
        batches = conn.execute("""SELECT count(*),
            percentile_cont(0.5) within group (order by duration_ms),
            percentile_cont(0.95) within group (order by duration_ms)
            FROM pipeline_metrics WHERE component='consumer'
            AND recorded_at>now()-interval '24 hours'""").fetchone()
    report = {'generated_at':datetime.now(timezone.utc).isoformat(),
              'scope':'observed samples in last 24 hours; may cover less than 24 hours',
              'configured_interval_seconds':cfg.interval, 'sources':[],
              'consumer_batches':batches[0], 'batch_p50_ms':batches[1], 'batch_p95_ms':batches[2]}
    for source, count, aircraft, first, last, p50, p95 in rows:
        report['sources'].append({'source':source, 'observations':count, 'distinct_aircraft':aircraft,
            'first_ingested_at':first.isoformat(), 'last_ingested_at':last.isoformat(),
            'observed_span_hours':(last-first).total_seconds()/3600,
            'poll_to_sink_p50_seconds':p50, 'poll_to_sink_p95_seconds':p95})
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
