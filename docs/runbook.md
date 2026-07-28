# Runbook

## Check API access

With credentials copied into `.secrets/opensky.json`:

```bash
docker compose run --rm --no-deps producer python -c "from app.config import Settings; from app.opensky import OpenSky; p,h=OpenSky(Settings()).poll(); print('Authenticated; aircraft:',len(p.get('states') or []))"
```

This performs one authenticated state request. No token or source record is printed.
An empty response is a valid result. For 401/403, verify the API-client credentials
in your OpenSky account. A 429 should be retried after the server-provided delay.
Do not paste credentials into an issue or commit `.env` / `.secrets`.

## Consumer outage and recovery

1. Run `docker compose stop consumer` and allow at least one 96-second polling interval.
2. Run `docker compose start consumer`.
3. Inspect consumer logs for `lag=0` and watch the latest-position dashboard as it catches up.
4. Explain that replay is limited by Kafka retention and cannot replace lost broker storage.

To replay the buffered source after investigating a problem, stop the consumer first:

```bash
docker compose stop consumer
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group flight-warehouse-v1 --topic flight-states --reset-offsets --to-earliest --execute
docker compose start consumer
```

Registry entries intentionally suppress identical message offsets. This is an
idempotency demonstration, not a way to rerun changed validation rules on the same
offsets. For a validation-rule migration, use a dedicated reprocessing job and an
explicit new processing version. Do not casually clear the registry in a live sink.

## Rebuild models

```bash
docker compose stop dbt-runner
docker compose run --rm --no-deps dbt-runner dbt build --full-refresh
docker compose run --rm --no-deps dbt-runner dbt source freshness
docker compose start dbt-runner
```

Only retained raw observations can be rebuilt. If no aircraft are observed in the
selected coverage, source freshness can warn despite successful empty polls; compare it with
the successful-poll panel. dbt errors are logged and retried on the next scheduled
run. Freshness and quality tests are signals, not an external notification service.

## Inspect the warehouse

```bash
docker compose exec postgres psql -U pipeline -d flight_warehouse
```

Useful queries:

```sql
select source, count(*) from raw_flight_states group by source;
select reject_reason, count(*) from rejected_flight_states group by 1;
select icao24, position_at, count(*) from raw_flight_states
group by 1,2 having count(*)>1;
select * from pipeline_metrics order by recorded_at desc limit 10;
```

## Measure before publishing performance claims

Generate a JSON report from the running pipeline:

```bash
docker compose exec consumer python -m scripts.measure
```

The report includes the actual sample span and separates demo/live observations.

Keep one source mode and coverage fixed for at least 24 hours. Record hardware,
versions, budget, downtime, row counts, and duration. Position age differs from
poll-to-sink latency; use the correct timestamp pair:

```sql
select source,
  percentile_cont(0.5) within group (order by extract(epoch from ingested_at-polled_at)) as poll_to_sink_p50_s,
  percentile_cont(0.95) within group (order by extract(epoch from ingested_at-polled_at)) as poll_to_sink_p95_s
from raw_flight_states
where ingested_at > now()-interval '24 hours'
group by source;

select percentile_cont(0.5) within group (order by duration_ms) as batch_p50_ms,
       percentile_cont(0.95) within group (order by duration_ms) as batch_p95_ms
from pipeline_metrics where component='consumer'
and recorded_at>now()-interval '24 hours';

select avg(batch_size) as mean_rows_per_poll, max(batch_size) as peak_rows_per_poll
from pipeline_metrics where component='producer'
and recorded_at>now()-interval '24 hours';
```

Do not label synthetic results as live API performance. Model-refresh duration and
KPI publication latency require separate dbt execution timestamps; the source-age
dashboard alone is not that measurement.

## Local troubleshooting

For local Python/dbt development, `docker-compose.dev.yml` optionally exposes
PostgreSQL at `127.0.0.1:55432`:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d postgres
```

Set `PGHOST=127.0.0.1`, `PGPORT=55432`, and `POSTGRES_PASSWORD` to the value from
your local `.env`. With `dbt-postgres` installed, run `python -m scripts.check_models`
to exercise full and incremental builds in a disposable database. With application
dependencies installed, set `RUN_DATABASE_TESTS=1` and run `python -m pytest -q tests`
to test database transaction rollback, replay, and retention without Kafka. The
broker integration test remains gated by `RUN_INTEGRATION=1`.

The standard `docker compose up -d` configuration removes this optional host port.

- Docker pipe permission errors: open Docker Desktop and run commands from an authorized terminal.
- Port 3000 already occupied: change only the host port in Compose.
- Dashboard empty initially: wait for an ingestion poll and dbt build; inspect service logs.
- Password change on an existing volume: update the PostgreSQL reader role and Grafana
  admin password explicitly; changing init environment variables does not migrate stored passwords.
- Stop with `docker compose down`. Named volumes are preserved. Deleting volumes is
  destructive and is intentionally not part of ordinary startup or shutdown.


## Worldwide coverage and airline selection

Set `OPENSKY_COVERAGE=global` and `SOURCE_MODE=live` in `.env`, then recreate the
producer with `docker compose up -d --no-deps producer`. A worldwide call uses an
estimated 4 credits; the default daily budget of 3,600 spaces calls by 96 seconds.
`regional` uses `BBOX_LAMIN`, `BBOX_LOMIN`, `BBOX_LAMAX`, `BBOX_LOMAX` instead.
The entire worldwide response is ingested; the traffic dashboard filters 20 airlines,
while pipeline health measures all live records.

Edit `grafana/airlines.json`, then run `python -m scripts.build_dashboards` to rebuild
the provisioned dashboards and airline seed. Grafana reloads provisioning; refresh
the browser to see the latest layout. Rebuild the dbt image and models after a seed
change. Prefixes must remain unique across groups to avoid double-counting aircraft.

Check disk usage as global observations accumulate: `docker system df` and
`SELECT pg_size_pretty(pg_database_size(current_database()));` in PostgreSQL.
Raw retention is controlled by `RAW_RETENTION_DAYS`; its default is 14 days.

## Empty map or outdated layout

- Wait for a completed poll and consumer catch-up; the map uses only the last five minutes.
- Check `SOURCE_MODE=live` and API authentication in producer logs.
- A company showing zero may have no recent observations or an unrecognized callsign.
- Regenerate dashboard JSON after editing the generator, then reload the browser.
- The basemap uses `osm-standard` in Grafana 11.6. It needs no CARTO API key.
- Health charts include the earlier regional period until it leaves the selected time range;
  changing coverage can therefore produce a visible jump in volume and latency.
