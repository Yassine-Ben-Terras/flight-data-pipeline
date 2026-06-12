SET TIME ZONE 'UTC';
CREATE TABLE IF NOT EXISTS raw_flight_states (
    icao24 text NOT NULL,
    position_at timestamptz NOT NULL,
    callsign text,
    origin_country text,
    longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    baro_altitude double precision,
    velocity double precision,
    on_ground boolean NOT NULL,
    source text NOT NULL CHECK (source IN ('demo','live')),
    payload jsonb NOT NULL,
    polled_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (icao24, position_at)
) PARTITION BY RANGE (position_at);
CREATE INDEX IF NOT EXISTS raw_ingested_at ON raw_flight_states (ingested_at);

CREATE TABLE IF NOT EXISTS flight_current (
    LIKE raw_flight_states INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
    PRIMARY KEY (icao24)
) WITH (fillfactor=70, autovacuum_vacuum_scale_factor=0.05);
CREATE INDEX IF NOT EXISTS current_position_at ON flight_current(position_at);

CREATE TABLE IF NOT EXISTS rejected_flight_states (
    topic text NOT NULL,
    partition_id integer NOT NULL,
    offset_id bigint NOT NULL,
    payload text NOT NULL,
    reject_reason text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (topic, partition_id, offset_id)
);
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    component text NOT NULL,
    source text,
    batch_size integer,
    inserted_rows integer,
    rejected_rows integer,
    duration_ms double precision,
    latency_seconds double precision,
    consumer_lag bigint,
    credits_estimated integer,
    credits_remaining integer
);
CREATE INDEX IF NOT EXISTS metrics_recorded_at ON pipeline_metrics(recorded_at);

-- This registry also makes batch metrics idempotent after an offset-commit failure.
CREATE TABLE IF NOT EXISTS processed_messages (
    topic text NOT NULL,
    partition_id integer NOT NULL,
    offset_id bigint NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(topic, partition_id, offset_id)
);

CREATE OR REPLACE FUNCTION ensure_raw_partition(day date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE partition_name text := 'raw_flight_states_' || to_char(day,'YYYYMMDD');
BEGIN
    PERFORM pg_advisory_xact_lock(721934);
    IF to_regclass('public.' || partition_name) IS NULL THEN
        EXECUTE format('CREATE TABLE %I PARTITION OF raw_flight_states FOR VALUES FROM (%L) TO (%L)',
                       partition_name, day::text || ' 00:00:00+00', (day+1)::text || ' 00:00:00+00');
    END IF;
END $$;
SELECT ensure_raw_partition((now() AT TIME ZONE 'UTC')::date);
CREATE SCHEMA IF NOT EXISTS analytics;
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='grafana_reader') THEN
        CREATE ROLE grafana_reader LOGIN;
    END IF;
END $$;
GRANT USAGE ON SCHEMA public, analytics TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public, analytics TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO grafana_reader;
