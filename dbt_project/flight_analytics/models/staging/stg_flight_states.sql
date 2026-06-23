{{ config(materialized='incremental', unique_key=['icao24','position_at'],
          incremental_strategy='delete+insert',
          post_hook="delete from {{ this }} where position_at < (current_date - interval '" ~ env_var('RAW_RETENTION_DAYS', '14') ~ " days')") }}

-- Anti-join avoids missing late DB commits behind a timestamp watermark.
-- The retained raw window is deliberately small (14 days by default).
select r.icao24, r.position_at, nullif(trim(r.callsign), '') as callsign,
       coalesce(upper(left(nullif(trim(r.callsign), ''), 3)), 'UNK') as callsign_prefix,
       r.origin_country, r.longitude, r.latitude, r.baro_altitude,
       r.velocity, r.on_ground, r.source, r.polled_at, r.ingested_at
from {{ source('landing', 'raw_flight_states') }} r
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t where t.icao24 = r.icao24 and t.position_at = r.position_at
)
{% endif %}
