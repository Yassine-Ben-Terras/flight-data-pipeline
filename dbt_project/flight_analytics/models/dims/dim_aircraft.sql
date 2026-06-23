-- ICAO identifiers observed in this project, not a licensed aircraft registry.
select icao24, min(position_at) as first_observed_at,
       max(position_at) as last_observed_at
from {{ ref('stg_flight_states') }}
group by icao24
