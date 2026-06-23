select date_trunc('minute', position_at) as minute_at, source,
       count(distinct icao24) as observed_aircraft,
       count(distinct icao24) filter (where not on_ground) as airborne_aircraft,
       count(distinct icao24) filter (where on_ground) as ground_aircraft
from {{ ref('stg_flight_states') }}
group by 1, 2
