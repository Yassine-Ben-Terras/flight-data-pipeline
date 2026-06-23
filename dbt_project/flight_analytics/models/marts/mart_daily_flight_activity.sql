-- These are aircraft observations, NOT a count of completed flights.
select date_trunc('day', s.position_at) as activity_date, s.source,
       coalesce(a.callsign_prefix, 'UNK') as airline_code,
       count(*) as observations, count(distinct s.icao24) as distinct_aircraft,
       avg(s.velocity) filter (where not s.on_ground) as avg_airborne_speed_ms,
       avg(s.baro_altitude) filter (where not s.on_ground) as avg_airborne_altitude_m
from {{ ref('stg_flight_states') }} s
left join {{ ref('dim_airlines') }} a on s.callsign_prefix = a.callsign_prefix
group by 1, 2, 3
