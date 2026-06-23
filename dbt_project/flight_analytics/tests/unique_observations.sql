select icao24, position_at from {{ ref('stg_flight_states') }}
group by 1, 2 having count(*) > 1
