select * from {{ ref('mart_active_flights') }}
where observed_aircraft < 0 or airborne_aircraft > observed_aircraft
   or ground_aircraft > observed_aircraft
