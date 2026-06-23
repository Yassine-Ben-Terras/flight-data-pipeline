select * from {{ ref('stg_flight_states') }}
where latitude is null or longitude is null
   or latitude not between -90 and 90 or longitude not between -180 and 180
