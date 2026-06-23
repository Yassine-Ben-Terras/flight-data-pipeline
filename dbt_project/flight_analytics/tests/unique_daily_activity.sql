select activity_date, source, airline_code
from {{ ref('mart_daily_flight_activity') }}
group by 1,2,3 having count(*)>1
