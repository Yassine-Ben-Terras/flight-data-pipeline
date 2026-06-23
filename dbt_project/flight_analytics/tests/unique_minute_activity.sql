select minute_at, source
from {{ ref('mart_active_flights') }}
group by 1,2 having count(*)>1
