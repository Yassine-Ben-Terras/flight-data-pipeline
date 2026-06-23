select callsign_prefix, airline_name from {{ ref('airlines') }}
