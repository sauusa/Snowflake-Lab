select 
* 
from {{ source('demo_bike', 'bike_details') }}
limit 10