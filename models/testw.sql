select 
* 
from {{ source('demo_weather', 'weather') }}
limit 10