with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv.tripid, 
    fhv.dispatching_base_num, 
    fhv.pickup_locationid, 
    coalesce(pickup_zone.borough, 'n. a.') as pickup_borough, 
    coalesce(pickup_zone.zone, 'n. a.') as pickup_zone, 
    fhv.dropoff_locationid,
    coalesce(dropoff_zone.borough, 'n. a.') as dropoff_borough, 
    coalesce(dropoff_zone.zone, 'n. a.') as dropoff_zone,  
    fhv.pickup_datetime, 
    fhv.dropoff_datetime, 
    fhv.sr_flag
from {{ ref('stg_fhv_tripdata') }} fhv
left join dim_zones as pickup_zone on
    fhv.pickup_locationid = pickup_zone.locationid
left join dim_zones as dropoff_zone on 
    fhv.dropoff_locationid = dropoff_zone.locationid