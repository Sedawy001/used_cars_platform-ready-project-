with trans as (
    select
        distinct transmission as transmission_type ,
        automatic_transmission
    from
        {{ source('landing_02', 'Cars') }}
    where
        transmission is not null
        and automatic_transmission is not null
        
)

select
    row_number() over () as transmission_id,
    *
from
    trans
