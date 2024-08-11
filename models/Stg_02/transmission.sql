with trans as (
    select distinct
        CASE 
            WHEN transmission IS NULL THEN 'Unknown'
            ELSE transmission
        END as transmission_type ,
        CASE 
            WHEN automatic_transmission IS NULL THEN -1
            ELSE automatic_transmission
        END as automatic_transmission
    from
        {{ source('landing_02', 'Cars') }}
    
        
)

select
    row_number() over () as transmission_id,
    *
from
    trans
