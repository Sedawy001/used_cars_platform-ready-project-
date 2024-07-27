with eng as (
    select
        distinct engine as engine_name ,
        engine_size
    from
        {{ source('landing_02', 'Cars') }}
    where
        engine is not null
        and engine_size is not null
        
)

select
    row_number() over () as engine_id,
    *
from
    eng
