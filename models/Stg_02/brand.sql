with distinct_brands as (
    select
        distinct brand
    from
        {{ source('landing_02', 'Cars') }}
    where
        brand is not null
)

select
    row_number() over () as brand_id,
    brand
from
    distinct_brands
