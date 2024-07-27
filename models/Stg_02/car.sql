with Cars as (
    select
        price,
        damaged,
        first_owner,
        personal_using,
        mileage,
        interior_color,
        exterior_color
    from
        {{ source('landing_02', 'Cars') }}
    where
        price is not null
        and damaged is not null
        and first_owner is not null
        and personal_using is not null
        and mileage is not null
        and interior_color is not null
        and exterior_color is not null
)

select
    row_number() over () as car_id,
    *
from
    Cars
