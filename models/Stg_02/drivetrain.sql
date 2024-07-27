with dt as (
    select
        distinct drivetrain as drivetrain_types
    from
        {{ source('landing_02', 'Cars') }}
    where
        drivetrain is not null
        
)

select
    row_number() over (order by drivetrain_types) as drivetrain_id,
    *
from
    dt
