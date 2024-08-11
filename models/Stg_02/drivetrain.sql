with dt as (
    select distinct
        CASE 
            WHEN drivetrain IS NULL THEN 'Unknown'
            ELSE drivetrain
        END as drivetrain_types

    from
        {{ source('landing_02', 'Cars') }}
    
        
)

select
    row_number() over (order by drivetrain_types) as drivetrain_id,
    *
from
    dt