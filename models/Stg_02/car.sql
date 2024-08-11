with Cars as (
    select
        id as car_id,
        price,
        CASE 
            WHEN damaged IS NULL THEN -1
            ELSE damaged
        END as damaged,
        CASE 
            WHEN first_owner IS NULL THEN -1
            ELSE first_owner
        END as first_owner,
        CASE 
            WHEN personal_using IS NULL THEN -1
            ELSE personal_using
        END as personal_using,
        mileage,
        turbo,
        CASE 
            WHEN min_mpg IS NULL THEN -1
            ELSE min_mpg
        END as min_mpg,
        CASE 
            WHEN max_mpg IS NULL THEN -1
            ELSE max_mpg
        END as max_mpg,
        year,
        CASE 
            WHEN interior_color IS NULL THEN 'Unknown'
            ELSE interior_color
        END as interior_color,
        CASE 
            WHEN exterior_color IS NULL THEN 'Unknown'
            ELSE exterior_color
        END as exterior_color,
        CASE 
            WHEN drivetrain IS NULL THEN 'Unknown'
            ELSE drivetrain
        END as drivetrain,
        model,
        CASE 
            WHEN transmission IS NULL THEN 'Unknown'
            ELSE transmission
        END as transmission,
        CASE 
            WHEN engine IS NULL THEN 'Unknown'
            ELSE engine
        END AS engine
    from
        {{ source('landing_02', 'Cars') }}



),
Model_data AS (
    SELECT
        model_id,
        model_name
    FROM
        {{ source('dbt_msedawy_Stg_02', 'model') }}
),
Transmission_data AS (
    SELECT
        transmission_id,
        transmission_type
    FROM
        {{ source('dbt_msedawy_Stg_02', 'transmission') }}
),
engine_data AS (
    SELECT
        engine_id,
        engine_name 
    FROM
        {{ source('dbt_msedawy_Stg_02', 'engine') }}
),
drivetrain_data AS (
    SELECT
        drivetrain_id,
        drivetrain_types
    FROM
        {{ source('dbt_msedawy_Stg_02', 'drivetrain') }}
)
SELECT
    c.car_id,
    c.price,
    c.damaged,
    c.first_owner,
    c.personal_using,
    c.mileage,
    c.turbo,
    c.min_mpg,
    c.max_mpg,
    c.year,
    c.interior_color,
    c.exterior_color,
    dt.drivetrain_id,
    m.model_id,
    t.transmission_id,
    e.engine_id
FROM
    Cars c
LEFT JOIN
    drivetrain_data dt
ON
    c.drivetrain = dt.drivetrain_types
LEFT JOIN
    Model_data m
ON
    c.model = m.model_name
LEFT JOIN
    Transmission_data t
ON
    c.transmission = t.transmission_type
LEFT JOIN
    engine_data e
ON
    c.engine = e.engine_name
