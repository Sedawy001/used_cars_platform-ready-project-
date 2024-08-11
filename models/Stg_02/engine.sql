with eng as (
    select distinct
        CASE 
            WHEN engine IS NULL THEN 'Unknown'
            ELSE engine
        END as engine_name ,
        CASE 
            WHEN engine_size IS NULL THEN -1
            ELSE engine_size
        END as engine_size ,
        fuel_type

    from
        {{ source('landing_02', 'Cars') }}
    

),
fuel_data AS (
    SELECT
        fuel_id,
        fuel_type
    FROM
        {{ source('dbt_msedawy_Stg_02', 'fuel') }}  


)
SELECT
    ROW_NUMBER() OVER () AS engine_id,
    e.engine_name,
    e.engine_size,
    fd.fuel_id
FROM
    eng e
JOIN
    fuel_data fd
ON
    e.fuel_type = fd.fuel_type
