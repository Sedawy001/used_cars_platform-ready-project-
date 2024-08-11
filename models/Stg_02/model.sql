-- models/model.sql
WITH distinct_models AS (
    SELECT DISTINCT
        model as model_name,
        brand 
    
    FROM
        {{ source('landing_02', 'Cars') }}
    

),
brand_data AS (
    SELECT
        brand_id,
        brand
    FROM
        {{ source('dbt_msedawy_Stg_02', 'brand') }}  
)

SELECT
    ROW_NUMBER() OVER () AS model_id,
    dm.model_name,
    bd.brand_id
FROM
    distinct_models dm
JOIN
    brand_data bd
ON
    dm.brand = bd.brand.brand

