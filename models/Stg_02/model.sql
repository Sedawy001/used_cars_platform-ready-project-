-- models/model.sql
WITH distinct_models AS (
    SELECT DISTINCT
        model,
        brand
    FROM
        {{ source('landing_02', 'Cars') }}
    WHERE
        model IS NOT NULL
        AND brand IS NOT NULL
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
    dm.model,
    bd.brand_id
FROM
    distinct_models dm
JOIN
    brand_data bd
ON
    dm.brand = bd.brand