with dim_brand AS (
    SELECT distinct model_name,  b.brand  , year , interior_color , exterior_color
    FROM {{ source('dbt_msedawy_Stg_02', 'brand') }} b 
    JOIN {{ source('dbt_msedawy_Stg_02', 'model') }}  m
    ON b.brand_id = m.brand_id 
    JOIN {{ source('dbt_msedawy_Stg_02', 'car') }} c
    ON m.model_id = c.model_id  
)

SELECT ROW_NUMBER() OVER() AS model_id , 
*
FROM dim_brand 