with dim_engine AS (
    SELECT distinct e.engine_name , e.engine_size , f.fuel_type , transmission_type , min_mpg , max_mpg 
    FROM {{ source('dbt_msedawy_Stg_02', 'fuel') }} f 
    JOIN {{ source('dbt_msedawy_Stg_02', 'engine') }}  e
    ON f.fuel_id = e.fuel_id 
    JOIN {{ source('dbt_msedawy_Stg_02', 'car') }} c
    ON e.engine_id = c.engine_id 
    JOIN  {{ source('dbt_msedawy_Stg_02', 'transmission') }} t
    ON c.transmission_id = t.transmission_id

)

SELECT ROW_NUMBER() OVER() AS engine_id , 
*
FROM dim_engine 