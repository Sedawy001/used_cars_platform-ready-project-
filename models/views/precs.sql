WITH perc AS (
  SELECT row_number() OVER (PARTITION BY e.fuel_type) AS rn,
         e.fuel_type,
         c.price
  FROM {{ source('dbt_msedawy_DMs', 'dim_engine') }} e
  JOIN {{ source('dbt_msedawy_DMs', 'fact_car') }} c
    ON e.engine_id = c.engine_id
)
SELECT fuel_type, 
       ROUND((COUNT(rn)/4768) * 100) AS percentage
FROM perc
WHERE fuel_type = 'Gasoline' 
   OR fuel_type LIKE 'Electric' 
   OR fuel_type = 'Hybrid'
GROUP BY fuel_type
