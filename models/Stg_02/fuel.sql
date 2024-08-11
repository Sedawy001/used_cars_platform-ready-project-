WITH fuel AS (
    SELECT DISTINCT
        fuel_type

    
    FROM
        {{ source('landing_02', 'Cars') }}
    
)

SELECT
    ROW_NUMBER() OVER () AS fuel_id,
    *
    
FROM
    fuel 
