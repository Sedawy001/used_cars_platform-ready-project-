select brand , m.model_name , price  
from {{ source('dbt_msedawy_DMs', 'dim_model') }} m
join {{ source('dbt_msedawy_DMs', 'fact_car') }} f 
on m.model_id = f.model_id
join {{ source('dbt_msedawy_DMs', 'dim_engine') }} e
on e.engine_id = f.engine_id
where fuel_type = 'Gasoline' and year >=  2019 and mileage < 50000
order by price asc
limit 17