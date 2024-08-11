select m.model_name , e.min_mpg , m.year 
from {{ source('dbt_msedawy_DMs', 'dim_model') }} m
join {{ source('dbt_msedawy_DMs', 'fact_car') }} f 
on m.model_id = f.model_id
join {{ source('dbt_msedawy_DMs', 'dim_engine') }} e
on e.engine_id = f.engine_id
where year >2019 and min_mpg !=-1 and min_mpg != 0 
order by min_mpg desc
limit 10