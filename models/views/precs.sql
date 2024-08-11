with perc as (
  select row_number()over(partition by fuel_type) as rn
  , fuel_type , c.price
  from {{ source('dbt_msedawy_DMs', 'dim_engine') }} e
  join {{ source('dbt_msedawy_DMs', 'fact_car') }} c
  on e.engine_id = c.engine_id
)
select fuel_type , concat(round((count(rn)/4768) * 100) , '%')
from perc
where fuel_type = 'Gasoline' or fuel_type Like 'Electric' or fuel_type = 'Hybrid'
group by fuel_type
