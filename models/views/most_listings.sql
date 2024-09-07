select m.brand ,  count(f.price )  as cnt
from {{ source('dbt_msedawy_DMs', 'dim_model') }} m 
inner join {{ source('dbt_msedawy_DMs', 'fact_car') }} f
on m.model_id = f.model_id
group by m.brand
order by cnt desc
limit 5