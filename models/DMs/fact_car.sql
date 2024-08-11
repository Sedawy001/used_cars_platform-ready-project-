WITH Cars AS (
    SELECT
        model_name, b.brand  , c.year , c.price, c.damaged,c.first_owner,c.personal_using,c.mileage,c.turbo,c.min_mpg,c.max_mpg,c.car_id,
        c.interior_color,c.exterior_color , d.drivetrain_types,t.transmission_type,t.automatic_transmission,e.engine_name,e.engine_size , fe.fuel_type,cf.alloy_wheels,cf.adaptive_cruise_control,
        cf.navigation_system,cf.power_liftgate,cf.backup_camera,cf.keyless_start,cf.remote_start,cf.sunroof_or_moonroof,cf.automatic_emergency_braking,
        cf.stability_control,cf.leather_seats,cf.memory_seat,cf.third_row_seating,cf.apple_car_play_or_android_auto,cf.bluetooth,cf.usb_port,cf.heated_seats

    FROM {{ source('dbt_msedawy_Stg_02', 'brand') }} b 
    JOIN {{ source('dbt_msedawy_Stg_02', 'model') }} m ON b.brand_id = m.brand_id 
    JOIN {{ source('dbt_msedawy_Stg_02', 'car') }} c ON m.model_id = c.model_id
    JOIN {{ source('dbt_msedawy_Stg_02', 'engine') }} e ON c.engine_id = e.engine_id
    JOIN {{ source('dbt_msedawy_Stg_02', 'transmission') }} t ON c.transmission_id = t.transmission_id
    JOIN {{ source('dbt_msedawy_Stg_02', 'drivetrain') }} d ON c.drivetrain_id = d.drivetrain_id
    JOIN {{ source('dbt_msedawy_Stg_02', 'car_features') }} cf ON c.car_id = cf.car_id
    JOIN {{ source('dbt_msedawy_Stg_02', 'fuel') }} fe ON e.fuel_id = fe.fuel_id
),
dim_model AS (
    SELECT
        model_id, model_name, brand, year , interior_color , exterior_color
    FROM {{ source('dbt_msedawy_DMs', 'dim_model') }}
),
dim_engine AS (
    SELECT
        engine_id, engine_name, engine_size, fuel_type, transmission_type, min_mpg, max_mpg
    FROM {{ source('dbt_msedawy_DMs', 'dim_engine') }}
),
junk_dim AS (
    SELECT
        junk_id, turbo, damaged, first_owner, personal_using, automatic_transmission, drivetrain_types, 
        alloy_wheels, adaptive_cruise_control, navigation_system, power_liftgate, backup_camera, 
        keyless_start, remote_start, sunroof_or_moonroof, automatic_emergency_braking, stability_control, 
        leather_seats, memory_seat, third_row_seating, apple_car_play_or_android_auto, bluetooth, usb_port, 
        heated_seats
    FROM {{ source('dbt_msedawy_DMs', 'junk_dim') }}
)
SELECT
    ROW_NUMBER() OVER () AS car_id,
    c.price,
    c.mileage,
    b.model_id,
    e.engine_id,
    j.junk_id
FROM Cars c
LEFT JOIN dim_model b ON c.model_name = b.model_name AND c.year = b.year AND c.interior_color = b.interior_color AND c.exterior_color = b.exterior_color
LEFT JOIN dim_engine e ON c.engine_name = e.engine_name AND c.fuel_type = e.fuel_type AND c.transmission_type = e.transmission_type AND c.min_mpg = e.min_mpg AND c.max_mpg = e.max_mpg
LEFT JOIN junk_dim j ON c.car_id = j.junk_id