with dim_1 AS (
    SELECT  c.turbo , c.damaged , c.first_owner , c.personal_using , t.automatic_transmission , d.drivetrain_types  , f.alloy_wheels,
f.adaptive_cruise_control,
f.navigation_system,
f.power_liftgate,
f.backup_camera,
f.keyless_start,
f.remote_start,
f.sunroof_or_moonroof,
f.automatic_emergency_braking,
f.stability_control,
f.leather_seats,
f.memory_seat,
f.third_row_seating,
f.apple_car_play_or_android_auto,
f.bluetooth,
f.usb_port,
f.heated_seats,
f.car_id
    FROM {{ source('dbt_msedawy_Stg_02', 'car') }} c 
    JOIN {{ source('dbt_msedawy_Stg_02', 'car_features') }} f
    ON c.car_id = f.car_id
    JOIN {{ source('dbt_msedawy_Stg_02', 'transmission') }}  t
    ON c.transmission_id = t.transmission_id 
    JOIN {{ source('dbt_msedawy_Stg_02', 'drivetrain') }} d
    ON c.drivetrain_id = d.drivetrain_id 
    
)

select car_id AS junk_id , 
f.turbo , f.damaged , f.first_owner , f.personal_using , f.automatic_transmission , f.drivetrain_types  , f.alloy_wheels,
f.adaptive_cruise_control,
f.navigation_system,
f.power_liftgate,
f.backup_camera,
f.keyless_start,
f.remote_start,
f.sunroof_or_moonroof,
f.automatic_emergency_braking,
f.stability_control,
f.leather_seats,
f.memory_seat,
f.third_row_seating,
f.apple_car_play_or_android_auto,
f.bluetooth,
f.usb_port,
f.heated_seats,
FROM dim_1 f