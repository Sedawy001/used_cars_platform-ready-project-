with features as (
    select
        id as car_id,
        alloy_wheels,
        adaptive_cruise_control,
        navigation_system,
        power_liftgate,
        backup_camera,
        keyless_start,
        remote_start,
        sunroof_or_moonroof,
        automatic_emergency_braking,
        stability_control,
        leather_seats,
        memory_seat,
        third_row_seating,
        apple_car_play_or_android_auto,
        bluetooth,
        usb_port,
        heated_seats

    from
        {{ source('landing_02', 'Cars') }}
    
)

select
    *
from
    features
