with features as (
    select
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
    where
        alloy_wheels is not null
        and adaptive_cruise_control is not null
        and navigation_system is not null
        and power_liftgate is not null
        and backup_camera is not null
        and keyless_start is not null
        and remote_start is not null
        and sunroof_or_moonroof is not null
        and automatic_emergency_braking is not null
        and stability_control is not null
        and leather_seats is not null
        and memory_seat is not null
        and third_row_seating is not null
        and apple_car_play_or_android_auto is not null
        and bluetooth is not null
        and usb_port is not null
        and heated_seats is not null
)

select
    row_number() over () as car_id,
    *
from
    features
