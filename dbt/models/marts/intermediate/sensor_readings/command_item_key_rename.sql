{{ config(materialized='ephemeral') }}

WITH commands AS ( 
    SELECT * FROM {{ ref('stg_commands') }}
),

remapping_command_item_keys AS ( 
    SELECT DISTINCT
    command,
    CASE 
        WHEN command = 'SwitchState'
        THEN 'Switch' 
        WHEN command = 'DimmerState'
        THEN 'Dimmer' 
        WHEN command = 'LockedState'
        THEN 'DoorLocked' 
        WHEN command = 'ThermostatMode'
        THEN 'ThermostatOperatingState' 
        WHEN command = 'ThermostatHeatSetPoint'
        THEN 'ThermostatHeatSetpoint' 
        WHEN command = 'ThermostatCoolSetPoint'
        THEN 'ThermostatCoolSetpoint' 
        ELSE command
    END AS command_item_key_matching_sensors
    FROM commands

)

SELECT * FROM remapping_command_item_keys