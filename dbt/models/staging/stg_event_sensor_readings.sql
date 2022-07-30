with sync_events_parsed as (
  select * from {{ ref('sync_events_parsed') }}
)

# this step removed duplicate command_uuid entries by only taking the 1st event 
,removing_duplicates as (
  select 
    *, 
    ROW_NUMBER() OVER (PARTITION BY sensor_uuid, item_key ORDER BY update_timestamp ASC) AS row_number
  FROM sync_events_parsed
)

,final as (
    select
       sensor_uuid
      ,update_timestamp
      ,event_timestamp
      ,device_id
      ,item_key
      ,humidity
      ,temperature 
      ,temp_unit_of_measure
      ,temp_raw
      ,battery_level
      ,thermostat_operating_state
      ,thermostat_cool_setpoint_value
      ,thermostat_cool_setpoint_unit
      ,thermostat_cool_setpoint_raw
      ,thermostat_heat_setpoint_value
      ,thermostat_heat_setpoint_unit
      ,thermostat_heat_setpoint_raw
      ,thermostat_mode_sensor
      ,switch
      ,door_locked
      ,dimmer_switch
      ,dimmer_percent_reading
      ,case 
        when item_key IN ('Humidity', 'Temperature', 'BatteryLevel')
        then TRUE
        else FALSE
      end as is_device_sensor
      ,case 
        when item_key IN ('ThermostatOperatingState',  'ThermostatCoolSetpoint', 'ThermostatHeatSetpoint', 'ThermostatMode', 'Switch', 'DoorLocked', 'Dimmer')
        then TRUE
        else FALSE
      end as is_command_sensor
    from
      removing_duplicates
    where
      item_key in ('Humidity', 'Temperature', 'BatteryLevel',  'ThermostatOperatingState',  'ThermostatCoolSetpoint', 'ThermostatHeatSetpoint', 'ThermostatMode', 'Switch', 'DoorLocked', 'Dimmer')  
      and sensor_uuid is not null
      and row_number = 1
)

select *
from final
