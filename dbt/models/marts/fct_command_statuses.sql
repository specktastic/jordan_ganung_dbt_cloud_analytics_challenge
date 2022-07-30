WITH commands AS (
    SELECT * FROM {{ ref('stg_commands') }}
),

actives AS (
    SELECT * FROM {{ ref('stg_command_actives') }}
),

results AS (
    SELECT * FROM {{ ref('stg_command_results') }}
),

device_sensor_readings as (
  SELECT distinct
      update_timestamp
      ,device_id
      ,humidity
      ,temperature 
      ,temp_unit_of_measure
      ,temp_raw
      ,battery_level
    FROM {{ ref('stg_event_sensor_readings') }} where is_device_sensor IS TRUE
),

command_sensor_readings as (
  SELECT distinct
      update_timestamp
      ,device_id
      ,item_key
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
  FROM {{ ref('stg_event_sensor_readings') }} WHERE is_command_sensor IS TRUE
),

command_item_key_rename as (
  SELECT * FROM {{ ref('command_item_key_rename') }}
),

time_between_commands as (
  SELECT * FROM {{ ref('time_between_commands') }}
),


command_status_base as (
SELECT
item_key
,command_item_key_matching_sensors 
,commands.command_uuid
,commands.device_id
,commands.command
,commands.command_client
,commands.thermostat_heat_set_point
,commands.thermostat_cool_set_point
,commands.thermostat_mode
,commands.slot
,commands.pin
,commands.switch_state
,commands.lock_state
,commands.dimmer_state
,commands.dimmer_percent
,commands.user_id
,commands.event_timestamp as command_timestamp
,actives.event_timestamp as active_timestamp
,actives.command_node_id as command_active_node_id
,results.command_node_id as command_result_node_id
,results.is_hub_success
,results.has_hub_response 
,commands.update_timestamp as command_update_timestamp
,actives.update_timestamp as active_update_timestamp
,results.update_timestamp as result_update_timestamp
,commands.command_origin
,commands.command_origin_id
,commands._raw_command_desired_state
FROM commands
LEFT JOIN command_item_key_rename 
    USING(command)
LEFT JOIN actives 
    USING(command_uuid, device_id)
LEFT JOIN 
    results USING(command_uuid, device_id)
),

adding_sensor_readings as ( 
SELECT 
command_status_base.*
,device_sensor_readings.humidity
,device_sensor_readings.battery_level
,device_sensor_readings.temperature
,device_sensor_readings.temp_unit_of_measure

,command_sensor_readings.thermostat_operating_state

,command_sensor_readings.thermostat_cool_setpoint_value
,command_sensor_readings.thermostat_cool_setpoint_unit
,command_sensor_readings.thermostat_heat_setpoint_value
,command_sensor_readings.thermostat_heat_setpoint_unit
,command_sensor_readings.thermostat_heat_setpoint_raw
,command_sensor_readings.thermostat_mode_sensor
,command_sensor_readings.switch
,command_sensor_readings.door_locked
,command_sensor_readings.dimmer_switch
,command_sensor_readings.dimmer_percent_reading

FROM command_status_base
LEFT JOIN device_sensor_readings -- this join is for device sensor readings
    ON command_status_base.device_id = device_sensor_readings.device_id
    AND  
    (
    CAST(FORMAT_DATETIME('%FT%X', device_sensor_readings.update_timestamp) AS DATETIME) 
    BETWEEN 
    CAST(command_status_base.command_timestamp as DATETIME) --start_time of command
    AND 
    CAST(CONCAT(CAST(command_status_base.command_timestamp AS DATE),' ', TIME_ADD(CAST(command_status_base.command_timestamp AS TIME), INTERVAL 15 SECOND)) AS DATETIME) -- This adds 15 seconds to the start time
    )
LEFT JOIN command_sensor_readings  -- this join is for command sensor readings
    ON command_status_base.device_id = command_sensor_readings.device_id
    and command_status_base.command_item_key_matching_sensors = command_sensor_readings.item_key
    and
    (
    CAST(FORMAT_DATETIME('%FT%X', command_sensor_readings.update_timestamp) AS DATETIME) 
    BETWEEN 
    CAST(command_status_base.command_timestamp as DATETIME) --start_time of command
    AND 
    CAST(CONCAT(CAST(command_status_base.command_timestamp AS DATE),' ', TIME_ADD(CAST(command_status_base.command_timestamp AS TIME), INTERVAL 15 SECOND)) AS DATETIME) -- This adds 15 seconds to the start time
    )
), 

building_arrays AS (

SELECT 
adding_sensor_readings.item_key
,adding_sensor_readings.command_uuid
,adding_sensor_readings.device_id
,adding_sensor_readings.command
,adding_sensor_readings.command_client
,adding_sensor_readings.thermostat_heat_set_point
,adding_sensor_readings.thermostat_cool_set_point
,adding_sensor_readings.thermostat_mode
,adding_sensor_readings.slot
,adding_sensor_readings.pin
,adding_sensor_readings.switch_state
,adding_sensor_readings.lock_state
,adding_sensor_readings.dimmer_state
,adding_sensor_readings.dimmer_percent
,adding_sensor_readings.user_id
,adding_sensor_readings.command_timestamp
,adding_sensor_readings.active_timestamp
,adding_sensor_readings.command_active_node_id
,adding_sensor_readings.command_result_node_id
,adding_sensor_readings.is_hub_success
,adding_sensor_readings.has_hub_response 
,adding_sensor_readings.command_update_timestamp
,adding_sensor_readings. active_update_timestamp
,adding_sensor_readings.result_update_timestamp
,adding_sensor_readings.command_origin
,adding_sensor_readings.command_origin_id
,adding_sensor_readings._raw_command_desired_state

,ARRAY_AGG(DISTINCT humidity IGNORE NULLS) as humidity_array
,ARRAY_AGG(DISTINCT battery_level IGNORE NULLS) as battery_level_array
,ARRAY_AGG(DISTINCT temperature IGNORE NULLS) as temperature_array

,ARRAY_AGG(DISTINCT thermostat_operating_state IGNORE NULLS) as thermostat_operating_state_array

,ARRAY_AGG(DISTINCT thermostat_cool_setpoint_value IGNORE NULLS) as thermostat_cool_setpoint_value_array
,ARRAY_AGG(DISTINCT thermostat_cool_setpoint_unit IGNORE NULLS) as thermostat_cool_setpoint_unit_array
,ARRAY_AGG(DISTINCT thermostat_heat_setpoint_value IGNORE NULLS) as thermostat_heat_setpoint_value_array
,ARRAY_AGG(DISTINCT thermostat_heat_setpoint_unit IGNORE NULLS) as thermostat_heat_setpoint_unit_array
,ARRAY_AGG(DISTINCT thermostat_mode_sensor IGNORE NULLS) as thermostat_mode_sensor_array
,ARRAY_AGG(DISTINCT switch IGNORE NULLS) as switch_array
,ARRAY_AGG(DISTINCT door_locked IGNORE NULLS) as door_locked_array
,ARRAY_AGG(DISTINCT dimmer_switch IGNORE NULLS) as dimmer_switch_array
,ARRAY_AGG(DISTINCT dimmer_percent_reading IGNORE NULLS) as dimmer_percent_array

FROM adding_sensor_readings 
--where command_uuid = 'CM013d29a0286646598a37c638b24f4a69' -- command = 'LockedState'
--where device_id = '223735'
GROUP BY 
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
),

adding_last_command_within_15_sec AS (
    SELECT 
        building_arrays.*,
         time_between_commands.is_last_command_within_15_seconds
    FROM building_arrays
    LEFT JOIN time_between_commands
        ON building_arrays.command_uuid = time_between_commands.command_uuid
        AND building_arrays.device_id = time_between_commands.device_id
),

adding_is_command_in_reading AS (
    SELECT 
        adding_last_command_within_15_sec.*,
        case 
        when (case when thermostat_mode = 'off' then 'Idle' end) IN unnest(thermostat_operating_state_array)
        then TRUE
        when LOWER(thermostat_mode) IN unnest(thermostat_operating_state_array) OR (case when thermostat_mode = 'heat' then 'Heat2' when thermostat_mode = 'cool' then 'Cool2' end) IN unnest(thermostat_operating_state_array)
        then TRUE
        when CONCAT(thermostat_heat_set_point,'.0') IN unnest(thermostat_heat_setpoint_value_array) 
        then TRUE
        when CONCAT(thermostat_cool_set_point,'.0') IN unnest(thermostat_cool_setpoint_value_array) 
        then TRUE
        when INITCAP(switch_state) IN unnest(switch_array) 
        then TRUE
        when (case when lock_state = 'Locked' then 'true' else 'false' end) IN unnest(door_locked_array) 
        then TRUE 
        when (case when dimmer_state = 'Multilevel Off' then 'off' when dimmer_state = 'Multilevel On' then 'On' end) IN unnest(dimmer_switch_array) 
        -- i was going to include criteria for the dimmer % but from what i could tell the commands always had 0 or 100 depending on if the command was on or off.  The readings were inconsistent ranging from 0-99
        then TRUE 
        else FALSE 
end as is_command_in_reading
    FROM adding_last_command_within_15_sec
)

SELECT * FROM adding_is_command_in_reading







/*
It wasnt technically part of this test but I would also be doing some work on the dates upstream to standardize on a timezone then be creating date keys in the fct table
that shifted it to local timezones.  To make this work we would also need to make a dim_date to be able to join to

I would also make keys to join to dim_user and any other dimensions we might be able to join to.  

I would do this to make it easier for end users to identify which columns they should use to make joins simipiler for them.
*/


