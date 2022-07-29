WITH commands AS (
    SELECT * FROM {{ ref('stg_commands') }}
),

actives AS (
    SELECT * FROM {{ ref('stg_command_actives') }}
),

results AS (
    SELECT * FROM {{ ref('stg_command_results') }}
)

SELECT
commands.command_uuid
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
LEFT JOIN actives 
    USING(command_uuid, device_id)
LEFT JOIN 
    results USING(command_uuid, device_id)


/*
It wasnt technically part of this test but I would also be doing some work on the dates upstream to standardize on a timezone then be creating date keys in the fct table
that shifted it to local timezones.  To make this work we would also need to make a dim_date to be able to join to

I would also make keys to join to dim_user and any other dimensions we might be able to join to.  

I would do this to make it easier for end users to identify which columns they should use to make joins simipiler for them.
*/


