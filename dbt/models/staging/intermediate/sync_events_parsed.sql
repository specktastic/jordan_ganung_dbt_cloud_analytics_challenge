{{ config(materialized='ephemeral') }}

with sync_events as (
    select * from {{ source('interview_source', 'raw_sync_events') }}
)

,column_renames as (
  select
    EventType as event_type
    ,DateCreated as update_timestamp
    ,MapRevision as map_revision
    ,ItemKey as item_key
    ,ItemRevision as item_revision
    ,EndpointId as twilio_sync_endpoint_id
    ,ItemData as item_data
    ,MapUniqueName as  device_id

    ,_source_file
    ,concat(MapUniqueName, '_', MapRevision, '_', STRING(DateCreated)) as _uid
  from
    sync_events
)

,extract_event_values as (
  select
    *

    ,JSON_EXTRACT_SCALAR(item_data, '$.uuid') as event_uuid
    ,JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') as command

    ,case
     when JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) < 26
      then safe_cast(JSON_EXTRACT_SCALAR(item_data, '$.timestamp') as timestamp)
      when JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) > 26
      then safe_cast(substr(JSON_EXTRACT_SCALAR(item_data, '$.timestamp'),0,26) as timestamp)
    end as event_timestamp

    ,case when item_key in ('Command') then
      JSON_EXTRACT(item_data, '$.desired_state')
      else null
    end as _raw_command_desired_state
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.client')
      else null
    end as command_client
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.user')
      else null
    end as user_id
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.origin')
      else null
     end as command_origin
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.origin_id')
      else null
     end as command_origin_id

    ,case when item_key in ('CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.result')
      else null
    end as command_result
    ,case when item_key in ('CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.failure_string')
      else null
    end as command_failure_string

    ,case when item_key in ('CommandActive', 'CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.node_id')
      else null
    end as command_node_id

    ,case when item_key in ('Command', 'CommandActive', 'CommandResult') then
      case
        when JSON_EXTRACT_SCALAR(item_data, '$.command_id') is not null
        then JSON_EXTRACT_SCALAR(item_data, '$.command_id')
        else JSON_EXTRACT_SCALAR(item_data, '$.uuid')
      end
      else null
    end as command_uuid

    -- NEW COLUMNS PARSING `$.desired_state` from item_data CAN BE ADDED HERE --
    ,case 
        when item_key in ('Command')
        AND JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'ThermostatHeatSetPoint'
        then CAST(JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]') AS INT64)
        end AS thermostat_heat_set_point
    
    ,case 
        when item_key in ('Command')
        AND JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'ThermostatCoolSetPoint'
        then CAST(JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]') AS INT64)
        end AS thermostat_cool_set_point

    ,case 
        when item_key in ('Command')
        AND JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'ThermostatMode'
        then JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
        end AS thermostat_mode

    ,case 
        when item_key in ('Command')
        AND JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'PinAssignment'
        then JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
        end AS slot

    ,case 
        when item_key in ('Command')
        AND JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'PinAssignment'
        then JSON_EXTRACT_SCALAR(item_data, '$.desired_state[2]')
        end AS pin

    ,case 
        when item_key in ('Command')
        AND JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'SwitchState'
        then JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
        end AS switch_state

    ,case 
        when item_key in ('Command')
        AND JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'LockedState'
        then JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
        end AS lock_state
    
  from
    column_renames
)

SELECT * FROM extract_event_values

