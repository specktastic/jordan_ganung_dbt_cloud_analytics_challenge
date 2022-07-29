
with sync_events_parsed as (
  select * from {{ ref('sync_events_parsed') }}
)

# this step removed duplicate command_uuid entries by only taking the 1st event 
,removing_duplicates as (
  select 
    *, 
    ROW_NUMBER() OVER (PARTITION BY command_uuid, item_key ORDER BY update_timestamp ASC) AS row_number
  FROM sync_events_parsed
)

,final as (
  select
    event_type	
  ,update_timestamp	
  ,map_revision
  ,item_key	
  ,item_revision	
  ,twilio_sync_endpoint_id		
  ,item_data		
  ,device_id			
  ,_source_file			
  ,_uid			
  ,event_uuid		
  ,command			
  ,event_timestamp		
  ,_raw_command_desired_state			
  ,command_client			
  ,user_id			
  ,command_origin		
  ,command_origin_id			
  ,command_result			
  ,command_failure_string			
  ,command_node_id			
  ,command_uuid		
  ,thermostat_heat_set_point		
  ,thermostat_cool_set_point	
  ,thermostat_mode			
  ,slot			
  ,pin		
  ,switch_state		
  ,lock_state
  from
    removing_duplicates
  where
    item_key = 'Command'
    and command_uuid is not null
    and row_number = 1
)

select 
  *
from final

