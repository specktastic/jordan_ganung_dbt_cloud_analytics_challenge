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
    command_uuid
    ,command_result
    ,command_failure_string
    ,command_node_id
    ,case 
      when command_result in ('true', 'success') 
      then TRUE 
      else FALSE 
    end as is_hub_success
    ,case 
      when command_failure_string IS NOT NULL 
      then TRUE
      else FALSE
    end as has_hub_response
    ,update_timestamp
    ,event_timestamp
    ,event_uuid
    ,device_id
    ,_uid
    ,_source_file
  from
    removing_duplicates
  where
    item_key = 'CommandResult'
    and command_uuid is not null
    and row_number = 1
)
select * from final

