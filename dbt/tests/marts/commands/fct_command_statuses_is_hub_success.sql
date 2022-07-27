select
  command_uuid
from
  {{ ref('fct_command_statuses') }}
where
  is_hub_success NOT IN (True, False)
limit 1