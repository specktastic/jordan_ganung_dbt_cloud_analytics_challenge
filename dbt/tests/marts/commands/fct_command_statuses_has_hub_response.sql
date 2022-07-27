select
  command_uuid
from
  {{ ref('fct_command_statuses') }}
where
  has_hub_response NOT IN (True, False)
limit 1