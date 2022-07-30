select
  command_uuid
from
  {{ ref('stg_commands') }}
where
  command IN ('ThermostatCoolSetPoint')
  and (
    thermostat_cool_set_point IS NULL
    or NOT REGEXP_CONTAINS(CAST(thermostat_cool_set_point AS STRING), '^([0-9]{1,3}\\.?[0-9]{1,3})$')
  )
limit 1