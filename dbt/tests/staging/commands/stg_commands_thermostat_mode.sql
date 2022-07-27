select
  command_uuid
from
  {{ ref('stg_commands') }}
where
  command IN ('ThermostatMode')
  and (
    thermostat_mode is null
    or thermostat_mode NOT IN (
      'cool'
      ,'settocool'
      ,'heat'
      ,'off'
      ,'operatingstate'
      ,'settoheat'
      ,'auto'
    )
  )
limit 1