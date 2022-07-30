with column_rename as (
  select
    CAST(uid AS STRING) as user_id
    ,username
    ,dateRegistered as date_registered
    ,last_modified
  from
    {{ source('interview_source', 'raw_users') }}
)

select
  *
from
  column_rename