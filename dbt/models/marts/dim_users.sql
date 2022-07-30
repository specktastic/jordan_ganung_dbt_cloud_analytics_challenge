
with users as (
    SELECT * FROM {{ ref('stg_users') }}
)

select 
    user_id
    ,username
    ,date_registered
    ,last_modified
from users