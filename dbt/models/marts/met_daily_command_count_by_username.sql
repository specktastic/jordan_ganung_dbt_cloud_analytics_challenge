with commands as (
    SELECT * FROM {{ ref('fct_command_statuses') }}
)

,min_command_date as (
    SELECT CAST(DATE_TRUNC(MIN(command_timestamp), DAY) AS DATE) as min_date FROM {{ ref('fct_command_statuses') }}
)
,max_command_date as (
    SELECT CAST(DATE_TRUNC(MAX(command_timestamp), DAY) AS DATE) AS max_date FROM {{ ref('fct_command_statuses') }}
)

,users as (
    SELECT * FROM {{ ref('dim_users') }}
)

,date_spine as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2018-01-01' as date)",
    end_date="cast('2021-01-01' as date)"
   )
}}
)

,base_calendar as (
    SELECT 
        CAST(date_day AS DATE) AS date_day 
    FROM date_spine  
    WHERE CAST(date_day AS DATE) >= (select * from min_command_date) and CAST(date_day AS DATE) <= (select * from max_command_date)
    -- I wanted to just include this where clause in the date spine macro to make it more dynamic but couldnt get it work in short order.  Im sure with time I would figure it out
)

,aggregation as (
select 
    users.username
    ,COUNT(DISTINCT command_uuid) AS number_of_commands
    ,COUNT(DISTINCT CASE WHEN is_hub_success IS TRUE THEN command_uuid END) AS number_of_successful_commands
    ,CAST(DATE_TRUNC(command_timestamp, DAY) AS DATE) AS timestamp_date
from commands
left join users 
    using(user_id)
group by 1,4
)

,final_table as (
SELECT 
date_day as timestamp_date
,COALESCE(aggregation.username, 'Unknown') as username -- there are null userids in the command table, may want to consider adding a test, removing those from the upstream table or fixing raw
,COALESCE(aggregation.number_of_commands,0) as number_of_commands
,COALESCE(aggregation.number_of_successful_commands,0) as number_of_successful_commands
FROM base_calendar
LEFT JOIN aggregation   
    ON base_calendar.date_day = aggregation.timestamp_date

)

select * from final_table
