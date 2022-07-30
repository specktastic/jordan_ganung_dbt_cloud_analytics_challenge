{{ config(materialized='ephemeral') }}

WITH commands AS ( 
    SELECT * FROM {{ ref('stg_commands') }}
),

last_command AS ( 
    SELECT 
        command_uuid, 
        device_id,
        update_timestamp, 
        LAG(update_timestamp, 1) OVER (PARTITION BY device_id ORDER BY update_timestamp ASC ) AS last_time
    FROM commands
    ORDER BY device_id, update_timestamp  ASC
), 

epoch_seconds AS (
    SELECT 
        *,
        UNIX_SECONDS(update_timestamp) AS update_time_epoch,
        UNIX_SECONDS(last_time) AS last_time_epoch,
        UNIX_SECONDS(update_timestamp) - UNIX_SECONDS(last_time)  AS time_difference
    FROM last_command
),

adding_flag AS ( 
    SELECT 
        *,
        CASE 
            WHEN time_difference > 15 
            THEN TRUE 
            ELSE FALSE 
        END as is_last_command_within_15_seconds
    FROM epoch_seconds
)

SELECT * FROM adding_flag

