-- Staging model for User interaction events (clickstream)
WITH raw_events AS (
    SELECT 
        src:event_id::STRING AS event_id,
        src:user_id::STRING AS user_id,
        src:event_type::STRING AS event_type,
        src:page::STRING AS page,
        src:session_id::STRING AS session_id,
        src:device_type::STRING AS device_type,
        to_timestamp(src:event_time / 1000) AS event_at
    FROM {{ source('raw', 'user_events') }}
)

SELECT * FROM raw_events
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_at DESC) = 1
