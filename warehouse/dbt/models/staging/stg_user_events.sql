-- Staging model for User interaction events (clickstream).
-- Reads the JSON payload loaded by the Olist Snowflake loader.
WITH raw_events AS (
    SELECT
        {{ payload() }}:event_id::STRING    AS event_id,
        {{ payload() }}:user_id::STRING     AS user_id,
        {{ payload() }}:event_type::STRING  AS event_type,
        {{ payload() }}:page::STRING        AS page,
        {{ payload() }}:session_id::STRING  AS session_id,
        {{ payload() }}:device_type::STRING AS device_type,
        TO_TIMESTAMP_NTZ({{ payload() }}:event_time::NUMBER / 1000) AS event_at
    FROM {{ source('raw', 'user_events') }}
)

SELECT * FROM raw_events
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_at DESC) = 1
