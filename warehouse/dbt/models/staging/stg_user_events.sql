-- Staging model for User interaction events (clickstream).
-- Reads the Avro payload landed by the Kafka Snowflake Sink Connector.
WITH raw_events AS (
    SELECT
        {{ payload() }}:event_id::STRING    AS event_id,
        {{ payload() }}:user_id::STRING     AS user_id,
        {{ payload() }}:event_type::STRING  AS event_type,
        {{ payload() }}:page::STRING        AS page,
        {{ payload() }}:session_id::STRING  AS session_id,
        {{ payload() }}:device_type::STRING AS device_type,
        to_timestamp({{ payload() }}:event_time / 1000) AS event_at
    FROM {{ source('raw', 'user_events') }}
)

SELECT * FROM raw_events
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_at DESC) = 1
