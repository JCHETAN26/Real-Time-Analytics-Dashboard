-- Staging model for Inventory adjustment events.
-- Reads the Avro payload landed by the Kafka Snowflake Sink Connector.
WITH raw_inventory AS (
    SELECT
        {{ payload() }}:product_id::STRING       AS product_id,
        {{ payload() }}:product_name::STRING     AS product_name,
        {{ payload() }}:category::STRING         AS category,
        {{ payload() }}:stock_adjustment::INT    AS stock_adjustment,
        {{ payload() }}:stock_on_hand::INT       AS stock_on_hand,
        {{ payload() }}:reason::STRING           AS reason,
        to_timestamp({{ payload() }}:event_time / 1000) AS event_at
    FROM {{ source('raw', 'inventory_events') }}
)

SELECT * FROM raw_inventory
-- Keep the latest snapshot per product per timestamp
QUALIFY row_number() OVER (
    PARTITION BY product_id, event_at
    ORDER BY event_at DESC
) = 1
