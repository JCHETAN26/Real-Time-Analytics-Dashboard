-- Staging model for Order and Sales events.
-- Reads the Avro payload landed by the Kafka Snowflake Sink Connector.
WITH raw_orders AS (
    SELECT
        {{ payload() }}:order_id::STRING     AS order_id,
        {{ payload() }}:user_id::STRING      AS user_id,
        {{ payload() }}:product_id::STRING   AS product_id,
        {{ payload() }}:product_name::STRING AS product_name,
        {{ payload() }}:category::STRING     AS category,
        {{ payload() }}:order_status::STRING AS status,
        {{ payload() }}:revenue::FLOAT       AS revenue_usd,
        {{ payload() }}:quantity::INT        AS quantity,
        {{ payload() }}:region::STRING       AS region,
        to_timestamp({{ payload() }}:order_time / 1000) AS ordered_at
    FROM {{ source('raw', 'order_events') }}
)

SELECT * FROM raw_orders
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) = 1
