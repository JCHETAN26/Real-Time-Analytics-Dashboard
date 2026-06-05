-- Staging model for Order and Sales events.
-- Reads the JSON payload loaded by the Olist Snowflake loader.
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
        TO_TIMESTAMP_NTZ({{ payload() }}:order_time::NUMBER / 1000) AS ordered_at
    FROM {{ source('raw', 'order_events') }}
)

SELECT * FROM raw_orders
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) = 1
