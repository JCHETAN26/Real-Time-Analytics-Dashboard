-- Staging model for Order and Sales events
WITH raw_orders AS (
    SELECT 
        src:order_id::STRING AS order_id,
        src:user_id::STRING AS user_id,
        src:product_id::STRING AS product_id,
        src:product_name::STRING AS product_name,
        src:category::STRING AS category,
        src:order_status::STRING AS status,
        src:revenue::FLOAT AS revenue_usd,
        src:quantity::INT AS quantity,
        src:region::STRING AS region,
        to_timestamp(src:order_time / 1000) AS ordered_at
    FROM {{ source('raw', 'order_events') }}
)

SELECT * FROM raw_orders
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) = 1
