-- Fact table of all order events, one row per order (latest status wins).
-- This is the primary table the AI Query Agent reasons over.
WITH orders AS (
    SELECT
        order_id,
        user_id,
        product_id,
        product_name,
        category,
        status        AS order_status,
        revenue_usd   AS revenue,
        quantity,
        region,
        ordered_at    AS order_time
    FROM {{ ref('stg_orders') }}
)

SELECT
    order_id,
    user_id,
    product_id,
    product_name,
    category,
    order_status,
    revenue,
    quantity,
    region,
    order_time,
    CURRENT_TIMESTAMP() AS updated_at
FROM orders
