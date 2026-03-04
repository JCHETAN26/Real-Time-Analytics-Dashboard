-- Aggregate revenue by minute for real-time visualization
WITH orders AS (
    SELECT 
        date_trunc('minute', ordered_at) as minute_bucket,
        revenue_usd,
        order_id,
        region
    FROM {{ ref('stg_orders') }}
)

SELECT 
    minute_bucket,
    SUM(revenue_usd) AS total_revenue,
    COUNT(DISTINCT order_id) AS order_count,
    AVG(revenue_usd) AS avg_order_value,
    region
FROM orders
GROUP BY minute_bucket, region
ORDER BY minute_bucket DESC
