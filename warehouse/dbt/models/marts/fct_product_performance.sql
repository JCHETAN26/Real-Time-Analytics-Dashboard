-- Product performance: funnel metrics (views -> add-to-cart -> purchase) and
-- conversion rate per product, computed over the full available window.
WITH events AS (
    SELECT
        page        AS product_key,
        event_type
    FROM {{ ref('stg_user_events') }}
),

funnel AS (
    SELECT
        product_key,
        COUNT_IF(event_type = 'page_view')   AS views,
        COUNT_IF(event_type = 'add_to_cart')  AS add_to_cart,
        COUNT_IF(event_type = 'checkout')     AS purchases
    FROM events
    GROUP BY product_key
),

sales AS (
    SELECT
        product_id,
        ANY_VALUE(product_name) AS product_name,
        ANY_VALUE(category)     AS category,
        SUM(revenue)            AS revenue,
        COUNT(DISTINCT order_id) AS order_count
    FROM {{ ref('fct_orders') }}
    WHERE order_status IN ('paid', 'fulfilled')
    GROUP BY product_id
)

SELECT
    s.product_id,
    s.product_name,
    s.category,
    COALESCE(f.views, 0)        AS views,
    COALESCE(f.add_to_cart, 0)  AS add_to_cart,
    COALESCE(f.purchases, s.order_count) AS purchases,
    s.revenue,
    -- Conversion rate guards against divide-by-zero on products with no views
    CASE
        WHEN COALESCE(f.views, 0) = 0 THEN 0
        ELSE COALESCE(f.purchases, s.order_count) / f.views
    END AS conversion_rate,
    DATEADD(hour, -24, CURRENT_TIMESTAMP()) AS window_start,
    CURRENT_TIMESTAMP()                     AS window_end
FROM sales s
LEFT JOIN funnel f ON s.product_id = f.product_key
