-- User dimension: one row per user with derived segment and lifetime metrics.
-- Built by joining clickstream activity (stg_user_events) with order facts.
WITH user_activity AS (
    SELECT
        user_id,
        MIN(event_at) AS first_seen,
        MAX(event_at) AS last_seen,
        -- Device most frequently used by the user
        MODE(device_type) AS device_type
    FROM {{ ref('stg_user_events') }}
    GROUP BY user_id
),

user_orders AS (
    SELECT
        user_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(revenue)             AS lifetime_value,
        MAX(order_time)          AS last_order_at
    FROM {{ ref('fct_orders') }}
    WHERE order_status IN ('paid', 'fulfilled')
    GROUP BY user_id
),

joined AS (
    SELECT
        a.user_id,
        a.first_seen,
        a.last_seen,
        a.device_type,
        COALESCE(o.total_orders, 0)    AS total_orders,
        COALESCE(o.lifetime_value, 0)  AS lifetime_value,
        o.last_order_at
    FROM user_activity a
    LEFT JOIN user_orders o ON a.user_id = o.user_id
)

SELECT
    user_id,
    -- Segment users by recency and value
    CASE
        WHEN total_orders = 0 THEN 'new'
        WHEN lifetime_value >= 1000 THEN 'vip'
        WHEN last_order_at < DATEADD(day, -30, CURRENT_TIMESTAMP()) THEN 'churned'
        WHEN last_order_at < DATEADD(day, -14, CURRENT_TIMESTAMP()) THEN 'at_risk'
        ELSE 'returning'
    END AS user_segment,
    'unknown' AS country,
    device_type,
    first_seen,
    last_seen,
    total_orders,
    lifetime_value
FROM joined
