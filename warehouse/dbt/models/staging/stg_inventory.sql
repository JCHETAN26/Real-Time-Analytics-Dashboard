-- Staging model for Inventory adjustment events.
-- Skipped when INVENTORY_EVENTS table is not populated.
-- Use `dbt run --exclude stg_inventory` to skip this model.
{{
  config(
    enabled = false
  )
}}
WITH raw_inventory AS (
    SELECT
        {{ payload() }}:product_id::STRING       AS product_id,
        {{ payload() }}:product_name::STRING     AS product_name,
        {{ payload() }}:category::STRING         AS category,
        {{ payload() }}:stock_adjustment::INT    AS stock_adjustment,
        {{ payload() }}:stock_on_hand::INT       AS stock_on_hand,
        {{ payload() }}:reason::STRING           AS reason,
        TO_TIMESTAMP_NTZ({{ payload() }}:event_time::NUMBER / 1000) AS event_at
    FROM {{ source('raw', 'inventory_events') }}
)

SELECT * FROM raw_inventory
QUALIFY row_number() OVER (
    PARTITION BY product_id, event_at
    ORDER BY event_at DESC
) = 1
