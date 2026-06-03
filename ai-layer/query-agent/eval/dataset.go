package eval

// EvalCase is a single Text-to-SQL evaluation example: a natural-language
// question paired with the ground-truth SQL we expect the agent to produce,
// plus lightweight structural assertions used for grading.
type EvalCase struct {
	ID          string   `json:"id"`
	Question    string   `json:"question"`
	GoldSQL     string   `json:"gold_sql"`
	MustTables  []string `json:"must_tables"`  // tables that MUST appear in generated SQL
	MustKeyword []string `json:"must_keyword"` // SQL keywords/clauses that MUST appear
	Mode        string   `json:"mode"`         // "technical" | "business"
}

// Dataset is the curated benchmark used to measure agent quality. It is small
// on purpose: every case is hand-verified against the documented Snowflake
// schema so scores are trustworthy.
var Dataset = []EvalCase{
	{
		ID:          "top-products-last-hour",
		Question:    "What were our top 5 products by revenue in the last hour?",
		GoldSQL:     "SELECT product_name, SUM(revenue) AS total_revenue FROM marts.fct_orders WHERE order_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP()) GROUP BY product_name ORDER BY total_revenue DESC LIMIT 5",
		MustTables:  []string{"marts.fct_orders"},
		MustKeyword: []string{"SUM", "GROUP BY", "ORDER BY", "LIMIT"},
		Mode:        "technical",
	},
	{
		ID:          "revenue-by-region-today",
		Question:    "How much revenue did each region generate today?",
		GoldSQL:     "SELECT region, SUM(total_revenue) AS revenue FROM marts.agg_revenue_per_minute WHERE minute_bucket >= DATE_TRUNC('day', CURRENT_TIMESTAMP()) GROUP BY region ORDER BY revenue DESC",
		MustTables:  []string{"marts.agg_revenue_per_minute"},
		MustKeyword: []string{"SUM", "GROUP BY", "region"},
		Mode:        "technical",
	},
	{
		ID:          "vip-user-count",
		Question:    "How many VIP customers do we have?",
		GoldSQL:     "SELECT COUNT(*) AS vip_users FROM marts.dim_users WHERE user_segment = 'vip'",
		MustTables:  []string{"marts.dim_users"},
		MustKeyword: []string{"COUNT", "user_segment"},
		Mode:        "technical",
	},
	{
		ID:          "revenue-timeseries-around-3pm",
		Question:    "Show me revenue per minute over the last hour",
		GoldSQL:     "SELECT minute_bucket, total_revenue FROM marts.agg_revenue_per_minute WHERE minute_bucket >= DATEADD(hour, -1, CURRENT_TIMESTAMP()) ORDER BY minute_bucket ASC LIMIT 60",
		MustTables:  []string{"marts.agg_revenue_per_minute"},
		MustKeyword: []string{"minute_bucket", "ORDER BY"},
		Mode:        "technical",
	},
	{
		ID:          "best-converting-products",
		Question:    "Which products have the highest conversion rate?",
		GoldSQL:     "SELECT product_name, conversion_rate FROM marts.fct_product_performance ORDER BY conversion_rate DESC LIMIT 10",
		MustTables:  []string{"marts.fct_product_performance"},
		MustKeyword: []string{"conversion_rate", "ORDER BY", "DESC"},
		Mode:        "technical",
	},
	{
		ID:          "refunded-orders-count",
		Question:    "How many orders were refunded today?",
		GoldSQL:     "SELECT COUNT(*) AS refunded FROM marts.fct_orders WHERE order_status = 'refunded' AND order_time >= DATE_TRUNC('day', CURRENT_TIMESTAMP())",
		MustTables:  []string{"marts.fct_orders"},
		MustKeyword: []string{"COUNT", "order_status"},
		Mode:        "technical",
	},
	{
		ID:          "checkout-events-by-device",
		Question:    "How many checkout events came from each device type?",
		GoldSQL:     "SELECT device_type, COUNT(*) AS checkouts FROM staging.stg_user_events WHERE event_type = 'checkout' GROUP BY device_type ORDER BY checkouts DESC",
		MustTables:  []string{"staging.stg_user_events"},
		MustKeyword: []string{"COUNT", "GROUP BY", "device_type"},
		Mode:        "technical",
	},
	{
		ID:          "avg-order-value",
		Question:    "What's our average order value right now?",
		GoldSQL:     "SELECT AVG(avg_order_value) AS aov FROM marts.agg_revenue_per_minute WHERE minute_bucket >= DATEADD(minute, -10, CURRENT_TIMESTAMP())",
		MustTables:  []string{"marts.agg_revenue_per_minute"},
		MustKeyword: []string{"AVG"},
		Mode:        "technical",
	},
	{
		ID:          "top-categories-revenue",
		Question:    "Which product categories make the most money?",
		GoldSQL:     "SELECT category, SUM(revenue) AS total_revenue FROM marts.fct_orders GROUP BY category ORDER BY total_revenue DESC LIMIT 10",
		MustTables:  []string{"marts.fct_orders"},
		MustKeyword: []string{"category", "SUM", "GROUP BY"},
		Mode:        "technical",
	},
	{
		ID:          "high-value-users",
		Question:    "Who are our top 10 customers by lifetime value?",
		GoldSQL:     "SELECT user_id, lifetime_value FROM marts.dim_users ORDER BY lifetime_value DESC LIMIT 10",
		MustTables:  []string{"marts.dim_users"},
		MustKeyword: []string{"lifetime_value", "ORDER BY", "LIMIT"},
		Mode:        "technical",
	},
}
