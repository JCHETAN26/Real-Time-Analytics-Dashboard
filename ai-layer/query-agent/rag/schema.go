// Package rag implements Retrieval-Augmented Generation for the StreamSense
// Text-to-SQL agent.
//
// Instead of injecting the full warehouse schema (all 5 tables) into every
// prompt, the RAG layer:
//
//  1. Embeds each table's description + column list at startup and stores the
//     vectors in Qdrant.
//  2. On each query, embeds the user's question and retrieves the top-k most
//     semantically relevant tables.
//  3. Builds a focused system prompt containing only those tables.
//
// This keeps prompts shorter, reduces hallucination on irrelevant tables, and
// demonstrates a production RAG pattern to interviewers.
package rag

// TableDoc describes a single warehouse table — the unit indexed in Qdrant.
type TableDoc struct {
	// ID is the unique identifier stored in Qdrant (slug form of the table name).
	ID string
	// QualifiedName is the fully-qualified Snowflake name, e.g. "marts.fct_orders".
	QualifiedName string
	// Description is a concise plain-English explanation of what this table contains.
	Description string
	// Columns lists each column with its name, type, and semantics.
	Columns []ColumnDoc
}

// ColumnDoc describes one column within a TableDoc.
type ColumnDoc struct {
	Name        string
	Type        string
	Description string
}

// EmbedText is the string that gets embedded for a TableDoc — the table name,
// its description, and a summary of its columns. Richer text → better retrieval.
func (t TableDoc) EmbedText() string {
	text := t.QualifiedName + ": " + t.Description + "\nColumns: "
	for i, c := range t.Columns {
		if i > 0 {
			text += ", "
		}
		text += c.Name + " (" + c.Type + ") — " + c.Description
	}
	return text
}

// SchemaText returns the formatted schema block injected into the prompt for
// this table — the same format the original hardcoded prompt used.
func (t TableDoc) SchemaText() string {
	s := "### " + t.QualifiedName + "\n"
	for _, c := range t.Columns {
		s += c.Name + " " + c.Type
		if c.Description != "" {
			s += " -- " + c.Description
		}
		s += "\n"
	}
	return s
}

// WarehouseTables is the complete knowledge base for the StreamSense warehouse.
// These are the documents indexed into Qdrant at startup.
var WarehouseTables = []TableDoc{
	{
		ID:            "fct-orders",
		QualifiedName: "marts_marts.fct_orders",
		Description:   "Fact table of all e-commerce orders. Use for revenue analysis, order counts, refunds, regional breakdowns, and product-level sales.",
		Columns: []ColumnDoc{
			{"order_id", "STRING", "unique order identifier"},
			{"user_id", "STRING", "customer identifier"},
			{"product_id", "STRING", "product SKU"},
			{"product_name", "STRING", "human-readable product name"},
			{"category", "STRING", "product category (Electronics, Clothing, Jewelry)"},
			{"order_status", "STRING", "placed | paid | fulfilled | refunded | cancelled"},
			{"revenue", "FLOAT", "order revenue in USD"},
			{"quantity", "INTEGER", "units ordered"},
			{"region", "STRING", "customer region (North America, Europe, Asia, South America, Oceania)"},
			{"order_time", "TIMESTAMP", "when the order was placed"},
			{"updated_at", "TIMESTAMP", "last status update"},
		},
	},
	{
		ID:            "dim-users",
		QualifiedName: "marts_marts.dim_users",
		Description:   "User dimension table. Use for customer segmentation, lifetime value, churn analysis, device breakdown, and VIP identification.",
		Columns: []ColumnDoc{
			{"user_id", "STRING", "unique user identifier"},
			{"user_segment", "STRING", "new | returning | vip | at_risk | churned"},
			{"country", "STRING", "user country"},
			{"device_type", "STRING", "mobile | desktop | tablet"},
			{"first_seen", "TIMESTAMP", "first event timestamp"},
			{"last_seen", "TIMESTAMP", "most recent event timestamp"},
			{"total_orders", "INTEGER", "lifetime order count"},
			{"lifetime_value", "FLOAT", "total revenue generated in USD"},
		},
	},
	{
		ID:            "agg-revenue-per-minute",
		QualifiedName: "marts_marts.agg_revenue_per_minute",
		Description:   "Pre-aggregated revenue by minute and region. Use for time-series revenue charts, trend detection, and real-time revenue monitoring.",
		Columns: []ColumnDoc{
			{"minute_bucket", "TIMESTAMP", "time truncated to the minute"},
			{"total_revenue", "FLOAT", "total revenue in that minute"},
			{"order_count", "INTEGER", "number of orders in that minute"},
			{"avg_order_value", "FLOAT", "average order value in that minute"},
			{"region", "STRING", "geographic region"},
		},
	},
	{
		ID:            "fct-product-performance",
		QualifiedName: "marts_marts.fct_product_performance",
		Description:   "Product funnel metrics: views, add-to-cart, purchases, and conversion rate. Use for product performance analysis, bestsellers, and funnel optimization.",
		Columns: []ColumnDoc{
			{"product_id", "STRING", "product SKU"},
			{"product_name", "STRING", "human-readable product name"},
			{"category", "STRING", "product category"},
			{"views", "INTEGER", "page views in the window"},
			{"add_to_cart", "INTEGER", "add-to-cart events"},
			{"purchases", "INTEGER", "completed purchases"},
			{"revenue", "FLOAT", "total revenue"},
			{"conversion_rate", "FLOAT", "purchases / views ratio"},
			{"window_start", "TIMESTAMP", "start of the aggregation window"},
			{"window_end", "TIMESTAMP", "end of the aggregation window"},
		},
	},
	{
		ID:            "stg-user-events",
		QualifiedName: "staging.stg_user_events",
		Description:   "Raw clickstream events: page views, searches, clicks, add-to-cart, and checkouts. Use for funnel analysis, session tracking, and device/page breakdowns.",
		Columns: []ColumnDoc{
			{"event_id", "STRING", "unique event identifier"},
			{"user_id", "STRING", "user identifier"},
			{"event_type", "STRING", "page_view | search | click | add_to_cart | checkout"},
			{"page", "STRING", "page or product interacted with"},
			{"session_id", "STRING", "session grouping identifier"},
			{"device_type", "STRING", "mobile | desktop | tablet"},
			{"event_time", "TIMESTAMP", "when the event occurred"},
		},
	},
}
