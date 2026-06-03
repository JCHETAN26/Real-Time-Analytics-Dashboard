package eval

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// PromptVariant captures a named system-prompt strategy so we can A/B test
// which phrasing yields more accurate SQL.
type PromptVariant struct {
	Name   string
	System string
}

// schemaBlock is the shared Snowflake schema context injected into every
// prompt variant. Keeping it identical isolates the effect of the surrounding
// instructions during A/B comparison.
const schemaBlock = `Tables (use fully-qualified names):
- marts.fct_orders(order_id, user_id, product_id, product_name, category, order_status, revenue, quantity, region, order_time, updated_at)
- marts.dim_users(user_id, user_segment, country, device_type, first_seen, last_seen, total_orders, lifetime_value)
- marts.agg_revenue_per_minute(minute_bucket, total_revenue, order_count, avg_order_value, region)
- marts.fct_product_performance(product_id, product_name, category, views, add_to_cart, purchases, revenue, conversion_rate, window_start, window_end)
- staging.stg_user_events(event_id, user_id, event_type, page, session_id, device_type, event_time)`

// PromptVariants returns the system-prompt strategies under evaluation.
func PromptVariants() []PromptVariant {
	return []PromptVariant{
		{
			Name: "baseline",
			System: "You are a Snowflake SQL generator for an e-commerce warehouse.\n" +
				schemaBlock +
				"\nReturn ONLY JSON: {\"sql\": \"...\"}. Use SELECT only.",
		},
		{
			Name: "strict-rules",
			System: "You are StreamSense, an expert Snowflake analytics engineer.\n" +
				schemaBlock +
				"\nRULES:\n" +
				"- Generate exactly one read-only SELECT statement.\n" +
				"- Always use CURRENT_TIMESTAMP() and DATEADD for time windows.\n" +
				"- Always add an explicit LIMIT for open-ended questions (default 20).\n" +
				"- Prefer marts.* tables over staging.*.\n" +
				"- Use fully-qualified table names.\n" +
				"Respond ONLY with JSON: {\"sql\": \"...\"}.",
		},
	}
}

// Validate is wired by the main package at runtime to the production SQL
// guardrail. Eval falls back to a permissive validator if it is unset, so the
// eval package has no hard dependency on main.
var Validate SafetyValidator = func(sql string) (string, error) { return sql, nil }

// ─── Offline (deterministic) generator ────────────────────────────────────────

// OfflineGenerator returns a generator that produces SQL without any network
// call. It is used for demos and CI: the "strict-rules" variant returns the
// gold SQL (simulating a well-instructed model), while "baseline" returns a
// deliberately weaker query for some cases so the A/B comparison is meaningful.
func OfflineGenerator(v PromptVariant) func(EvalCase) string {
	return func(c EvalCase) string {
		if v.Name == "strict-rules" {
			return c.GoldSQL
		}
		// Baseline: simulate a less-instructed model that sometimes omits
		// LIMIT or picks a coarser table. Deterministic per case ID.
		switch c.ID {
		case "top-products-last-hour":
			// Missing LIMIT — still correct table, lower keyword score.
			return "SELECT product_name, SUM(revenue) AS total_revenue FROM marts.fct_orders GROUP BY product_name ORDER BY total_revenue DESC"
		case "avg-order-value":
			// Picks raw orders instead of the aggregate mart.
			return "SELECT AVG(revenue) FROM marts.fct_orders"
		default:
			return c.GoldSQL
		}
	}
}

// ─── Live generator (Gradient AI) ─────────────────────────────────────────────

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type gradientRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	MaxTokens   int           `json:"max_tokens"`
}

type gradientResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

// LiveGenerator calls the configured Gradient AI model with the given prompt
// variant and extracts the SQL field from the JSON response.
func LiveGenerator(v PromptVariant) func(EvalCase) string {
	return func(c EvalCase) string {
		apiKey := os.Getenv("GRADIENT_AI_KEY")
		apiBase := envOr("GRADIENT_AI_BASE_URL", "https://inference.do-ai.run/v1")
		model := envOr("GRADIENT_AI_MODEL", "meta-llama/Meta-Llama-3.3-70B-Instruct")
		if apiKey == "" {
			return ""
		}

		reqBody := gradientRequest{
			Model: model,
			Messages: []chatMessage{
				{Role: "system", Content: v.System},
				{Role: "user", Content: c.Question},
			},
			Temperature: 0.1,
			MaxTokens:   400,
		}
		b, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", apiBase+"/chat/completions", bytes.NewBuffer(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)

		client := &http.Client{Timeout: 20 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		var gr gradientResponse
		if err := json.Unmarshal(body, &gr); err != nil || len(gr.Choices) == 0 {
			return ""
		}
		return extractSQL(gr.Choices[0].Message.Content)
	}
}

// extractSQL pulls the "sql" field out of a (possibly fenced) JSON reply.
func extractSQL(reply string) string {
	cleaned := strings.TrimSpace(reply)
	for _, fence := range []string{"```json", "```"} {
		if idx := strings.Index(cleaned, fence); idx != -1 {
			cleaned = cleaned[idx+len(fence):]
		}
	}
	if idx := strings.LastIndex(cleaned, "```"); idx != -1 {
		cleaned = cleaned[:idx]
	}
	cleaned = strings.TrimSpace(cleaned)

	var parsed struct {
		SQL string `json:"sql"`
	}
	if err := json.Unmarshal([]byte(cleaned), &parsed); err == nil && parsed.SQL != "" {
		return parsed.SQL
	}
	// Fallback: if the model returned bare SQL, use it as-is.
	if strings.HasPrefix(strings.ToUpper(cleaned), "SELECT") || strings.HasPrefix(strings.ToUpper(cleaned), "WITH") {
		return cleaned
	}
	return ""
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
