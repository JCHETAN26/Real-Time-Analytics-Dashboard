package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/streamsense-ai/ai-layer/query-agent/guard"
	_ "github.com/snowflakedb/gosnowflake"
)

// ─── Types ────────────────────────────────────────────────────────────────────

type ChatMessage struct {
	Role    string `json:"role"` // "system" | "user" | "assistant"
	Content string `json:"content"`
}

type QueryRequest struct {
	Question  string        `json:"question" binding:"required"`
	SessionID string        `json:"session_id"`
	History   []ChatMessage `json:"history"`
	Mode      string        `json:"mode"` // "technical" | "business"
}

type QueryResult struct {
	Columns []string                 `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
}

type AIResponse struct {
	Question            string      `json:"question"`
	SQL                 string      `json:"sql"`
	Explanation         string      `json:"explanation"`
	Insight             string      `json:"insight"`
	FollowUpSuggestions []string    `json:"follow_up_suggestions"`
	Results             QueryResult `json:"results"`
	Mode                string      `json:"mode"`
	SessionID           string      `json:"session_id"`
}

type GradientRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	MaxTokens   int           `json:"max_tokens"`
}

type GradientResponse struct {
	Choices []struct {
		Message ChatMessage `json:"message"`
	} `json:"choices"`
}

// ─── Session Store (In-memory for hackathon) ─────────────────────────────────

var (
	sessions   = make(map[string][]ChatMessage)
	sessionsMu sync.Mutex
)

func getHistory(sessionID string) []ChatMessage {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	return sessions[sessionID]
}

func appendHistory(sessionID string, msg ChatMessage) {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	sessions[sessionID] = append(sessions[sessionID], msg)
	// Keep rolling window of last 10 messages to avoid token limits
	if len(sessions[sessionID]) > 10 {
		sessions[sessionID] = sessions[sessionID][len(sessions[sessionID])-10:]
	}
}

// ─── System Prompt ────────────────────────────────────────────────────────────

func buildSystemPrompt(mode string) string {
	base := `You are StreamSense, an intelligent e-commerce analytics AI. You have access to a real-time Snowflake data warehouse.

## Snowflake Schema:

### marts.fct_orders
order_id STRING, user_id STRING, product_id STRING, product_name STRING, category STRING,
order_status STRING (placed|paid|fulfilled|refunded|cancelled), revenue FLOAT, quantity INTEGER,
region STRING, order_time TIMESTAMP, updated_at TIMESTAMP

### marts.dim_users
user_id STRING, user_segment STRING (new|returning|vip|at_risk|churned), country STRING,
device_type STRING (mobile|desktop|tablet), first_seen TIMESTAMP, last_seen TIMESTAMP,
total_orders INTEGER, lifetime_value FLOAT

### marts.agg_revenue_per_minute
minute_bucket TIMESTAMP, total_revenue FLOAT, order_count INTEGER, avg_order_value FLOAT, region STRING

### marts.fct_product_performance
product_id STRING, product_name STRING, category STRING, views INTEGER, add_to_cart INTEGER,
purchases INTEGER, revenue FLOAT, conversion_rate FLOAT, window_start TIMESTAMP, window_end TIMESTAMP

### staging.stg_user_events
event_id STRING, user_id STRING, event_type STRING (page_view|search|click|add_to_cart|checkout),
page STRING, session_id STRING, device_type STRING, event_time TIMESTAMP

## Rules:
- Use CURRENT_TIMESTAMP() for "now", DATEADD for windows
- Always LIMIT results (default 20)
- Only SELECT statements
- Use fully qualified names (marts.table_name)

## ALWAYS respond in this exact JSON format:
{
  "sql": "SELECT ...",
  "explanation": "...",
  "insight": "...",
  "follow_up_suggestions": ["...", "..."]
}`

	if mode == "business" {
		base += `

## IMPORTANT - BUSINESS MODE ACTIVE:
- Do NOT show SQL to the user
- Explain results in plain English with emojis
- Focus on actionable business insights
- Use phrases like "your store", "your customers", "right now"
- Keep explanations under 3 sentences`
	}

	return base
}

// ─── Gradient AI Call ─────────────────────────────────────────────────────────

func callGradientAI(messages []ChatMessage) (string, error) {
	apiKey := os.Getenv("GRADIENT_AI_KEY")
	apiBase := os.Getenv("GRADIENT_AI_BASE_URL")
	model := os.Getenv("GRADIENT_AI_MODEL")

	if apiKey == "" {
		return "", fmt.Errorf("GRADIENT_AI_KEY not set")
	}
	if apiBase == "" {
		apiBase = "https://inference.do-ai.run/v1"
	}
	if model == "" {
		model = "meta-llama/Meta-Llama-3.3-70B-Instruct"
	}

	reqBody := GradientRequest{
		Model:       model,
		Messages:    messages,
		Temperature: 0.2,
		MaxTokens:   1024,
	}

	bodyBytes, _ := json.Marshal(reqBody)
	httpReq, err := http.NewRequest("POST", apiBase+"/chat/completions", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("Gradient AI error %d: %s", resp.StatusCode, string(body))
	}

	var gradientResp GradientResponse
	if err := json.Unmarshal(body, &gradientResp); err != nil {
		return "", err
	}
	if len(gradientResp.Choices) == 0 {
		return "", fmt.Errorf("no choices returned from Gradient AI")
	}

	return gradientResp.Choices[0].Message.Content, nil
}

// ─── Snowflake Query Execution ────────────────────────────────────────────────

func executeSQL(db *sql.DB, query string) (QueryResult, error) {
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var result QueryResult
	result.Columns = cols

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)

		row := make(map[string]interface{})
		for i, col := range cols {
			row[col] = vals[i]
		}
		result.Rows = append(result.Rows, row)
	}
	return result, nil
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	dsn := os.Getenv("SNOWFLAKE_DSN")
	var db *sql.DB
	var err error

	if dsn != "" {
		db, err = sql.Open("snowflake", dsn)
		if err != nil {
			log.Fatalf("Failed to open Snowflake: %s", err)
		}
		defer db.Close()
		log.Println("✅ Snowflake connected!")
	} else {
		log.Println("⚠️  SNOWFLAKE_DSN not set — SQL execution disabled (demo mode)")
	}

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.Use(cors.Default())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":    "StreamSense AI Online",
			"snowflake": dsn != "",
			"gradient":  os.Getenv("GRADIENT_AI_KEY") != "",
		})
	})

	// Clear session
	r.DELETE("/session/:id", func(c *gin.Context) {
		sessionID := c.Param("id")
		sessionsMu.Lock()
		delete(sessions, sessionID)
		sessionsMu.Unlock()
		c.JSON(200, gin.H{"cleared": sessionID})
	})

	// Main query endpoint — multi-turn
	r.POST("/query", func(c *gin.Context) {
		var req QueryRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		sessionID := req.SessionID
		if sessionID == "" {
			sessionID = "default"
		}
		mode := req.Mode
		if mode == "" {
			mode = "technical"
		}

		// 1. Build messages: system + history + new question
		messages := []ChatMessage{
			{Role: "system", Content: buildSystemPrompt(mode)},
		}

		// Append session history
		history := getHistory(sessionID)
		messages = append(messages, history...)

		// Append current question
		userMsg := ChatMessage{Role: "user", Content: req.Question}
		messages = append(messages, userMsg)
		appendHistory(sessionID, userMsg)

		fmt.Printf("🤖 AI Query [session:%s] [mode:%s]: %s\n", sessionID, mode, req.Question)

		// 2. Call Gradient AI
		rawReply, err := callGradientAI(messages)
		if err != nil {
			log.Printf("Gradient AI error: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("AI Error: %v", err)})
			return
		}

		// Store assistant reply in history
		appendHistory(sessionID, ChatMessage{Role: "assistant", Content: rawReply})

		// 3. Parse JSON from LLM response
		// Strip markdown code fences if present
		cleaned := strings.TrimSpace(rawReply)
		if idx := strings.Index(cleaned, "```json"); idx != -1 {
			cleaned = cleaned[idx+7:]
		} else if idx := strings.Index(cleaned, "```"); idx != -1 {
			cleaned = cleaned[idx+3:]
		}
		if idx := strings.LastIndex(cleaned, "```"); idx != -1 {
			cleaned = cleaned[:idx]
		}
		cleaned = strings.TrimSpace(cleaned)

		var parsed struct {
			SQL                 string   `json:"sql"`
			Explanation         string   `json:"explanation"`
			Insight             string   `json:"insight"`
			FollowUpSuggestions []string `json:"follow_up_suggestions"`
			Error               string   `json:"error"`
		}

		resp := AIResponse{
			Question:  req.Question,
			Mode:      mode,
			SessionID: sessionID,
		}

		if err := json.Unmarshal([]byte(cleaned), &parsed); err != nil {
			// Fallback — return raw response if JSON parse failed
			resp.Explanation = rawReply
			c.JSON(200, resp)
			return
		}

		resp.SQL = parsed.SQL
		resp.Explanation = parsed.Explanation
		resp.Insight = parsed.Insight
		resp.FollowUpSuggestions = parsed.FollowUpSuggestions

		// 4. Validate + execute SQL on Snowflake
		if parsed.SQL != "" {
			safeSQL, vErr := guard.ValidateSQL(parsed.SQL)
			if vErr != nil {
				log.Printf("🛑 Blocked unsafe SQL: %v", vErr)
				resp.Insight = fmt.Sprintf("⚠️ Generated query was blocked by the safety guardrail: %v", vErr)
				resp.SQL = parsed.SQL // surface the rejected SQL for transparency
				c.JSON(200, resp)
				return
			}
			resp.SQL = safeSQL

			if db != nil {
				results, err := executeSQL(db, safeSQL)
				if err != nil {
					log.Printf("Snowflake error: %v", err)
					resp.Insight = fmt.Sprintf("⚠️ SQL executed but failed: %v", err)
				} else {
					resp.Results = results
				}
			}
		}

		c.JSON(200, resp)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8085"
	}
	fmt.Printf("🚀 StreamSense AI | Query Agent running on :%s\n", port)
	r.Run(":" + port)
}
