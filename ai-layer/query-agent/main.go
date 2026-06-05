package main

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/snowflakedb/gosnowflake"
	"github.com/streamsense-ai/ai-layer/query-agent/guard"
	"github.com/streamsense-ai/ai-layer/query-agent/rag"
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
	RetrievedTables     []string    `json:"retrieved_tables,omitempty"` // RAG: which tables were injected
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

// GeminiContent / GeminiRequest / GeminiResponse match the Gemini generateContent API.
type GeminiPart struct {
	Text string `json:"text"`
}
type GeminiContent struct {
	Role  string       `json:"role"` // "user" | "model"
	Parts []GeminiPart `json:"parts"`
}
type GeminiRequest struct {
	Contents         []GeminiContent `json:"contents"`
	SystemInstruction *GeminiContent `json:"systemInstruction,omitempty"`
}
type GeminiResponse struct {
	Candidates []struct {
		Content GeminiContent `json:"content"`
	} `json:"candidates"`
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

// buildSystemPrompt constructs the LLM system prompt using only the schema
// tables retrieved by the RAG pipeline for the current question.
// retrievedSchema is the formatted schema block from rag.Retriever.RetrieveSchema.
// retrievedTables is a slice of qualified table names for the log line.
func buildSystemPrompt(retrievedSchema, mode string) string {
	base := `You are StreamSense, an intelligent e-commerce analytics AI. You have access to a real-time Snowflake data warehouse.

## Relevant Snowflake Tables (retrieved for this query):

` + retrievedSchema + `
## Rules:
- Use CURRENT_TIMESTAMP() for "now", DATEADD for windows
- Always LIMIT results (default 20)
- Only SELECT statements
- Use fully qualified names (schema.table_name)

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

// ─── AI Dispatch ──────────────────────────────────────────────────────────────
// callAI routes to Gemini if GEMINI_API_KEY is set, otherwise Gradient AI.

func callAI(messages []ChatMessage) (string, error) {
	if os.Getenv("GEMINI_API_KEY") != "" {
		return callGemini(messages)
	}
	return callGradientAI(messages)
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

// ─── Gemini AI Call ───────────────────────────────────────────────────────────

func callGemini(messages []ChatMessage) (string, error) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	model := os.Getenv("GEMINI_MODEL")
	if model == "" {
		model = "gemini-2.0-flash"
	}

	// Split system message from conversation messages
	var systemPrompt string
	var contents []GeminiContent
	for _, m := range messages {
		switch m.Role {
		case "system":
			systemPrompt = m.Content
		case "user":
			contents = append(contents, GeminiContent{
				Role:  "user",
				Parts: []GeminiPart{{Text: m.Content}},
			})
		case "assistant":
			contents = append(contents, GeminiContent{
				Role:  "model",
				Parts: []GeminiPart{{Text: m.Content}},
			})
		}
	}

	reqBody := GeminiRequest{Contents: contents}
	if systemPrompt != "" {
		reqBody.SystemInstruction = &GeminiContent{
			Parts: []GeminiPart{{Text: systemPrompt}},
		}
	}

	bodyBytes, _ := json.Marshal(reqBody)
	url := fmt.Sprintf(
		"https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s",
		model, apiKey,
	)
	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("Gemini request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("Gemini error %d: %s", resp.StatusCode, string(body))
	}

	var geminiResp GeminiResponse
	if err := json.Unmarshal(body, &geminiResp); err != nil {
		return "", fmt.Errorf("could not parse Gemini response: %s", string(body))
	}
	if len(geminiResp.Candidates) == 0 || len(geminiResp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("empty response from Gemini")
	}
	return geminiResp.Candidates[0].Content.Parts[0].Text, nil
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

// ─── Snowflake key-pair helpers ───────────────────────────────────────────────

type snowflakeConfig struct {
	Account       string
	User          string
	Database      string
	Schema        string
	Warehouse     string
	Authenticator string
	PrivateKey    *rsa.PrivateKey
}

func decodePEM(data []byte) (*pem.Block, []byte) {
	return pem.Decode(data)
}

func parsePrivateKey(der []byte) (*rsa.PrivateKey, error) {
	key, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, err
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key is not RSA")
	}
	return rsaKey, nil
}

func buildSnowflakeDSN(cfg *snowflakeConfig) (string, error) {
	sfCfg := &gosnowflake.Config{
		Account:       cfg.Account,
		User:          cfg.User,
		Database:      cfg.Database,
		Schema:        "MARTS_MARTS",
		Warehouse:     cfg.Warehouse,
		Authenticator: gosnowflake.AuthTypeJwt,
		PrivateKey:    cfg.PrivateKey,
	}
	return gosnowflake.DSN(sfCfg)
}

// ─── Snowflake connection ─────────────────────────────────────────────────────

func openSnowflake() (*sql.DB, error) {
	// Key-pair auth (bypasses MFA) — preferred when SNOWFLAKE_PRIVATE_KEY_PATH is set
	keyPath := os.Getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
	if keyPath != "" {
		account := os.Getenv("SNOWFLAKE_ACCOUNT")
		user := os.Getenv("SNOWFLAKE_USER")
		warehouse := os.Getenv("SNOWFLAKE_WAREHOUSE")
		if warehouse == "" {
			warehouse = "STREAMSENSE_WH"
		}
		if account == "" || user == "" {
			return nil, fmt.Errorf("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER required with key-pair auth")
		}

		keyBytes, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("cannot read private key %s: %w", keyPath, err)
		}

		block, _ := decodePEM(keyBytes)
		if block == nil {
			return nil, fmt.Errorf("failed to decode PEM from %s", keyPath)
		}
		parsedKey, err := parsePrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse PKCS8 key: %w", err)
		}

		cfg := &snowflakeConfig{
			Account:       account,
			User:          user,
			Database:      "STREAMSENSE",
			Schema:        "MARTS",
			Warehouse:     warehouse,
			Authenticator: "snowflake_jwt",
			PrivateKey:    parsedKey,
		}
		dsn, err := buildSnowflakeDSN(cfg)
		if err != nil {
			return nil, err
		}
		return sql.Open("snowflake", dsn)
	}

	// Fallback: plain DSN (password auth)
	dsn := os.Getenv("SNOWFLAKE_DSN")
	if dsn == "" {
		return nil, nil // no Snowflake configured — demo mode
	}
	return sql.Open("snowflake", dsn)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	var db *sql.DB

	db, err := openSnowflake()
	if err != nil {
		log.Fatalf("Failed to open Snowflake: %s", err)
	}
	if db != nil {
		defer db.Close()
		if err := db.PingContext(context.Background()); err != nil {
			log.Fatalf("Snowflake ping failed: %s", err)
		}
		log.Println("✅ Snowflake connected!")
	} else {
		log.Println("⚠️  No Snowflake credentials — SQL execution disabled (demo mode)")
	}

	// Initialise the RAG retriever — indexes all warehouse tables into Qdrant.
	// Continues gracefully if Qdrant is not running.
	retriever := rag.NewRetriever()

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.Use(cors.Default())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":    "StreamSense AI Online",
			"snowflake": db != nil,
			"gemini":    os.Getenv("GEMINI_API_KEY") != "",
			"gradient":  os.Getenv("GRADIENT_AI_KEY") != "",
			"rag":       retriever.IsAvailable(),
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

		// 1. RAG: retrieve the most relevant tables for this question.
		schemaCtx, retrievedTables, _ := retriever.RetrieveSchema(req.Question, 3)
		fmt.Printf("📚 RAG: injecting tables: %v\n", retrievedTables)

		// Build messages: system (with retrieved schema) + history + question
		messages := []ChatMessage{
			{Role: "system", Content: buildSystemPrompt(schemaCtx, mode)},
		}

		// Append session history
		history := getHistory(sessionID)
		messages = append(messages, history...)

		// Append current question
		userMsg := ChatMessage{Role: "user", Content: req.Question}
		messages = append(messages, userMsg)
		appendHistory(sessionID, userMsg)

		fmt.Printf("🤖 AI Query [session:%s] [mode:%s]: %s\n", sessionID, mode, req.Question)

		// 2. Call AI (Gemini if GEMINI_API_KEY set, otherwise Gradient AI)
		rawReply, err := callAI(messages)
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
			Question:        req.Question,
			Mode:            mode,
			SessionID:       sessionID,
			RetrievedTables: retrievedTables,
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
