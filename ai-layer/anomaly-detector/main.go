package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// ─── Types ────────────────────────────────────────────────────────────────────

type DiagnosticSignal struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // "ok" | "degraded" | "critical"
	Value   string `json:"value"`
	Healthy bool   `json:"healthy"`
}

type RootCauseAnalysis struct {
	Hypothesis   string             `json:"hypothesis"`
	Confidence   float64            `json:"confidence"`
	Signals      []DiagnosticSignal `json:"signals"`
	ImpactPerMin float64            `json:"impact_per_min"`
	Recommended  string             `json:"recommended_action"`
}

type Anomaly struct {
	ID          string             `json:"id"`
	Severity    string             `json:"severity"` // "critical" | "warning" | "info"
	Title       string             `json:"title"`
	Description string             `json:"description"`
	Metric      string             `json:"metric"`
	Current     float64            `json:"current"`
	Baseline    float64            `json:"baseline"`
	ChangePC    float64            `json:"change_pct"`
	DetectedAt  time.Time          `json:"detected_at"`
	Region      string             `json:"region,omitempty"`
	RCA         *RootCauseAnalysis `json:"rca,omitempty"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
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

// Alert rule types
type AlertRule struct {
	ID         string     `json:"id"`
	NLDesc     string     `json:"nl_description"`
	GoRule     string     `json:"go_rule"`
	Metric     string     `json:"metric"`
	Operator   string     `json:"operator"`
	Threshold  float64    `json:"threshold"`
	Region     string     `json:"region,omitempty"`
	WebhookURL string     `json:"webhook_url,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	LastFired  *time.Time `json:"last_fired,omitempty"`
}

// ─── State ────────────────────────────────────────────────────────────────────

var (
	anomalyClients   = make(map[chan Anomaly]bool)
	anomalyClientsMu sync.Mutex

	revenueWindow   []float64
	revenueWindowMu sync.Mutex

	alertRules   []AlertRule
	alertRulesMu sync.Mutex
)

// ─── SSE Client Management ────────────────────────────────────────────────────

func registerClient(ch chan Anomaly) {
	anomalyClientsMu.Lock()
	anomalyClients[ch] = true
	anomalyClientsMu.Unlock()
}

func unregisterClient(ch chan Anomaly) {
	anomalyClientsMu.Lock()
	delete(anomalyClients, ch)
	anomalyClientsMu.Unlock()
	close(ch)
}

func broadcastAnomaly(a Anomaly) {
	anomalyClientsMu.Lock()
	defer anomalyClientsMu.Unlock()
	for ch := range anomalyClients {
		select {
		case ch <- a:
		default:
		}
	}
}

// ─── Gradient AI helpers ──────────────────────────────────────────────────────

func callGradient(prompt string, maxTokens int) string {
	apiKey := os.Getenv("GRADIENT_AI_KEY")
	apiBase := os.Getenv("GRADIENT_AI_BASE_URL")
	model := os.Getenv("GRADIENT_AI_MODEL")

	if apiKey == "" {
		return "" // caller will use fallback
	}
	if apiBase == "" {
		apiBase = "https://inference.do-ai.run/v1"
	}
	if model == "" {
		model = "meta-llama/Meta-Llama-3.3-70B-Instruct"
	}

	reqBody := GradientRequest{
		Model:       model,
		Messages:    []ChatMessage{{Role: "user", Content: prompt}},
		Temperature: 0.3,
		MaxTokens:   maxTokens,
	}
	bodyBytes, _ := json.Marshal(reqBody)
	httpReq, _ := http.NewRequest("POST", apiBase+"/chat/completions", bytes.NewBuffer(bodyBytes))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 12 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var gradientResp GradientResponse
	if err := json.Unmarshal(body, &gradientResp); err != nil || len(gradientResp.Choices) == 0 {
		return ""
	}
	return gradientResp.Choices[0].Message.Content
}

// ─── WHY ENGINE — Causal Root Cause Analysis ─────────────────────────────────

func runWhyEngine(a Anomaly) RootCauseAnalysis {
	// Step 1: Compute multi-signal diagnostics from observed metrics.
	// In production these come from Snowflake; here a derived provider projects
	// a coherent metric snapshot from the observed revenue anomaly.
	signals := computeSignals(snapshotForAnomaly(a))

	// Step 2: Build degraded signal summary for AI
	degraded := []string{}
	for _, s := range signals {
		if !s.Healthy {
			degraded = append(degraded, fmt.Sprintf("- %s: %s (status: %s)", s.Name, s.Value, s.Status))
		}
	}

	degradedSummary := "None found"
	if len(degraded) > 0 {
		degradedSummary = strings.Join(degraded, "\n")
	}

	// Step 3: Ask Gradient AI for causal hypothesis
	prompt := fmt.Sprintf(`You are a senior data engineer analyzing an e-commerce revenue anomaly.

Anomaly: %s
Region: %s
Revenue change: %.1f%% (Current: $%.0f, Baseline: $%.0f)
Detected at: %s

Diagnostic signals checked:
%s

Degraded signals found:
%s

Respond ONLY in this exact JSON format:
{
  "hypothesis": "One sentence stating the root cause with high confidence",
  "confidence": 0.87,
  "recommended_action": "Specific immediate action for the ops team",
  "impact_per_min": %.1f
}`,
		a.Title, a.Region, a.ChangePC, a.Current, a.Baseline,
		a.DetectedAt.Format("15:04:05"),
		buildSignalContext(signals),
		degradedSummary,
		(a.Baseline - a.Current),
	)

	aiReply := callGradient(prompt, 300)
	rca := parseRCAResponse(aiReply, a, signals)
	return rca
}

func buildSignalContext(signals []DiagnosticSignal) string {
	lines := []string{}
	for _, s := range signals {
		icon := "✓"
		if !s.Healthy {
			icon = "✗"
		}
		lines = append(lines, fmt.Sprintf("%s %s: %s", icon, s.Name, s.Value))
	}
	return strings.Join(lines, "\n")
}

func parseRCAResponse(reply string, a Anomaly, signals []DiagnosticSignal) RootCauseAnalysis {
	// Strip markdown code fences
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
		Hypothesis        string  `json:"hypothesis"`
		Confidence        float64 `json:"confidence"`
		RecommendedAction string  `json:"recommended_action"`
		ImpactPerMin      float64 `json:"impact_per_min"`
	}

	if err := json.Unmarshal([]byte(cleaned), &parsed); err != nil || parsed.Hypothesis == "" {
		// Fallback: derive from signals
		return fallbackRCA(a, signals)
	}

	return RootCauseAnalysis{
		Hypothesis:   parsed.Hypothesis,
		Confidence:   parsed.Confidence,
		Signals:      signals,
		ImpactPerMin: parsed.ImpactPerMin,
		Recommended:  parsed.RecommendedAction,
	}
}

func fallbackRCA(a Anomaly, signals []DiagnosticSignal) RootCauseAnalysis {
	// Find worst signal for fallback hypothesis
	worst := "Unknown system degradation"
	for _, s := range signals {
		if !s.Healthy && s.Status == "critical" {
			worst = s.Name
			break
		}
	}
	return RootCauseAnalysis{
		Hypothesis:   fmt.Sprintf("%s detected as primary failure point in %s.", worst, a.Region),
		Confidence:   confidenceFromSignals(signals),
		Signals:      signals,
		ImpactPerMin: a.Baseline - a.Current,
		Recommended:  "Check ops dashboards and payment provider status pages immediately.",
	}
}

// ─── Anomaly Detection Engine ─────────────────────────────────────────────────

func runDetector() {
	regions := []string{"North America", "Europe", "Asia", "South America"}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	revenueWindowMu.Lock()
	for i := 0; i < 5; i++ {
		revenueWindow = append(revenueWindow, 1000+rand.Float64()*500)
	}
	revenueWindowMu.Unlock()

	for range ticker.C {
		newRevenue := 1000 + rand.Float64()*500
		scenario := rand.Float64()
		var anomaly *Anomaly

		revenueWindowMu.Lock()
		baseline := average(revenueWindow)

		if scenario < 0.12 {
			newRevenue = baseline * (0.45 + rand.Float64()*0.25)
			revenueWindow = append(revenueWindow, newRevenue)
			if len(revenueWindow) > 10 {
				revenueWindow = revenueWindow[1:]
			}
			revenueWindowMu.Unlock()

			region := regions[rand.Intn(len(regions))]
			changePct := ((newRevenue - baseline) / baseline) * 100
			severity := "warning"
			if changePct < -30 {
				severity = "critical"
			}

			a := Anomaly{
				ID:          fmt.Sprintf("anomaly-%d", time.Now().UnixMilli()),
				Severity:    severity,
				Title:       fmt.Sprintf("Revenue Drop Detected — %s", region),
				Description: "Investigating root cause...",
				Metric:      "revenue_per_minute",
				Current:     newRevenue,
				Baseline:    baseline,
				ChangePC:    changePct,
				DetectedAt:  time.Now(),
				Region:      region,
			}
			anomaly = &a

		} else if scenario < 0.18 {
			newRevenue = baseline * (1.5 + rand.Float64()*0.5)
			revenueWindow = append(revenueWindow, newRevenue)
			if len(revenueWindow) > 10 {
				revenueWindow = revenueWindow[1:]
			}
			revenueWindowMu.Unlock()

			region := regions[rand.Intn(len(regions))]
			changePct := ((newRevenue - baseline) / baseline) * 100
			a := Anomaly{
				ID:          fmt.Sprintf("anomaly-%d", time.Now().UnixMilli()),
				Severity:    "info",
				Title:       fmt.Sprintf("🚀 Revenue Spike — %s", region),
				Description: "Positive anomaly detected. Investigating cause...",
				Metric:      "revenue_per_minute",
				Current:     newRevenue,
				Baseline:    baseline,
				ChangePC:    changePct,
				DetectedAt:  time.Now(),
				Region:      region,
			}
			anomaly = &a
		} else {
			revenueWindow = append(revenueWindow, newRevenue)
			if len(revenueWindow) > 10 {
				revenueWindow = revenueWindow[1:]
			}
			revenueWindowMu.Unlock()
		}

		if anomaly != nil {
			log.Printf("🚨 Anomaly: %s (%.1f%%)", anomaly.Title, anomaly.ChangePC)
			go func(a Anomaly) {
				// Run Why Engine concurrently
				rca := runWhyEngine(a)
				a.RCA = &rca
				a.Description = rca.Hypothesis
				broadcastAnomaly(a)
			}(*anomaly)
		}

		// Check custom alert rules
		go checkAlertRules(newRevenue)
	}
}

// ─── Alert Rule Engine ────────────────────────────────────────────────────────

func checkAlertRules(currentRevenue float64) {
	alertRulesMu.Lock()
	rules := make([]AlertRule, len(alertRules))
	copy(rules, alertRules)
	alertRulesMu.Unlock()

	revenueWindowMu.Lock()
	baseline := average(revenueWindow)
	revenueWindowMu.Unlock()

	for i := range rules {
		rule := &rules[i]
		triggered := false

		switch rule.Operator {
		case "lt":
			triggered = currentRevenue < rule.Threshold
		case "gt":
			triggered = currentRevenue > rule.Threshold
		case "drop_pct":
			if baseline > 0 {
				changePct := ((currentRevenue - baseline) / baseline) * 100
				triggered = changePct < -rule.Threshold
			}
		}

		if triggered {
			// Debounce: don't fire more than once per 5 minutes
			if rule.LastFired != nil && time.Since(*rule.LastFired) < 5*time.Minute {
				continue
			}

			now := time.Now()
			alertRulesMu.Lock()
			for j := range alertRules {
				if alertRules[j].ID == rule.ID {
					alertRules[j].LastFired = &now
				}
			}
			alertRulesMu.Unlock()

			log.Printf("🔔 Alert fired: %s", rule.NLDesc)

			// Broadcast as anomaly
			a := Anomaly{
				ID:          fmt.Sprintf("alert-%d", now.UnixMilli()),
				Severity:    "warning",
				Title:       fmt.Sprintf("🔔 Custom Alert: %s", rule.NLDesc),
				Description: fmt.Sprintf("Rule triggered. Revenue: $%.0f, Threshold: $%.0f", currentRevenue, rule.Threshold),
				Metric:      rule.Metric,
				Current:     currentRevenue,
				Baseline:    baseline,
				ChangePC:    ((currentRevenue - baseline) / baseline) * 100,
				DetectedAt:  now,
			}
			broadcastAnomaly(a)

			// Fire webhook if configured
			if rule.WebhookURL != "" {
				go fireWebhook(rule.WebhookURL, a)
			}
		}
	}
}

func fireWebhook(url string, a Anomaly) {
	body, _ := json.Marshal(map[string]interface{}{
		"text": fmt.Sprintf("🚨 StreamSense Alert: %s\n%s\nChange: %.1f%%", a.Title, a.Description, a.ChangePC),
	})
	http.Post(url, "application/json", bytes.NewBuffer(body))
}

// ─── NL Alert Builder endpoint ────────────────────────────────────────────────

func parseAlertFromNL(description string) AlertRule {
	prompt := fmt.Sprintf(`You are an alert rule parser for a real-time e-commerce analytics system.
Convert this natural language alert description into a structured rule.

Description: "%s"

Available metrics: revenue_per_minute, order_count, conversion_rate, cart_abandonment
Available operators: "lt" (less than), "gt" (greater than), "drop_pct" (percent drop from baseline)

Respond ONLY in this exact JSON:
{
  "metric": "revenue_per_minute",
  "operator": "drop_pct",
  "threshold": 30.0,
  "region": "Europe",
  "go_rule": "if changePct < -30 && region == 'Europe' { fire() }"
}`, description)

	reply := callGradient(prompt, 200)

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
		Metric    string  `json:"metric"`
		Operator  string  `json:"operator"`
		Threshold float64 `json:"threshold"`
		Region    string  `json:"region"`
		GoRule    string  `json:"go_rule"`
	}

	if err := json.Unmarshal([]byte(cleaned), &parsed); err != nil {
		// Fallback basic rule
		return AlertRule{
			ID:        fmt.Sprintf("rule-%d", time.Now().UnixMilli()),
			NLDesc:    description,
			Metric:    "revenue_per_minute",
			Operator:  "drop_pct",
			Threshold: 30.0,
			GoRule:    "revenue drop > 30% from baseline",
			CreatedAt: time.Now(),
		}
	}

	return AlertRule{
		ID:        fmt.Sprintf("rule-%d", time.Now().UnixMilli()),
		NLDesc:    description,
		Metric:    parsed.Metric,
		Operator:  parsed.Operator,
		Threshold: parsed.Threshold,
		Region:    parsed.Region,
		GoRule:    parsed.GoRule,
		CreatedAt: time.Now(),
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func average(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	go runDetector()

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.Use(cors.Default())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":   "StreamSense Anomaly Detector + Why Engine Online",
			"gradient": os.Getenv("GRADIENT_AI_KEY") != "",
		})
	})

	// SSE: live anomaly stream with RCA
	r.GET("/anomalies", func(c *gin.Context) {
		ch := make(chan Anomaly, 5)
		registerClient(ch)

		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")

		c.Stream(func(w io.Writer) bool {
			if a, ok := <-ch; ok {
				data, _ := json.Marshal(a)
				c.SSEvent("anomaly", string(data))
				return true
			}
			return false
		})

		c.Request.Context().Done()
		unregisterClient(ch)
	})

	// List alert rules
	r.GET("/alerts", func(c *gin.Context) {
		alertRulesMu.Lock()
		defer alertRulesMu.Unlock()
		c.JSON(200, alertRules)
	})

	// Create alert from natural language
	r.POST("/alerts", func(c *gin.Context) {
		var req struct {
			Description string `json:"description" binding:"required"`
			WebhookURL  string `json:"webhook_url"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		rule := parseAlertFromNL(req.Description)
		rule.WebhookURL = req.WebhookURL

		alertRulesMu.Lock()
		alertRules = append(alertRules, rule)
		alertRulesMu.Unlock()

		log.Printf("🔔 New alert rule created: %s → %s %s %.0f",
			rule.NLDesc, rule.Metric, rule.Operator, rule.Threshold)
		c.JSON(201, rule)
	})

	// Delete alert rule
	r.DELETE("/alerts/:id", func(c *gin.Context) {
		id := c.Param("id")
		alertRulesMu.Lock()
		defer alertRulesMu.Unlock()
		for i, r := range alertRules {
			if r.ID == id {
				alertRules = append(alertRules[:i], alertRules[i+1:]...)
				c.JSON(200, gin.H{"deleted": id})
				return
			}
		}
		c.JSON(404, gin.H{"error": "rule not found"})
	})

	port := os.Getenv("ANOMALY_PORT")
	if port == "" {
		port = "8086"
	}
	fmt.Printf("🚨 StreamSense | Why Engine + Alert Builder running on :%s\n", port)
	r.Run(":" + port)
}
