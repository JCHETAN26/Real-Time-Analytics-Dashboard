package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// ─── Types ────────────────────────────────────────────────────────────────────

type RevenueSample struct {
	Time    time.Time `json:"time"`
	Revenue float64   `json:"revenue"`
	Orders  int       `json:"orders"`
}

type ForecastPoint struct {
	Label    string  `json:"label"`
	Actual   float64 `json:"actual,omitempty"`
	Forecast float64 `json:"forecast,omitempty"`
	Upper    float64 `json:"upper,omitempty"`
	Lower    float64 `json:"lower,omitempty"`
}

type Forecast struct {
	CurrentVelocity  float64         `json:"current_velocity"`    // $/min right now
	ForecastNext30   float64         `json:"forecast_next_30"`    // $ in next 30 min
	ForecastEndOfDay float64         `json:"forecast_end_of_day"` // $ by end of day
	DailyTarget      float64         `json:"daily_target"`
	TrajectoryGap    float64         `json:"trajectory_gap"` // negative = below target
	RiskScore        float64         `json:"risk_score"`     // 0.0 - 1.0
	Trend            string          `json:"trend"`          // "accelerating" | "stable" | "decelerating"
	Points           []ForecastPoint `json:"points"`
	UpdatedAt        time.Time       `json:"updated_at"`
	AIRecommendation string          `json:"ai_recommendation"`
}

// ─── State ────────────────────────────────────────────────────────────────────

var (
	samples   []RevenueSample
	samplesMu sync.Mutex

	forecastClients   = make(map[chan Forecast]bool)
	forecastClientsMu sync.Mutex

	dailyTarget = 1_200_000.0 // $1.2M daily target
)

// ─── SSE Client Management ────────────────────────────────────────────────────

func registerForecastClient(ch chan Forecast) {
	forecastClientsMu.Lock()
	forecastClients[ch] = true
	forecastClientsMu.Unlock()
}

func unregisterForecastClient(ch chan Forecast) {
	forecastClientsMu.Lock()
	delete(forecastClients, ch)
	forecastClientsMu.Unlock()
	close(ch)
}

func broadcastForecast(f Forecast) {
	forecastClientsMu.Lock()
	defer forecastClientsMu.Unlock()
	for ch := range forecastClients {
		select {
		case ch <- f:
		default:
		}
	}
}

// ─── Data Ingestor (Simulates Kafka consumer feeding revenue) ─────────────────

func runSampler() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Seed history: simulate last 60 minutes
	now := time.Now()
	for i := 59; i >= 0; i-- {
		t := now.Add(-time.Duration(i) * time.Minute)
		// Add time-of-day shape
		hour := t.Hour()
		base := 800.0 + float64(hour)*30 + rand.Float64()*200

		samplesMu.Lock()
		samples = append(samples, RevenueSample{
			Time:    t,
			Revenue: base,
			Orders:  int(base / 25),
		})
		samplesMu.Unlock()
	}

	for range ticker.C {
		hour := time.Now().Hour()
		base := 800.0 + float64(hour)*30 + rand.Float64()*300
		// Occasionally dip
		if rand.Float64() < 0.08 {
			base *= 0.5
		} else if rand.Float64() < 0.05 {
			base *= 1.6
		}

		s := RevenueSample{
			Time:    time.Now(),
			Revenue: base,
			Orders:  int(base / 25),
		}
		samplesMu.Lock()
		samples = append(samples, s)
		if len(samples) > 120 { // Keep 2hr window
			samples = samples[1:]
		}
		samplesMu.Unlock()

		forecast := computeForecast()
		broadcastForecast(forecast)
	}
}

// ─── Forecast Engine ──────────────────────────────────────────────────────────

func computeForecast() Forecast {
	samplesMu.Lock()
	snap := make([]RevenueSample, len(samples))
	copy(snap, samples)
	samplesMu.Unlock()

	if len(snap) < 5 {
		return Forecast{UpdatedAt: time.Now()}
	}

	// 1. Current velocity = avg of last 5 samples
	last5 := snap[len(snap)-5:]
	velocity := 0.0
	for _, s := range last5 {
		velocity += s.Revenue
	}
	velocity /= float64(len(last5))

	// 2. Trend: compare last 5 vs prev 5
	trend := "stable"
	if len(snap) >= 10 {
		prev5 := snap[len(snap)-10 : len(snap)-5]
		prevAvg := 0.0
		for _, s := range prev5 {
			prevAvg += s.Revenue
		}
		prevAvg /= 5
		ratio := velocity / prevAvg
		if ratio > 1.08 {
			trend = "accelerating"
		} else if ratio < 0.92 {
			trend = "decelerating"
		}
	}

	// 3. Simple linear regression for forecast
	n := float64(len(snap))
	sumX, sumY, sumXY, sumX2 := 0.0, 0.0, 0.0, 0.0
	for i, s := range snap {
		x := float64(i)
		y := s.Revenue
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}
	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)
	intercept := (sumY - slope*sumX) / n

	// Std dev for confidence bands
	variance := 0.0
	for i, s := range snap {
		pred := slope*float64(i) + intercept
		variance += (s.Revenue - pred) * (s.Revenue - pred)
	}
	stddev := math.Sqrt(variance / n)

	// 4. Build chart points: last 30 actuals + 30 forecast
	now := time.Now()
	points := []ForecastPoint{}

	// Actual points (last 30)
	recent := snap
	if len(recent) > 30 {
		recent = recent[len(recent)-30:]
	}
	for i, s := range recent {
		points = append(points, ForecastPoint{
			Label:  s.Time.Format("15:04"),
			Actual: s.Revenue,
			// Show 1-sigma band on actuals too
			Upper: s.Revenue + stddev*0.5,
			Lower: math.Max(0, s.Revenue-stddev*0.5),
		})
		_ = i
	}

	// Forecast next 30 minutes
	baseIdx := float64(len(snap) - 1)
	for i := 1; i <= 30; i++ {
		fi := baseIdx + float64(i)
		fValue := slope*fi + intercept
		if fValue < 0 {
			fValue = 0
		}
		futureTime := now.Add(time.Duration(i) * time.Minute)
		points = append(points, ForecastPoint{
			Label:    futureTime.Format("15:04"),
			Forecast: fValue,
			Upper:    fValue + stddev*1.2,
			Lower:    math.Max(0, fValue-stddev*1.2),
		})
	}

	// 5. End-of-day calculation
	minutesLeft := float64((24-now.Hour())*60 - now.Minute())
	todayRevenue := 0.0
	for _, s := range snap {
		if s.Time.Day() == now.Day() {
			todayRevenue += s.Revenue
		}
	}
	endOfDayForecast := todayRevenue + velocity*minutesLeft

	// 6. Risk score (0=safe, 1=critical)
	gap := endOfDayForecast - dailyTarget
	riskScore := 0.0
	if gap < 0 {
		riskScore = math.Min(1.0, math.Abs(gap)/dailyTarget*3)
	}
	if trend == "decelerating" {
		riskScore = math.Min(1.0, riskScore+0.2)
	}

	// 7. AI recommendation
	rec := generateRecommendation(velocity, gap, trend, riskScore)

	return Forecast{
		CurrentVelocity:  velocity,
		ForecastNext30:   velocity * 30,
		ForecastEndOfDay: endOfDayForecast,
		DailyTarget:      dailyTarget,
		TrajectoryGap:    gap,
		RiskScore:        riskScore,
		Trend:            trend,
		Points:           points,
		UpdatedAt:        now,
		AIRecommendation: rec,
	}
}

func generateRecommendation(velocity, gap float64, trend string, risk float64) string {
	if risk < 0.2 && trend == "accelerating" {
		return "📈 Revenue trajectory is strong. You're on track to exceed your daily target. Consider scaling ad spend to capture peak momentum."
	}
	if risk > 0.6 {
		shortfall := -gap
		return fmt.Sprintf("🔴 High risk. At current velocity you'll miss your daily target by $%.0f. Activate flash promotions or mobile discount codes immediately.", shortfall)
	}
	if trend == "decelerating" {
		return fmt.Sprintf("⚠️  Revenue velocity is decelerating. Current: $%.0f/min, forecasting $%.0f end-of-day. Consider email retargeting for cart abandoners.", velocity, velocity*float64((24-time.Now().Hour())*60))
	}
	return fmt.Sprintf("✅ Revenue running at $%.0f/minute. Forecast stable — no interventions needed.", velocity)
}

// ─── HTTP Server ──────────────────────────────────────────────────────────────

func main() {
	go runSampler()

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.Use(cors.Default())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "StreamSense Forecast Engine Online"})
	})

	// Snapshot: get current forecast
	r.GET("/forecast", func(c *gin.Context) {
		f := computeForecast()
		c.JSON(200, f)
	})

	// SSE: live forecast updates every 10s
	r.GET("/forecast/stream", func(c *gin.Context) {
		ch := make(chan Forecast, 2)
		registerForecastClient(ch)

		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")

		// Send current immediately
		snapshot := computeForecast()
		data, _ := json.Marshal(snapshot)
		fmt.Fprintf(c.Writer, "event: forecast\ndata: %s\n\n", data)
		c.Writer.Flush()

		c.Stream(func(w io.Writer) bool {
			if f, ok := <-ch; ok {
				data, _ := json.Marshal(f)
				c.SSEvent("forecast", string(data))
				return true
			}
			return false
		})

		c.Request.Context().Done()
		unregisterForecastClient(ch)
	})

	port := os.Getenv("FORECAST_PORT")
	if port == "" {
		port = "8087"
	}
	fmt.Printf("📈 StreamSense | Forecast Engine running on :%s\n", port)
	r.Run(":" + port)
}
