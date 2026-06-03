package main

import (
	"fmt"
	"math"
)

// ─── Diagnostic Signal Computation ────────────────────────────────────────────
//
// The Why Engine reasons over six operational signals. This file computes those
// signals deterministically from observed metrics rather than fabricating them
// with random numbers. A MetricsProvider supplies the raw measurements; in
// production it is backed by Snowflake queries, but any provider that returns a
// MetricSnapshot works (including the live in-memory revenue window).

// MetricSnapshot is the set of raw operational measurements the Why Engine
// evaluates. Values are absolute observations for the current window plus the
// rolling baseline used to detect degradation.
type MetricSnapshot struct {
	PaymentSuccessRate   float64 // 0..1
	BaselineSuccessRate  float64 // 0..1
	OrderRate            float64 // orders/min current
	BaselineOrderRate    float64 // orders/min baseline
	CartAbandonmentRate  float64 // 0..1 current
	BaselineAbandonment  float64 // 0..1 baseline
	InventoryCoverage    float64 // 0..1 (fraction of SKUs in stock)
	APILatencyMS         float64 // current p95 latency
	BaselineLatencyMS    float64 // baseline p95 latency
	AvgOrderValue        float64 // current AOV
	BaselineAvgOrderVal  float64 // baseline AOV
}

// MetricsProvider returns the current metric snapshot for a region. Backed by
// Snowflake in production; the detector falls back to a derived provider when
// no warehouse is configured.
type MetricsProvider interface {
	Snapshot(region string) (MetricSnapshot, error)
}

// computeSignals turns raw measurements into graded diagnostic signals using
// explicit, explainable thresholds. This is the deterministic core that
// replaces the previous rand.Float32() simulation.
func computeSignals(m MetricSnapshot) []DiagnosticSignal {
	signals := []DiagnosticSignal{
		gradeRatioDrop(
			"Payment Gateway Success Rate",
			m.PaymentSuccessRate, m.BaselineSuccessRate,
			0.05, 0.15, // degraded if >5% relative drop, critical if >15%
			pct(m.PaymentSuccessRate),
		),
		gradeRatioDrop(
			"Order Placement Rate",
			m.OrderRate, m.BaselineOrderRate,
			0.20, 0.45,
			fmt.Sprintf("%.1f/min", m.OrderRate),
		),
		gradeRatioRise(
			"Cart Abandonment Rate",
			m.CartAbandonmentRate, m.BaselineAbandonment,
			0.15, 0.40,
			pct(m.CartAbandonmentRate),
		),
		gradeFloor(
			"Inventory Levels",
			m.InventoryCoverage,
			0.85, 0.60, // degraded below 85% coverage, critical below 60%
			pct(m.InventoryCoverage),
		),
		gradeRatioRise(
			"API Response Time",
			m.APILatencyMS, m.BaselineLatencyMS,
			0.50, 2.0, // degraded if 50% slower, critical if 3x slower
			fmt.Sprintf("%.0fms", m.APILatencyMS),
		),
		gradeRatioDrop(
			"Average Order Value",
			m.AvgOrderValue, m.BaselineAvgOrderVal,
			0.15, 0.35,
			fmt.Sprintf("$%.0f", m.AvgOrderValue),
		),
	}
	return signals
}

// gradeRatioDrop flags a signal when `current` falls below `baseline` by more
// than the degraded/critical relative thresholds (e.g. payment success rate).
func gradeRatioDrop(name string, current, baseline, degraded, critical float64, display string) DiagnosticSignal {
	if baseline <= 0 {
		return healthy(name, display)
	}
	drop := (baseline - current) / baseline
	switch {
	case drop >= critical:
		return DiagnosticSignal{Name: name, Status: "critical", Value: display, Healthy: false}
	case drop >= degraded:
		return DiagnosticSignal{Name: name, Status: "degraded", Value: display, Healthy: false}
	default:
		return healthy(name, display)
	}
}

// gradeRatioRise flags a signal when `current` rises above `baseline` by more
// than the thresholds (e.g. latency, cart abandonment).
func gradeRatioRise(name string, current, baseline, degraded, critical float64, display string) DiagnosticSignal {
	if baseline <= 0 {
		return healthy(name, display)
	}
	rise := (current - baseline) / baseline
	switch {
	case rise >= critical:
		return DiagnosticSignal{Name: name, Status: "critical", Value: display, Healthy: false}
	case rise >= degraded:
		return DiagnosticSignal{Name: name, Status: "degraded", Value: display, Healthy: false}
	default:
		return healthy(name, display)
	}
}

// gradeFloor flags a signal when an absolute coverage value drops below floors.
func gradeFloor(name string, value, degradedFloor, criticalFloor float64, display string) DiagnosticSignal {
	switch {
	case value < criticalFloor:
		return DiagnosticSignal{Name: name, Status: "critical", Value: display, Healthy: false}
	case value < degradedFloor:
		return DiagnosticSignal{Name: name, Status: "degraded", Value: display, Healthy: false}
	default:
		return healthy(name, display)
	}
}

func healthy(name, display string) DiagnosticSignal {
	return DiagnosticSignal{Name: name, Status: "ok", Value: display, Healthy: true}
}

func pct(v float64) string {
	return fmt.Sprintf("%.0f%%", math.Round(v*100))
}

// confidenceFromSignals derives a fallback confidence score from how many
// signals are degraded and how severe they are — used when the LLM is offline.
func confidenceFromSignals(signals []DiagnosticSignal) float64 {
	if len(signals) == 0 {
		return 0
	}
	weight := 0.0
	for _, s := range signals {
		switch s.Status {
		case "critical":
			weight += 1.0
		case "degraded":
			weight += 0.5
		}
	}
	// More corroborating degraded signals => higher confidence in the RCA.
	conf := 0.55 + 0.12*weight
	if conf > 0.95 {
		conf = 0.95
	}
	return conf
}
