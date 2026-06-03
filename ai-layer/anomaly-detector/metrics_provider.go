package main

import "math"

// DerivedMetricsProvider synthesizes a metric snapshot from the observed
// revenue anomaly when no Snowflake warehouse is connected. Unlike the previous
// rand-based approach, signals are derived deterministically from the magnitude
// and direction of the revenue change, so the Why Engine reasons over an
// internally consistent picture (a 40% revenue drop produces correspondingly
// degraded payment/order signals, not random noise).
type DerivedMetricsProvider struct{}

func (DerivedMetricsProvider) Snapshot(region string) (MetricSnapshot, error) {
	// Baselines represent a healthy store.
	return MetricSnapshot{
		PaymentSuccessRate:  0.92,
		BaselineSuccessRate: 0.92,
		OrderRate:           12.0,
		BaselineOrderRate:   12.0,
		CartAbandonmentRate: 0.32,
		BaselineAbandonment: 0.32,
		InventoryCoverage:   0.95,
		APILatencyMS:        130,
		BaselineLatencyMS:   120,
		AvgOrderValue:       127,
		BaselineAvgOrderVal: 127,
	}, nil
}

// snapshotForAnomaly projects how operational signals would look given an
// observed revenue change. A larger drop pushes the dominant failure signals
// further into degraded/critical territory. The dominantCause selector makes
// the scenario coherent: a drop is attributed primarily to one subsystem.
func snapshotForAnomaly(a Anomaly) MetricSnapshot {
	base, _ := DerivedMetricsProvider{}.Snapshot(a.Region)

	changeRatio := a.ChangePC / 100.0 // negative for drops
	severity := math.Abs(changeRatio) // 0..~0.6

	if changeRatio >= 0 {
		// Spike: signals are healthy or better; nothing degraded.
		base.OrderRate = base.BaselineOrderRate * (1 + severity)
		base.AvgOrderValue = base.BaselineAvgOrderVal * (1 + severity*0.3)
		return base
	}

	// Revenue drop: pick a dominant cause deterministically from the region
	// hash so demos vary but a given anomaly is reproducible.
	switch dominantCause(a.ID) {
	case 0: // payment failure
		base.PaymentSuccessRate = base.BaselineSuccessRate * (1 - severity*1.4)
		base.APILatencyMS = base.BaselineLatencyMS * (1 + severity*3)
	case 1: // traffic / order collapse
		base.OrderRate = base.BaselineOrderRate * (1 - severity*1.5)
	default: // checkout friction
		base.CartAbandonmentRate = math.Min(0.95, base.BaselineAbandonment*(1+severity*2))
		base.AvgOrderValue = base.BaselineAvgOrderVal * (1 - severity)
	}
	return base
}

// dominantCause maps an anomaly ID to one of three failure modes via a simple
// stable hash, so the same anomaly always yields the same scenario.
func dominantCause(id string) int {
	h := 0
	for i := 0; i < len(id); i++ {
		h = h*31 + int(id[i])
	}
	if h < 0 {
		h = -h
	}
	return h % 3
}
