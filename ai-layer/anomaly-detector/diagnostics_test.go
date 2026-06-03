package main

import "testing"

func TestComputeSignals_HealthyBaselineAllOK(t *testing.T) {
	snap, _ := DerivedMetricsProvider{}.Snapshot("Europe")
	signals := computeSignals(snap)
	if len(signals) != 6 {
		t.Fatalf("expected 6 signals, got %d", len(signals))
	}
	for _, s := range signals {
		if !s.Healthy {
			t.Errorf("signal %q should be healthy at baseline, got %s", s.Name, s.Status)
		}
	}
}

func TestComputeSignals_PaymentCollapseFlagsCritical(t *testing.T) {
	snap, _ := DerivedMetricsProvider{}.Snapshot("Asia")
	snap.PaymentSuccessRate = 0.40 // huge drop from 0.92 baseline
	signals := computeSignals(snap)

	var found bool
	for _, s := range signals {
		if s.Name == "Payment Gateway Success Rate" {
			found = true
			if s.Healthy || s.Status != "critical" {
				t.Errorf("expected critical payment signal, got %+v", s)
			}
		}
	}
	if !found {
		t.Fatal("payment signal not present")
	}
}

func TestSnapshotForAnomaly_DropProducesDegradation(t *testing.T) {
	a := Anomaly{
		ID:       "anomaly-test-1",
		Region:   "North America",
		ChangePC: -45,
		Current:  600,
		Baseline: 1100,
	}
	signals := computeSignals(snapshotForAnomaly(a))

	degraded := 0
	for _, s := range signals {
		if !s.Healthy {
			degraded++
		}
	}
	if degraded == 0 {
		t.Error("a 45% revenue drop must produce at least one degraded signal")
	}
}

func TestSnapshotForAnomaly_SpikeStaysHealthy(t *testing.T) {
	a := Anomaly{
		ID:       "anomaly-spike-1",
		Region:   "Europe",
		ChangePC: 60,
		Current:  1600,
		Baseline: 1000,
	}
	signals := computeSignals(snapshotForAnomaly(a))
	for _, s := range signals {
		if !s.Healthy {
			t.Errorf("revenue spike should not degrade signal %q (%s)", s.Name, s.Status)
		}
	}
}

func TestConfidenceFromSignals_MoreDegradedHigherConfidence(t *testing.T) {
	low := []DiagnosticSignal{{Status: "ok"}, {Status: "ok"}}
	high := []DiagnosticSignal{{Status: "critical"}, {Status: "degraded"}, {Status: "critical"}}
	if confidenceFromSignals(high) <= confidenceFromSignals(low) {
		t.Error("more severe signals should yield higher confidence")
	}
	if c := confidenceFromSignals(high); c > 0.95 {
		t.Errorf("confidence must be capped at 0.95, got %v", c)
	}
}

func TestDominantCause_StableForSameID(t *testing.T) {
	if dominantCause("anomaly-123") != dominantCause("anomaly-123") {
		t.Error("dominantCause must be deterministic for the same ID")
	}
}
