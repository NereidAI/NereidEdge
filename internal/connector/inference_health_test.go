// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"errors"
	"testing"
	"time"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func TestInferenceHealthReportsOptionalJobStats(t *testing.T) {
	a, _, rec := newInferenceApplier(t)
	reader := &fakeInferenceReader{
		state: device.ConnectorStateApplied,
		metrics: map[string]any{
			"SuccessTotal": int64(18234),
			"ErrorTotal":   int64(12),
			"P50Ms":        float64(47.5),
			"P95Ms":        float64(88.25),
		},
		metricFailures: make(map[string]bool),
	}
	a.inferenceCommands = reader
	a.mu.Lock()
	a.applied["spec-health"] = appliedSpec{
		revision: 3, deviceID: infChildUUID, deviceName: "inference-spec-health", connectorType: TypeEdgeInference,
	}
	// The lifecycle state is already APPLIED. A metrics-only change must still
	// produce a status frame on the polling cadence.
	a.inferenceState["spec-health"] = device.ConnectorStateApplied + "\x00"
	a.mu.Unlock()
	rawStatuses := make(chan []byte, 4)
	a.reportRawStatus = func(_ context.Context, raw []byte) error {
		rawStatuses <- append([]byte(nil), raw...)
		return nil
	}

	a.pollInferenceStatus(context.Background())
	frame := waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if frame.Revision != 3 || frame.Stats == nil {
		t.Fatalf("health frame=%+v", frame)
	}
	if frame.Stats.SuccessTotal == nil || *frame.Stats.SuccessTotal != 18234 ||
		frame.Stats.ErrorTotal == nil || *frame.Stats.ErrorTotal != 12 ||
		frame.Stats.P50MS == nil || *frame.Stats.P50MS != 47.5 ||
		frame.Stats.P95MS == nil || *frame.Stats.P95MS != 88.25 {
		t.Fatalf("job stats=%+v", frame.Stats)
	}

	// No state or metric delta means no duplicate frame.
	a.pollInferenceStatus(context.Background())
	select {
	case raw := <-rawStatuses:
		t.Fatalf("unchanged health produced duplicate frame: %s", raw)
	case <-time.After(30 * time.Millisecond):
	}

	reader.metrics["SuccessTotal"] = int64(18235)
	a.pollInferenceStatus(context.Background())
	frame = waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if *frame.Stats.SuccessTotal != 18235 {
		t.Fatalf("updated success_total=%v", *frame.Stats.SuccessTotal)
	}

	// A later unavailable sample is omitted, not persisted as a stale value or
	// rewritten to zero. Without optional fields it uses the legacy typed path.
	for _, command := range []string{"SuccessTotal", "ErrorTotal", "P50Ms", "P95Ms"} {
		reader.metricFailures[command] = true
	}
	a.pollInferenceStatus(context.Background())
	legacy := rec.waitFor(t, device.ConnectorStateApplied, "")
	if legacy.State != device.ConnectorStateApplied {
		t.Fatalf("legacy health status=%+v", legacy)
	}
	_, _, stats := a.readInferenceHealth(context.Background(), "inference-spec-health")
	if stats != nil {
		t.Fatalf("unavailable metrics must be omitted, got %+v", stats)
	}
}

func TestInferenceHealthPublicationFailureRetriesUnchangedSample(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	reader := &fakeInferenceReader{
		state: device.ConnectorStateApplied,
		metrics: map[string]any{
			"SuccessTotal": int64(4), "ErrorTotal": int64(0),
			"P50Ms": float64(3), "P95Ms": float64(5),
		},
	}
	a.inferenceCommands = reader
	a.mu.Lock()
	a.applied["spec-health-retry"] = appliedSpec{
		revision: 1, deviceID: infChildUUID, deviceName: "inference-spec-health-retry",
		connectorType: TypeEdgeInference,
	}
	a.inferenceState["spec-health-retry"] = device.ConnectorStateCreating + "\x00"
	a.mu.Unlock()
	rawStatuses := make(chan []byte, 2)
	attempts := 0
	a.reportRawStatus = func(_ context.Context, raw []byte) error {
		attempts++
		if attempts == 1 {
			return errors.New("health status publish failed")
		}
		rawStatuses <- raw
		return nil
	}
	a.pollInferenceStatus(context.Background())
	a.pollInferenceStatus(context.Background())
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if attempts != 2 {
		t.Fatalf("health publish attempts=%d, want failure plus retry", attempts)
	}
}

func TestInferenceHealthOmitsLatencyUntilFirstSuccess(t *testing.T) {
	tests := []struct {
		name       string
		errorTotal int64
	}{
		{name: "startup", errorTotal: 0},
		{name: "errors only", errorTotal: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, _, _ := newInferenceApplier(t)
			reader := &fakeInferenceReader{
				state: device.ConnectorStateApplied,
				metrics: map[string]any{
					"SuccessTotal": int64(0),
					"ErrorTotal":   tt.errorTotal,
					"P50Ms":        float64(0),
					"P95Ms":        float64(0),
				},
			}
			a.inferenceCommands = reader

			_, _, stats := a.readInferenceHealth(context.Background(), "inference-job")
			if stats == nil || stats.SuccessTotal == nil || *stats.SuccessTotal != 0 ||
				stats.ErrorTotal == nil || *stats.ErrorTotal != uint64(tt.errorTotal) {
				t.Fatalf("zero-valued counters must remain present: %+v", stats)
			}
			if stats.P50MS != nil || stats.P95MS != nil {
				t.Fatalf("latency before the first success must be omitted: %+v", stats)
			}
		})
	}
}

func TestInferenceCounterRejectsOutOfRangeFloat(t *testing.T) {
	if got := inferenceCounter(float64(1 << 64)); got != nil {
		t.Fatalf("out-of-range counter=%d, want unavailable", *got)
	}
}

func TestInferenceHealthOmitsOnlyInvalidMetric(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	reader := &fakeInferenceReader{
		state: device.ConnectorStateApplied,
		metrics: map[string]any{
			"SuccessTotal": int64(9), "ErrorTotal": int64(-1),
			"P50Ms": float64(4.5), "P95Ms": "not-a-number",
		},
	}
	a.inferenceCommands = reader
	_, _, stats := a.readInferenceHealth(context.Background(), "inference-job")
	if stats == nil || stats.SuccessTotal == nil || *stats.SuccessTotal != 9 || stats.P50MS == nil {
		t.Fatalf("valid metrics were dropped: %+v", stats)
	}
	if stats.ErrorTotal != nil || stats.P95MS != nil {
		t.Fatalf("invalid metrics must be omitted independently: %+v", stats)
	}
}
