// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nereidai/nereidedge/internal/corecmd"
)

// fakeCoreCommand serves the handful of core-command routes probeInferenceOnce
// touches: PUT Trigger (setStatus) and GET State/LastError/InferLatencyMs.
func fakeCoreCommand(t *testing.T, setStatus int, lastError string) *httptest.Server {
	t.Helper()
	event := func(resource, valueType, value string) []byte {
		b, _ := json.Marshal(map[string]any{
			"apiVersion": "v3",
			"event": map[string]any{
				"readings": []map[string]any{{
					"resourceName": resource,
					"valueType":    valueType,
					"value":        value,
				}},
			},
		})
		return b
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/Trigger") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(setStatus)
			_, _ = w.Write([]byte(`{"apiVersion":"v3","statusCode":503,"message":"request timeout"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/State"):
			_, _ = w.Write(event("State", "String", "APPLIED"))
		case strings.HasSuffix(r.URL.Path, "/LastError"):
			_, _ = w.Write(event("LastError", "String", lastError))
		case strings.HasSuffix(r.URL.Path, "/InferLatencyMs"):
			_, _ = w.Write(event("InferLatencyMs", "Float64", "42.1"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// TestProbeInferenceOnceTriggerFailureUsesJobReason pins the §15.7 fault
// distinction: NI answers a synchronous Trigger with non-200 when the attempt
// failed (e.g. stream acquisition timeout), but its authoritative reason is
// already on LastError. A non-200 Set must therefore surface source-no-frame,
// not a blanket runtime-unreachable.
func TestProbeInferenceOnceTriggerFailureUsesJobReason(t *testing.T) {
	srv := fakeCoreCommand(t, http.StatusServiceUnavailable, "source-no-frame")
	defer srv.Close()

	app := &myApp{inferenceCore: corecmd.New(srv.URL, time.Second)}
	ok, latency, reason := app.probeInferenceOnce(context.Background(), "inference-test")
	if ok || latency != 0 {
		t.Fatalf("ok=%v latency=%v, want false/0", ok, latency)
	}
	if reason != "source-no-frame" {
		t.Fatalf("reason=%q, want source-no-frame from NI LastError", reason)
	}
}

// TestProbeInferenceOnceTriggerFailureNoReason is the genuine unreachable
// case: the Trigger call failed and no DS-side reason is readable.
func TestProbeInferenceOnceTriggerFailureNoReason(t *testing.T) {
	srv := fakeCoreCommand(t, http.StatusServiceUnavailable, "")
	defer srv.Close()

	app := &myApp{inferenceCore: corecmd.New(srv.URL, time.Second)}
	if _, _, reason := app.probeInferenceOnce(context.Background(), "inference-test"); reason != "runtime-unreachable" {
		t.Fatalf("reason=%q, want runtime-unreachable with empty LastError", reason)
	}
}

// TestProbeInferenceOnceSuccess verifies the healthy path still returns the
// measured latency when Trigger is accepted and the job reads APPLIED.
func TestProbeInferenceOnceSuccess(t *testing.T) {
	srv := fakeCoreCommand(t, http.StatusOK, "")
	defer srv.Close()

	app := &myApp{inferenceCore: corecmd.New(srv.URL, time.Second)}
	ok, latency, reason := app.probeInferenceOnce(context.Background(), "inference-test")
	if !ok {
		t.Fatalf("ok=false reason=%q, want healthy verdict", reason)
	}
	if latency != 42.1 {
		t.Fatalf("latency=%v, want 42.1", latency)
	}
}
