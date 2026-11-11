// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestHelloInferenceRoundTrip verifies the optional inference capability
// block survives a hello/ack JSON round trip, keeps backend order, and is
// omitted when nil (older peers must not see the key).
func TestHelloInferenceRoundTrip(t *testing.T) {
	hello := Hello{
		Inference: &InferenceAdvert{
			Contract: InferenceContractModel1,
			Backends: []string{"kserve", "openai"},
		},
	}
	raw, err := json.Marshal(hello)
	if err != nil {
		t.Fatalf("encode hello: %v", err)
	}
	if !strings.Contains(string(raw),
		`"inference":{"contract":"model/1","backends":["kserve","openai"]}`) {
		t.Fatalf("hello inference block missing/out of order: %s", raw)
	}
	var back Hello
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	if back.Inference == nil || back.Inference.Contract != InferenceContractModel1 ||
		len(back.Inference.Backends) != 2 || back.Inference.Backends[0] != "kserve" {
		t.Fatalf("hello inference round trip: %+v", back.Inference)
	}

	// A nil pointer on either frame must leave the wire untouched.
	if raw, err := json.Marshal(Hello{}); err != nil || strings.Contains(string(raw), "inference") {
		t.Fatalf("empty hello must omit inference: %s %v", raw, err)
	}
	if raw, err := json.Marshal(HelloAck{}); err != nil || strings.Contains(string(raw), "inference") {
		t.Fatalf("empty ack must omit inference: %s %v", raw, err)
	}
}

// TestManifestModelsSegmentDecode mirrors the ADR-0039 manifest example.
func TestManifestModelsSegmentDecode(t *testing.T) {
	raw := `{
	  "devices": ["dev-a"],
	  "models": [
	    {
	      "id": "yiqisoft-yolov8n",
	      "version": "1.0.0",
	      "revision": "76937",
	      "task": "detection",
	      "family": "yolov8",
	      "backend": "kserve",
	      "model_name": "yolov8n",
	      "state": "READY",
	      "reason": "",
	      "labels": ["person"],
	      "active_bindings": 1
	    }
	  ],
	  "revision": 3
	}`
	var frame Manifest
	if err := json.Unmarshal([]byte(raw), &frame); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if frame.Models == nil || len(*frame.Models) != 1 {
		t.Fatalf("models segment: %+v", frame.Models)
	}
	m := (*frame.Models)[0]
	if m.ID != "yiqisoft-yolov8n" || m.Version != "1.0.0" || m.Revision != "76937" ||
		m.Task != "detection" || m.Family != "yolov8" || m.Backend != "kserve" ||
		m.ModelName != "yolov8n" || m.State != ModelStateReady ||
		len(m.Labels) != 1 || m.Labels[0] != "person" || m.ActiveBindings != 1 {
		t.Fatalf("model entry: %+v", m)
	}
}

// TestManifestModelsOmission distinguishes an absent segment (older edge:
// keep stored inventory) from an explicit empty list (zero inventory, the
// cloud must prune every row).
func TestManifestModelsOmission(t *testing.T) {
	// Absent: the key must not be on the wire and decode to a nil pointer.
	var absent Manifest
	if err := json.Unmarshal([]byte(`{"devices":[]}`), &absent); err != nil {
		t.Fatalf("decode absent: %v", err)
	}
	if absent.Models != nil {
		t.Fatalf("absent models must decode to nil, got %+v", *absent.Models)
	}
	if raw, err := json.Marshal(Manifest{Devices: []string{}}); err != nil ||
		strings.Contains(string(raw), "models") {
		t.Fatalf("nil segment must be omitted: %s %v", raw, err)
	}

	// Explicit empty: marshals "models":[] and decodes to a non-nil pointer.
	empty := []ManifestModel{}
	encoded, err := json.Marshal(Manifest{Devices: []string{}, Models: &empty})
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}
	if !strings.Contains(string(encoded), `"models":[]`) {
		t.Fatalf("explicit empty list missing: %s", encoded)
	}
	var back Manifest
	if err := json.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if back.Models == nil || len(*back.Models) != 0 {
		t.Fatalf("explicit empty list must decode to non-nil zero slice: %+v", back.Models)
	}
}

// TestManifestModelsRoundTrip covers a failed model with a reason and
// confirms a successful model carries no reason key.
func TestManifestModelsRoundTrip(t *testing.T) {
	frame := Manifest{
		Devices: []string{"d"},
		Models: &[]ManifestModel{
			{ID: "m1", Version: "1.0.0", Task: "detection", Backend: "openai",
				State: ModelStateReady},
			{ID: "m2", Version: "2.0.0", Revision: "9", Task: "classification",
				Backend: "kserve", State: ModelStateFailed, Reason: "package-invalid",
				Labels: []string{"a", "b"}, ActiveBindings: 2},
		},
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var back Manifest
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.Models == nil || len(*back.Models) != 2 {
		t.Fatalf("round trip: %+v", back.Models)
	}
	if (*back.Models)[1].Reason != "package-invalid" ||
		(*back.Models)[1].ActiveBindings != 2 {
		t.Fatalf("failed model: %+v", (*back.Models)[1])
	}
	if strings.Contains(string(raw), `"reason":""`) {
		t.Fatalf("empty reason must be omitted: %s", raw)
	}
}

// TestInferenceConstantsAreVerbatim pins the wire literals that must agree
// with the NereidLink server-side constants byte for byte.
func TestInferenceConstantsAreVerbatim(t *testing.T) {
	if FeatureInference != "inference" {
		t.Fatalf("FeatureInference = %q, want \"inference\"", FeatureInference)
	}
	if InferenceContractModel1 != "model/1" {
		t.Fatalf("InferenceContractModel1 = %q", InferenceContractModel1)
	}
	for got, want := range map[string]string{
		ModelStateExtracting: "EXTRACTING",
		ModelStateValidating: "VALIDATING",
		ModelStateReady:      "READY",
		ModelStateFailed:     "FAILED",
		ModelStateRemoved:    "REMOVED",
	} {
		if got != want {
			t.Fatalf("model state literal = %q, want %q", got, want)
		}
	}
}
