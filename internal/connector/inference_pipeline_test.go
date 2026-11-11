// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"encoding/json"
	"strings"
	"testing"
)

func expandedPipelineConfig() map[string]any {
	return map[string]any{
		"PipelineId":      "face-recognition-v1",
		"PublishMode":     "nonempty",
		"PublishReadings": []string{"PipelineJSON", "Embedding"},
		"SourceKind":      "onvif-camera",
		"SourceDevice":    infCameraUUID,
		"StreamProfile":   "main",
		"DecodeAccel":     "vaapi",
		"VAAPIDevice":     "renderD128",
		"TargetDevice":    "GPU.0",
		"FPS":             5,
	}
}

func TestTranslateExpandedPipelineBinding(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	translated, err := a.inferenceDeviceConfig(inferencePush("spec-pipeline", expandedPipelineConfig()))
	if err != nil {
		t.Fatalf("translate pipeline binding: %v", err)
	}
	if translated.ProfileName != inferencePipelineProfile || translated.ProfileName == inferenceProfile {
		t.Fatalf("pipeline profile=%q, want %q", translated.ProfileName, inferencePipelineProfile)
	}
	props := translated.Protocols[inferenceProtocol]
	want := map[string]string{
		"PipelineId":      "face-recognition-v1",
		"PublishMode":     "nonempty",
		"PublishReadings": "PipelineJSON,Embedding",
		"SourceKind":      "onvif-camera",
		"SourceDevice":    infCameraName,
		"StreamProfile":   "main",
		"DecodeAccel":     "vaapi",
		"VAAPIDevice":     "renderD128",
		"TargetDevice":    "GPU.0",
		"Trigger":         "continuous",
		"FPS":             "5",
	}
	for key, expected := range want {
		if got, ok := props[key].(string); !ok || got != expected {
			t.Errorf("%s=%#v, want string %q", key, props[key], expected)
		}
	}
	for _, forbidden := range []string{"ModelID", "ModelVersion", "EmitFrame"} {
		if value, exists := props[forbidden]; exists {
			t.Errorf("pipeline binding must omit %s, got %#v", forbidden, value)
		}
	}
}

func TestPipelineBindingRejectsNoneSource(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	config := expandedPipelineConfig()
	config["SourceKind"] = "none"

	if _, err := a.inferenceDeviceConfig(inferencePush("spec-none-source", config)); err == nil {
		t.Fatal("pipeline SourceKind=none must be rejected")
	}
}

func TestPipelineIDSafeNameValidation(t *testing.T) {
	maximumID := strings.Repeat("a", 128)
	tests := []struct {
		name string
		id   string
		want string
	}{
		{name: "trimmed safe characters", id: "  Face.recognition+v1  ", want: "Face.recognition+v1"},
		{name: "maximum length", id: maximumID, want: maximumID},
		{name: "blank", id: "   "},
		{name: "leading hyphen", id: "-face"},
		{name: "leading dot", id: ".face"},
		{name: "leading underscore", id: "_face"},
		{name: "embedded whitespace", id: "face recognition"},
		{name: "path separator", id: "face/recognition"},
		{name: "too long", id: maximumID + "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, _, _ := newInferenceApplier(t)
			config := expandedPipelineConfig()
			config["PipelineId"] = tt.id
			translated, err := a.inferenceDeviceConfig(inferencePush("spec-pipeline-id", config))
			if tt.want == "" {
				if err == nil {
					t.Fatalf("PipelineId %q must be rejected", tt.id)
				}
				return
			}
			if err != nil {
				t.Fatalf("PipelineId %q must be accepted: %v", tt.id, err)
			}
			if got := translated.Protocols[inferenceProtocol]["PipelineId"]; got != tt.want {
				t.Fatalf("PipelineId=%#v, want %q", got, tt.want)
			}
		})
	}
}

func TestPipelinePublishReadingsValidationAndCSV(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	config := expandedPipelineConfig()
	config["PublishReadings"] = []string{"  PipelineJSON  ", "Embedding_2"}
	translated, err := a.inferenceDeviceConfig(inferencePush("spec-trimmed-readings", config))
	if err != nil {
		t.Fatalf("valid readings must be accepted: %v", err)
	}
	if got := translated.Protocols[inferenceProtocol]["PublishReadings"]; got != "PipelineJSON,Embedding_2" {
		t.Fatalf("PublishReadings=%#v, want trimmed CSV", got)
	}

	tests := []struct {
		name     string
		readings []string
	}{
		{name: "duplicate after trimming", readings: []string{"PipelineJSON", " PipelineJSON "}},
		{name: "empty", readings: []string{""}},
		{name: "whitespace", readings: []string{"   "}},
		{name: "leading digit", readings: []string{"1Embedding"}},
		{name: "leading underscore", readings: []string{"_Embedding"}},
		{name: "hyphen", readings: []string{"Pipeline-JSON"}},
		{name: "embedded whitespace", readings: []string{"Pipeline JSON"}},
		{name: "too long", readings: []string{"R" + strings.Repeat("1", 64)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, _, _ := newInferenceApplier(t)
			config := expandedPipelineConfig()
			config["PublishReadings"] = tt.readings
			if _, err := a.inferenceDeviceConfig(inferencePush("spec-invalid-readings", config)); err == nil {
				t.Fatalf("PublishReadings %q must be rejected", tt.readings)
			}
		})
	}
}

func TestPipelineSchemaValidation(t *testing.T) {
	const (
		pipelineIDPattern = `^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`
		readingPattern    = `^[A-Za-z][A-Za-z0-9_]{0,63}$`
	)
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		OneOf      []struct {
			Required   []string                   `json:"required"`
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"oneOf"`
	}
	if err := json.Unmarshal(inferenceSchema, &schema); err != nil {
		t.Fatal(err)
	}
	var pipelineID struct {
		Pattern string `json:"pattern"`
	}
	if err := json.Unmarshal(schema.Properties["PipelineId"], &pipelineID); err != nil {
		t.Fatal(err)
	}
	if pipelineID.Pattern != pipelineIDPattern {
		t.Fatalf("PipelineId pattern=%q, want %q", pipelineID.Pattern, pipelineIDPattern)
	}
	var readings struct {
		UniqueItems bool `json:"uniqueItems"`
		Items       struct {
			MaxLength int    `json:"maxLength"`
			Pattern   string `json:"pattern"`
		} `json:"items"`
	}
	if err := json.Unmarshal(schema.Properties["PublishReadings"], &readings); err != nil {
		t.Fatal(err)
	}
	if !readings.UniqueItems || readings.Items.Pattern != readingPattern || readings.Items.MaxLength != 64 {
		t.Fatalf("PublishReadings schema=%+v, want unique safe names", readings)
	}

	var pipelineBranch map[string]json.RawMessage
	for _, branch := range schema.OneOf {
		for _, required := range branch.Required {
			if required == "PipelineId" {
				pipelineBranch = branch.Properties
			}
		}
	}
	if pipelineBranch == nil {
		t.Fatal("schema lacks the PipelineId oneOf branch")
	}
	var sourceKind struct {
		Enum []string `json:"enum"`
	}
	if err := json.Unmarshal(pipelineBranch["SourceKind"], &sourceKind); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(sourceKind.Enum, ","), "onvif-camera,onvif-snapshot,rtsp,rtsp-udp,mpjpeg,jpeg"; got != want {
		t.Fatalf("pipeline SourceKind enum=%q, want %q", got, want)
	}
	// The shared property retains the legacy enum; only the pipeline branch
	// narrows it, so existing model_ref documents validate exactly as before.
	if err := json.Unmarshal(schema.Properties["SourceKind"], &sourceKind); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(sourceKind.Enum, ","), "none,onvif-camera,onvif-snapshot,rtsp,rtsp-udp,mpjpeg,jpeg"; got != want {
		t.Fatalf("shared SourceKind enum=%q, want unchanged %q", got, want)
	}
}

func TestExternalPipelineOmitsTargetDevice(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	config := expandedPipelineConfig()
	config["ServableID"] = "external-face-service"
	delete(config, "TargetDevice")

	translated, err := a.inferenceDeviceConfig(inferencePush("spec-external", config))
	if err != nil {
		t.Fatal(err)
	}
	props := translated.Protocols[inferenceProtocol]
	if got := props["ServableID"]; got != "external-face-service" {
		t.Fatalf("ServableID=%#v", got)
	}
	if value, exists := props["TargetDevice"]; exists {
		t.Fatalf("external pipeline must omit TargetDevice, got %#v", value)
	}
}

func TestPipelineAndModelBindingsAreMutuallyExclusive(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	both := expandedPipelineConfig()
	both["source"] = map[string]any{"device": infCameraUUID, "resource": "snapshot_jpeg"}
	both["model_ref"] = map[string]any{"id": "person", "version": "1.0.0"}
	if _, err := a.inferenceDeviceConfig(inferencePush("spec-both", both)); err == nil {
		t.Fatal("model_ref plus PipelineId must be rejected")
	}

	neither := map[string]any{
		"SourceKind":   "onvif-camera",
		"SourceDevice": infCameraUUID,
	}
	if _, err := a.inferenceDeviceConfig(inferencePush("spec-neither", neither)); err == nil {
		t.Fatal("a binding without model_ref or PipelineId must be rejected")
	}
}

func TestPipelineBindingCountsAsActiveWithoutModelLease(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	raw := inferencePush("spec-pipeline-count", expandedPipelineConfig())
	a.mu.Lock()
	a.applied[raw.SpecID] = appliedSpec{
		revision:      raw.Revision,
		deviceID:      raw.DeviceID,
		deviceName:    inferenceDeviceName(raw.SpecID),
		connectorType: TypeEdgeInference,
		config:        raw.Config,
	}
	a.mu.Unlock()

	if got := a.ActiveInferenceBindings(); got != 1 {
		t.Fatalf("active bindings=%d, want 1", got)
	}
	if counts := a.InferenceBindingCounts(); len(counts) != 0 {
		t.Fatalf("pipeline must not invent a model lease count: %v", counts)
	}
}
