// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	device "github.com/nereidai/nereidlink/sdk/device/go"

	"github.com/nereidai/nereidedge/internal/binding"
)

func testUplinkFilter(t *testing.T) *UplinkFilterManager {
	t.Helper()
	manager := NewUplinkFilterManager()
	raw := json.RawMessage(`{
		"default_action":"allow",
		"rules":[
			{"action":"allow","device_names":["face-conclusions"],"resource_names":["matched","personId","score"]},
			{"action":"deny","device_names":["face-conclusions"]},
			{"action":"deny","profile_names":["nereid-inference-*"],"resource_names":["PipelineJSON","Embedding*"]},
			{"action":"deny","profile_names":["nereid-inference-detection"],"resource_names":["DetectionCount","FrameJPEG"]}
		]
	}`)
	if _, _, err := manager.Apply("filter-spec", 1, "hash", raw); err != nil {
		t.Fatalf("apply filter: %v", err)
	}
	return manager
}

func simpleReading(t *testing.T, deviceName, profile, resource, value string) dtos.BaseReading {
	t.Helper()
	reading, err := dtos.NewSimpleReading(deviceName, profile, resource, common.ValueTypeString, value)
	if err != nil {
		t.Fatal(err)
	}
	return reading
}

func TestUplinkFilterBlocksWholeRawResourcesWithoutParsingValues(t *testing.T) {
	filter := testUplinkFilter(t)
	event := &dtos.Event{
		DeviceName:  "inference-spec-1",
		ProfileName: "nereid-inference-pipeline",
		Readings: []dtos.BaseReading{
			simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "PipelineJSON", `{"objects":[{"embedding":[1,2]}]}`),
			simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "EmbeddingVector", `[0.1,0.2]`),
			simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "DetectionJSON", `{}`),
		},
	}

	readings, allowed := filter.FilterReadings(event)
	if !allowed || len(readings) != 1 || readings[0].ResourceName != "DetectionJSON" {
		t.Fatalf("filtered readings=%+v allowed=%v, want only DetectionJSON", readings, allowed)
	}
	event.Readings = event.Readings[:2]
	if readings, allowed := filter.FilterReadings(event); allowed || readings != nil {
		t.Fatalf("raw-only event passed filter: readings=%+v allowed=%v", readings, allowed)
	}
}

func TestUplinkFilterMandatoryRawGuardSurvivesPolicyChanges(t *testing.T) {
	tests := []struct {
		name   string
		config string
	}{
		{name: "policy absent"},
		{name: "policy disabled", config: `{"enabled":false,"default_action":"allow"}`},
		{name: "explicit allow", config: `{
			"default_action":"deny",
			"rules":[{"action":"allow","resource_names":["PipelineJSON","Embedding*","DetectionJSON"]}]
		}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := NewUplinkFilterManager()
			if tt.config != "" {
				if _, _, err := filter.Apply("filter-spec", 1, "hash", json.RawMessage(tt.config)); err != nil {
					t.Fatalf("apply filter: %v", err)
				}
			}
			event := &dtos.Event{
				DeviceName: "inference-spec-1", ProfileName: "nereid-inference-pipeline",
				Readings: []dtos.BaseReading{
					simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "PipelineJSON", `{}`),
					simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "Embedding", `[0.1]`),
					simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "EmbeddingVector", `[0.2]`),
					simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "DetectionJSON", `{}`),
				},
			}

			readings, allowed := filter.FilterReadings(event)
			if !allowed || len(readings) != 1 || readings[0].ResourceName != "DetectionJSON" {
				t.Fatalf("filtered readings=%+v allowed=%v, want only DetectionJSON", readings, allowed)
			}
			event.Readings = event.Readings[:3]
			if readings, allowed := filter.FilterReadings(event); allowed || readings != nil {
				t.Fatalf("raw-only event passed mandatory guard: readings=%+v allowed=%v", readings, allowed)
			}
		})
	}
}

func TestTelemetryPublisherDropsBlockedEventBeforeUplink(t *testing.T) {
	resolver := binding.NewResolver(nil)
	resolver.SetProvisioned("inference-spec-1", "raw-output-uuid")
	published := 0
	publisher := &TelemetryPublisher{
		Resolver:      resolver,
		UplinkFilters: NewUplinkFilterManager(),
		publishScalar: func(context.Context, device.GatewayFrame) error {
			published++
			return nil
		},
	}
	event := &dtos.Event{
		DeviceName:  "inference-spec-1",
		ProfileName: "nereid-inference-pipeline",
		Readings: []dtos.BaseReading{
			simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "PipelineJSON", `{"objects":[]}`),
			simpleReading(t, "inference-spec-1", "nereid-inference-pipeline", "Embedding", `[0.1]`),
		},
	}

	if ok, _ := publisher.Publish(testAppContext{}, event); ok {
		t.Fatal("raw pipeline event reached the NL publish path")
	}
	if published != 0 {
		t.Fatalf("publish calls=%d, want zero", published)
	}
}

func TestTelemetryPublisherFiltersRawDetectionResourcesBeforeNormalization(t *testing.T) {
	resolver := binding.NewResolver(nil)
	resolver.SetProvisioned("inference-spec-1", "raw-output-uuid")
	published := 0
	publisher := &TelemetryPublisher{
		Resolver:      resolver,
		UplinkFilters: testUplinkFilter(t),
		publishScalar: func(context.Context, device.GatewayFrame) error {
			published++
			return nil
		},
		publishRaw: func(context.Context, []byte) error {
			published++
			return nil
		},
	}
	countReading, err := dtos.NewSimpleReading(
		"inference-spec-1", inferenceDetectionProfile, "DetectionCount", common.ValueTypeInt32, int32(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	event := &dtos.Event{
		DeviceName:  "inference-spec-1",
		ProfileName: inferenceDetectionProfile,
		Readings: []dtos.BaseReading{
			countReading,
			dtos.NewBinaryReading(
				"inference-spec-1", inferenceDetectionProfile, "FrameJPEG", []byte("jpeg"), "image/jpeg",
			),
		},
	}

	// The policy selects the raw DS names, not detection_count/frame. With
	// default_action=allow, moving filtering after normalization would publish both.
	if ok, _ := publisher.Publish(testAppContext{}, event); ok {
		t.Fatal("raw detection readings reached the NL publish path after aliasing")
	}
	if published != 0 {
		t.Fatalf("publish calls=%d, want zero for blocked raw DetectionCount and FrameJPEG", published)
	}
}

func TestUplinkFilterAllowsOnlySelectedConclusionResources(t *testing.T) {
	filter := testUplinkFilter(t)
	event := &dtos.Event{
		DeviceName:  "face-conclusions",
		ProfileName: "face-access-v1",
		Readings: []dtos.BaseReading{
			simpleReading(t, "face-conclusions", "face-access-v1", "matched", "true"),
			simpleReading(t, "face-conclusions", "face-access-v1", "personId", "person-7"),
			simpleReading(t, "face-conclusions", "face-access-v1", "score", "0.99"),
			simpleReading(t, "face-conclusions", "face-access-v1", "rawFact", "private"),
		},
	}
	readings, allowed := filter.FilterReadings(event)
	if !allowed || len(readings) != 3 {
		t.Fatalf("conclusion readings=%+v allowed=%v, want three selected resources", readings, allowed)
	}
	for _, reading := range readings {
		if reading.ResourceName == "rawFact" {
			t.Fatal("unselected conclusion resource passed the allow rule")
		}
	}

	unrelated := &dtos.Event{
		DeviceName: "modbus-1", ProfileName: "modbus",
		Readings: []dtos.BaseReading{simpleReading(t, "modbus-1", "modbus", "temperature", "21")},
	}
	if readings, allowed := filter.FilterReadings(unrelated); !allowed || len(readings) != 1 {
		t.Fatal("default_action=allow changed unrelated legacy telemetry")
	}
}

func TestUplinkFilterRejectsInvalidRuleAndSecondOwner(t *testing.T) {
	filter := NewUplinkFilterManager()
	invalid := json.RawMessage(`{"rules":[{"action":"deny","resource_names":["["]}]}`)
	if _, _, err := filter.Apply("bad", 1, "h", invalid); err == nil {
		t.Fatal("invalid selector pattern must fail validation")
	}
	valid := json.RawMessage(`{"rules":[{"action":"deny","resource_names":["PipelineJSON"]}]}`)
	if _, _, err := filter.Apply("owner", 1, "h", valid); err != nil {
		t.Fatal(err)
	}
	if _, _, err := filter.Apply("other", 1, "h2", valid); err == nil {
		t.Fatal("a second gateway-level filter owner must be rejected")
	}
	filter.Delete("other")
	if filter.NeedsRestart("owner") {
		t.Fatal("deleting a non-owner removed the active filter")
	}
	filter.Delete("owner")
	if !filter.NeedsRestart("owner") {
		t.Fatal("deleting the owner did not remove the active filter")
	}
}
