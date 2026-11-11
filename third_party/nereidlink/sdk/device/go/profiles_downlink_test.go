// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestProfileSpecFrameDecode mirrors the ADR-0038 protocol example, including
// a removing entry without a document. Field names must match the platform
// constants byte for byte; any drift silently downgrades delivery.
func TestProfileSpecFrameDecode(t *testing.T) {
	raw := `{
	  "tenant_id": "8e1f",
	  "gateway_id": "5c2a",
	  "entries": [
	    {
	      "profile_id": "a1b2",
	      "name": "A1",
	      "revision": 2,
	      "format": "edgex-device-profile/3",
	      "target": {"service": "device-modbus"},
	      "document": {
	        "name": "A1",
	        "deviceResources": [
	          {"name": "temperature",
	           "properties": {"valueType": "Float32", "units": "Cel", "readWrite": "R"}}
	        ]
	      }
	    },
	    {"profile_id": "c3d4", "name": "Old", "revision": 1,
	     "format": "edgex-device-profile/3", "removing": true}
	  ]
	}`

	var frame ProfileSpecFrame
	if err := json.Unmarshal([]byte(raw), &frame); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if frame.TenantID != "8e1f" || frame.GatewayID != "5c2a" || len(frame.Entries) != 2 {
		t.Fatalf("frame header: %+v", frame)
	}
	first := frame.Entries[0]
	if first.ProfileID != "a1b2" || first.Name != "A1" || first.Revision != 2 ||
		first.Format != ProfileFormatEdgeX || first.Removing {
		t.Fatalf("first entry: %+v", first)
	}
	if first.Target["service"] != "device-modbus" {
		t.Fatalf("target hint lost: %+v", first.Target)
	}
	docName, _ := first.Document["name"].(string)
	if docName != "A1" {
		t.Fatalf("document lost: %+v", first.Document)
	}
	old := frame.Entries[1]
	if !old.Removing || old.Document != nil {
		t.Fatalf("removing entry: %+v", old)
	}
}

// TestProfileSpecFrameEncodeOmitsRemovingDocument verifies the wire stays
// compact on removing entries and empty optional fields.
func TestProfileSpecFrameEncodeOmitsRemovingDocument(t *testing.T) {
	frame := ProfileSpecFrame{
		TenantID:  "t",
		GatewayID: "g",
		Entries: []ProfileEntry{
			{ProfileID: "p1", Name: "P1", Revision: 1, Format: ProfileFormatNeutral,
				Document: map[string]any{"name": "P1", "points": []any{}}},
			{ProfileID: "p2", Name: "P2", Revision: 3, Format: ProfileFormatNeutral,
				Removing: true},
		},
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	s := string(raw)
	if !strings.Contains(s, `"removing":true`) {
		t.Fatalf("removing flag missing: %s", s)
	}
	// The removing entry carries no document key: count document occurrences
	// (one for P1 only; map key ordering is deterministic for one key set).
	if strings.Count(s, `"document"`) != 1 {
		t.Fatalf("want exactly one document key: %s", s)
	}
}

// TestProfileStatusFrameRoundTrip covers the status uplink shape and the
// reason omission on success.
func TestProfileStatusFrameRoundTrip(t *testing.T) {
	frame := ProfileStatusFrame{Reports: []ProfileStatus{
		{Name: "A1", Revision: 2, Status: ProfileStateApplied},
		{Name: "Bad", Revision: 3, Status: ProfileStateFailed, Reason: "invalid profile document"},
	}}
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var back ProfileStatusFrame
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(back.Reports) != 2 || back.Reports[0] != frame.Reports[0] {
		t.Fatalf("round trip: %+v", back)
	}
	if back.Reports[1].Reason != "invalid profile document" {
		t.Fatalf("reason lost: %+v", back.Reports[1])
	}
	if strings.Contains(string(raw), `"reason":""`) {
		t.Fatalf("empty reason must be omitted: %s", raw)
	}
}

// TestProfileConstantsAreVerbatim pins the wire literals that must agree with
// the NereidLink server-side constants.
func TestProfileConstantsAreVerbatim(t *testing.T) {
	if FeatureProfiles != "profiles" {
		t.Fatalf("FeatureProfiles = %q, want \"profiles\"", FeatureProfiles)
	}
	if ProfileFormatNeutral != "nereid-neutral/1" {
		t.Fatalf("ProfileFormatNeutral = %q", ProfileFormatNeutral)
	}
	if ProfileFormatEdgeX != "edgex-device-profile/3" {
		t.Fatalf("ProfileFormatEdgeX = %q", ProfileFormatEdgeX)
	}
	for got, want := range map[string]string{
		ProfileStateApplied:  "APPLIED",
		ProfileStateFailed:   "FAILED",
		ProfileStateRejected: "REJECTED",
		ProfileStateRemoved:  "REMOVED",
	} {
		if got != want {
			t.Fatalf("state literal = %q, want %q", got, want)
		}
	}
	if TopicGatewayProfilesSpec != "v1/gateway/profiles/spec" {
		t.Fatalf("spec topic = %q", TopicGatewayProfilesSpec)
	}
	if TopicGatewayProfilesStatus != "v1/gateway/profiles/status" {
		t.Fatalf("status topic = %q", TopicGatewayProfilesStatus)
	}
	// The pre-existing uplink topic must stay untouched.
	if TopicGatewayProfiles != "v1/gateway/profiles" {
		t.Fatalf("uplink topic = %q", TopicGatewayProfiles)
	}
}

// TestHelloProfileFormatsRoundTrip verifies the hello/ack extension keeps the
// preference order and omits the field when empty (older peers).
func TestHelloProfileFormatsRoundTrip(t *testing.T) {
	hello := Hello{ProfileFormats: []string{ProfileFormatEdgeX, ProfileFormatNeutral}}
	raw, err := json.Marshal(hello)
	if err != nil {
		t.Fatalf("encode hello: %v", err)
	}
	if !strings.Contains(string(raw), `"profile_formats":["edgex-device-profile/3","nereid-neutral/1"]`) {
		t.Fatalf("hello profile_formats missing/out of order: %s", raw)
	}
	var back Hello
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	if len(back.ProfileFormats) != 2 || back.ProfileFormats[0] != ProfileFormatEdgeX {
		t.Fatalf("hello formats: %+v", back.ProfileFormats)
	}

	empty, err := json.Marshal(HelloAck{})
	if err != nil {
		t.Fatalf("encode ack: %v", err)
	}
	if strings.Contains(string(empty), "profile_formats") {
		t.Fatalf("empty ack must omit profile_formats: %s", empty)
	}
}
