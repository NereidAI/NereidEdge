// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestTelemetryFrameEncode pins the flat scalar wire shape shared by the
// HTTP and MQTT ingresses.
func TestTelemetryFrameEncode(t *testing.T) {
	raw, err := NewFrame().
		SetFloat("temperature", 21.5).
		SetBool("online", true).
		SetString("mode", "AUTO").EncodeJSON()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded["temperature"] != 21.5 || decoded["online"] != true || decoded["mode"] != "AUTO" {
		t.Fatalf("unexpected frame: %s", raw)
	}
}

// TestTelemetryFrameRejectsStructuredValues mirrors the server rule that
// null and structured values never become Envelopes.
func TestTelemetryFrameRejectsStructuredValues(t *testing.T) {
	f := NewFrame()
	if err := f.Set("good", 1); err != nil {
		t.Fatalf("scalar rejected: %v", err)
	}
	for _, bad := range []any{nil, []any{1}, map[string]any{"a": 1}} {
		if err := f.Set("bad", bad); err == nil {
			t.Fatalf("expected rejection for %v", bad)
		}
	}
}

// TestGatewayBatchShape pins the reserved ts key and outer device map.
func TestGatewayBatchShape(t *testing.T) {
	raw, err := EncodeGatewayBatch(
		GatewayFrame{DeviceID: "dev-a", TS: 1727360000123,
			Measurements: NewFrame().SetFloat("temperature", 21.5)},
		GatewayFrame{DeviceID: "dev-b", Measurements: NewFrame().SetBool("switch", true)},
	)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded map[string]map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded["dev-a"]["ts"] != float64(1727360000123) {
		t.Fatalf("missing ts: %s", raw)
	}
	if _, hasTS := decoded["dev-b"]["ts"]; hasTS {
		t.Fatalf("zero ts must be omitted: %s", raw)
	}
}

// TestHelloAckDecode pins the negotiation response field names against
// docs/protocols/nereid-edge/v1.md section 3.
func TestHelloAckDecode(t *testing.T) {
	doc := `{
	  "session_id": "s_new",
	  "status": "ok",
	  "selected": "1.4",
	  "server": {"min": "1.0", "max": "2.0", "server_version": "nereidlink-1.5.0"},
	  "features": ["telemetry","presence","ota"],
	  "limits": {"heartbeat_s": 30, "hello_timeout_s": 10,
	             "upload": {"max_mb": 256, "qps": 20}},
	  "time": {"server_time": 1727360000456, "offset_ms": -312, "clock_warning": false},
	  "resume": {"accepted": true},
	  "connector_schemas": [{"type":"modbus-tcp","type_version":"2.1","accepted":true}]
	}`
	var ack HelloAck
	if err := json.Unmarshal([]byte(doc), &ack); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ack.Status != HelloStatusOK || ack.SessionID != "s_new" || ack.Selected != "1.4" {
		t.Fatalf("bad ack header: %+v", ack)
	}
	if ack.Limits.HeartbeatS != 30 || ack.Limits.Upload.MaxMB != 256 {
		t.Fatalf("bad limits: %+v", ack.Limits)
	}
	if !ack.Resume.Accepted || ack.Time.OffsetMS != -312 {
		t.Fatalf("bad resume/time: %+v", ack)
	}
	if len(ack.ConnectorSchemas) != 1 || !ack.ConnectorSchemas[0].Accepted {
		t.Fatalf("bad schemas: %+v", ack.ConnectorSchemas)
	}
}

// TestCanonicalConfigHash pins the drift hash algorithm from the protocol
// document section 7: compact, key-sorted {"config":...,"mappings":...}.
func TestCanonicalConfigHash(t *testing.T) {
	// Key order differs between the two marshal inputs; sorted output must
	// still yield the same digest.
	config1 := json.RawMessage(`{"b":2,"a":1}`)
	config2 := json.RawMessage(`{"a":1,"b":2}`)
	mappings := json.RawMessage(`[{"x":1}]`)
	h1, err := CanonicalConfigHash(config1, mappings)
	if err != nil {
		t.Fatalf("hash1: %v", err)
	}
	h2, err := CanonicalConfigHash(config2, mappings)
	if err != nil {
		t.Fatalf("hash2: %v", err)
	}
	if h1 != h2 || len(h1) != 64 || strings.ToLower(h1) != h1 {
		t.Fatalf("hash must be stable lowercase hex, got %q vs %q", h1, h2)
	}
}

// TestManifestConnectorsOmission ensures an absent connectors segment is
// distinguishable from an explicit empty list (skip reconcile vs all
// missing).
func TestManifestConnectorsOmission(t *testing.T) {
	absent, err := json.Marshal(Manifest{Devices: []string{"dev-a"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(absent), "connectors") {
		t.Fatalf("nil connectors must be omitted: %s", absent)
	}
	empty := []ManifestConnectorInstance{}
	present, err := json.Marshal(Manifest{Devices: []string{}, Connectors: &empty})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(present), `"connectors":[]`) {
		t.Fatalf("explicit empty list must be present: %s", present)
	}
}

// TestCommandFrameDecode pins downlink command field names.
func TestCommandFrameDecode(t *testing.T) {
	doc := `{"id":"e16f236e-a6af-4b38-9a8f-bcf4b045a343","name":"set-led","params":{"color":"green"}}`
	var cmd Command
	if err := json.Unmarshal([]byte(doc), &cmd); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cmd.ID == "" || cmd.Name != "set-led" || string(cmd.Params) == "" {
		t.Fatalf("bad command: %+v", cmd)
	}
}

// TestHelloOperationsEncode pins the hello operations[] advertisement
// shape (ADR-0035) and the nil-means-omitted rule.
func TestHelloOperationsEncode(t *testing.T) {
	hello := Hello{
		EdgeID: "edge-1",
		Operations: []OperationAdvert{{
			Operation:   "diag.ping",
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Ping host", "zh": "Ping 探测"},
			Interaction: OperationInteractionRequestResponse,
			TimeoutMs:   10000,
			Risk:        OperationRiskRead,
			ParamSchema: json.RawMessage(`{"type":"object","required":["host"],` +
				`"properties":{"host":{"type":"string"},"count":{"type":"integer"}}}`),
		}},
	}
	raw, err := json.Marshal(hello)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	ops, ok := decoded["operations"].([]any)
	if !ok || len(ops) != 1 {
		t.Fatalf("operations missing: %s", raw)
	}
	first := ops[0].(map[string]any)
	if first["operation"] != "diag.ping" || first["risk"] != "read" ||
		first["interaction"] != "request-response" ||
		first["timeout_ms"] != float64(10000) {
		t.Fatalf("bad advert: %s", raw)
	}

	// A nil Operations slice must be omitted entirely.
	if absent, err := json.Marshal(Hello{EdgeID: "edge-1"}); err != nil ||
		strings.Contains(string(absent), "operations") {
		t.Fatalf("nil operations must be omitted, got %s (%v)", absent, err)
	}
}

// TestHelloAckOperationsDecode pins the per-entry ack.operations[] result
// and distinguishes it from the execution result frame.
func TestHelloAckOperationsDecode(t *testing.T) {
	doc := `{
	  "session_id": "s_new",
	  "status": "ok",
	  "operations": [
	    {"operation":"diag.ping","type_version":"1","accepted":true},
	    {"operation":"diag.reboot","type_version":"1","accepted":false,
	     "reason":"invalid schema: ..."}
	  ]
	}`
	var ack HelloAck
	if err := json.Unmarshal([]byte(doc), &ack); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(ack.Operations) != 2 {
		t.Fatalf("expected 2 operation results, got %d", len(ack.Operations))
	}
	if !ack.Operations[0].Accepted || ack.Operations[0].Operation != "diag.ping" {
		t.Fatalf("bad accepted entry: %+v", ack.Operations[0])
	}
	if ack.Operations[1].Accepted || ack.Operations[1].Reason == "" {
		t.Fatalf("bad rejected entry: %+v", ack.Operations[1])
	}
}

// TestOperationResultFrameDecode confirms the execution result frame
// (Edge -> NL .../result) still decodes independently of the ack entry.
func TestOperationResultFrameDecode(t *testing.T) {
	doc := `{"id":"task-1","ok":true,"result":{"loss":0}}`
	var res OperationResult
	if err := json.Unmarshal([]byte(doc), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !res.OK || res.ID != "task-1" || string(res.Result) == "" {
		t.Fatalf("bad result frame: %+v", res)
	}
}

func TestConnectorStatusOptionalStatsAndObservedReadback(t *testing.T) {
	zero := uint64(0)
	latency := 47.5
	observed := int64(7)
	raw, err := json.Marshal(ConnectorStatus{
		SpecID: "spec-1", State: ConnectorStateApplied, Revision: 7,
		Stats:            &ConnectorStatusStats{SuccessTotal: &zero, P50MS: &latency},
		ObservedRevision: &observed, ObservedHash: "abc123",
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{`"success_total":0`, `"p50_ms":47.5`,
		`"observed_revision":7`, `"observed_hash":"abc123"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("status omitted %s: %s", want, text)
		}
	}
	for _, absent := range []string{"error_total", "p95_ms"} {
		if strings.Contains(text, absent) {
			t.Fatalf("absent optional field %s was serialized: %s", absent, text)
		}
	}
	var decoded ConnectorStatus
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Stats == nil || decoded.Stats.SuccessTotal == nil || *decoded.Stats.SuccessTotal != 0 ||
		decoded.Stats.ErrorTotal != nil || decoded.Stats.P95MS != nil ||
		decoded.ObservedRevision == nil || *decoded.ObservedRevision != 7 {
		t.Fatalf("optional status round-trip changed availability: %+v", decoded)
	}

	for _, invalid := range []string{
		`{"spec_id":"s","state":"APPLIED","revision":1,"stats":{"success_total":-1}}`,
		`{"spec_id":"s","state":"APPLIED","revision":1,"stats":{"error_total":1.5}}`,
	} {
		if err := json.Unmarshal([]byte(invalid), &decoded); err == nil {
			t.Fatalf("invalid counter accepted: %s", invalid)
		}
	}
}
