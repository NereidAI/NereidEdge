// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

func validExportJSON(t *testing.T) json.RawMessage {
	t.Helper()
	return json.RawMessage(`{"broker_url":"mqtt://broker:1883","topic_template":"nereid/{deviceName}/{sourceName}"}`)
}

func TestParseExportConfigValid(t *testing.T) {
	cfg, err := ParseExportConfig(validExportJSON(t))
	if err != nil {
		t.Fatalf("valid config rejected: %s", err)
	}
	if cfg.BrokerURL != "mqtt://broker:1883" {
		t.Errorf("broker_url parsed as %q", cfg.BrokerURL)
	}
	if cfg.KeepAliveS != defaultKeepAliveS {
		t.Errorf("keep_alive default = %d, want %d", cfg.KeepAliveS, defaultKeepAliveS)
	}
	if cfg.PayloadFormat != "json" {
		t.Errorf("payload_format default = %q, want json", cfg.PayloadFormat)
	}
	if !cfg.EnabledOrDefault() {
		t.Error("absent enabled must default to on")
	}
}

func TestParseExportConfigErrors(t *testing.T) {
	cases := map[string]json.RawMessage{
		"empty":            json.RawMessage(``),
		"not json":         json.RawMessage(`{`),
		"missing broker":   json.RawMessage(`{"topic_template":"t/{deviceName}"}`),
		"bad broker":       json.RawMessage(`{"broker_url":"ws://x","topic_template":"t/{deviceName}"}`),
		"missing template": json.RawMessage(`{"broker_url":"mqtt://x"}`),
		"qos high":         json.RawMessage(`{"broker_url":"mqtt://x","topic_template":"t","qos":3}`),
		"qos negative":     json.RawMessage(`{"broker_url":"mqtt://x","topic_template":"t","qos":-1}`),
		"bad format":       json.RawMessage(`{"broker_url":"mqtt://x","topic_template":"t","payload_format":"xml"}`),
		"keepalive high":   json.RawMessage(`{"broker_url":"mqtt://x","topic_template":"t","keep_alive_s":301}`),
		"keepalive neg":    json.RawMessage(`{"broker_url":"mqtt://x","topic_template":"t","keep_alive_s":-1}`),
	}
	for name, raw := range cases {
		if _, err := ParseExportConfig(raw); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}

func TestParseExportConfigNormalizesReconnect(t *testing.T) {
	raw := json.RawMessage(`{"broker_url":"tcp://x","topic_template":"t","reconnect":{"min_backoff_ms":40000,"max_backoff_ms":500}}`)
	cfg, err := ParseExportConfig(raw)
	if err != nil {
		t.Fatalf("parse: %s", err)
	}
	if cfg.Reconnect.MinBackoffMS > cfg.Reconnect.MaxBackoffMS {
		t.Errorf("min %d > max %d after normalize", cfg.Reconnect.MinBackoffMS, cfg.Reconnect.MaxBackoffMS)
	}
	if cfg.Reconnect.MinBackoffMS < 100 || cfg.Reconnect.MaxBackoffMS < 1000 {
		t.Errorf("backoff not clamped to ranges: %+v", cfg.Reconnect)
	}

	def, _ := ParseExportConfig(json.RawMessage(`{"broker_url":"tcp://x","topic_template":"t"}`))
	if def.Reconnect.MinBackoffMS != defaultMinBackoffMS || def.Reconnect.MaxBackoffMS != defaultMaxBackoffMS {
		t.Errorf("backoff defaults = %+v", def.Reconnect)
	}
}

func TestParseExportConfigDefaults(t *testing.T) {
	cfg, err := ParseExportConfig(json.RawMessage(`{"broker_url":"ssl://x","topic_template":"t","keep_alive_s":60,"reconnect":{"min_backoff_ms":100,"max_backoff_ms":1000}}`))
	if err != nil {
		t.Fatalf("parse: %s", err)
	}
	if cfg.Reconnect.MinBackoffMS != 100 {
		t.Errorf("min backoff = %d, want 100", cfg.Reconnect.MinBackoffMS)
	}
	if cfg.Reconnect.MaxBackoffMS != 1000 {
		t.Errorf("max backoff = %d, want 1000", cfg.Reconnect.MaxBackoffMS)
	}
	if cfg.EnabledOrDefault() != true {
		t.Error("default enabled should be on")
	}
}

func TestEnabledOrDefault(t *testing.T) {
	on, off := true, false
	zero := ExportConfig{}
	if !zero.EnabledOrDefault() {
		t.Error("zero config must default to enabled")
	}
	enabled := ExportConfig{Enabled: &on}
	if !enabled.EnabledOrDefault() {
		t.Error("enabled:true must be on")
	}
	disabled := ExportConfig{Enabled: &off}
	if disabled.EnabledOrDefault() {
		t.Error("enabled:false must be off")
	}
}

func TestBackoffDelay(t *testing.T) {
	min, max := time.Second, 8*time.Second
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{10, 8 * time.Second},
	}
	for _, c := range cases {
		if got := backoffDelay(c.attempt, min, max); got != c.want {
			t.Errorf("backoffDelay(%d) = %s, want %s", c.attempt, got, c.want)
		}
	}
}

func TestRenderTopic(t *testing.T) {
	ev := &dtos.Event{
		Id: "abc", DeviceName: "d1", ProfileName: "p1",
		SourceName: "s1", Origin: 123,
	}
	{
		topic, unknown := RenderTopic("nereid/{deviceName}/{sourceName}", ev)
		if topic != "nereid/d1/s1" {
			t.Errorf("topic = %q", topic)
		}
		if unknown {
			t.Error("known tokens must not report unknown")
		}
	}
	{
		topic, unknown := RenderTopic("x/{deviceName}/{bogusToken}", ev)
		if !unknown {
			t.Error("bogus token must report unknown")
		}
		if topic != "nereid-edge/d1/p1/s1" && topic != DefaultTopicTemplate {
			t.Errorf("fallback topic = %q, want default rendering", topic)
		}
	}
	{
		topic, unknown := RenderTopic("plain/{deviceName}", ev)
		if topic != "plain/d1" {
			t.Errorf("topic = %q", topic)
		}
		if unknown {
			t.Error("plain template must not report unknown")
		}
	}
}

func TestRenderTopicDefaults(t *testing.T) {
	ev := &dtos.Event{DeviceName: "d", ProfileName: "p", SourceName: "s"}
	if got := renderTopicTemplate(DefaultTopicTemplate, ev); got != "nereid-edge/d/p/s" {
		t.Errorf("default render = %q", got)
	}
}

func TestMatchingReadings(t *testing.T) {
	base := func() *dtos.Event {
		return &dtos.Event{
			DeviceName: "d1", ProfileName: "p1", SourceName: "s1",
			Readings: []dtos.BaseReading{
				{ResourceName: "temp", ValueType: "Float32", SimpleReading: dtos.SimpleReading{Value: "22.5"}},
				{ResourceName: "hum", ValueType: "Int64", SimpleReading: dtos.SimpleReading{Value: "55"}},
			},
		}
	}
	cfg := func(f ExportFilter) ExportConfig {
		return ExportConfig{Filter: f}
	}

	if got := matchingReadings(cfg(ExportFilter{}), base()); len(got) != 2 {
		t.Errorf("no filter should match all readings, got %d", len(got))
	}
	if got := matchingReadings(cfg(ExportFilter{DeviceNames: []string{"other"}}), base()); got != nil {
		t.Error("device filter must reject")
	}
	if got := matchingReadings(cfg(ExportFilter{ProfileNames: []string{"other"}}), base()); got != nil {
		t.Error("profile filter must reject")
	}
	if got := matchingReadings(cfg(ExportFilter{ResourceNames: []string{"temp"}}), base()); len(got) != 1 || got[0].ResourceName != "temp" {
		t.Errorf("resource filter wrong: %+v", got)
	}
	if got := matchingReadings(cfg(ExportFilter{MinReadings: 3}), base()); got != nil {
		t.Error("min_readings must reject")
	}
	zero := base()
	zero.Readings = nil
	if got := matchingReadings(cfg(ExportFilter{}), zero); got != nil {
		t.Error("empty readings must reject")
	}
}

func TestMatchingReadingsMandatoryRawGuard(t *testing.T) {
	event := func() *dtos.Event {
		return &dtos.Event{
			DeviceName: "d1", ProfileName: "p1", SourceName: "s1",
			Readings: []dtos.BaseReading{
				{ResourceName: "PipelineJSON", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "{}"}},
				{ResourceName: "Embedding", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "[]"}},
				{ResourceName: "EmbeddingVector", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "[]"}},
				{ResourceName: "matched", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "alice"}},
			},
		}
	}
	cfg := func(f ExportFilter) ExportConfig { return ExportConfig{Filter: f} }

	// A match-all export filter still strips every edge-local raw resource.
	got := matchingReadings(cfg(ExportFilter{}), event())
	if len(got) != 1 || got[0].ResourceName != "matched" {
		t.Fatalf("mandatory guard not applied on match-all filter: %+v", got)
	}

	// An explicit allowlist naming a local resource cannot override the guard.
	got = matchingReadings(cfg(ExportFilter{ResourceNames: []string{"PipelineJSON", "matched"}}), event())
	if len(got) != 1 || got[0].ResourceName != "matched" {
		t.Fatalf("guard must beat the explicit resource_names allowlist: %+v", got)
	}

	// An event carrying only local resources is skipped as a whole.
	rawOnly := event()
	rawOnly.Readings = event().Readings[:3]
	if got := matchingReadings(cfg(ExportFilter{}), rawOnly); got != nil {
		t.Fatalf("raw-only event must stay fully edge-local, got %+v", got)
	}

	// The local resources must not satisfy min_readings either: the surviving
	// safe reading alone is below the threshold, so the event is skipped.
	if got := matchingReadings(cfg(ExportFilter{MinReadings: 2}), event()); got != nil {
		t.Fatalf("stripped readings must not count toward min_readings: %+v", got)
	}
}

func TestBuildPayloadJSON(t *testing.T) {
	cfg := ExportConfig{PayloadFormat: "json"}
	ev := &dtos.Event{Id: "e1", DeviceName: "d", ProfileName: "p", SourceName: "s", Origin: 7,
		Readings: []dtos.BaseReading{{ResourceName: "temp", ValueType: "Float32", SimpleReading: dtos.SimpleReading{Value: "22.5"}}}}
	payload, err := BuildPayload(cfg, ev, ev.Readings)
	if err != nil {
		t.Fatalf("build: %s", err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("payload not json: %s (%s)", payload, err)
	}
	if got["device_name"] != "d" || got["event_id"] != "e1" || got["origin"] != float64(7) {
		t.Errorf("envelope wrong: %v", got)
	}
	readings, ok := got["readings"].([]any)
	if !ok || len(readings) != 1 {
		t.Fatalf("readings wrong: %v", got)
	}
	rd := readings[0].(map[string]any)
	if rd["resource_name"] != "temp" || rd["value"] != "22.5" {
		t.Errorf("envelope reading wrong: %v", rd)
	}
}

func TestBuildPayloadRaw(t *testing.T) {
	cfg := ExportConfig{PayloadFormat: "raw"}
	ev := &dtos.Event{DeviceName: "d"}
	one := []dtos.BaseReading{{ResourceName: "temp", ValueType: "Float32", SimpleReading: dtos.SimpleReading{Value: "22.5"}}}
	if payload, err := BuildPayload(cfg, ev, one); err != nil || string(payload) != "22.5" {
		t.Errorf("single raw = %q, %v", payload, err)
	}
	two := []dtos.BaseReading{
		{ResourceName: "temp", ValueType: "Float32", SimpleReading: dtos.SimpleReading{Value: "22.5"}},
		{ResourceName: "count", ValueType: "Int64", SimpleReading: dtos.SimpleReading{Value: "3"}},
	}
	payload, err := BuildPayload(cfg, ev, two)
	if err != nil {
		t.Fatalf("multi raw: %s", err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("multi raw not object: %s (%s)", payload, err)
	}
	if got["temp"] != 22.5 || got["count"] != float64(3) {
		t.Errorf("typed values wrong: %v", got)
	}
}

func TestTypedValue(t *testing.T) {
	if got := typedValue(dtos.BaseReading{ValueType: "Bool", SimpleReading: dtos.SimpleReading{Value: "true"}}); got != true {
		t.Errorf("bool = %v", got)
	}
	if got := typedValue(dtos.BaseReading{ValueType: "Int64", SimpleReading: dtos.SimpleReading{Value: "-5"}}); got != int64(-5) {
		t.Errorf("int = %v", got)
	}
	if got := typedValue(dtos.BaseReading{ValueType: "Float64", SimpleReading: dtos.SimpleReading{Value: "1.5"}}); got != 1.5 {
		t.Errorf("float = %v", got)
	}
	if got := typedValue(dtos.BaseReading{ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "keep"}}); got != "keep" {
		t.Errorf("string = %v", got)
	}
	if got := typedValue(dtos.BaseReading{ValueType: "Int32", SimpleReading: dtos.SimpleReading{Value: "NaN"}}); got != "NaN" {
		t.Errorf("fallback = %v", got)
	}
}

func TestExternalClientID(t *testing.T) {
	if got := externalClientID(ExportConfig{ClientID: "my-id"}, "s1"); got != "my-id" {
		t.Errorf("explicit client id = %q", got)
	}
	if got := externalClientID(ExportConfig{}, "my spec id"); got != "nereid-edge-export-my-spec-id" {
		t.Errorf("derived client id = %q", got)
	}
	if got := externalClientID(ExportConfig{}, strings.Repeat("a/b", 100)); len(got) > 64 {
		t.Errorf("client id too long: %d", len(got))
	}
}

func TestBuildTLSNilForPlaintext(t *testing.T) {
	cfg, _ := ParseExportConfig(json.RawMessage(`{"broker_url":"mqtt://x","topic_template":"t"}`))
	tlsCfg, err := buildTLS(cfg)
	if err != nil || tlsCfg != nil {
		t.Errorf("plaintext must yield nil TLS, got %v (err %v)", tlsCfg, err)
	}
	cfg2, _ := ParseExportConfig(json.RawMessage(`{"broker_url":"mqtts://x","topic_template":"t"}`))
	tlsCfg, err = buildTLS(cfg2)
	if err != nil || tlsCfg == nil {
		t.Errorf("mqtts must yield TLS config, err %v", err)
	}
}
