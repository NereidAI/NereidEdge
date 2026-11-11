// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

// ExportConfig is the parsed edge-export-mqtt connector config (see
// NODE_CONNECTOR_DESIGN.md section 3.2). NL validates the pushed config
// against the schema advertised in hello; the edge still parses defensively.
type ExportConfig struct {
	Enabled            *bool             `json:"enabled,omitempty"`
	BrokerURL          string            `json:"broker_url"`
	ClientID           string            `json:"client_id,omitempty"`
	QoS                byte              `json:"qos,omitempty"`
	Retained           bool              `json:"retained,omitempty"`
	KeepAliveS         int               `json:"keep_alive_s,omitempty"`
	Username           string            `json:"username,omitempty"`
	Password           string            `json:"password,omitempty"`
	CACert             string            `json:"ca_cert,omitempty"`
	ClientCert         string            `json:"client_cert,omitempty"`
	ClientKey          string            `json:"client_key,omitempty"`
	InsecureSkipVerify bool              `json:"insecure_skip_verify,omitempty"`
	TopicTemplate      string            `json:"topic_template"`
	PayloadFormat      string            `json:"payload_format,omitempty"`
	Filter             ExportFilter      `json:"filter,omitempty"`
	Buffer             ExportBuffer      `json:"buffer,omitempty"`
	Reconnect          ExportReconnect   `json:"reconnect,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
}

// ExportFilter selects which EdgeX events an export instance publishes.
// Empty lists mean "match everything". The filter is operator policy; the
// NE-owned raw-NI privacy guard (PipelineJSON/Embedding*) is always enforced
// on top of it in matchingReadings and cannot be relaxed through these lists.
type ExportFilter struct {
	DeviceNames   []string `json:"device_names,omitempty"`
	ProfileNames  []string `json:"profile_names,omitempty"`
	ResourceNames []string `json:"resource_names,omitempty"`
	MinReadings   int      `json:"min_readings,omitempty"`
}

// ExportBuffer is accepted for wire compatibility. Batching is not
// implemented in the first runtime slice; exports publish per event.
type ExportBuffer struct {
	MaxBatch int `json:"max_batch,omitempty"`
	FlushMS  int `json:"flush_ms,omitempty"`
}

// ExportReconnect configures the exponential backoff reconnect policy.
// MaxAttempts 0 means retry forever.
type ExportReconnect struct {
	MinBackoffMS int `json:"min_backoff_ms,omitempty"`
	MaxBackoffMS int `json:"max_backoff_ms,omitempty"`
	MaxAttempts  int `json:"max_attempts,omitempty"`
}

// Export runtime state values reported to the applier. They mirror the
// connector status constants from the device SDK without importing it here.
const (
	ExportStateApplied  = "APPLIED"
	ExportStateDegraded = "DEGRADED"
	ExportStateFailed   = "FAILED"
)

const (
	defaultKeepAliveS   = 30
	defaultConnectWait  = 10 * time.Second
	defaultQueueDepth   = 256
	defaultDrainWindow  = 500 * time.Millisecond
	defaultMinBackoffMS = 500
	defaultMaxBackoffMS = 5000
)

var (
	brokerURLPattern = regexp.MustCompile(`^(mqtt|mqtts|tcp|ssl)://.+`)
	placeholderRE    = regexp.MustCompile(`\{[^{}]+\}`)
)

// knownTopicTokens are the placeholders supported by topic_template.
var knownTopicTokens = map[string]struct{}{
	"{deviceName}": {}, "{profileName}": {}, "{sourceName}": {},
	"{serviceName}": {}, "{eventId}": {}, "{origin}": {},
}

// ParseExportConfig parses and validates an edge-export-mqtt config and fills
// defaults. It is the defensive edge-side counterpart of hello schema
// validation; every returned error is suitable as a FAILED status reason.
func ParseExportConfig(raw json.RawMessage) (ExportConfig, error) {
	var cfg ExportConfig
	if len(raw) == 0 {
		return cfg, errors.New("config is empty")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("invalid edge-export-mqtt config: %w", err)
	}
	cfg.BrokerURL = strings.TrimSpace(cfg.BrokerURL)
	cfg.TopicTemplate = strings.TrimSpace(cfg.TopicTemplate)
	if cfg.BrokerURL == "" {
		return cfg, errors.New("config requires broker_url")
	}
	if !brokerURLPattern.MatchString(cfg.BrokerURL) {
		return cfg, fmt.Errorf("broker_url %q must use mqtt://, mqtts://, tcp:// or ssl://", cfg.BrokerURL)
	}
	if cfg.TopicTemplate == "" {
		return cfg, errors.New("config requires topic_template")
	}
	if cfg.QoS > 2 {
		return cfg, fmt.Errorf("qos must be 0..2, got %d", cfg.QoS)
	}
	if cfg.KeepAliveS == 0 {
		cfg.KeepAliveS = defaultKeepAliveS
	}
	if cfg.KeepAliveS < 1 || cfg.KeepAliveS > 300 {
		return cfg, fmt.Errorf("keep_alive_s must be 1..300, got %d", cfg.KeepAliveS)
	}
	switch cfg.PayloadFormat {
	case "", "json":
		cfg.PayloadFormat = "json"
	case "raw":
	default:
		return cfg, fmt.Errorf("payload_format must be json or raw, got %q", cfg.PayloadFormat)
	}
	normalizeReconnect(&cfg.Reconnect)
	return cfg, nil
}

// Enabled reports whether the spec should start a pipeline. Absent means on.
func (c ExportConfig) EnabledOrDefault() bool {
	return c.Enabled == nil || *c.Enabled
}

// normalizeReconnect clamps reconnect settings to the advertised ranges and
// defaults them when unset.
func normalizeReconnect(r *ExportReconnect) {
	if r.MinBackoffMS == 0 {
		r.MinBackoffMS = defaultMinBackoffMS
	}
	if r.MaxBackoffMS == 0 {
		r.MaxBackoffMS = defaultMaxBackoffMS
	}
	if r.MinBackoffMS < 100 {
		r.MinBackoffMS = 100
	}
	if r.MinBackoffMS > 60000 {
		r.MinBackoffMS = 60000
	}
	if r.MaxBackoffMS < 1000 {
		r.MaxBackoffMS = 1000
	}
	if r.MaxBackoffMS > 300000 {
		r.MaxBackoffMS = 300000
	}
	if r.MinBackoffMS > r.MaxBackoffMS {
		r.MinBackoffMS = r.MaxBackoffMS
	}
}

// backoffDelay returns the exponential wait before reconnect attempt number
// attempt (1-based): min, 2min, 4min, ... capped at max. The loop form
// (rather than min<<(attempt-1)) cannot overflow time.Duration.
func backoffDelay(attempt int, min, max time.Duration) time.Duration {
	d := min
	for i := 1; i < attempt; i++ {
		if d >= max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

// DefaultTopicTemplate is used when a custom template renders an unknown
// placeholder, so one bad template cannot suppress all exports.
const DefaultTopicTemplate = "nereid-edge/{deviceName}/{profileName}/{sourceName}"

// RenderTopic renders template for the event. Unknown placeholders make it
// fall back to the default template; unknown reports whether that happened so
// the caller can warn once.
func RenderTopic(template string, ev *dtos.Event) (topic string, unknown bool) {
	rendered := renderTopicTemplate(template, ev)
	for _, tok := range placeholderRE.FindAllString(rendered, -1) {
		if _, ok := knownTopicTokens[tok]; !ok {
			return renderTopicTemplate(DefaultTopicTemplate, ev), true
		}
	}
	return rendered, false
}

func renderTopicTemplate(template string, ev *dtos.Event) string {
	r := strings.NewReplacer(
		"{deviceName}", ev.DeviceName,
		"{profileName}", ev.ProfileName,
		"{sourceName}", ev.SourceName,
		"{serviceName}", "",
		"{eventId}", ev.Id,
		"{origin}", strconv.FormatInt(ev.Origin, 10),
	)
	return r.Replace(template)
}

// matchingReadings returns the readings allowed by the resource_names filter.
// A nil result means the whole event must be skipped (device/profile/resource
// filters or min_readings rejected it). The mandatory NE-owned privacy guard
// runs first: PipelineJSON and every Embedding* resource stays edge-local on
// this auxiliary egress path exactly as on the NL uplink, and an explicit
// resource_names allowlist cannot override it (mandatory platform privacy
// guard for the auxiliary egress path).
func matchingReadings(cfg ExportConfig, ev *dtos.Event) []dtos.BaseReading {
	if len(cfg.Filter.DeviceNames) > 0 && !contains(cfg.Filter.DeviceNames, ev.DeviceName) {
		return nil
	}
	if len(cfg.Filter.ProfileNames) > 0 && !contains(cfg.Filter.ProfileNames, ev.ProfileName) {
		return nil
	}
	out := make([]dtos.BaseReading, 0, len(ev.Readings))
	for _, rd := range ev.Readings {
		if rawNIReadingLocal(rd.ResourceName) {
			continue
		}
		if len(cfg.Filter.ResourceNames) == 0 || contains(cfg.Filter.ResourceNames, rd.ResourceName) {
			out = append(out, rd)
		}
	}
	if len(out) == 0 || len(out) < cfg.Filter.MinReadings {
		return nil
	}
	return out
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// exportEnvelope is the slim payload for payload_format=json.
type exportEnvelope struct {
	EventID     string          `json:"event_id"`
	DeviceName  string          `json:"device_name"`
	ProfileName string          `json:"profile_name"`
	SourceName  string          `json:"source_name"`
	Origin      int64           `json:"origin"`
	Tags        dtos.Tags       `json:"tags,omitempty"`
	Readings    []exportReading `json:"readings"`
}

// exportReading is one reading in the slim payload.
type exportReading struct {
	ResourceName string `json:"resource_name"`
	ValueType    string `json:"value_type"`
	Value        string `json:"value"`
	Origin       int64  `json:"origin,omitempty"`
}

// BuildPayload builds the MQTT payload for one event using the readings that
// already passed matchingReadings. "json" is the slim Event/Reading envelope;
// "raw" is the single reading value for one-reading events and a compact
// {resource: typed value} object otherwise.
func BuildPayload(cfg ExportConfig, ev *dtos.Event, readings []dtos.BaseReading) ([]byte, error) {
	if cfg.PayloadFormat == "raw" {
		return buildRaw(readings)
	}
	env := exportEnvelope{
		EventID:     ev.Id,
		DeviceName:  ev.DeviceName,
		ProfileName: ev.ProfileName,
		SourceName:  ev.SourceName,
		Origin:      ev.Origin,
		Tags:        ev.Tags,
		Readings:    make([]exportReading, 0, len(readings)),
	}
	for _, rd := range readings {
		env.Readings = append(env.Readings, exportReading{
			ResourceName: rd.ResourceName,
			ValueType:    rd.ValueType,
			Value:        rd.Value,
			Origin:       rd.Origin,
		})
	}
	return json.Marshal(env)
}

// buildRaw renders the "raw" payload: a single reading goes out as its literal
// value, multiple readings as a compact resource->value JSON object.
func buildRaw(readings []dtos.BaseReading) ([]byte, error) {
	if len(readings) == 1 {
		return []byte(readings[0].Value), nil
	}
	out := make(map[string]any, len(readings))
	for _, rd := range readings {
		out[rd.ResourceName] = typedValue(rd)
	}
	return json.Marshal(out)
}

// typedValue converts a reading string to its native type, falling back to
// the raw string.
func typedValue(rd dtos.BaseReading) any {
	switch rd.ValueType {
	case "Bool":
		if v, err := strconv.ParseBool(rd.Value); err == nil {
			return v
		}
	case "Int8", "Int16", "Int32", "Int64":
		if v, err := strconv.ParseInt(rd.Value, 10, 64); err == nil {
			return v
		}
	case "Uint8", "Uint16", "Uint32", "Uint64":
		if v, err := strconv.ParseUint(rd.Value, 10, 64); err == nil {
			return v
		}
	case "Float32", "Float64":
		if v, err := strconv.ParseFloat(rd.Value, 64); err == nil {
			return v
		}
	}
	return rd.Value
}
