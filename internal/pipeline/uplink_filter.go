// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

const (
	UplinkAllow = "allow"
	UplinkDeny  = "deny"
)

// UplinkFilterConfig is the neutral routing policy for the main NL telemetry
// uplink. Rules are evaluated in order; the first matching rule wins. Empty
// selector lists match every value. An empty configuration adds no custom
// rules; the NE-owned raw-NI guard in FilterReadings still applies.
type UplinkFilterConfig struct {
	Enabled       *bool        `json:"enabled,omitempty"`
	DefaultAction string       `json:"default_action,omitempty"`
	Rules         []UplinkRule `json:"rules,omitempty"`
}

// UplinkRule selects whole events by device/profile and individual readings by
// resource. Name entries accept path.Match patterns, so "Embedding*" blocks a
// family without inspecting any values.
type UplinkRule struct {
	Action        string   `json:"action"`
	DeviceNames   []string `json:"device_names,omitempty"`
	ProfileNames  []string `json:"profile_names,omitempty"`
	ResourceNames []string `json:"resource_names,omitempty"`
}

type uplinkFilterInstance struct {
	specID   string
	revision int64
	hash     string
	config   UplinkFilterConfig
}

// UplinkFilterManager owns the singleton gateway-level filter policy. A
// singleton avoids ambiguous rule ordering between independent NL specs.
type UplinkFilterManager struct {
	mu   sync.RWMutex
	inst *uplinkFilterInstance
}

func NewUplinkFilterManager() *UplinkFilterManager {
	return &UplinkFilterManager{}
}

// Apply validates and atomically swaps the complete policy.
func (m *UplinkFilterManager) Apply(
	specID string, revision int64, hash string, raw json.RawMessage,
) (state, reason string, err error) {
	var cfg UplinkFilterConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", "", fmt.Errorf("invalid edge-northbound-filter config: %w", err)
	}
	if err := validateUplinkFilter(cfg); err != nil {
		return "", "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inst != nil && m.inst.specID != specID {
		return "", "", fmt.Errorf("edge-northbound-filter is already owned by spec %s", m.inst.specID)
	}
	if m.inst != nil && m.inst.hash == hash && m.inst.revision >= revision {
		return ExportStateApplied, "", nil
	}
	m.inst = &uplinkFilterInstance{specID: specID, revision: revision, hash: hash, config: cfg}
	return ExportStateApplied, "", nil
}

func validateUplinkFilter(cfg UplinkFilterConfig) error {
	if cfg.DefaultAction == "" {
		cfg.DefaultAction = UplinkAllow
	}
	if cfg.DefaultAction != UplinkAllow && cfg.DefaultAction != UplinkDeny {
		return fmt.Errorf("invalid edge-northbound-filter default_action %q", cfg.DefaultAction)
	}
	for i, rule := range cfg.Rules {
		if rule.Action != UplinkAllow && rule.Action != UplinkDeny {
			return fmt.Errorf("invalid edge-northbound-filter rule %d action %q", i, rule.Action)
		}
		for _, patterns := range [][]string{rule.DeviceNames, rule.ProfileNames, rule.ResourceNames} {
			for _, pattern := range patterns {
				if strings.TrimSpace(pattern) == "" {
					return fmt.Errorf("invalid edge-northbound-filter rule %d: empty selector", i)
				}
				if _, err := path.Match(pattern, ""); err != nil {
					return fmt.Errorf("invalid edge-northbound-filter rule %d selector %q: %w", i, pattern, err)
				}
			}
		}
	}
	return nil
}

// Delete removes the active policy only when the deleted spec owns it.
func (m *UplinkFilterManager) Delete(specID string) {
	m.mu.Lock()
	if m.inst != nil && m.inst.specID == specID {
		m.inst = nil
	}
	m.mu.Unlock()
}

// NeedsRestart reports whether the desired spec has no live policy.
func (m *UplinkFilterManager) NeedsRestart(specID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.inst == nil || m.inst.specID != specID
}

// FilterReadings applies device/profile/resource routing before any resource
// renaming or NL-device resolution. The boolean is false when the entire event
// must stay off the NL uplink. Input reading values are never decoded.
func (m *UplinkFilterManager) FilterReadings(event *dtos.Event) ([]dtos.BaseReading, bool) {
	m.mu.RLock()
	var cfg UplinkFilterConfig
	configured := m.inst != nil
	if configured {
		cfg = m.inst.config
	}
	m.mu.RUnlock()
	if configured && cfg.Enabled != nil && !*cfg.Enabled {
		configured = false
	}
	defaultAction := cfg.DefaultAction
	if defaultAction == "" {
		defaultAction = UplinkAllow
	}
	out := make([]dtos.BaseReading, 0, len(event.Readings))
	for _, reading := range event.Readings {
		// Raw pipeline facts and embeddings are edge-local by contract. This
		// NE-owned guard cannot be disabled or overridden by an NL allow rule.
		if rawNIReadingLocal(reading.ResourceName) {
			continue
		}
		action := defaultAction
		if configured {
			for _, rule := range cfg.Rules {
				if !matchesUplinkSelectors(rule.DeviceNames, event.DeviceName) ||
					!matchesUplinkSelectors(rule.ProfileNames, event.ProfileName) ||
					!matchesUplinkSelectors(rule.ResourceNames, reading.ResourceName) {
					continue
				}
				action = rule.Action
				break
			}
		}
		if action == UplinkAllow {
			out = append(out, reading)
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	if len(out) == len(event.Readings) {
		return event.Readings, true
	}
	return out, true
}

func rawNIReadingLocal(resource string) bool {
	return resource == "PipelineJSON" || strings.HasPrefix(resource, "Embedding")
}

func matchesUplinkSelectors(patterns []string, value string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		matched, err := path.Match(pattern, value)
		if err == nil && matched {
			return true
		}
	}
	return false
}
