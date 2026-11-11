// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package settings applies the well-known gateway-scope SHARED_SCOPE
// attributes NereidLink holds for the gateway's own identity to the running
// agent.
//
// A gateway-scope attribute is a knob of the NE process, the NE<->NL link or a
// gateway-wide policy — it has exactly one value per process, which is why it
// is the gateway's own attribute and never pushed to a sub-device. The only
// such key the edge honors today is log.level: the process logger level. The
// other gateway-scope keys (debug.mqtt_trace, health.*, forward.unmapped) are
// deliberately not applied — a sub-device's probe cadence already comes from
// the NereidLink connector spec, so a second, gateway-level control would only
// fight the spec, and the transport/forwarding toggles are not operator knobs
// here.
//
// The applier is reconciliation-based: the attribute cache always holds the
// authoritative set, so on every change the applier recomputes the known key
// and applies it only when it differs from what is currently in force. A key
// removed platform-side reverts its knob to the startup configuration default,
// so deleting the attribute is the undo.
package settings

import (
	"strings"
	"sync"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// KeyLogLevel sets the agent logger level (debug/info/warn/error). It is a
// process-wide singleton, so it is a gateway attribute and cannot be a
// sub-device one.
const KeyLogLevel = "log.level"

// logLevels is the set of logger levels the applier accepts.
var logLevels = map[string]bool{
	"debug": true,
	"info":  true,
	"warn":  true,
	"error": true,
}

// Knobs are the runtime setters, one per known attribute key. A nil knob
// leaves that key unwired: the applier logs a warning and skips it.
type Knobs struct {
	// SetLogLevel changes the process logger level. The applier passes an
	// upper-case level.
	SetLogLevel func(level string) error
	// Logger records applied and rejected values. A nil logger is replaced
	// with a mock.
	Logger logger.LoggingClient
}

// Defaults are the startup configuration values the applier reverts to when
// the key is absent or invalid. They mirror what the agent already runs with,
// so the first reconciliation with an empty attribute set is a no-op.
type Defaults struct {
	LogLevel string
}

// Applier reconciles the gateway's SHARED_SCOPE attributes onto the runtime
// knobs. It is safe for concurrent use: the attribute callbacks fire on the
// paho goroutine and the reconnect sweep may race them.
type Applier struct {
	k Knobs
	d Defaults

	mu  sync.Mutex
	cur string // last log level put in force
}

// New creates an Applier. The current state starts at the default, so only a
// value that actually differs from the startup configuration triggers a knob.
func New(knobs Knobs, defaults Defaults) *Applier {
	if knobs.Logger == nil {
		knobs.Logger = logger.NewMockClient()
	}
	if !isValidLogLevel(defaults.LogLevel) {
		defaults.LogLevel = "INFO"
	}
	return &Applier{k: knobs, d: defaults, cur: normalizeLogLevel(defaults.LogLevel)}
}

// Apply reconciles the gateway's own attribute cache. self is keyed by scope
// (SERVER_SCOPE/SHARED_SCOPE); only SHARED_SCOPE carries the well-known keys.
func (a *Applier) Apply(self map[string]map[string]any) {
	shared := self[device.AttributeScopeShared]

	a.mu.Lock()
	defer a.mu.Unlock()
	a.applyLogLevel(shared)
}

func (a *Applier) applyLogLevel(shared map[string]any) {
	desired := normalizeLogLevel(a.d.LogLevel)
	if raw, ok := shared[KeyLogLevel]; ok {
		s, ok := asString(raw)
		if !ok || !isValidLogLevel(s) {
			a.k.Logger.Warnf("ignoring %s attribute: %v is not one of debug/info/warn/error", KeyLogLevel, raw)
		} else {
			desired = normalizeLogLevel(s)
		}
	}
	if desired == a.cur {
		return
	}
	if a.k.SetLogLevel == nil {
		a.k.Logger.Warnf("no log-level setter wired; %s not applied", KeyLogLevel)
		return
	}
	if err := a.k.SetLogLevel(desired); err != nil {
		a.k.Logger.Errorf("apply %s=%s failed: %s", KeyLogLevel, desired, err.Error())
		return
	}
	a.cur = desired
	a.k.Logger.Infof("applied gateway attribute %s=%s", KeyLogLevel, desired)
}

func normalizeLogLevel(s string) string {
	return strings.ToUpper(strings.TrimSpace(s))
}

func isValidLogLevel(s string) bool {
	return logLevels[strings.ToLower(strings.TrimSpace(s))]
}

// asString coerces a decoded JSON value to a non-empty string.
func asString(v any) (string, bool) {
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	s = strings.TrimSpace(s)
	return s, s != ""
}
