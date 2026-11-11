// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package inferencebridge polls the singleton NereidInference manager device
// through ordinary core-command reads and exposes its ModelsJSON payload as
// the ADR-0039 manifest models segment. NereidEdge owns no model packages and
// performs no inference: the device service is the sole owner of package
// lifecycle, runtime readiness and binding counts; this package only
// translates its EdgeX reading into the SDK inventory type.
//
// Safety rule: a failed manager read must never become an authoritative zero
// inventory. The bridge keeps the last good payload but reports it as
// unavailable until a fresh read succeeds, so a transient device-service or
// core-command outage omits the manifest segment instead of pruning NL.
package inferencebridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

const (
	// defaultInterval matches the NL stale watchdog contract: the edge refreshes
	// well inside the 3 x probe_interval silence window.
	defaultInterval = 30 * time.Second
	// readQuery keeps the manager poll off the message bus while still
	// returning the event in the HTTP response.
	readQuery = common.PushEvent + "=false&" + common.ReturnEvent + "=true"

	runtimeOnline   = "ONLINE"
	runtimeDegraded = "DEGRADED"
	runtimeOffline  = "OFFLINE"
	runtimeUnknown  = "UNKNOWN"
)

// CoreReader is the core-command subset used by the bridge.
type CoreReader interface {
	Read(ctx context.Context, edgexName, commandName, rawQuery string) (responses.EventResponse, int, error)
}

// Logger is the logging surface required by background polling.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
	Debugf(format string, args ...any)
}

// Config configures the manager-device bridge.
type Config struct {
	Core          CoreReader
	ManagerDevice string
	Resource      string
	Interval      time.Duration
	// Negotiated reports whether the current hello ack accepted the
	// inference feature. Before negotiation the bridge performs no reads and
	// provides no segment, so old-platform sessions behave exactly as before.
	Negotiated func() bool
	OnChange   func()
	Logger     Logger
}

// RuntimeStatus is the DS-derived aggregate used by the gateway shadow.
type RuntimeStatus struct {
	Available      bool
	State          string
	Protocol       string
	ModelsReady    int
	ModelsTotal    int
	ActiveBindings int
	CheckedAt      time.Time
	ReadLatencyMS  float64
}

// Bridge caches the last successful manager-device inventory.
type Bridge struct {
	core       CoreReader
	deviceName string
	resource   string
	interval   time.Duration
	negotiated func() bool
	onChange   func()
	lc         Logger

	mu        sync.RWMutex
	models    []device.ManifestModel
	available bool
	checkedAt time.Time
	latencyMS float64
}

// New creates a bridge with protocol defaults applied to empty fields.
func New(cfg Config) *Bridge {
	if cfg.ManagerDevice == "" {
		cfg.ManagerDevice = "nereid-inference"
	}
	if cfg.Resource == "" {
		cfg.Resource = "ModelsJSON"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.Negotiated == nil {
		cfg.Negotiated = func() bool { return false }
	}
	return &Bridge{
		core:       cfg.Core,
		deviceName: cfg.ManagerDevice,
		resource:   cfg.Resource,
		interval:   cfg.Interval,
		negotiated: cfg.Negotiated,
		onChange:   cfg.OnChange,
		lc:         cfg.Logger,
	}
}

// Start performs an immediate refresh when negotiated and polls until ctx is
// cancelled. Polling pauses automatically while the feature is not negotiated.
func (b *Bridge) Start(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	b.refresh(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.refresh(ctx)
		}
	}
}

// Refresh forces one read; it is used after a fresh (re)hello without waiting
// for the periodic boundary.
func (b *Bridge) Refresh(ctx context.Context) { b.refresh(ctx) }

// Models returns the last authoritative inventory. The boolean is false until
// the most recent manager read succeeded, in which case callers must omit the
// manifest models segment entirely (never send an empty slice on failure).
//
// On a successful read of a zero inventory the returned slice is non-nil and
// empty: callers distinguish "authoritative []" (prune every row) from
// "unavailable" (omit the segment). append() on a zero-length source would
// produce a nil slice, which would marshal as JSON null instead of [], so the
// copy is sized explicitly.
func (b *Bridge) Models() ([]device.ManifestModel, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.available {
		return nil, false
	}
	out := make([]device.ManifestModel, len(b.models))
	copy(out, b.models)
	return out, true
}

// ApplyBindingCounts overwrites the DS-reported per-model active_bindings
// with the NE-owned count of live edge-inference specs. The device service
// counts every Job device it hosts, including hand-built and orphaned jobs
// that are not bound to any cloud spec; only the NE knows which jobs exist to
// serve a current edge-inference binding, so §15.2 inventory must carry the
// NE-authoritative number.
//
// counts is keyed by model id + "\x00" + semantic version. Specs that float
// (empty version) are attributed to the inventory entry with that id only
// when exactly one installed version matches; otherwise their count cannot
// be assigned safely and is dropped rather than guessed.
func ApplyBindingCounts(models []device.ManifestModel, counts map[string]int) []device.ManifestModel {
	if len(models) == 0 {
		return models
	}
	floating := make(map[string]int)
	for key, n := range counts {
		id, version, ok := strings.Cut(key, "\x00")
		if !ok {
			continue
		}
		if version == "" {
			floating[id] += n
		}
	}
	out := make([]device.ManifestModel, len(models))
	copy(out, models)
	idIndex := make(map[string][]int, len(out))
	for i := range out {
		out[i].ActiveBindings = 0
		idIndex[out[i].ID] = append(idIndex[out[i].ID], i)
	}
	for i := range out {
		if n, ok := counts[out[i].ID+"\x00"+out[i].Version]; ok {
			out[i].ActiveBindings += n
		}
	}
	for id, n := range floating {
		if idx := idIndex[id]; len(idx) == 1 {
			out[idx[0]].ActiveBindings += n
		}
	}
	return out
}

// LookupModel reports whether one cached inventory entry is READY.
func (b *Bridge) LookupModel(id, version string) (device.ManifestModel, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, m := range b.models {
		if m.ID == id && (version == "" || m.Version == version) {
			return m, m.State == device.ModelStateReady
		}
	}
	return device.ManifestModel{}, false
}

// Status returns the runtime-plane aggregate derived from the last read.
func (b *Bridge) Status() RuntimeStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	status := RuntimeStatus{
		Available:      b.available,
		ModelsReady:    countReady(b.models),
		ModelsTotal:    len(b.models),
		ActiveBindings: sumBindings(b.models),
		Protocol:       dominantProtocol(b.models),
		CheckedAt:      b.checkedAt,
		ReadLatencyMS:  b.latencyMS,
	}
	switch {
	case !b.available:
		status.State = runtimeOffline
	case len(b.models) == 0:
		status.State = runtimeUnknown
	case status.ModelsReady == 0:
		status.State = runtimeDegraded
	default:
		status.State = runtimeOnline
	}
	return status
}

// IntervalS exposes the configured probe interval for the shadow block.
func (b *Bridge) IntervalS() int { return int(b.interval / time.Second) }

func (b *Bridge) refresh(ctx context.Context) {
	if !b.negotiated() {
		return
	}
	started := time.Now()
	event, status, err := b.core.Read(ctx, b.deviceName, b.resource, readQuery)
	latency := float64(time.Since(started).Microseconds()) / 1000.0
	if err != nil {
		b.markUnavailable(latency, fmt.Sprintf("read manager device: %v", err))
		return
	}
	if status != http.StatusOK {
		b.markUnavailable(latency, fmt.Sprintf("manager read HTTP %d", status))
		return
	}
	raw := readingValue(event.Event.Readings, b.resource)
	if raw == "" {
		b.markUnavailable(latency, "manager event lacks ModelsJSON reading")
		return
	}
	models, err := decodeModels(raw)
	if err != nil {
		b.markUnavailable(latency, err.Error())
		return
	}
	changed := b.replace(models, latency)
	if changed && b.onChange != nil {
		b.onChange()
	}
}

func (b *Bridge) markUnavailable(latency float64, message string) {
	if b.lc != nil {
		b.lc.Warnf("inference inventory unavailable: %s", message)
	}
	b.mu.Lock()
	b.available = false
	b.checkedAt = time.Now().UTC()
	b.latencyMS = latency
	b.mu.Unlock()
	if b.onChange != nil {
		// A DS outage also changes the runtime shadow state; the manifest
		// segment itself stays omitted by Models(), so this cannot prune NL.
		b.onChange()
	}
}

func (b *Bridge) replace(next []device.ManifestModel, latency float64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	changed := !sameModels(b.models, next)
	b.models = next
	b.available = true
	b.checkedAt = time.Now().UTC()
	b.latencyMS = latency
	return changed
}

// decodeModels parses the DS-local JSON contract. Its field set is intentionally
// identical to device.ManifestModel; the explicit local struct keeps the
// boundary tolerant of one side adding an unknown JSON field.
func decodeModels(raw string) ([]device.ManifestModel, error) {
	var entries []struct {
		ID             string   `json:"id"`
		Version        string   `json:"version"`
		Revision       string   `json:"revision"`
		Task           string   `json:"task"`
		Family         string   `json:"family"`
		Backend        string   `json:"backend"`
		ModelName      string   `json:"model_name"`
		State          string   `json:"state"`
		Reason         string   `json:"reason"`
		Labels         []string `json:"labels"`
		ActiveBindings int      `json:"active_bindings"`
	}
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("decode ModelsJSON: %w", err)
	}
	models := make([]device.ManifestModel, 0, len(entries))
	for _, e := range entries {
		if e.ID == "" || e.Version == "" {
			return nil, fmt.Errorf("inventory entry lacks id/version")
		}
		if !validState(e.State) {
			return nil, fmt.Errorf("inventory entry %s/%s has unknown state %q", e.ID, e.Version, e.State)
		}
		models = append(models, device.ManifestModel{
			ID:             e.ID,
			Version:        e.Version,
			Revision:       e.Revision,
			Task:           e.Task,
			Family:         e.Family,
			Backend:        e.Backend,
			ModelName:      e.ModelName,
			State:          e.State,
			Reason:         e.Reason,
			Labels:         append([]string(nil), e.Labels...),
			ActiveBindings: e.ActiveBindings,
		})
	}
	return models, nil
}

func validState(state string) bool {
	switch state {
	case device.ModelStateExtracting, device.ModelStateValidating,
		device.ModelStateReady, device.ModelStateFailed, device.ModelStateRemoved:
		return true
	default:
		return false
	}
}

func readingValue(readings []dtos.BaseReading, name string) string {
	for _, reading := range readings {
		if reading.ResourceName == name {
			return reading.Value
		}
	}
	return ""
}

func sameModels(a, b []device.ManifestModel) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

func countReady(models []device.ManifestModel) int {
	var n int
	for _, m := range models {
		if m.State == device.ModelStateReady {
			n++
		}
	}
	return n
}

func sumBindings(models []device.ManifestModel) int {
	var n int
	for _, m := range models {
		n += m.ActiveBindings
	}
	return n
}

// dominantProtocol picks kserve ahead of openai when a deployment serves both
// families; the value is a protocol family, never a vendor/server name.
func dominantProtocol(models []device.ManifestModel) string {
	hasOpenAI := false
	for _, m := range models {
		if m.Backend == "kserve" {
			return "kserve"
		}
		if m.Backend == "openai" {
			hasOpenAI = true
		}
	}
	if hasOpenAI {
		return "openai"
	}
	return ""
}
