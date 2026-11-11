// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package probe answers NL on-demand sub-device probes.
//
// NL groups the devices of one task by gateway and sends a single aggregated
// frame on v1/gateway/probe (device.ProbeRequest). The gateway reads each
// device through EdgeX core-command and reports one response frame per device
// on v1/gateway/probe/response, carrying either an opaque result or an error.
//
// The platform passes mode and result through untouched, so the semantics of
// mode live here:
//
//   - fresh (default): read live, trying every readable command until one
//     returns at least one reading. A 200 with no readings is inconclusive and
//     never counts as reachable.
//   - strict: read live, but only the device's first readable command, with no
//     fallback; it must return a non-empty reading set.
//   - cached: reuse the last successful probe result for the device while it
//     is within the cache TTL, otherwise behave like fresh.
package probe

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	"github.com/nereidai/nereidedge/internal/binding"
	"github.com/nereidai/nereidedge/internal/corecmd"
	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// defaultFrameTimeout bounds a whole aggregated probe when the frame carries
// no timeout_ms, mirroring the platform default.
const defaultFrameTimeout = 10 * time.Second

// defaultCacheTTL is how long a successful result stays answerable in cached
// mode.
const defaultCacheTTL = 30 * time.Second

// reportTimeout bounds publishing one per-device response. It is deliberately
// independent of the probe deadline so the failure frame still goes out when a
// device check exhausts timeout_ms.
const reportTimeout = 5 * time.Second

// Per-device failure reasons reported in the response `error` field. The set
// is intentionally small and stable; NL treats any non-empty error as a
// timeout outcome but the console renders the string verbatim.
const (
	reasonNotAttached = "device not attached to this gateway"
	reasonTimeout     = "timeout"
	reasonNoResponse  = "no response"
	reasonNoReadings  = "no readings"
	reasonNoCommand   = "no readable command"
)

// probeQuery keeps the probe reading off the message bus (ds-pushevent=false)
// while still returning the event in the HTTP response, so a probe can require
// a non-empty reading set. A 200 without readings means the device service did
// not actually produce device data.
const probeQuery = common.PushEvent + "=false&" + common.ReturnEvent + "=true"

// Gateway is the subset of the NereidLink gateway client surface the probe
// handler needs. *nlgateway.Client satisfies it; tests substitute a recording
// fake.
type Gateway interface {
	SubscribeProbes(ctx context.Context, qos byte, onRequest func(device.ProbeRequest)) error
	ProbeResult(ctx context.Context, probeID, deviceID string, result any, qos byte) error
	ProbeError(ctx context.Context, probeID, deviceID, reason string, qos byte) error
}

// Config carries the dependencies for the probe Handler.
type Config struct {
	// Gateway is the connected NereidLink gateway client.
	Gateway Gateway
	// Resolver maps NL sub-device UUIDs to EdgeX device names.
	Resolver *binding.Resolver
	// CoreCommandURL is the base URL of the EdgeX core-command service.
	// Defaults to corecmd.DefaultURL when empty.
	CoreCommandURL string
	// Logger is the application logging client.
	Logger logger.LoggingClient
	// Timeout bounds a whole probe frame when it carries no timeout_ms.
	Timeout time.Duration
	// CacheTTL is how long a successful result stays answerable in cached
	// mode.
	CacheTTL time.Duration
}

// probeResult is the opaque per-device success payload reported to the
// platform. Its shape is gateway-defined.
type probeResult struct {
	// Reachable is always true on the success path; failures use the error
	// channel instead.
	Reachable bool `json:"reachable"`
	// RTTMs is the measured read round-trip in milliseconds.
	RTTMs int64 `json:"rtt_ms"`
	// Resource is the readable command that produced the readings.
	Resource string `json:"resource,omitempty"`
	// Values maps each returned resource to its typed value.
	Values map[string]any `json:"values,omitempty"`
	// Cached is set when the result was served from the probe cache.
	Cached bool `json:"cached,omitempty"`
	// AgeMS is the age of a cached result in milliseconds.
	AgeMS int64 `json:"age_ms,omitempty"`
}

// cacheEntry is a successful result retained for cached-mode probes.
type cacheEntry struct {
	result probeResult
	at     time.Time
}

// Handler answers aggregated NL on-demand probes.
type Handler struct {
	gw       Gateway
	resolver *binding.Resolver
	core     *corecmd.Client
	timeout  time.Duration
	cacheTTL time.Duration
	qos      byte
	lc       logger.LoggingClient

	mu    sync.Mutex
	cache map[string]cacheEntry
}

// New creates a probe handler.
func New(cfg Config) *Handler {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultFrameTimeout
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = defaultCacheTTL
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewMockClient()
	}
	return &Handler{
		gw:       cfg.Gateway,
		resolver: cfg.Resolver,
		core:     corecmd.New(cfg.CoreCommandURL, 0),
		timeout:  cfg.Timeout,
		cacheTTL: cfg.CacheTTL,
		lc:       cfg.Logger,
		cache:    make(map[string]cacheEntry),
	}
}

// Start subscribes to NL probe downlinks. Each frame is handled on its own
// goroutine so a slow device read never blocks the MQTT dispatch.
func (h *Handler) Start(ctx context.Context, qos byte) error {
	if h.gw == nil {
		h.lc.Warn("probe handler has no gateway; skipping subscription")
		return nil
	}
	h.qos = qos
	return h.gw.SubscribeProbes(ctx, qos, func(req device.ProbeRequest) {
		go h.Handle(req)
	})
}

// Handle processes one aggregated probe frame: every device is probed
// concurrently under a single frame deadline, and Handle blocks until all
// per-device responses have been reported.
func (h *Handler) Handle(req device.ProbeRequest) {
	timeout := h.timeout
	if req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup
	seen := make(map[string]bool, len(req.Devices))
	for _, id := range req.Devices {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		wg.Add(1)
		go func(deviceID string) {
			defer wg.Done()
			h.probeDevice(ctx, req.ProbeID, deviceID, req.Mode)
		}(id)
	}
	wg.Wait()
}

// probeDevice reverse-resolves the NL sub-device UUID, probes it and reports
// exactly one response frame.
func (h *Handler) probeDevice(ctx context.Context, probeID, deviceID, mode string) {
	edgexName, ok := h.resolver.Name(deviceID)
	if !ok {
		h.reportError(probeID, deviceID, reasonNotAttached)
		return
	}
	result, err := h.probe(ctx, edgexName, mode)
	if err != nil {
		h.lc.Debugf("probe %s device %s (%s) failed: %s", probeID, deviceID, edgexName, err.Error())
		h.reportError(probeID, deviceID, err.Error())
		return
	}
	h.reportResult(probeID, deviceID, result)
}

// probe runs one device probe under the requested mode.
func (h *Handler) probe(ctx context.Context, edgexName, mode string) (probeResult, error) {
	if mode == device.ProbeModeCached {
		if cached, ok := h.cached(edgexName); ok {
			return cached, nil
		}
	}
	// strict forbids fallback; fresh and cached fall through to the full
	// candidate set.
	return h.liveRead(ctx, edgexName, mode != device.ProbeModeStrict)
}

// liveRead reads the device through core-command. When allowFallback is set it
// tries every readable command until one returns a reading; otherwise it tries
// only the first. A successful result is cached for cached-mode probes.
func (h *Handler) liveRead(ctx context.Context, edgexName string, allowFallback bool) (probeResult, error) {
	dc, status, err := h.core.DeviceCoreCommands(ctx, edgexName)
	if err != nil {
		h.lc.Debugf("probe device %s: device commands failed: %v (http %d)", edgexName, err, status)
		return probeResult{}, probeReason(ctx, reasonNoResponse)
	}
	candidates := readableCommands(dc)
	if len(candidates) == 0 {
		return probeResult{}, errors.New(reasonNoCommand)
	}
	if !allowFallback {
		candidates = candidates[:1]
	}

	inconclusive := false
	for _, command := range candidates {
		start := time.Now()
		envelope, status, err := h.core.Read(ctx, edgexName, command, probeQuery)
		rtt := time.Since(start).Milliseconds()
		if err != nil || status != http.StatusOK {
			h.lc.Debugf("probe device %s command %s failed: %v (http %d)", edgexName, command, err, status)
			if ctx.Err() != nil {
				return probeResult{}, errors.New(reasonTimeout)
			}
			continue
		}
		if len(envelope.Event.Readings) == 0 {
			h.lc.Debugf("probe device %s command %s returned 200 with no readings", edgexName, command)
			inconclusive = true
			continue
		}
		result := probeResult{
			Reachable: true,
			RTTMs:     rtt,
			Resource:  command,
			Values:    corecmd.ReadingsToMap(envelope.Event.Readings),
		}
		h.store(edgexName, result)
		return result, nil
	}
	if inconclusive {
		return probeResult{}, errors.New(reasonNoReadings)
	}
	return probeResult{}, probeReason(ctx, reasonNoResponse)
}

// probeReason returns the timeout reason when the probe context has expired,
// otherwise the supplied fallback reason, so an exhausted deadline is always
// reported as a timeout.
func probeReason(ctx context.Context, fallback string) error {
	if ctx.Err() != nil {
		return errors.New(reasonTimeout)
	}
	return errors.New(fallback)
}

// cached returns the last successful probe for a device when still within the
// cache TTL.
func (h *Handler) cached(edgexName string) (probeResult, bool) {
	h.mu.Lock()
	entry, ok := h.cache[edgexName]
	h.mu.Unlock()
	if !ok {
		return probeResult{}, false
	}
	age := time.Since(entry.at)
	if age > h.cacheTTL {
		return probeResult{}, false
	}
	result := entry.result
	result.Cached = true
	result.AgeMS = age.Milliseconds()
	return result, true
}

// store retains a successful result for cached-mode probes.
func (h *Handler) store(edgexName string, result probeResult) {
	h.mu.Lock()
	h.cache[edgexName] = cacheEntry{result: result, at: time.Now()}
	h.mu.Unlock()
}

// reportResult publishes one successful per-device probe under its own short
// deadline so it is not starved by an exhausted probe deadline.
func (h *Handler) reportResult(probeID, deviceID string, result probeResult) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	if err := h.gw.ProbeResult(ctx, probeID, deviceID, result, h.qos); err != nil {
		h.lc.Errorf("report probe result %s device %s failed: %s", probeID, deviceID, err.Error())
	}
}

// reportError publishes one failed per-device probe under its own short
// deadline so the failure reason still reaches NL after a probe timeout.
func (h *Handler) reportError(probeID, deviceID, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	if err := h.gw.ProbeError(ctx, probeID, deviceID, reason, h.qos); err != nil {
		h.lc.Errorf("report probe error %s device %s failed: %s", probeID, deviceID, err.Error())
	}
}

// readableCommands returns the readable command names in device order.
func readableCommands(dc dtos.DeviceCoreCommand) []string {
	out := make([]string, 0, len(dc.CoreCommands))
	for i := range dc.CoreCommands {
		if dc.CoreCommands[i].Get {
			out = append(out, dc.CoreCommands[i].Name)
		}
	}
	return out
}
