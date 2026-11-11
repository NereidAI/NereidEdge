// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package shadow implements the gateway's own device shadow (ADR-0011) over
// the direct-device MQTT identity.
//
// Unlike a sub-device, the gateway's reported state is its real runtime state,
// not an echo of platform desired state. The handler builds a snapshot of the
// agent (build/protocol/features, effective gateway knobs, link counters and
// attached-device count) and publishes it on v1/me/shadow/report: on every
// (re)connect and whenever any of those facts change. The platform reads it to
// observe the agent; it is the only writer of that state.
//
// The gateway has no runtime consumer for its own desired state: gateway knobs
// (log.level and friends) arrive as SHARED_SCOPE attributes, so a desired push
// is tracked (for the reported version) rather than applied. A pushed version
// is echoed back on the next report so the platform can measure drift.
//
// Only the gateway's own identity participates: sub-device shadow (the
// v1/gateway/shadow/* topics) is out of scope.
package shadow

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

// reportTimeout bounds the publish of one shadow report.
const reportTimeout = 10 * time.Second

// StateFunc builds the gateway's current reported-state snapshot. It must
// return a fresh map on every call so the handler may add negotiated fields
// without mutating shared state.
type StateFunc func() map[string]any

// Client is the subset of the device MQTT client the handler uses; it is an
// interface so the report path is unit-testable without a broker.
type Client interface {
	SubscribeShadowDesired(ctx context.Context, qos byte, onPush func(device.DesiredPush)) error
	ReportShadow(ctx context.Context, patch device.ReportedPatch, qos byte) error
}

// Handler publishes the gateway's real reported state and tracks the desired
// version the platform has pushed. It is safe for concurrent use: reports are
// triggered from the paho callback goroutine, the health monitor and reconnect.
type Handler struct {
	mq    Client
	lc    logger.LoggingClient
	state StateFunc

	mu         sync.Mutex
	version    int64
	hasVersion bool
	proto      string
	features   []string
	lastSent   string // de-dupe key of the last successfully published report
}

// New creates a shadow Handler.
func New(mq Client, lc logger.LoggingClient, state StateFunc) *Handler {
	if lc == nil {
		lc = logger.NewMockClient()
	}
	return &Handler{mq: mq, lc: lc, state: state}
}

// Start subscribes the desired-state handler. It must run before hello and be
// re-issued after every reconnect, because the SDK uses a clean session and a
// transport reconnect drops every broker-side subscription.
func (h *Handler) Start(ctx context.Context, qos byte) error {
	if h.mq == nil {
		return nil
	}
	return h.mq.SubscribeShadowDesired(ctx, qos, h.onDesired)
}

// SetIdentity records the negotiated protocol version and the enabled feature
// intersection reported in the shadow. It is called after every (re)hello.
func (h *Handler) SetIdentity(proto string, features []string) {
	h.mu.Lock()
	h.proto = proto
	h.features = append([]string(nil), features...)
	h.mu.Unlock()
}

// onDesired records the pushed desired version and re-publishes the real
// reported state so the platform learns the version has been processed. A
// stale (non-monotonic) version is ignored.
func (h *Handler) onDesired(push device.DesiredPush) {
	h.mu.Lock()
	stale := h.hasVersion && push.Version < h.version
	if !stale {
		h.version = push.Version
		h.hasVersion = true
	}
	h.mu.Unlock()
	if stale {
		h.lc.Debugf("shadow: ignoring stale desired version %d", push.Version)
		return
	}
	h.Report(false)
}

// Report publishes the gateway's real reported state. force bypasses the
// change de-duplication and is used after every (re)connect, when the broker
// has forgotten the previously reported state.
func (h *Handler) Report(force bool) {
	if h.mq == nil || h.state == nil {
		return
	}

	h.mu.Lock()
	state := h.state()
	if h.proto != "" {
		state["proto"] = h.proto
	}
	state["features"] = append([]string{}, h.features...)
	encoded, err := json.Marshal(state)
	if err != nil {
		h.mu.Unlock()
		h.lc.Errorf("shadow: encode reported state failed: %s", err.Error())
		return
	}
	key := string(encoded)
	if h.hasVersion {
		key = strconv.FormatInt(h.version, 10) + ":" + key
	}
	if !force && h.lastSent == key {
		h.mu.Unlock()
		return
	}
	patch := device.NewReportedPatch(state)
	if h.hasVersion {
		patch = device.NewReportedPatchForVersion(state, h.version)
	}
	h.lastSent = key
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	if err := h.mq.ReportShadow(ctx, patch, 1); err != nil {
		// Drop the de-dupe key so the next trigger retries the same state.
		h.mu.Lock()
		if h.lastSent == key {
			h.lastSent = ""
		}
		h.mu.Unlock()
		h.lc.Errorf("shadow: report failed: %s", err.Error())
		return
	}
	h.lc.Infof("shadow: reported %d field(s)", len(state))
}
