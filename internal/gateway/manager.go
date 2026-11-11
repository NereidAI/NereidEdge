// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package gateway provides the NereidLink gateway client lifecycle
// integrated with the EdgeX Application Service.
package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	device "github.com/nereidai/nereidlink/sdk/device/go"
	nlgateway "github.com/nereidai/nereidlink/sdk/device/go/gateway"
)

// Manager manages gateway state including attached devices and manifest.
type Manager struct {
	mapper  *DeviceMapper
	devices map[string]struct{}
	mu      sync.RWMutex
	ack     *device.HelloAck

	// connectors supplies the full applied connector-instance list for the
	// manifest connector segment. When nil the segment is omitted.
	connectors func(context.Context) []device.ManifestConnectorInstance

	// models supplies the local model inventory bridged from the inference
	// device service (ADR-0039). When nil the segment is omitted.
	models func(context.Context) []device.ManifestModel

	// notify triggers an immediate, debounced manifest send.
	notify chan struct{}
}

// NewManager creates a new gateway manager.
func NewManager(mapper *DeviceMapper) *Manager {
	return &Manager{
		mapper:  mapper,
		devices: make(map[string]struct{}),
		notify:  make(chan struct{}, 1),
	}
}

// SetConnectorProvider registers the source of applied connector instances.
func (m *Manager) SetConnectorProvider(fn func(context.Context) []device.ManifestConnectorInstance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectors = fn
}

// SetModelProvider registers the inventory source for the manifest models
// segment (ADR-0039). The gateway itself owns no models; the provider bridges
// the inventory the inference device service reports through EdgeX readings.
// The segment is sent only when the platform accepted the inference feature
// (ack block present), so an older platform never gets a models segment.
func (m *Manager) SetModelProvider(fn func(context.Context) []device.ManifestModel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.models = fn
}

// InferenceNegotiated reports whether the current session accepted the edge
// model-inference capability (the hello ack carries the echoed block).
func (m *Manager) InferenceNegotiated() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ack != nil && m.ack.Inference != nil
}

// NotifyManifestChange requests an immediate manifest send. Coalesced: bursts
// of specs (e.g. hello replay) result in one manifest a few seconds later.
func (m *Manager) NotifyManifestChange() {
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

// SetAck stores negotiation result.
func (m *Manager) SetAck(ack *device.HelloAck) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ack = ack
}

// AddDevice adds a device to attached set.
func (m *Manager) AddDevice(nlDeviceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.devices[nlDeviceID] = struct{}{}
}

// AddDevices adds multiple devices to attached set.
func (m *Manager) AddDevices(nlDeviceIDs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, d := range nlDeviceIDs {
		m.devices[d] = struct{}{}
	}
}

// ClearDevices removes every tracked attached device. It is used on
// gateway session loss before presence is replayed.
func (m *Manager) ClearDevices() {
	m.mu.Lock()
	m.devices = make(map[string]struct{})
	m.mu.Unlock()
}

// RemoveDevice removes a device from attached set.
func (m *Manager) RemoveDevice(nlDeviceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.devices, nlDeviceID)
}

// GetDevices returns a copy of attached device list.
func (m *Manager) GetDevices() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	devs := make([]string, 0, len(m.devices))
	for d := range m.devices {
		devs = append(devs, d)
	}
	return devs
}

// BuildManifest builds a manifest for current attached devices.
func (m *Manager) BuildManifest(ctx context.Context) device.Manifest {
	devs := m.GetDevices()
	manifest := device.Manifest{
		Devices: devs,
	}
	m.mu.RLock()
	provider := m.connectors
	m.mu.RUnlock()
	if provider != nil {
		instances := provider(ctx)
		manifest.Connectors = &instances
	}
	// ADR-0039: the models segment rides the negotiated feature only. An
	// explicit non-nil slice is an authoritative (possibly empty) inventory;
	// the provider being absent keeps the segment off the wire.
	var modelProvider func(context.Context) []device.ManifestModel
	var negotiated bool
	m.mu.RLock()
	modelProvider = m.models
	negotiated = m.ack != nil && m.ack.Inference != nil
	m.mu.RUnlock()
	if modelProvider != nil && negotiated {
		if inv := modelProvider(ctx); inv != nil {
			// A nil return means "read failed/unavailable": omit the key
			// entirely so NL keeps stored inventory. Attaching &nil would
			// marshal as "models":null, which is neither the omission nor
			// the authoritative [] and is handled inconsistently cloud-side.
			manifest.Models = &inv
		}
	}
	return manifest
}

// boundaryIndex returns the wall-clock boundary-grid index containing t.
// The grid is aligned to the Unix epoch, so every gateway computes the same
// index for the same instant and reports on the same 5:00/10:00/... grid.
func boundaryIndex(t time.Time, interval time.Duration) int64 {
	return t.UnixNano() / int64(interval)
}

// nextBoundary returns the first boundary-grid instant strictly after now.
// Protocol v1 §6 requires the manifest epoch to fire on these absolute
// boundaries rather than on a per-process relative ticker.
func nextBoundary(now time.Time, interval time.Duration) time.Time {
	if interval <= 0 {
		interval = time.Minute
	}
	return time.Unix(0, (boundaryIndex(now, interval)+1)*int64(interval))
}

// boundaryGap reports how many full boundary-grid steps were skipped between
// two consecutive aligned sends. lastIndex is the grid index of the previous
// aligned send, idx the current one; -1 means no previous aligned send (a
// first send cannot have missed anything).
func boundaryGap(lastIndex, idx int64) int {
	if lastIndex < 0 || idx <= lastIndex {
		return 0
	}
	return int(idx - lastIndex - 1)
}

// StartManifestLoop starts periodic manifest sending aligned to absolute
// wall-clock boundaries of interval (protocol v1 §6: every 5:00/10:00/...).
// An initial manifest is sent shortly after start so reconciliation state
// exists right after hello. Change notifications trigger a debounced
// immediate send so newly applied specs converge in seconds instead of at the
// next boundary. Every skipped aligned boundary before an actual send is
// reported as missed_cycles in the frame.
func (m *Manager) StartManifestLoop(ctx context.Context, gwClient *Client, interval time.Duration, lc logger.LoggingClient) {
	const debounce = 3 * time.Second
	initial := time.NewTimer(10 * time.Second)
	defer initial.Stop()
	var boundaryC <-chan time.Time
	var debounceC <-chan time.Time
	// A zero-value time.Timer panics on Stop in Go 1.26, so start from a real
	// timer whose first firing is the next boundary.
	timer := time.NewTimer(time.Until(nextBoundary(time.Now(), interval)))
	boundaryC = timer.C
	arm := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(time.Until(nextBoundary(time.Now(), interval)))
		boundaryC = timer.C
	}
	// Boundary index of the last grid-aligned frame we sent. -1 means no
	// grid-aligned frame yet (first send cannot have missed anything).
	lastIndex := int64(-1)
	// missed is reported on the next frame: full boundary-grid steps
	// skipped since the last aligned send.
	missed := 0
	send := func(aligned bool) {
		if gwClient.Gateway() == nil {
			return
		}
		if aligned {
			idx := boundaryIndex(time.Now(), interval)
			if gap := boundaryGap(lastIndex, idx); gap > 0 {
				missed = gap
			} else {
				missed = 0
			}
			lastIndex = idx
		}
		mf := m.BuildManifest(ctx)
		mf.MissedCycles = missed
		missed = 0
		connCount := 0
		if mf.Connectors != nil {
			connCount = len(*mf.Connectors)
		}
		modelCount := 0
		if mf.Models != nil {
			modelCount = len(*mf.Models)
		}
		if lc != nil {
			lc.Infof("Sending manifest: devices=%d connectors=%d models=%d missed_cycles=%d",
				len(mf.Devices), connCount, modelCount, mf.MissedCycles)
		}
		if err := gwClient.Gateway().SendManifest(ctx, mf, 1); err != nil && lc != nil {
			lc.Errorf("Failed to send manifest: %s", err.Error())
		}
	}
	arm()
	for {
		select {
		case <-ctx.Done():
			return
		case <-initial.C:
			send(false)
			arm()
		case <-boundaryC:
			send(true)
			arm()
		case <-m.notify:
			if debounceC == nil {
				debounceC = time.After(debounce)
			}
		case <-debounceC:
			debounceC = nil
			// Drain notifications coalesced during the wait.
			select {
			case <-m.notify:
			default:
			}
			send(false)
		}
	}
}

// EnsureDevice ensures device is tracked and connected. It returns true when
// the device was newly attached on this manager instance.
func (m *Manager) EnsureDevice(ctx context.Context, gw *nlgateway.Client, nlDeviceID string) bool {
	if nlDeviceID == "" || gw == nil {
		return false
	}
	m.mu.Lock()
	_, exists := m.devices[nlDeviceID]
	if !exists {
		m.devices[nlDeviceID] = struct{}{}
	}
	m.mu.Unlock()
	if !exists {
		if err := gw.ConnectDevices(ctx, []string{nlDeviceID}, 1); err != nil {
			return false
		}
	}
	return !exists
}

// DisconnectAll disconnects all tracked devices.
func (m *Manager) DisconnectAll(ctx context.Context, gw *nlgateway.Client) {
	devs := m.GetDevices()
	if len(devs) == 0 || gw == nil {
		return
	}
	_ = gw.DisconnectDevices(ctx, devs, 1)
}
