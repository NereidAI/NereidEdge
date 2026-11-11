// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package healthmonitor verifies sub-device reachability and drives the
// gateway presence announcements to NereidLink.
//
// Two probe regimes coexist:
//   - Per-instance probes: a connector spec carrying health_check runs one
//     independent loop per device with its own resource/interval/timeout/
//     threshold. Probe readings are discarded (ds-pushevent=false); a probe
//     only confirms reachability when the response event carries readings, and
//     a 200 without readings is treated as inconclusive. Only state edges are
//     announced (connect on recovery, disconnect after N consecutive
//     failures).
//   - Legacy global probes: without health_check the edge keeps the previous
//     global-switch behavior with candidate enumeration across readable
//     commands.
//
// Local resources (GPIO) skip probing and follow the gateway lifecycle.
package healthmonitor

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	"github.com/nereidai/nereidedge/internal/corecmd"
	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// Default tuning values applied when policies leave them zero.
const (
	defaultInterval  = 30 * time.Second
	defaultThreshold = 3
	defaultProbeTime = 10 * time.Second
	// defaultInstanceProbeTime aligns with the NL-published example timeout.
	defaultInstanceProbeTime = 5 * time.Second

	// probeConcurrency bounds how many legacy devices are probed
	// simultaneously during one periodic sweep.
	probeConcurrency = 4
	// actionTimeout bounds an online/offline presence announcement.
	actionTimeout = 10 * time.Second
)

// ProfileLookup fetches an EdgeX device profile by name so the legacy probe
// can find a resource marked for health checks. It is implemented by
// *profile.Fetcher.GetByName.
type ProfileLookup func(ctx context.Context, profileName string) (dtos.DeviceProfile, error)

// PresenceFunc announces one sub-device attachment or detachment.
type PresenceFunc func(ctx context.Context, nlUUID string) error

// Config carries the dependencies for a Monitor.
type Config struct {
	// Core is the EdgeX core-command client.
	Core *corecmd.Client
	// LookupProfile resolves EdgeX profiles for the legacy health-check
	// marker. When nil the legacy probe falls back to the first readable
	// command.
	LookupProfile ProfileLookup
	// Connect announces a device coming online.
	Connect PresenceFunc
	// Disconnect announces a device going offline.
	Disconnect PresenceFunc
	// Notify requests an immediate manifest reconciliation.
	Notify func()
	// GlobalEnabled controls the legacy regime: true makes specs without
	// health_check probe via candidate enumeration; false treats them online
	// once applied.
	GlobalEnabled bool
	// Interval is the legacy sweep cadence. Zero/negative selects defaultInterval.
	Interval time.Duration
	// FailureThreshold is the legacy consecutive-failure count. Zero/negative
	// selects defaultThreshold.
	FailureThreshold int
	// ProbeTimeout bounds one legacy probe read. Zero/negative selects defaultProbeTime.
	ProbeTimeout time.Duration
	// Logger is the application logging client.
	Logger logger.LoggingClient
}

// deviceState is the tracked state of one sub-device.
type deviceState struct {
	edgexName   string
	uuid        string
	profileName string
	preferred   string
	local       bool
	online      bool
	failures    int
	probe       string
	hc          *device.ConnectorHealthCheck
	stop        chan struct{}
	generation  int
}

// probePolicy is the resolved per-instance probe policy.
type probePolicy struct {
	resource  string
	interval  time.Duration
	timeout   time.Duration
	threshold int
}

// Monitor tracks sub-device reachability and drives presence transitions.
type Monitor struct {
	cfg      Config
	root     context.Context
	mu       sync.Mutex
	devices  map[string]*deviceState
	inflight map[string]struct{}
	nextGen  int
}

// New creates a health monitor from cfg, applying defaults for zero tuning.
func New(cfg Config) *Monitor {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = defaultThreshold
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = defaultProbeTime
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewMockClient()
	}
	if cfg.Notify == nil {
		cfg.Notify = func() {}
	}
	return &Monitor{
		cfg:      cfg,
		root:     context.Background(),
		devices:  make(map[string]*deviceState),
		inflight: make(map[string]struct{}),
	}
}

// Start stores the lifecycle context and runs the legacy periodic sweep until
// ctx is canceled. Per-instance loops are independent of this loop.
func (m *Monitor) Start(ctx context.Context) {
	m.mu.Lock()
	m.root = ctx
	m.mu.Unlock()
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweep(ctx)
		}
	}
}

// OnAttach registers a spec device. hc carries the per-instance probe policy;
// nil selects the legacy regime. local devices skip probing and follow the
// gateway. Updating an existing device smoothly replaces a changed probe
// policy; dropping health_check stops the instance loop.
func (m *Monitor) OnAttach(edgexName, uuid, profileName, preferred string, local bool, hc *device.ConnectorHealthCheck) {
	if edgexName == "" {
		return
	}
	hcEnabled := hc != nil && hc.Resource != ""

	m.mu.Lock()
	st, exists := m.devices[edgexName]
	if !exists {
		st = &deviceState{edgexName: edgexName, generation: m.nextGen}
		m.nextGen++
		m.devices[edgexName] = st
	}
	wasHC := st.hc != nil && st.hc.Resource != ""

	st.uuid = uuid
	st.profileName = profileName
	st.preferred = preferred
	st.local = local

	var startLoop, stopLoop, runLegacy, immediateOnline bool
	policy := probePolicy{}
	switch {
	case local:
		if st.stop != nil {
			stopLoop = true
		}
		immediateOnline = !st.online
		// Local devices are announced synchronously below. Track the
		// optimistic online state here (the probe path only sets it inside
		// check()): without it OnDetach sees wasOnline=false and skips the
		// withdrawal, leaving a deleted spec's sub-device in devices[].
		st.online = true
	case hcEnabled:
		policy = makePolicy(hc)
		switch {
		case st.stop == nil:
			startLoop = true
		case !sameHealthCheck(st.hc, hc):
			stopLoop = true
			startLoop = true
		}
	default:
		if st.stop != nil {
			stopLoop = true
		}
		st.probe = ""
		switch {
		case m.cfg.GlobalEnabled:
			if !exists || wasHC {
				runLegacy = true
			}
		case !st.online:
			immediateOnline = true
			// Same optimistic marking as the local branch: an immediate
			// announcement below counts as online for withdrawal.
			st.online = true
		}
	}
	st.hc = hc
	gen := st.generation
	if stopLoop {
		close(st.stop)
		st.stop = nil
		st.generation = m.nextGen
		m.nextGen++
		gen = st.generation
	}
	if startLoop {
		st.stop = make(chan struct{})
		st.generation = m.nextGen
		m.nextGen++
		gen = st.generation
		st.failures = 0
	}
	stop := st.stop
	m.mu.Unlock()

	if local {
		m.cfg.Logger.Infof("health attach device=%s mode=local (no probe)", edgexName)
		if immediateOnline {
			m.bringOnline(uuid)
		}
		return
	}
	switch {
	case startLoop:
		m.cfg.Logger.Infof("health attach device=%s mode=per-instance resource=%q interval=%s timeout=%s threshold=%d generation=%d",
			edgexName, policy.resource, policy.interval, policy.timeout, policy.threshold, gen)
		m.runInstanceLoop(edgexName, gen, stop, policy)
	case runLegacy:
		m.cfg.Logger.Infof("health attach device=%s mode=legacy-candidate preferred=%q", edgexName, preferred)
		go m.check(context.Background(), edgexName)
	case immediateOnline:
		m.cfg.Logger.Infof("health attach device=%s mode=online-immediately (global probing disabled, no health_check)", edgexName)
		m.bringOnline(uuid)
	default:
		m.cfg.Logger.Debugf("health attach device=%s unchanged (existing loop/policy retained)", edgexName)
	}
}

// OnDetach removes a spec device, stops any instance loop, and withdraws its
// presence when online.
func (m *Monitor) OnDetach(edgexName string) {
	m.mu.Lock()
	st, ok := m.devices[edgexName]
	if !ok {
		m.mu.Unlock()
		return
	}
	uuid := st.uuid
	wasOnline := st.online
	if st.stop != nil {
		close(st.stop)
		st.stop = nil
	}
	delete(m.devices, edgexName)
	m.mu.Unlock()

	if wasOnline {
		m.takeOffline(uuid)
	}
}

// makePolicy resolves a probePolicy from hc, filling zero values with the
// defaults.
func makePolicy(hc *device.ConnectorHealthCheck) probePolicy {
	p := probePolicy{resource: hc.Resource, threshold: hc.FailThreshold}
	if hc.IntervalMS > 0 {
		p.interval = time.Duration(hc.IntervalMS) * time.Millisecond
	} else {
		p.interval = defaultInterval
	}
	if hc.TimeoutMS > 0 {
		p.timeout = time.Duration(hc.TimeoutMS) * time.Millisecond
	} else {
		p.timeout = defaultInstanceProbeTime
	}
	if p.threshold <= 0 {
		p.threshold = defaultThreshold
	}
	return p
}

// sameHealthCheck compares two probe policies, treating two nils as equal.
func sameHealthCheck(a, b *device.ConnectorHealthCheck) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Resource == b.Resource &&
		a.IntervalMS == b.IntervalMS &&
		a.TimeoutMS == b.TimeoutMS &&
		a.FailThreshold == b.FailThreshold
}

// runInstanceLoop runs one per-device probe loop until the loop is replaced,
// the device detaches, or the root context ends.
func (m *Monitor) runInstanceLoop(edgexName string, generation int, stop <-chan struct{}, p probePolicy) {
	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		m.instanceProbe(edgexName, generation, p)
		for {
			m.mu.Lock()
			root := m.root
			m.mu.Unlock()
			select {
			case <-root.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				m.instanceProbe(edgexName, generation, p)
			}
		}
	}()
}

// instanceProbe performs one per-instance read and drives the state edge.
// The reading is discarded: only the success/failure matters.
func (m *Monitor) instanceProbe(edgexName string, generation int, p probePolicy) {
	m.mu.Lock()
	root := m.root
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(root, p.timeout)
	defer cancel()
	ev, status, err := m.cfg.Core.Read(ctx, edgexName, p.resource, probeQuery)
	outcome := probeUnreachable
	switch {
	case err == nil && status == http.StatusOK && len(ev.Event.Readings) > 0:
		outcome = probeReachable
	case err == nil && status == http.StatusOK:
		outcome = probeInconclusive
	}

	m.mu.Lock()
	st, ok := m.devices[edgexName]
	if !ok || st.generation != generation {
		m.mu.Unlock()
		m.cfg.Logger.Debugf("health probe device=%s resource=%q stale generation, ignoring", edgexName, p.resource)
		return
	}
	var cameOnline, wentOffline bool
	uuid := st.uuid
	switch outcome {
	case probeReachable:
		st.failures = 0
		if !st.online {
			st.online = true
			cameOnline = true
		}
		m.cfg.Logger.Debugf("health probe device=%s resource=%q OK (failures=%d)", edgexName, p.resource, st.failures)
	case probeInconclusive:
		// The device service answered but produced no readings: neither
		// confirm reachability nor count a failure, so a malformed response
		// cannot flap the state in either direction.
		m.cfg.Logger.Warnf("health probe device=%s resource=%q inconclusive: 200 with no readings (failures unchanged=%d/%d online=%v)",
			edgexName, p.resource, st.failures, p.threshold, st.online)
	default:
		st.failures++
		m.cfg.Logger.Debugf("health probe device=%s resource=%q FAIL http=%d err=%v failures=%d/%d online=%v",
			edgexName, p.resource, status, err, st.failures, p.threshold, st.online)
		if st.online && st.failures >= p.threshold {
			st.online = false
			wentOffline = true
		}
	}
	m.mu.Unlock()

	switch {
	case cameOnline:
		m.cfg.Logger.Infof("health device=%s ONLINE (first successful probe)", edgexName)
		m.bringOnline(uuid)
	case wentOffline:
		m.cfg.Logger.Warnf("health device=%s OFFLINE after %d consecutive probe failures", edgexName, p.threshold)
		m.takeOffline(uuid)
	}
}

// sweep probes every legacy (no health_check, non-local) device once, with
// bounded concurrency.
func (m *Monitor) sweep(ctx context.Context) {
	m.mu.Lock()
	names := make([]string, 0, len(m.devices))
	for name, st := range m.devices {
		if !st.local && st.hc == nil {
			names = append(names, name)
		}
	}
	m.mu.Unlock()

	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			m.check(ctx, name)
		}(name)
	}
	wg.Wait()
}

// bringOnline runs the Connect presence announcement and requests a manifest
// reconciliation.
func (m *Monitor) bringOnline(nlUUID string) {
	ctx, cancel := context.WithTimeout(context.Background(), actionTimeout)
	defer cancel()
	if err := m.cfg.Connect(ctx, nlUUID); err != nil {
		m.cfg.Logger.Errorf("announce device %s online failed: %s", nlUUID, err.Error())
		return
	}
	m.cfg.Notify()
}

// takeOffline runs the Disconnect presence announcement and requests a
// manifest reconciliation.
func (m *Monitor) takeOffline(nlUUID string) {
	ctx, cancel := context.WithTimeout(context.Background(), actionTimeout)
	defer cancel()
	if err := m.cfg.Disconnect(ctx, nlUUID); err != nil {
		m.cfg.Logger.Errorf("announce device %s offline failed: %s", nlUUID, err.Error())
		return
	}
	m.cfg.Notify()
}
