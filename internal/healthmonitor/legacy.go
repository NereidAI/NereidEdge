// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package healthmonitor

import (
	"context"
)

// check performs one legacy reachability probe with candidate enumeration
// and drives the presence transition. Only one check per device runs at a
// time, and a generation guard prevents a stale probe from acting after the
// device was detached or switched to a per-instance policy.
func (m *Monitor) check(parent context.Context, edgexName string) {
	m.mu.Lock()
	if _, busy := m.inflight[edgexName]; busy {
		m.mu.Unlock()
		return
	}
	st, ok := m.devices[edgexName]
	if !ok || st.local || st.hc != nil {
		m.mu.Unlock()
		return
	}
	m.inflight[edgexName] = struct{}{}
	snapshot := *st
	generation := st.generation
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.inflight, edgexName)
		m.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(parent, m.cfg.ProbeTimeout)
	defer cancel()
	outcome, resource := m.probe(ctx, &snapshot)

	m.mu.Lock()
	current, exists := m.devices[edgexName]
	if !exists || current.generation != generation {
		m.mu.Unlock()
		return
	}
	var cameOnline, wentOffline bool
	uuid := current.uuid
	switch outcome {
	case probeReachable:
		current.failures = 0
		if resource != "" {
			current.probe = resource
		}
		if !current.online {
			current.online = true
			cameOnline = true
		}
	case probeUnreachable:
		current.failures++
		if current.online && current.failures >= m.cfg.FailureThreshold {
			current.online = false
			wentOffline = true
		}
		// probeInconclusive: 200 without readings neither clears failures
		// nor accumulates one, so a malformed response cannot flap state.
	}
	m.mu.Unlock()

	switch {
	case cameOnline:
		m.bringOnline(uuid)
	case wentOffline:
		m.takeOffline(uuid)
	}
}
