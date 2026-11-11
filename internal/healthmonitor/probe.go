// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package healthmonitor

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
)

// probeOutcome is the tri-state result of one health probe read.
type probeOutcome int

const (
	// probeUnreachable means the read failed (transport error, non-200
	// status, timeout): the device could not be confirmed reachable.
	probeUnreachable probeOutcome = iota
	// probeInconclusive means the device service answered 200 but the event
	// carried no readings, so the probe neither confirms reachability nor
	// proves failure. It must not move the online/offline state in either
	// direction; the next cycle re-probes.
	probeInconclusive
	// probeReachable means the read returned 200 with at least one reading.
	probeReachable
)

// probeQuery keeps the probe reading off the message bus (ds-pushevent=false)
// while still returning the event in the HTTP response, so the probe can
// require a non-empty reading set. An EdgeX success event always carries at
// least one reading; a 200 without readings means the device service did not
// actually produce device data.
const probeQuery = common.PushEvent + "=false&" + common.ReturnEvent + "=true"

// probe performs one reachability check against an ordered candidate set:
// a device is reachable when ANY readable command returns an event with
// readings, so a marked resource unsupported on this particular device never
// causes a false offline. It returns the outcome and the winning candidate
// when the probe had to switch (or first resolve) resources, so the caller can
// cache it for cheap steady-state checks. An inconclusive candidate (200 with
// no readings) never wins and does not stop enumeration.
func (m *Monitor) probe(ctx context.Context, st *deviceState) (probeOutcome, string) {
	best := probeUnreachable
	if st.probe != "" {
		// Fast path: try the cached winning command without re-enumerating.
		switch out, _ := m.readProbe(ctx, st.edgexName, st.probe); out {
		case probeReachable:
			return probeReachable, ""
		case probeInconclusive:
			// The cached command stopped producing readings; re-enumerate
			// rather than trusting or punishing it.
			best = probeInconclusive
		}
		// The cached command failed (device variance, profile update): fall
		// through to re-enumeration across the full candidate set.
	}

	candidates, err := m.resolveCandidates(ctx, st)
	if err != nil {
		m.cfg.Logger.Warnf("resolve health probe candidates for device %s failed: %s",
			st.edgexName, err.Error())
		return probeUnreachable, ""
	}
	for _, candidate := range candidates {
		if candidate == st.probe {
			// Already tried above in this check.
			continue
		}
		out, _ := m.readProbe(ctx, st.edgexName, candidate)
		if out == probeReachable {
			return probeReachable, candidate
		}
		if out == probeInconclusive {
			best = probeInconclusive
		}
		// Remaining candidates stop immediately once the per-check deadline
		// is reached; the device is retried on the next cycle.
		if ctx.Err() != nil {
			break
		}
	}
	return best, ""
}

// readProbe executes the probe GET and classifies the response.
func (m *Monitor) readProbe(ctx context.Context, edgexName, resource string) (probeOutcome, error) {
	ev, status, err := m.cfg.Core.Read(ctx, edgexName, resource, probeQuery)
	if err != nil || status != http.StatusOK {
		m.cfg.Logger.Debugf("health probe device %s command %s failed: %v (http %d)",
			edgexName, resource, err, status)
		return probeUnreachable, err
	}
	if len(ev.Event.Readings) == 0 {
		m.cfg.Logger.Warnf("health probe device %s command %s returned 200 with no readings; treating as inconclusive",
			edgexName, resource)
		return probeInconclusive, nil
	}
	return probeReachable, nil
}

// resolveCandidates returns the ordered readable commands used for health
// checks: the spec-designated probe command first, then a resource marked
// with the healthcheck profile attribute, then the remaining readable
// commands in their device-core-command order.
func (m *Monitor) resolveCandidates(ctx context.Context, st *deviceState) ([]string, error) {
	dc, _, err := m.cfg.Core.DeviceCoreCommands(ctx, st.edgexName)
	if err != nil {
		return nil, err
	}
	readable := make([]string, 0, len(dc.CoreCommands))
	for i := range dc.CoreCommands {
		if dc.CoreCommands[i].Get {
			readable = append(readable, dc.CoreCommands[i].Name)
		}
	}
	if len(readable) == 0 {
		return nil, fmt.Errorf("no readable command on device %s", st.edgexName)
	}

	ordered := make([]string, 0, len(readable))
	used := make(map[string]bool, len(readable))
	// The spec author designates the baseline command the device supports.
	if st.preferred != "" {
		for _, name := range readable {
			if name == st.preferred && !used[name] {
				ordered = append(ordered, name)
				used[name] = true
			}
		}
	}
	// A profile marker is the second choice for specs without an explicit
	// designation.
	if markedName := m.markedResource(ctx, st, readable); markedName != "" && !used[markedName] {
		ordered = append(ordered, markedName)
		used[markedName] = true
	}
	// All remaining readables are last-resort fallbacks.
	for _, name := range readable {
		if !used[name] {
			ordered = append(ordered, name)
			used[name] = true
		}
	}
	return ordered, nil
}

// markedResource resolves the profile resource carrying the healthcheck
// attribute, returning "" when the profile is unavailable or the marked
// resource is not readable.
func (m *Monitor) markedResource(ctx context.Context, st *deviceState, readable []string) string {
	if st.profileName == "" || m.cfg.LookupProfile == nil {
		return ""
	}
	profile, err := m.cfg.LookupProfile(ctx, st.profileName)
	if err != nil {
		return ""
	}
	for i := range profile.DeviceResources {
		dr := &profile.DeviceResources[i]
		if !marked(dr.Attributes) {
			continue
		}
		for _, name := range readable {
			if name == dr.Name {
				return dr.Name
			}
		}
	}
	return ""
}

// marked reports whether a device resource is explicitly designated for
// health checks via a healthcheck attribute.
func marked(attrs map[string]any) bool {
	v, ok := attrs["healthcheck"]
	if !ok {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "1", "yes":
			return true
		}
	}
	return false
}
