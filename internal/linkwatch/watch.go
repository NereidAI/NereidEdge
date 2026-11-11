// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package linkwatch implements the NereidEdge local link-health watchdog
// (ADR-0026). The platform marks a gateway DOWN only after ~90s of missed
// heartbeats and never sees the edge's own view of an outage. This watcher
// mirrors the protocol state machine locally: it opens a TRANSPORT_DOWN
// episode the moment the connection drops, excludes planned shutdowns,
// detects flapping inside a sliding window and keeps counters. Closed
// unplanned episodes are delivered to the OnEpisode hook so the reconnect
// path can re-report them to NL once the v1/gateway/linkhealth uplink
// protocol ships.
package linkwatch

import (
	"sync"
	"time"
)

// Reason classifies a transport break. The future uplink report echoes it
// verbatim so NL can tell unplanned outages from planned stops.
type Reason string

const (
	// ReasonTransportDown is an unplanned transport loss.
	ReasonTransportDown Reason = "transport_down"
	// ReasonPlanned is an intentional stop (planned goodbye before
	// disconnect). It never opens an episode.
	ReasonPlanned Reason = "planned"
)

// Episode is one closed link-down interval observed locally.
type Episode struct {
	// Start is when the transport drop was first observed.
	Start time.Time
	// End is when a (re)negotiated session came up again.
	End time.Time
	// Reason classifies the break.
	Reason Reason
}

// Logger is the minimal logging surface the watcher needs.
type Logger interface {
	Infof(format string, v ...any)
	Warnf(format string, v ...any)
}

// Config carries linkwatch options. Zero values select protocol defaults.
type Config struct {
	// FlapWindow is the sliding window for flap detection; default 10m.
	FlapWindow time.Duration
	// FlapThreshold is the number of unplanned episodes inside the window
	// that constitute flapping; default 5.
	FlapThreshold int
	// OnEpisode receives each closed unplanned episode, in no particular
	// order relative to other goroutines. It may be nil.
	OnEpisode func(Episode)
	// Logger is optional diagnostics.
	Logger Logger
	// Now injects the clock; default time.Now.
	Now func() time.Time
}

// Watch is the local link-health state machine. All methods are safe for
// concurrent use; the underlying reconnect path already serializes most
// transitions through the reconnect gate, but the planned-shutdown flag and
// the connection-lost hook can race during a teardown.
type Watch struct {
	mu        sync.Mutex
	now       func() time.Time
	window    time.Duration
	threshold int
	episode   func(Episode)
	lc        Logger

	up        bool
	planned   bool
	open      *Episode
	flapArmed bool
	flaps     []time.Time // unplanned episode start times inside the window

	downEvents int64
	unplanned  int64
	flapEvents int64
}

// New creates a watchdog with protocol defaults applied.
func New(cfg Config) *Watch {
	w := &Watch{
		now:       cfg.Now,
		window:    cfg.FlapWindow,
		threshold: cfg.FlapThreshold,
		episode:   cfg.OnEpisode,
		lc:        cfg.Logger,
	}
	if w.now == nil {
		w.now = time.Now
	}
	if w.window <= 0 {
		w.window = 10 * time.Minute
	}
	if w.threshold <= 0 {
		w.threshold = 5
	}
	return w
}

// ReportDown records a transport break starting now. Repeated unplanned
// calls are idempotent while a break is already open (the reconnect loop
// may observe the same loss several times); only distinct breaks advance
// DownEvents.
func (w *Watch) ReportDown(reason Reason, planned bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if planned {
		// Recorded as a distinct (intentional) break; the imminent
		// OnConnectionLost from the local disconnect must not open an
		// unplanned episode.
		w.downEvents++
		w.planned = true
		return
	}
	if w.open != nil {
		// Same outage observed again (reconnect loop re-reports the loss);
		// keep the open episode, do not double count.
		return
	}
	w.downEvents++
	if w.planned {
		// Local teardown handshake: swallow the unplanned-shaped call.
		w.planned = false
		w.up = false
		return
	}
	start := w.now()
	w.open = &Episode{Start: start, Reason: reason}
	w.up = false
	if w.lc != nil {
		w.lc.Warnf("linkwatch: link down at %s (%s)", start.Format(time.RFC3339), reason)
	}
}

// BeginPlannedShutdown records an intentional stop so the transport-down
// observed after the local disconnect does not open an unplanned episode.
func (w *Watch) BeginPlannedShutdown() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.planned = true
}

// ReportUp marks a fresh (re)negotiated session live. If a break was open it
// closes the episode; unplanned closures are delivered to the OnEpisode hook
// and counted into the flap window.
func (w *Watch) ReportUp() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.up = true
	if w.open == nil {
		return
	}
	ep := *w.open
	ep.End = w.now()
	w.open = nil
	if ep.Reason != ReasonTransportDown {
		return
	}
	w.unplanned++
	w.pushFlapLocked(ep.Start)
	if w.lc != nil {
		w.lc.Warnf("linkwatch: unplanned link episode closed down=%s up=%s (total %d)",
			ep.Start.Format(time.RFC3339), ep.End.Format(time.RFC3339), w.unplanned)
	}
	if w.episode != nil {
		w.episode(ep)
	}
}

// pushFlapLocked slides the flap window and raises a flap event at most once
// per burst: it re-arms only after the window slides out below the threshold.
func (w *Watch) pushFlapLocked(start time.Time) {
	cutoff := start.Add(-w.window)
	kept := w.flaps[:0]
	for _, since := range w.flaps {
		if since.After(cutoff) {
			kept = append(kept, since)
		}
	}
	kept = append(kept, start)
	w.flaps = kept
	if !w.flapArmed && len(w.flaps) >= w.threshold {
		w.flapArmed = true
		w.flapEvents++
		if w.lc != nil {
			w.lc.Warnf("linkwatch: flapping: %d unplanned episodes in %s", len(w.flaps), w.window)
		}
	}
	if w.flapArmed && len(w.flaps) < w.threshold {
		w.flapArmed = false
	}
}

// Snapshot is a race-free view of the watcher counters.
type Snapshot struct {
	// Up reports the last negotiated session status.
	Up bool
	// DownEvents counts every distinct transport break, planned or not.
	DownEvents int64
	// Unplanned counts closed unplanned episodes.
	Unplanned int64
	// FlapEvents counts flapping bursts raised.
	FlapEvents int64
	// OpenSince is the start of the currently open episode, or zero.
	OpenSince time.Time
}

// Snapshot returns the current counters.
func (w *Watch) Snapshot() Snapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := Snapshot{
		Up:         w.up,
		DownEvents: w.downEvents,
		Unplanned:  w.unplanned,
		FlapEvents: w.flapEvents,
	}
	if w.open != nil {
		s.OpenSince = w.open.Start
	}
	return s
}
