// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"sync"
	"sync/atomic"
)

// ReconnectGate serializes the transport-reconnect replay work so a reconnect
// storm (repeated quick disconnects driving OnConnect each time) cannot pile
// up concurrent hello/replay loops. Only one replay runs at a time; a
// reconnect arriving while one is in flight is recorded as a pending re-run
// and coalesced into a follow-up once the current run returns.
//
// A generation counter detects transport drops: Invalidate is called from
// the connection-lost hook, and any in-flight replay can test Stale to abort
// its retry loop instead of hammering a broker that just went away again.
type ReconnectGate struct {
	// mu guards running/pending so the single-flight hand-off has no
	// lost-wakeup window: every transition happens under the lock.
	mu      sync.Mutex
	running bool
	pending bool

	gen atomic.Int64
}

// NewReconnectGate creates an idle single-flight gate.
func NewReconnectGate() *ReconnectGate {
	return &ReconnectGate{}
}

// Invalidate records a transport drop, bumping the generation seen by any
// in-flight replay so it stops retrying against a dead broker.
func (g *ReconnectGate) Invalidate() {
	g.gen.Add(1)
}

// Stale reports whether the connection dropped since generation gen was
// captured, meaning retries are pointless and the replay should abort.
func (g *ReconnectGate) Stale(gen int64) bool {
	return g.gen.Load() != gen
}

// Run starts fn on the single-flight slot: at most one replay runs at a time,
// and reconnects witnessed while a run is in flight are coalesced into one
// follow-up run. fn receives the generation it started under and should call
// g.Stale(gen) before retrying to abort early if the connection dropped. The
// call returns immediately; all work happens in a background goroutine.
func (g *ReconnectGate) Run(fn func(gen int64)) {
	g.mu.Lock()
	if g.running {
		g.pending = true
		g.mu.Unlock()
		return
	}
	g.running = true
	g.mu.Unlock()
	go g.work(fn)
}

// work owns the slot; it runs fn once, then consumes any coalesced pending
// requests one at a time before handing the slot back.
func (g *ReconnectGate) work(fn func(gen int64)) {
	for {
		gen := g.gen.Load()
		if g.Stale(gen) {
			g.finish()
			return
		}
		fn(gen)

		g.mu.Lock()
		if !g.pending {
			g.running = false
			g.mu.Unlock()
			return
		}
		g.pending = false
		g.mu.Unlock()
	}
}

// finish releases the slot after an aborted run. Any pending coalesced
// request is left set: a subsequent Run picks it up as a fresh replay, and if
// none arrives the stale flag is harmless until the next real reconnect.
func (g *ReconnectGate) finish() {
	g.mu.Lock()
	g.running = false
	g.mu.Unlock()
}
