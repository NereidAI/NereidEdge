// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNextBoundary(t *testing.T) {
	const interval = 5 * time.Minute
	cases := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{
			name: "mid cycle",
			now:  time.Date(2026, 10, 8, 10, 2, 0, 0, time.UTC),
			want: time.Date(2026, 10, 8, 10, 5, 0, 0, time.UTC),
		},
		{
			name: "exactly on boundary returns next",
			now:  time.Date(2026, 10, 8, 10, 5, 0, 0, time.UTC),
			want: time.Date(2026, 10, 8, 10, 10, 0, 0, time.UTC),
		},
		{
			name: "before any boundary",
			now:  time.Date(2026, 10, 8, 10, 0, 30, 0, time.UTC),
			want: time.Date(2026, 10, 8, 10, 5, 0, 0, time.UTC),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextBoundary(tc.now, interval); !got.Equal(tc.want) {
				t.Errorf("nextBoundary(%s) = %s, want %s", tc.now.Format(time.RFC3339), got.Format(time.RFC3339), tc.want.Format(time.RFC3339))
			}
		})
	}
}

func TestBoundaryIndex(t *testing.T) {
	const interval = 5 * time.Minute
	a := boundaryIndex(time.Date(2026, 10, 8, 10, 4, 59, 0, time.UTC), interval)
	b := boundaryIndex(time.Date(2026, 10, 8, 10, 5, 0, 0, time.UTC), interval)
	c := boundaryIndex(time.Date(2026, 10, 8, 10, 9, 59, 0, time.UTC), interval)
	d := boundaryIndex(time.Date(2026, 10, 8, 10, 10, 0, 0, time.UTC), interval)
	if b != a+1 {
		t.Errorf("index at boundary = %d, want %d", b, a+1)
	}
	if c != b {
		t.Errorf("index mid-next cycle = %d, want %d", c, b)
	}
	if d != b+1 {
		t.Errorf("index at next boundary = %d, want %d", d, b+1)
	}
}

func TestBoundaryGap(t *testing.T) {
	cases := []struct {
		name      string
		lastIndex int64
		idx       int64
		want      int
	}{
		{name: "first aligned send", lastIndex: -1, idx: 5, want: 0},
		{name: "consecutive boundaries", lastIndex: 4, idx: 5, want: 0},
		{name: "one skipped boundary", lastIndex: 4, idx: 6, want: 1},
		{name: "three skipped", lastIndex: 4, idx: 8, want: 3},
		{name: "no forward movement", lastIndex: 6, idx: 6, want: 0},
		{name: "going backwards (clock skew)", lastIndex: 6, idx: 4, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := boundaryGap(tc.lastIndex, tc.idx); got != tc.want {
				t.Errorf("boundaryGap(%d,%d) = %d, want %d", tc.lastIndex, tc.idx, got, tc.want)
			}
		})
	}
}

func TestReconnectGateRunsOnceAndCoalesces(t *testing.T) {
	g := NewReconnectGate()
	var calls atomic.Int64
	var active atomic.Int64
	var maxActive atomic.Int64

	firstRan := make(chan struct{})
	release := make(chan struct{}, 1)
	var released atomic.Bool
	var firstOnce sync.Once

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			<-start
			// Simulate a reconnect storm of 3 rapid OnConnect callbacks.
			g.Run(func(gen int64) {
				if g.Stale(gen) {
					return
				}
				n := active.Add(1)
				for {
					m := maxActive.Load()
					if n <= m || maxActive.CompareAndSwap(m, n) {
						break
					}
				}
				// Slow down the very first run so the 3 concurrent Run
				// calls all land while it is in flight; subsequent coalesced
				// follow-ups never block.
				if !released.Load() {
					firstOnce.Do(func() { close(firstRan) })
					<-release
				}
				active.Add(-1)
				calls.Add(1)
			})
		}()
	}

	close(start)
	// Wait until a replay is actually running.
	select {
	case <-firstRan:
	case <-time.After(2 * time.Second):
		t.Fatal("replay never started")
	}
	// Wait for every Run call to return while the first replay is blocked.
	// Otherwise a delayed follower can arrive after the coalesced follow-up and
	// incorrectly start a third replay. wg tracks the callers, not the async work.
	wg.Wait()
	released.Store(true)
	release <- struct{}{}

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if maxActive.Load() != 1 {
		t.Errorf("max concurrent active = %d, want 1 (single-flight violated)", maxActive.Load())
	}
	if calls.Load() != 2 {
		t.Errorf("replay calls = %d, want 2 (first run + one coalesced follow-up)", calls.Load())
	}
}

func TestReconnectGateInvalidateAbortsInFlight(t *testing.T) {
	g := NewReconnectGate()
	var ranGen int64
	run := make(chan struct{})

	g.Run(func(gen int64) {
		ranGen = gen
		close(run)
		// Simulate retry loop: poll Stale until the connection drop is
		// observed (Invalidate bumps gen), which is the abort condition used
		// inside the real reconnect retry loop.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if g.Stale(gen) {
				return // aborted, as main.go does
			}
			time.Sleep(time.Millisecond)
		}
	})

	<-run
	g.Invalidate()
	time.Sleep(50 * time.Millisecond)
	if g.Stale(ranGen) != true {
		t.Errorf("Stale(%d) = false after Invalidate, want true", ranGen)
	}
}
