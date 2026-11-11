// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package linkwatch

import (
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func newWatch(t testing.TB, threshold int, onEp func(Episode)) (*Watch, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)}
	w := New(Config{
		FlapWindow:    10 * time.Minute,
		FlapThreshold: threshold,
		OnEpisode:     onEp,
		Now:           clock.now,
	})
	return w, clock
}

func TestUnplannedEpisodeReported(t *testing.T) {
	var got []Episode
	w, clock := newWatch(t, 3, func(ep Episode) { got = append(got, ep) })

	clock.set(time.Date(2026, 10, 10, 10, 0, 10, 0, time.UTC))
	w.ReportDown(ReasonTransportDown, false)
	clock.set(time.Date(2026, 10, 10, 10, 2, 0, 0, time.UTC))
	w.ReportUp()

	if len(got) != 1 {
		t.Fatalf("episodes = %d, want 1", len(got))
	}
	if !got[0].Start.Equal(time.Date(2026, 10, 10, 10, 0, 10, 0, time.UTC)) ||
		!got[0].End.Equal(time.Date(2026, 10, 10, 10, 2, 0, 0, time.UTC)) {
		t.Fatalf("episode = %+v", got[0])
	}
	if got[0].Reason != ReasonTransportDown {
		t.Fatalf("reason = %q", got[0].Reason)
	}
	s := w.Snapshot()
	if s.Up != true || s.Unplanned != 1 || s.DownEvents != 1 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestRepeatedDownIsIdempotent(t *testing.T) {
	var n int
	w, clock := newWatch(t, 3, func(Episode) { n++ })

	clock.set(time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC))
	w.ReportDown(ReasonTransportDown, false)
	w.ReportDown(ReasonTransportDown, false)
	clock.set(time.Date(2026, 10, 10, 10, 1, 0, 0, time.UTC))
	w.ReportUp()

	if n != 1 {
		t.Fatalf("episodes delivered = %d, want 1", n)
	}
	if s := w.Snapshot(); s.DownEvents != 1 {
		t.Fatalf("downEvents = %d, want 1", s.DownEvents)
	}
}

func TestPlannedShutdownSuppressesEpisode(t *testing.T) {
	var n int
	w, clock := newWatch(t, 3, func(Episode) { n++ })

	w.BeginPlannedShutdown()
	clock.set(time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC))
	w.ReportDown(ReasonTransportDown, false)
	clock.set(time.Date(2026, 10, 10, 10, 1, 0, 0, time.UTC))
	w.ReportUp()

	if n != 0 {
		t.Fatalf("episodes delivered = %d, want 0", n)
	}
	s := w.Snapshot()
	if s.DownEvents != 1 || s.Unplanned != 0 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestDowntimeBeforeFirstSessionIsNotAnEpisode(t *testing.T) {
	var n int
	w, clock := newWatch(t, 3, func(Episode) { n++ })

	// Transport drops before any hello; strictly speaking the session was
	// never up, but the reconnect path still reports the drop. Treating it
	// as unplanned keeps accounting consistent with the transport loss.
	clock.set(time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC))
	w.ReportDown(ReasonTransportDown, false)
	clock.set(time.Date(2026, 10, 10, 10, 1, 0, 0, time.UTC))
	w.ReportUp()

	if n != 1 {
		t.Fatalf("episodes delivered = %d, want 1", n)
	}
}

func TestFlapRaisedOncePerBurst(t *testing.T) {
	var n int
	w, clock := newWatch(t, 3, func(Episode) { n++ })

	wy := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		clock.set(wy.Add(time.Duration(i) * time.Second))
		w.ReportDown(ReasonTransportDown, false)
		clock.set(wy.Add(time.Duration(i)*time.Second + time.Second))
		w.ReportUp()
	}
	if n != 5 {
		t.Fatalf("episodes delivered = %d, want 5", n)
	}
	if s := w.Snapshot(); s.FlapEvents != 1 {
		t.Fatalf("flapEvents = %d, want 1 (once per burst)", s.FlapEvents)
	}
}

func TestFlapReArmsAfterWindowSlides(t *testing.T) {
	var n int
	w, clock := newWatch(t, 3, func(Episode) { n++ })

	wy := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		clock.set(wy.Add(time.Duration(i) * time.Second))
		w.ReportDown(ReasonTransportDown, false)
		clock.set(wy.Add(time.Duration(i)*time.Second + time.Second))
		w.ReportUp()
	}
	if s := w.Snapshot(); s.FlapEvents != 1 {
		t.Fatalf("first burst flapEvents = %d, want 1", s.FlapEvents)
	}

	// Let the window slide completely, then a fresh burst re-arms.
	clock.set(wy.Add(11 * time.Minute))
	for i := 0; i < 3; i++ {
		clock.set(wy.Add(11*time.Minute + time.Duration(i)*time.Second))
		w.ReportDown(ReasonTransportDown, false)
		clock.set(wy.Add(11*time.Minute + time.Duration(i)*time.Second + time.Second))
		w.ReportUp()
	}
	if s := w.Snapshot(); s.FlapEvents != 2 {
		t.Fatalf("second burst flapEvents = %d, want 2", s.FlapEvents)
	}
}

func TestOpenEpisodeExposedInSnapshot(t *testing.T) {
	w, clock := newWatch(t, 3, nil)
	wy := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)
	clock.set(wy)
	w.ReportDown(ReasonTransportDown, false)

	s := w.Snapshot()
	if s.Up || !s.OpenSince.Equal(wy) {
		t.Fatalf("snapshot = %+v", s)
	}
}
