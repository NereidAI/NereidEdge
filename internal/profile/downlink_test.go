// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// fakeDownlinkGateway records status frames and exposes the spec callback.
type fakeDownlinkGateway struct {
	mu        sync.Mutex
	frames    []device.ProfileStatusFrame
	onSpec    func(device.ProfileSpecFrame)
	subErr    error
	reportErr error
}

func (f *fakeDownlinkGateway) SubscribeProfiles(_ context.Context, _ byte, on func(device.ProfileSpecFrame)) error {
	if f.subErr != nil {
		return f.subErr
	}
	f.onSpec = on
	return nil
}

func (f *fakeDownlinkGateway) ReportProfileStatus(_ context.Context, frame device.ProfileStatusFrame, _ byte) error {
	if f.reportErr != nil {
		return f.reportErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frames = append(f.frames, frame)
	return nil
}

func (f *fakeDownlinkGateway) lastFrame() (device.ProfileStatusFrame, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.frames) == 0 {
		return device.ProfileStatusFrame{}, false
	}
	return f.frames[len(f.frames)-1], true
}

// fakeApplier records Apply/Remove calls for one format.
type fakeApplier struct {
	format    string
	mu        sync.Mutex
	applied   []string
	removed   []string
	entries   []device.ProfileEntry
	applyErr  error
	removeErr error
}

func (f *fakeApplier) Format() string { return f.format }

func (f *fakeApplier) Apply(_ context.Context, entry device.ProfileEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, entry.Name)
	f.entries = append(f.entries, entry)
	return f.applyErr
}

func (f *fakeApplier) Remove(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, name)
	return f.removeErr
}

func (f *fakeApplier) applyCount(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, x := range f.applied {
		if x == name {
			n++
		}
	}
	return n
}

func (f *fakeApplier) removedOnce(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range f.removed {
		if x == name {
			return true
		}
	}
	return false
}

func newDownlinkHandler(t *testing.T, appliers ...ProfileApplier) (*DownlinkHandler, *DownlinkStore, *fakeDownlinkGateway) {
	t.Helper()
	store, err := NewDownlinkStore(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	gw := &fakeDownlinkGateway{}
	h := NewDownlinkHandler(DownlinkConfig{
		Gateway:  gw,
		Store:    store,
		Appliers: appliers,
		Logger:   logger.NewMockClient(),
	})
	h.SetNegotiation(true, []string{device.ProfileFormatEdgeX, device.ProfileFormatNeutral})
	return h, store, gw
}

func profileStatusByName(t *testing.T, gw *fakeDownlinkGateway, name string) device.ProfileStatus {
	t.Helper()
	frame, ok := gw.lastFrame()
	if !ok {
		t.Fatalf("no status frame published")
	}
	for _, report := range frame.Reports {
		if report.Name == name {
			return report
		}
	}
	t.Fatalf("no report for %q in %+v", name, frame)
	return device.ProfileStatus{}
}

func neutralEntry(name string, revision int64) device.ProfileEntry {
	return device.ProfileEntry{
		ProfileID: "id-" + name,
		Name:      name,
		Revision:  revision,
		Format:    device.ProfileFormatNeutral,
		Document: map[string]any{
			"name": name,
			"points": []any{
				map[string]any{"name": "temperature", "type": "float", "unit": "C"},
			},
		},
	}
}

func edgexEntry(name string, revision int64) device.ProfileEntry {
	return device.ProfileEntry{
		ProfileID: "id-" + name,
		Name:      name,
		Revision:  revision,
		Format:    device.ProfileFormatEdgeX,
		Document: map[string]any{
			"name": name,
			"deviceResources": []any{
				map[string]any{"name": "temperature",
					"properties": map[string]any{"valueType": "Float32", "readWrite": "R"}},
			},
		},
	}
}

func writeCorrupt(path string) error {
	return os.WriteFile(path, []byte("{not-json"), downlinkFilePerm)
}

func TestDownlinkStartSubscribes(t *testing.T) {
	h, _, gw := newDownlinkHandler(t)
	if err := h.Start(context.Background(), 1); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if gw.onSpec == nil {
		t.Fatal("Start did not register a spec callback")
	}
}

func TestDownlinkStartPropagatesSubscribeError(t *testing.T) {
	h, _, gw := newDownlinkHandler(t)
	gw.subErr = errors.New("no broker")
	if err := h.Start(context.Background(), 1); err == nil {
		t.Fatal("Start ignored the subscribe error")
	}
}

func TestDownlinkDropsFramesBeforeNegotiation(t *testing.T) {
	store, err := NewDownlinkStore(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	gw := &fakeDownlinkGateway{}
	h := NewDownlinkHandler(DownlinkConfig{Gateway: gw, Store: store, Logger: logger.NewMockClient()})

	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{neutralEntry("A1", 1)}})
	if _, ok := gw.lastFrame(); ok {
		t.Fatal("pre-negotiation frame produced a report")
	}

	h.SetNegotiation(true, nil)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{neutralEntry("A1", 1)}})
	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateApplied {
		t.Fatalf("status=%+v", got)
	}
}

func TestDownlinkFrameRacingAckWaitsForNegotiation(t *testing.T) {
	store, err := NewDownlinkStore(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	gw := &fakeDownlinkGateway{}
	h := NewDownlinkHandler(DownlinkConfig{Gateway: gw, Store: store, Logger: logger.NewMockClient()})

	done := make(chan struct{})
	go func() {
		h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{neutralEntry("A1", 1)}})
		close(done)
	}()

	// The ack is processed a moment after paho already dispatched the spec.
	time.Sleep(100 * time.Millisecond)
	if _, ok := gw.lastFrame(); ok {
		t.Fatal("frame must not pass the gate before negotiation")
	}
	h.SetNegotiation(true, []string{device.ProfileFormatNeutral})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("frame did not drain after negotiation")
	}
	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateApplied {
		t.Fatalf("status=%+v", got)
	}
}

func TestDownlinkFrameWithoutNegotiationIsDropped(t *testing.T) {
	store, err := NewDownlinkStore(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	gw := &fakeDownlinkGateway{}
	h := NewDownlinkHandler(DownlinkConfig{Gateway: gw, Store: store, Logger: logger.NewMockClient()})
	start := time.Now()
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{neutralEntry("A1", 1)}})
	if time.Since(start) < negotiationWait {
		t.Fatalf("drop should wait out the grace window, took %v", time.Since(start))
	}
	if _, ok := gw.lastFrame(); ok {
		t.Fatal("unnegotiated frame produced a report")
	}
}

func TestDownlinkAppliesNeutralDocument(t *testing.T) {
	h, store, gw := newDownlinkHandler(t)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{neutralEntry("A1", 1)}})

	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateApplied || got.Revision != 1 {
		t.Fatalf("status=%+v", got)
	}
	rec, ok := store.Get("A1")
	if !ok || rec.Status != device.ProfileStateApplied || rec.Document["name"] != "A1" {
		t.Fatalf("ledger: %+v ok=%v", rec, ok)
	}
}

func TestDownlinkAppliesEdgeXDocumentViaApplier(t *testing.T) {
	applier := &fakeApplier{format: device.ProfileFormatEdgeX}
	h, store, gw := newDownlinkHandler(t, applier)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 2)}})

	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateApplied || got.Revision != 2 {
		t.Fatalf("status=%+v", got)
	}
	if applier.applyCount("A1") != 1 {
		t.Fatal("external applier not invoked once")
	}
	rec, ok := store.Get("A1")
	if !ok || rec.Format != device.ProfileFormatEdgeX || rec.Document != nil {
		t.Fatalf("ledger must record coordinates only: %+v", rec)
	}
}

func TestDownlinkIdempotentOnSameOrOlderRevision(t *testing.T) {
	applier := &fakeApplier{format: device.ProfileFormatEdgeX}
	h, _, gw := newDownlinkHandler(t, applier)
	entry := edgexEntry("A1", 5)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{entry}})
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{entry}})
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 4)}})

	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateApplied {
		t.Fatalf("status=%+v", got)
	}
	if applier.applyCount("A1") != 1 {
		t.Fatalf("applier invoked %d times, want 1", applier.applyCount("A1"))
	}
}

func TestDownlinkNewerRevisionReapplies(t *testing.T) {
	applier := &fakeApplier{format: device.ProfileFormatEdgeX}
	h, store, _ := newDownlinkHandler(t, applier)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 1)}})
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 2)}})
	if applier.applyCount("A1") != 2 {
		t.Fatalf("applier invoked %d times, want 2", applier.applyCount("A1"))
	}
	if rec, _ := store.Get("A1"); rec.Revision != 2 {
		t.Fatalf("ledger revision=%d, want 2", rec.Revision)
	}
}

func TestDownlinkRetriesFailedRevision(t *testing.T) {
	applier := &fakeApplier{format: device.ProfileFormatEdgeX, applyErr: ErrInvalidProfileDocument}
	h, store, gw := newDownlinkHandler(t, applier)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 2)}})
	got := profileStatusByName(t, gw, "A1")
	if got.Status != device.ProfileStateFailed || got.Reason != reasonInvalidDocument {
		t.Fatalf("status=%+v", got)
	}
	if rec, _ := store.Get("A1"); rec.Status != device.ProfileStateFailed {
		t.Fatalf("failure must be recorded: %+v", rec)
	}

	applier.applyErr = nil
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 2)}})
	if applier.applyCount("A1") != 2 {
		t.Fatal("failed revision not retried")
	}
	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateApplied {
		t.Fatalf("status=%+v", got)
	}
}

func TestDownlinkUnsupportedFormatFailedAndNotLanded(t *testing.T) {
	h, store, gw := newDownlinkHandler(t) // neutral-only
	entry := edgexEntry("A1", 1)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{entry}})
	got := profileStatusByName(t, gw, "A1")
	if got.Status != device.ProfileStateFailed || got.Reason != reasonUnsupportedFormat {
		t.Fatalf("status=%+v", got)
	}
	if _, ok := store.Get("A1"); ok {
		t.Fatal("unsupported format must not write the ledger")
	}
}

func TestDownlinkInvalidNeutralDocumentFailed(t *testing.T) {
	h, store, gw := newDownlinkHandler(t)
	entry := neutralEntry("A1", 1)
	entry.Document["name"] = "Other"
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{entry}})
	got := profileStatusByName(t, gw, "A1")
	if got.Status != device.ProfileStateFailed || got.Reason != reasonInvalidDocument {
		t.Fatalf("status=%+v", got)
	}
	if rec, _ := store.Get("A1"); rec.Status != device.ProfileStateFailed {
		t.Fatalf("failure ledger record: %+v", rec)
	}
}

func TestDownlinkApplierErrorReasons(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		reason string
	}{
		{"invalid", ErrInvalidProfileDocument, reasonInvalidDocument},
		{"in-use", ErrProfileInUse, reasonProfileInUse},
		{"generic", errors.New("metadata 500"), reasonApplyFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applier := &fakeApplier{format: device.ProfileFormatEdgeX, applyErr: tc.err}
			h, _, gw := newDownlinkHandler(t, applier)
			h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 1)}})
			got := profileStatusByName(t, gw, "A1")
			if got.Status != device.ProfileStateFailed || got.Reason != tc.reason {
				t.Fatalf("status=%+v, want reason %q", got, tc.reason)
			}
		})
	}
}

func TestDownlinkRemovingOwnedEdgeXProfile(t *testing.T) {
	applier := &fakeApplier{format: device.ProfileFormatEdgeX}
	h, store, gw := newDownlinkHandler(t, applier)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 1)}})
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{
		{Name: "A1", Revision: 2, Format: device.ProfileFormatEdgeX, Removing: true},
	}})

	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateRemoved {
		t.Fatalf("status=%+v", got)
	}
	if !applier.removedOnce("A1") {
		t.Fatal("external removal not invoked")
	}
	if _, ok := store.Get("A1"); ok {
		t.Fatal("ledger record survived removal")
	}
}

func TestDownlinkRemovingNeutralProfile(t *testing.T) {
	h, store, gw := newDownlinkHandler(t)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{neutralEntry("A1", 1)}})
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{
		{Name: "A1", Revision: 1, Format: device.ProfileFormatNeutral, Removing: true},
	}})
	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateRemoved {
		t.Fatalf("status=%+v", got)
	}
	if _, ok := store.Get("A1"); ok {
		t.Fatal("neutral document survived removal")
	}
}

func TestDownlinkRemovingUnknownNameDoesNotTouchExternal(t *testing.T) {
	applier := &fakeApplier{format: device.ProfileFormatEdgeX}
	h, _, gw := newDownlinkHandler(t, applier)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{
		{Name: "ghost", Revision: 1, Format: device.ProfileFormatEdgeX, Removing: true},
	}})
	if got := profileStatusByName(t, gw, "ghost"); got.Status != device.ProfileStateRemoved {
		t.Fatalf("status=%+v", got)
	}
	if applier.removedOnce("ghost") {
		t.Fatal("external applier must not remove a profile this downlink never owned")
	}
}

func TestDownlinkRemovingInUseKeepsRecordAndFails(t *testing.T) {
	applier := &fakeApplier{format: device.ProfileFormatEdgeX}
	h, store, gw := newDownlinkHandler(t, applier)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 1)}})
	applier.removeErr = ErrProfileInUse
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{
		{Name: "A1", Revision: 2, Format: device.ProfileFormatEdgeX, Removing: true},
	}})
	got := profileStatusByName(t, gw, "A1")
	if got.Status != device.ProfileStateFailed || got.Reason != reasonProfileInUse {
		t.Fatalf("status=%+v", got)
	}
	if _, ok := store.Get("A1"); !ok {
		t.Fatal("record must remain so the removing redrive is retried")
	}
}

func TestDownlinkMissingEntryNeverRemoves(t *testing.T) {
	applier := &fakeApplier{format: device.ProfileFormatEdgeX}
	h, store, gw := newDownlinkHandler(t, applier)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{
		edgexEntry("A1", 1), edgexEntry("A2", 1),
	}})
	// A2 disappears from the next full-set frame: it must stay landed.
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{edgexEntry("A1", 1)}})
	if _, ok := store.Get("A2"); !ok {
		t.Fatal("missing entry was implicitly removed")
	}
	if applier.removedOnce("A2") {
		t.Fatal("missing entry triggered external removal")
	}
	if got := profileStatusByName(t, gw, "A1"); got.Status != device.ProfileStateApplied {
		t.Fatalf("A1 status=%+v", got)
	}
}

func TestDownlinkSkipsEmptyName(t *testing.T) {
	h, _, gw := newDownlinkHandler(t)
	entry := neutralEntry("", 1)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{entry}})
	if _, ok := gw.lastFrame(); ok {
		t.Fatal("empty-name entry produced a report")
	}
}

func TestDownlinkReportsEveryEntryInOneFrame(t *testing.T) {
	h, _, gw := newDownlinkHandler(t)
	h.Handle(device.ProfileSpecFrame{Entries: []device.ProfileEntry{
		neutralEntry("a", 1), neutralEntry("b", 1),
	}})
	frame, ok := gw.lastFrame()
	if !ok || len(frame.Reports) != 2 {
		t.Fatalf("frame=%+v ok=%v", frame, ok)
	}
}

func TestDownlinkEmptyFramePublishesNothing(t *testing.T) {
	h, _, gw := newDownlinkHandler(t)
	h.Handle(device.ProfileSpecFrame{})
	if _, ok := gw.lastFrame(); ok {
		t.Fatal("empty frame produced a report")
	}
}
