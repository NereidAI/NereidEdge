// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// fakeGateway records status frames and exposes the sync callback.
type fakeGateway struct {
	mu        sync.Mutex
	frames    []device.SecretStatusFrame
	onSync    func(device.SecretSync)
	subErr    error
	reportErr error
}

func (f *fakeGateway) SubscribeSecrets(_ context.Context, _ byte, on func(device.SecretSync)) error {
	if f.subErr != nil {
		return f.subErr
	}
	f.onSync = on
	return nil
}

func (f *fakeGateway) ReportSecretStatus(_ context.Context, frame device.SecretStatusFrame, _ byte) error {
	if f.reportErr != nil {
		return f.reportErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frames = append(f.frames, frame)
	return nil
}

func (f *fakeGateway) lastFrame() (device.SecretStatusFrame, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.frames) == 0 {
		return device.SecretStatusFrame{}, false
	}
	return f.frames[len(f.frames)-1], true
}

// fakeBinder records binding calls and can be told to fail.
type fakeBinder struct {
	mu        sync.Mutex
	applied   []string
	removed   []string
	applyErr  error
	removeErr error
}

func (f *fakeBinder) Apply(_ context.Context, entry device.SecretEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, entry.Name)
	return f.applyErr
}

func (f *fakeBinder) Remove(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, name)
	return f.removeErr
}

func (f *fakeBinder) applies(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.applied {
		if n == name {
			return true
		}
	}
	return false
}

func (f *fakeBinder) removals(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.removed {
		if n == name {
			return true
		}
	}
	return false
}

func newTestHandler(t *testing.T) (*Handler, *Store, *fakeGateway) {
	t.Helper()
	codec, err := NewCodec(testKey())
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	store, err := NewStore(t.TempDir(), codec, false)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	gw := &fakeGateway{}
	h := New(Config{Gateway: gw, Store: store, Logger: logger.NewMockClient()})
	return h, store, gw
}

// statusByName returns the reported status for a name in the last frame.
func statusByName(t *testing.T, gw *fakeGateway, name string) device.SecretStatus {
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
	t.Fatalf("no report for %q in frame %+v", name, frame)
	return device.SecretStatus{}
}

func TestStartSubscribes(t *testing.T) {
	h, _, gw := newTestHandler(t)
	if err := h.Start(context.Background(), 1); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if gw.onSync == nil {
		t.Fatal("Start did not register a sync callback")
	}
}

func TestStartPropagatesSubscribeError(t *testing.T) {
	h, _, gw := newTestHandler(t)
	gw.subErr = errors.New("no broker")
	if err := h.Start(context.Background(), 1); err == nil {
		t.Fatal("Start ignored the subscribe error")
	}
}

func TestHandleAppliesNewEntry(t *testing.T) {
	h, store, gw := newTestHandler(t)
	doc := map[string]any{"username": "admin", "password": "hunter2"}
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{entry("cam", 1, doc)}})

	if got := statusByName(t, gw, "cam"); got.Status != device.SecretStateApplied || got.Revision != 1 {
		t.Fatalf("status=%+v, want APPLIED revision 1", got)
	}
	stored, ok, err := store.Document("cam")
	if err != nil || !ok || stored["password"] != "hunter2" {
		t.Fatalf("stored=%v ok=%v err=%v", stored, ok, err)
	}
}

func TestHandleIsIdempotentPerRevision(t *testing.T) {
	h, store, gw := newTestHandler(t)
	e := entry("cam", 5, map[string]any{"password": "hunter2"})
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{e}})
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{e}})

	if got := statusByName(t, gw, "cam"); got.Status != device.SecretStateApplied || got.Revision != 5 {
		t.Fatalf("status=%+v, want APPLIED revision 5", got)
	}
	if rev, _ := store.Revision("cam"); rev != 5 {
		t.Fatalf("Revision=%d, want 5", rev)
	}
}

func TestHandleSkipsStaleRevision(t *testing.T) {
	h, store, gw := newTestHandler(t)
	if err := store.Upsert(entry("cam", 9, map[string]any{"password": "newest"})); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{entry("cam", 3, map[string]any{"password": "stale"})}})

	if got := statusByName(t, gw, "cam"); got.Status != device.SecretStateApplied {
		t.Fatalf("status=%+v, want APPLIED", got)
	}
	stored, _, _ := store.Document("cam")
	if stored["password"] != "newest" {
		t.Fatalf("stale revision overwrote the store: %v", stored)
	}
	if rev, _ := store.Revision("cam"); rev != 9 {
		t.Fatalf("Revision=%d, want 9", rev)
	}
}

func TestHandleRemovingErases(t *testing.T) {
	h, store, gw := newTestHandler(t)
	if err := store.Upsert(entry("cam", 1, map[string]any{"password": "hunter2"})); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{{Name: "cam", Revision: 2, Removing: true}}})

	if got := statusByName(t, gw, "cam"); got.Status != device.SecretStateRemoved {
		t.Fatalf("status=%+v, want REMOVED", got)
	}
	if _, ok := store.Revision("cam"); ok {
		t.Fatal("removed secret still present")
	}
}

func TestHandleRemovingMissingIsIdempotent(t *testing.T) {
	h, _, gw := newTestHandler(t)
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{{Name: "ghost", Revision: 1, Removing: true}}})
	if got := statusByName(t, gw, "ghost"); got.Status != device.SecretStateRemoved {
		t.Fatalf("status=%+v, want REMOVED", got)
	}
}

func TestHandleSkipsEmptyName(t *testing.T) {
	h, _, gw := newTestHandler(t)
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{entry("", 1, map[string]any{"password": "x"})}})
	if _, ok := gw.lastFrame(); ok {
		t.Fatal("empty-name entry produced a status frame")
	}
}

func TestHandleStoreFailureReportsFailed(t *testing.T) {
	codec, _ := NewCodec(testKey())
	store, err := NewStore(t.TempDir(), codec, false)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	// Break the store path so persisting the sealed copy fails.
	store.path = store.path + "/missing/secrets.enc"
	gw := &fakeGateway{}
	h := New(Config{Gateway: gw, Store: store, Logger: logger.NewMockClient()})

	h.Handle(device.SecretSync{Entries: []device.SecretEntry{entry("cam", 1, map[string]any{"password": "hunter2"})}})

	got := statusByName(t, gw, "cam")
	if got.Status != device.SecretStateFailed {
		t.Fatalf("status=%+v, want FAILED", got)
	}
	if got.Reason == "" {
		t.Fatal("FAILED report carries no reason")
	}
}

func TestHandleEmptyFramePublishesNothing(t *testing.T) {
	h, _, gw := newTestHandler(t)
	h.Handle(device.SecretSync{})
	if _, ok := gw.lastFrame(); ok {
		t.Fatal("empty frame produced a status publish")
	}
}

func TestHandleReportsEveryEntryInOneFrame(t *testing.T) {
	h, _, gw := newTestHandler(t)
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{
		entry("a", 1, map[string]any{"password": "a"}),
		entry("b", 1, map[string]any{"password": "b"}),
	}})
	frame, ok := gw.lastFrame()
	if !ok || len(frame.Reports) != 2 {
		t.Fatalf("frame=%+v ok=%v, want 2 reports", frame, ok)
	}
}

func TestHandleBinderApplyFailureReportsFailed(t *testing.T) {
	h, _, gw := newTestHandler(t)
	bd := &fakeBinder{applyErr: errors.New("device service down")}
	h.binder = bd
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{entry("cam", 2, map[string]any{"password": "x"})}})

	got := statusByName(t, gw, "cam")
	if got.Status != device.SecretStateFailed || got.Reason != reasonBindFailed {
		t.Fatalf("status=%+v, want FAILED reason %q", got, reasonBindFailed)
	}
	if rev, _ := h.store.Revision("cam"); rev != 2 {
		t.Fatalf("binder failure must keep the encrypted copy, Revision=%d", rev)
	}
}

func TestHandleBinderNoTargetReason(t *testing.T) {
	h, _, gw := newTestHandler(t)
	h.binder = &fakeBinder{applyErr: ErrNoTarget}
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{entry("cam", 2, map[string]any{"password": "x"})}})

	got := statusByName(t, gw, "cam")
	if got.Reason != reasonTargetNotFound {
		t.Fatalf("reason=%q, want %q", got.Reason, reasonTargetNotFound)
	}
}

func TestHandleReaffirmsBindingOnSameRevision(t *testing.T) {
	h, store, gw := newTestHandler(t)
	bd := &fakeBinder{}
	h.binder = bd
	e := entry("cam", 7, map[string]any{"password": "x"})
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{e}})
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{e}})

	if got := statusByName(t, gw, "cam"); got.Status != device.SecretStateApplied {
		t.Fatalf("status=%+v, want APPLIED", got)
	}
	if rev, _ := store.Revision("cam"); rev != 7 {
		t.Fatalf("Revision=%d, want 7", rev)
	}
	if !bd.applies("cam") {
		t.Fatal("same-revision re-delivery did not re-affirm the binding")
	}
}

func TestHandleBinderRemoveAndErase(t *testing.T) {
	h, store, gw := newTestHandler(t)
	if err := store.Upsert(entry("cam", 1, map[string]any{"password": "x"})); err != nil {
		t.Fatalf("seed: %v", err)
	}
	bd := &fakeBinder{}
	h.binder = bd
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{{Name: "cam", Revision: 3, Removing: true}}})

	if got := statusByName(t, gw, "cam"); got.Status != device.SecretStateRemoved {
		t.Fatalf("status=%+v, want REMOVED", got)
	}
	if !bd.removals("cam") {
		t.Fatal("binder removal was not invoked")
	}
}

func TestHandleBinderRemoveFailureReportsFailed(t *testing.T) {
	h, _, gw := newTestHandler(t)
	h.binder = &fakeBinder{removeErr: errors.New("keeper unreachable")}
	h.Handle(device.SecretSync{Entries: []device.SecretEntry{{Name: "cam", Revision: 3, Removing: true}}})

	got := statusByName(t, gw, "cam")
	if got.Status != device.SecretStateFailed || got.Reason != reasonEraseFailed {
		t.Fatalf("status=%+v, want FAILED reason %q", got, reasonEraseFailed)
	}
}
