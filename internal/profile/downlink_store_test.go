// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"testing"
	"time"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func newTestStore(t *testing.T) *DownlinkStore {
	t.Helper()
	store, err := NewDownlinkStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewDownlinkStore: %v", err)
	}
	return store
}

func TestDownlinkStorePutGetRemove(t *testing.T) {
	store := newTestStore(t)
	if _, ok := store.Get("p1"); ok {
		t.Fatal("empty store reports a record")
	}
	rec := downlinkRecord{
		ProfileID: "id-1", Revision: 2, Format: device.ProfileFormatNeutral,
		Status: device.ProfileStateApplied, AppliedAt: time.Now().UTC(),
		Document: map[string]any{"name": "p1", "points": []any{}},
	}
	if err := store.Put("p1", rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok := store.Get("p1")
	if !ok || got.Revision != 2 || got.Status != device.ProfileStateApplied ||
		got.Document["name"] != "p1" {
		t.Fatalf("record round trip: %+v ok=%v", got, ok)
	}
	existed, err := store.Remove("p1")
	if err != nil || !existed {
		t.Fatalf("Remove: existed=%v err=%v", existed, err)
	}
	if _, ok := store.Get("p1"); ok {
		t.Fatal("record survived removal")
	}
	existed, err = store.Remove("p1")
	if err != nil || existed {
		t.Fatalf("repeat Remove: existed=%v err=%v", existed, err)
	}
}

func TestDownlinkStorePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := NewDownlinkStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Put("p1", downlinkRecord{
		Revision: 7, Format: device.ProfileFormatEdgeX, Status: device.ProfileStateApplied,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	reopened, err := NewDownlinkStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	rec, ok := reopened.Get("p1")
	if !ok || rec.Revision != 7 || rec.Format != device.ProfileFormatEdgeX {
		t.Fatalf("ledger not reloaded: %+v ok=%v", rec, ok)
	}
}

func TestNewDownlinkStoreMissingDirIsCreated(t *testing.T) {
	store, err := NewDownlinkStore(t.TempDir() + "/nested/data")
	if err != nil {
		t.Fatalf("NewDownlinkStore: %v", err)
	}
	if err := store.Put("p1", downlinkRecord{Revision: 1, Status: device.ProfileStateApplied}); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

func TestNewDownlinkStoreCorruptFileIsFatal(t *testing.T) {
	dir := t.TempDir()
	store, err := NewDownlinkStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Put("p1", downlinkRecord{Revision: 1, Status: device.ProfileStateApplied}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := writeCorrupt(store.path); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if _, err := NewDownlinkStore(dir); err == nil {
		t.Fatal("corrupt ledger must not be silently ignored")
	}
}
