// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	codec, err := NewCodec(testKey())
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	store, err := NewStore(t.TempDir(), codec, false)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return store
}

func entry(name string, revision int64, document map[string]any) device.SecretEntry {
	return device.SecretEntry{
		Name:      name,
		Type:      "onvif-basic",
		Version:   "1",
		Revision:  revision,
		Selectors: []device.SecretSelector{{Kind: "mac", Value: "aa:bb:cc:dd:ee:ff"}},
		Document:  document,
	}
}

func TestStoreUpsertAndDocument(t *testing.T) {
	store := newTestStore(t)
	doc := map[string]any{"username": "admin", "password": "hunter2"}
	if err := store.Upsert(entry("cam", 1, doc)); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if rev, ok := store.Revision("cam"); !ok || rev != 1 {
		t.Fatalf("Revision=%d ok=%v, want 1 true", rev, ok)
	}
	got, ok, err := store.Document("cam")
	if err != nil {
		t.Fatalf("Document: %v", err)
	}
	if !ok || got["username"] != "admin" || got["password"] != "hunter2" {
		t.Fatalf("Document=%v ok=%v", got, ok)
	}
}

func TestStoreFileHasNoPlaintext(t *testing.T) {
	store := newTestStore(t)
	if err := store.Upsert(entry("cam", 1, map[string]any{"password": "hunter2"})); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	raw, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatalf("read store: %v", err)
	}
	if strings.Contains(string(raw), "hunter2") {
		t.Fatal("store file contains secret plaintext")
	}
	info, err := os.Stat(store.path)
	if err != nil {
		t.Fatalf("stat store: %v", err)
	}
	if perm := info.Mode().Perm(); perm != filePerm {
		t.Fatalf("store perm=%o, want %o", perm, filePerm)
	}
}

func TestStoreReloadsFromDisk(t *testing.T) {
	dir := t.TempDir()
	codec, _ := NewCodec(testKey())
	store, _ := NewStore(dir, codec, false)
	if err := store.Upsert(entry("cam", 3, map[string]any{"password": "hunter2"})); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	reopened, err := NewStore(dir, codec, false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if rev, ok := reopened.Revision("cam"); !ok || rev != 3 {
		t.Fatalf("reopened Revision=%d ok=%v, want 3 true", rev, ok)
	}
	got, ok, err := reopened.Document("cam")
	if err != nil || !ok || got["password"] != "hunter2" {
		t.Fatalf("reopened Document=%v ok=%v err=%v", got, ok, err)
	}
}

func TestStoreDiscardIgnoresExisting(t *testing.T) {
	dir := t.TempDir()
	codec, _ := NewCodec(testKey())
	store, _ := NewStore(dir, codec, false)
	if err := store.Upsert(entry("cam", 1, map[string]any{"password": "hunter2"})); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	discarded, err := NewStore(dir, codec, true)
	if err != nil {
		t.Fatalf("discard reopen: %v", err)
	}
	if _, ok := discarded.Revision("cam"); ok {
		t.Fatal("discarded store retained an entry")
	}
}

func TestStoreUpsertReplacesRevision(t *testing.T) {
	store := newTestStore(t)
	if err := store.Upsert(entry("cam", 1, map[string]any{"password": "old"})); err != nil {
		t.Fatalf("Upsert old: %v", err)
	}
	if err := store.Upsert(entry("cam", 2, map[string]any{"password": "new"})); err != nil {
		t.Fatalf("Upsert new: %v", err)
	}
	got, _, _ := store.Document("cam")
	if got["password"] != "new" {
		t.Fatalf("Document=%v, want updated value", got)
	}
	if rev, _ := store.Revision("cam"); rev != 2 {
		t.Fatalf("Revision=%d, want 2", rev)
	}
}

func TestStoreRemove(t *testing.T) {
	store := newTestStore(t)
	_ = store.Upsert(entry("cam", 1, map[string]any{"password": "hunter2"}))

	existed, err := store.Remove("cam")
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if !existed {
		t.Fatal("Remove reported no existing copy")
	}
	if _, ok := store.Revision("cam"); ok {
		t.Fatal("entry still present after Remove")
	}
	raw, _ := os.ReadFile(store.path)
	if strings.Contains(string(raw), "hunter2") {
		t.Fatal("removed secret plaintext still on disk")
	}

	// A repeated removal is idempotent and reports no prior copy.
	existed, err = store.Remove("cam")
	if err != nil || existed {
		t.Fatalf("second Remove existed=%v err=%v, want false nil", existed, err)
	}
}

func TestStorePersistFailureIsReported(t *testing.T) {
	codec, _ := NewCodec(testKey())
	store, err := NewStore(t.TempDir(), codec, false)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	// Point the store at a path whose parent directory does not exist so the
	// atomic temp write fails deterministically.
	store.path = filepath.Join(store.path, "missing", "secrets.enc")
	if err := store.Upsert(entry("cam", 1, map[string]any{"password": "hunter2"})); err == nil {
		t.Fatal("Upsert succeeded despite an unwritable store path")
	}
}
