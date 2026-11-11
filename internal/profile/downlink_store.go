// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Downlink store layout and permissions.
const (
	// downlinkDefaultDir is the store directory when config leaves it empty,
	// matching the secret store and the container's persistent volume.
	downlinkDefaultDir = "/app/data"
	// downlinkStoreFileName is the ledger file inside the store directory.
	downlinkStoreFileName = "profiles-down.json"
	// downlinkStoreVersion is the on-disk schema version.
	downlinkStoreVersion = 1
	// downlinkDirPerm keeps the store directory private to the service.
	downlinkDirPerm = 0o700
	// downlinkFilePerm keeps the ledger private to the service.
	downlinkFilePerm = 0o600
)

// downlinkRecord is one locally landed profile, keyed by name. It is the
// ledger the spec state machine de-duplicates against: an APPLIED record at
// an equal or newer revision makes a re-delivered entry a no-op, while a
// FAILED record keeps being retried. Neutral-format documents are landed in
// the ledger itself; EdgeX documents live in local core-metadata, so their
// Document stays nil and only the coordinates are recorded.
type downlinkRecord struct {
	// ProfileID is the library profile id (correlation on the wire is by
	// Name; this is informational only).
	ProfileID string `json:"profile_id,omitempty"`
	// Revision is the last revision the record refers to.
	Revision int64 `json:"revision"`
	// Format is the render format the document was landed in. It selects the
	// removal path when a removing entry omits its format.
	Format string `json:"format"`
	// Status is one of the device SDK ProfileState* constants. FAILED records
	// are retried on the next delivery rather than treated as applied.
	Status string `json:"status"`
	// Reason carries the transport-safe failure explanation for the record.
	Reason string `json:"reason,omitempty"`
	// AppliedAt records the last successful landing.
	AppliedAt time.Time `json:"applied_at,omitempty"`
	// Document is the rendered neutral document. EdgeX documents are not
	// duplicated here because core-metadata is their landing owner.
	Document map[string]any `json:"document,omitempty"`
}

// downlinkFile is the single-file ledger schema.
type downlinkFile struct {
	Version int                       `json:"version"`
	Entries map[string]downlinkRecord `json:"entries"`
}

// DownlinkStore persists the locally landed profile set in one JSON file. The
// full desired set is re-sent after every hello, so this ledger only has to
// answer three questions: what revision is applied per name, in which
// format, and with what status.
type DownlinkStore struct {
	path string

	mu   sync.Mutex
	data downlinkFile
}

// NewDownlinkStore opens (or initializes) the ledger under dir. A missing
// file starts empty; a corrupt file is fatal rather than silently dropping
// the revision ledger and re-applying every profile after a restart.
func NewDownlinkStore(dir string) (*DownlinkStore, error) {
	if dir == "" {
		dir = downlinkDefaultDir
	}
	if err := os.MkdirAll(dir, downlinkDirPerm); err != nil {
		return nil, fmt.Errorf("create profile store dir: %w", err)
	}
	s := &DownlinkStore{
		path: filepath.Join(dir, downlinkStoreFileName),
		data: downlinkFile{Version: downlinkStoreVersion, Entries: map[string]downlinkRecord{}},
	}

	raw, err := os.ReadFile(s.path) //nolint:gosec // service-owned store path
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read profile store: %w", err)
	}
	var data downlinkFile
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("decode profile store: %w", err)
	}
	if data.Entries == nil {
		data.Entries = map[string]downlinkRecord{}
	}
	if data.Version == 0 {
		data.Version = downlinkStoreVersion
	}
	s.data = data
	return s, nil
}

// Get returns the record for name.
func (s *DownlinkStore) Get(name string) (downlinkRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.data.Entries[name]
	return rec, ok
}

// Put writes the record for name and flushes the ledger atomically.
func (s *DownlinkStore) Put(name string, rec downlinkRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Entries[name] = rec
	return s.persistLocked()
}

// Remove deletes the record for name and reports whether one existed.
func (s *DownlinkStore) Remove(name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Entries[name]; !ok {
		return false, nil
	}
	delete(s.data.Entries, name)
	if err := s.persistLocked(); err != nil {
		return false, err
	}
	return true, nil
}

// persistLocked writes the ledger atomically: a private temp file in the
// same directory is renamed over the target so a crash never leaves a
// half-written ledger. The caller must hold s.mu.
func (s *DownlinkStore) persistLocked() error {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode profile store: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, downlinkFilePerm); err != nil {
		return fmt.Errorf("write profile store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("commit profile store: %w", err)
	}
	return nil
}
