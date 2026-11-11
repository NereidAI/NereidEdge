// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// Store layout and permissions.
const (
	// defaultDir is the store directory when the config leaves it empty.
	defaultDir = "/app/data"
	// storeFileName is the encrypted store file inside the store directory.
	storeFileName = "secrets.enc"
	// storeVersion is the on-disk schema version.
	storeVersion = 1
	// dirPerm keeps the store directory private to the service user.
	dirPerm = 0o700
	// filePerm keeps the store file private to the service user.
	filePerm = 0o600
)

// record is one sealed secret copy. Only Sealed carries secret material; the
// rest is non-sensitive metadata used for idempotent re-delivery.
type record struct {
	Type      string                  `json:"type,omitempty"`
	Version   string                  `json:"version,omitempty"`
	Revision  int64                   `json:"revision"`
	Selectors []device.SecretSelector `json:"selectors,omitempty"`
	// Sealed is base64(nonce||ciphertext) over the marshalled document.
	Sealed string `json:"sealed"`
}

// fileData is the single-file store schema.
type fileData struct {
	Version int               `json:"version"`
	Entries map[string]record `json:"entries"`
}

// Store persists sealed secret copies in one encrypted file. The document
// plaintext is decrypted only on demand and never retained.
type Store struct {
	path  string
	codec *Codec

	mu   sync.Mutex
	data fileData
}

// NewStore opens (or initializes) the encrypted store under dir. When discard
// is true any existing file is ignored and overwritten on the next write; that
// is required when the codec key is ephemeral and cannot decrypt a previous
// run's store.
func NewStore(dir string, codec *Codec, discard bool) (*Store, error) {
	if dir == "" {
		dir = defaultDir
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("create secret store dir: %w", err)
	}
	s := &Store{
		path:  filepath.Join(dir, storeFileName),
		codec: codec,
		data:  fileData{Version: storeVersion, Entries: map[string]record{}},
	}
	if discard {
		return s, nil
	}

	raw, err := os.ReadFile(s.path) //nolint:gosec // service-owned store path
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read secret store: %w", err)
	}
	var data fileData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("decode secret store: %w", err)
	}
	if data.Entries == nil {
		data.Entries = map[string]record{}
	}
	if data.Version == 0 {
		data.Version = storeVersion
	}
	s.data = data
	return s, nil
}

// Revision returns the stored revision for name and whether it exists.
func (s *Store) Revision(name string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.data.Entries[name]
	return rec.Revision, ok
}

// Upsert seals entry.Document and stores (or replaces) the copy under name,
// then flushes the file. The plaintext document is not retained.
func (s *Store) Upsert(entry device.SecretEntry) error {
	document, err := json.Marshal(entry.Document)
	if err != nil {
		return fmt.Errorf("marshal secret document: %w", err)
	}
	sealed, err := s.codec.Seal(document)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Entries[entry.Name] = record{
		Type:      entry.Type,
		Version:   entry.Version,
		Revision:  entry.Revision,
		Selectors: entry.Selectors,
		Sealed:    sealed,
	}
	return s.persistLocked()
}

// Remove erases the stored copy for name and flushes the file. It reports
// whether a copy existed so a repeated removal is still idempotent.
func (s *Store) Remove(name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.data.Entries[name]
	if !existed {
		return false, nil
	}
	delete(s.data.Entries, name)
	if err := s.persistLocked(); err != nil {
		return false, err
	}
	return true, nil
}

// Document decrypts and returns the stored document for name. The second
// result is false when no copy exists.
func (s *Store) Document(name string) (map[string]any, bool, error) {
	s.mu.Lock()
	rec, ok := s.data.Entries[name]
	s.mu.Unlock()
	if !ok {
		return nil, false, nil
	}
	plaintext, err := s.codec.Open(rec.Sealed)
	if err != nil {
		return nil, true, err
	}
	var document map[string]any
	if err := json.Unmarshal(plaintext, &document); err != nil {
		return nil, true, fmt.Errorf("decode secret document: %w", err)
	}
	return document, true, nil
}

// persistLocked writes the store atomically: a private temp file in the same
// directory is flushed and renamed over the target so a crash never leaves a
// partially written store. The caller must hold s.mu.
func (s *Store) persistLocked() error {
	raw, err := json.Marshal(s.data)
	if err != nil {
		return fmt.Errorf("encode secret store: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, filePerm); err != nil {
		return fmt.Errorf("write secret store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("commit secret store: %w", err)
	}
	return nil
}
