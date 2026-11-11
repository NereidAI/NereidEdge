// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadKeyEphemeral(t *testing.T) {
	key, ephemeral, err := LoadKey("")
	if err != nil {
		t.Fatalf("LoadKey: %v", err)
	}
	if !ephemeral {
		t.Fatal("LoadKey empty path should report ephemeral")
	}
	if len(key) != keySize {
		t.Fatalf("key len=%d, want %d", len(key), keySize)
	}
	other, _, _ := LoadKey("")
	if string(key) == string(other) {
		t.Fatal("two ephemeral keys were identical")
	}
}

func TestLoadKeyRaw(t *testing.T) {
	path := filepath.Join(t.TempDir(), "k")
	if err := os.WriteFile(path, testKey(), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	key, ephemeral, err := LoadKey(path)
	if err != nil {
		t.Fatalf("LoadKey: %v", err)
	}
	if ephemeral {
		t.Fatal("file-backed key reported ephemeral")
	}
	if string(key) != string(testKey()) {
		t.Fatal("loaded key does not match file")
	}
}

func TestLoadKeyBase64(t *testing.T) {
	path := filepath.Join(t.TempDir(), "k")
	encoded := base64.StdEncoding.EncodeToString(testKey()) + "\n"
	if err := os.WriteFile(path, []byte(encoded), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	key, _, err := LoadKey(path)
	if err != nil {
		t.Fatalf("LoadKey: %v", err)
	}
	if string(key) != string(testKey()) {
		t.Fatal("decoded key does not match")
	}
}

func TestLoadKeyWrongSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "k")
	if err := os.WriteFile(path, []byte("too-short"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if _, _, err := LoadKey(path); !errors.Is(err, ErrInvalidKeyMaterial) {
		t.Fatalf("LoadKey err=%v, want ErrInvalidKeyMaterial", err)
	}
}

func TestLoadKeyMissingFile(t *testing.T) {
	if _, _, err := LoadKey(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("LoadKey missing file succeeded")
	}
}
