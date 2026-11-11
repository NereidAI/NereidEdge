// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
)

// LoadKey returns the AES-256 key that seals the local secret store. When
// path is empty it generates a random ephemeral key and reports
// ephemeral=true: the encrypted store is then unreadable after a restart and
// the platform re-delivers the full set on reconnect. A non-empty path must
// point at an operator-provisioned key (32 raw bytes or base64 of 32 bytes);
// the file is only read, never written, so key material never lands on disk
// through this process.
func LoadKey(path string) (key []byte, ephemeral bool, err error) {
	if path == "" {
		generated := make([]byte, keySize)
		if _, err := rand.Read(generated); err != nil {
			return nil, false, fmt.Errorf("generate ephemeral secret key: %w", err)
		}
		return generated, true, nil
	}

	raw, err := os.ReadFile(path) //nolint:gosec // operator-selected key path
	if err != nil {
		return nil, false, fmt.Errorf("read secret key: %w", err)
	}
	if len(raw) == keySize {
		return raw, false, nil
	}
	// Accept a base64-encoded key so operators can generate one with
	// `openssl rand -base64 32` without managing binary files. Trim so a
	// trailing newline from a file does not break decoding.
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(decoded) != keySize {
		return nil, false, fmt.Errorf("%w: key file must hold %d raw bytes or base64 of %d bytes", ErrInvalidKeyMaterial, keySize, keySize)
	}
	return decoded, false, nil
}
