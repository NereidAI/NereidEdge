// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

// keySize is the AES-256 key length in bytes.
const keySize = 32

// ErrInvalidKeyMaterial marks a wrong-size key or a corrupt sealed blob.
var ErrInvalidKeyMaterial = errors.New("invalid secret encryption material")

// Codec seals and opens secret documents under a 32-byte AES-256 key. The key
// is held in memory only; it is never written next to the store.
type Codec struct {
	key []byte
}

// NewCodec builds a codec from an explicit 32-byte key. The key is copied so
// the caller may zero its own buffer.
func NewCodec(key []byte) (*Codec, error) {
	if len(key) != keySize {
		return nil, fmt.Errorf("%w: key must be exactly %d bytes", ErrInvalidKeyMaterial, keySize)
	}
	cp := make([]byte, keySize)
	copy(cp, key)
	return &Codec{key: cp}, nil
}

// Seal encrypts plaintext and returns base64(nonce||ciphertext). Each call
// uses a fresh random nonce, so sealing the same document twice yields
// different output.
func (c *Codec) Seal(plaintext []byte) (string, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return "", fmt.Errorf("secret cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("secret aead: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("secret nonce: %w", err)
	}
	sealed := gcm.Seal(nonce, nonce, plaintext, nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Open reverses Seal. A wrong key or a tampered blob fails authentication.
func (c *Codec) Open(encoded string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed ciphertext", ErrInvalidKeyMaterial)
	}
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, fmt.Errorf("secret cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secret aead: %w", err)
	}
	if len(raw) < gcm.NonceSize() {
		return nil, fmt.Errorf("%w: truncated ciphertext", ErrInvalidKeyMaterial)
	}
	nonce, ciphertext := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: authentication failed", ErrInvalidKeyMaterial)
	}
	return plaintext, nil
}
