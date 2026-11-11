// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"errors"
	"strings"
	"testing"
)

func testKey() []byte {
	return []byte("0123456789abcdef0123456789abcdef")
}

func TestNewCodecRejectsWrongKeySize(t *testing.T) {
	if _, err := NewCodec([]byte("short")); !errors.Is(err, ErrInvalidKeyMaterial) {
		t.Fatalf("NewCodec short key err=%v, want ErrInvalidKeyMaterial", err)
	}
}

func TestCodecSealOpenRoundTrip(t *testing.T) {
	codec, err := NewCodec(testKey())
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	plaintext := []byte(`{"password":"hunter2"}`)
	sealed, err := codec.Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if strings.Contains(sealed, "hunter2") {
		t.Fatal("ciphertext leaks plaintext")
	}
	opened, err := codec.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(opened) != string(plaintext) {
		t.Fatalf("Open=%q, want %q", opened, plaintext)
	}
}

func TestCodecSealIsNonDeterministic(t *testing.T) {
	codec, _ := NewCodec(testKey())
	first, _ := codec.Seal([]byte("same"))
	second, _ := codec.Seal([]byte("same"))
	if first == second {
		t.Fatal("two seals produced identical ciphertext; nonce not random")
	}
}

func TestCodecOpenWrongKeyFails(t *testing.T) {
	sealer, _ := NewCodec(testKey())
	sealed, _ := sealer.Seal([]byte("secret"))
	other, _ := NewCodec([]byte("ffffffffffffffffffffffffffffffff"))
	if _, err := other.Open(sealed); !errors.Is(err, ErrInvalidKeyMaterial) {
		t.Fatalf("Open wrong key err=%v, want ErrInvalidKeyMaterial", err)
	}
}

func TestCodecOpenRejectsTampering(t *testing.T) {
	codec, _ := NewCodec(testKey())
	sealed, _ := codec.Seal([]byte("secret"))
	// Flip the final base64 character to corrupt the authentication tag.
	tampered := sealed[:len(sealed)-1] + string(flipBase64(sealed[len(sealed)-1]))
	if _, err := codec.Open(tampered); !errors.Is(err, ErrInvalidKeyMaterial) {
		t.Fatalf("Open tampered err=%v, want ErrInvalidKeyMaterial", err)
	}
}

func TestCodecOpenRejectsGarbage(t *testing.T) {
	codec, _ := NewCodec(testKey())
	if _, err := codec.Open("not base64 !!!"); !errors.Is(err, ErrInvalidKeyMaterial) {
		t.Fatalf("Open garbage err=%v, want ErrInvalidKeyMaterial", err)
	}
}

// flipBase64 maps 'A' to 'B' and anything else to 'A'.
func flipBase64(b byte) byte {
	if b == 'A' {
		return 'B'
	}
	return 'A'
}
