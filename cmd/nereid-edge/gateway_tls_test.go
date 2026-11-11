// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nereidai/nereidedge/internal/config"
)

func writeTempPEM(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func TestLoadGatewayTLSMaterialsEmpty(t *testing.T) {
	root, cert, key, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if root != nil || cert != nil || key != nil {
		t.Fatalf("empty config must yield nil PEMs, got %q/%q/%q", root, cert, key)
	}
}

func TestLoadGatewayTLSMaterialsRootCAOnly(t *testing.T) {
	caPath := writeTempPEM(t, "ca.pem", "ca-bundle")
	root, cert, key, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{RootCAFile: caPath})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(root) != "ca-bundle" || cert != nil || key != nil {
		t.Fatalf("unexpected PEMs: %q/%q/%q", root, cert, key)
	}
}

func TestLoadGatewayTLSMaterialsClientCertPair(t *testing.T) {
	caPath := writeTempPEM(t, "ca.pem", "ca-bundle")
	certPath := writeTempPEM(t, "client.crt", "client-cert")
	keyPath := writeTempPEM(t, "client.key", "client-key")
	root, cert, key, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{
		RootCAFile:     caPath,
		ClientCertFile: certPath,
		ClientKeyFile:  keyPath,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(root) != "ca-bundle" || string(cert) != "client-cert" || string(key) != "client-key" {
		t.Fatalf("unexpected PEMs: %q/%q/%q", root, cert, key)
	}
}

func TestLoadGatewayTLSMaterialsHalfPair(t *testing.T) {
	certPath := writeTempPEM(t, "client.crt", "client-cert")
	if _, _, _, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{ClientCertFile: certPath}); err == nil {
		t.Fatal("cert without key must fail before any read")
	}
	keyPath := writeTempPEM(t, "client.key", "client-key")
	if _, _, _, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{ClientKeyFile: keyPath}); err == nil {
		t.Fatal("key without cert must fail before any read")
	}
}

func TestLoadGatewayTLSMaterialsMissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.pem")
	if _, _, _, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{RootCAFile: missing}); err == nil {
		t.Fatal("a missing PEM file must fail fast")
	}
}

func TestLoadGatewayTLSMaterialsInlinePEM(t *testing.T) {
	root, cert, key, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{
		RootCAPEM:     "inline-ca",
		ClientCertPEM: "inline-cert",
		ClientKeyPEM:  "inline-key",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(root) != "inline-ca" || string(cert) != "inline-cert" || string(key) != "inline-key" {
		t.Fatalf("unexpected PEMs: %q/%q/%q", root, cert, key)
	}
}

func TestLoadGatewayTLSMaterialsInlineWinsOverFile(t *testing.T) {
	caPath := writeTempPEM(t, "ca.pem", "file-ca")
	root, _, _, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{
		RootCAFile: caPath,
		RootCAPEM:  "inline-ca",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(root) != "inline-ca" {
		t.Fatalf("inline PEM must win, got %q", root)
	}
}

func TestLoadGatewayTLSMaterialsRejectsMixedDeliveryModes(t *testing.T) {
	certPath := writeTempPEM(t, "client.crt", "client-cert")
	keyPath := writeTempPEM(t, "client.key", "client-key")
	cases := []struct {
		name string
		cfg  config.NereidEdgeConfig
	}{
		{
			name: "inline cert with file key",
			cfg:  config.NereidEdgeConfig{ClientCertPEM: "inline-cert", ClientKeyFile: keyPath},
		},
		{
			name: "file cert with inline key",
			cfg:  config.NereidEdgeConfig{ClientCertFile: certPath, ClientKeyPEM: "inline-key"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := loadGatewayTLSMaterials(tc.cfg); err == nil {
				t.Fatal("mixed inline/file delivery must be rejected")
			}
		})
	}
}

func TestNormalizeInlinePEMEscapes(t *testing.T) {
	oneLine := `-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n`
	got := normalizeInlinePEM(oneLine)
	if strings.Contains(got, `\n`) {
		t.Fatalf("literal \\n escapes must become real newlines: %q", got)
	}
	want := "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n"
	if got != want {
		t.Fatalf("normalized PEM = %q, want %q", got, want)
	}
	multiLine := "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n"
	if normalizeInlinePEM(multiLine) != multiLine {
		t.Fatal("an already multi-line PEM must pass through unchanged")
	}
}

// TestLoadGatewayTLSMaterialsSingleLineEnvPEMParses proves the .env-style
// single-line form (literal \n escapes) survives normalization and is a valid
// PEM bundle for the Go x509 verifier the SDK ultimately uses.
func TestLoadGatewayTLSMaterialsSingleLineEnvPEMParses(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "nereidlink-test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	// Flatten the real PEM into the literal-\n single line a .env file holds.
	singleLine := strings.ReplaceAll(string(pemBytes), "\n", `\n`)
	root, _, _, err := loadGatewayTLSMaterials(config.NereidEdgeConfig{RootCAPEM: singleLine})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(root) {
		t.Fatal("normalized single-line PEM must parse as an x509 certificate")
	}
}
