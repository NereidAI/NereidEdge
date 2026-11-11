// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func TestCredentialDefaultsToBasic(t *testing.T) {
	c := &Client{cfg: Config{CredentialID: "gw-1", CredentialSecret: "secret"}}
	cred, err := c.credential()
	if err != nil {
		t.Fatalf("credential() unexpected error: %v", err)
	}
	basic, ok := cred.(device.BasicCredential)
	if !ok {
		t.Fatalf("credential type = %T, want device.BasicCredential", cred)
	}
	if basic.ID != "gw-1" || basic.Secret != "secret" {
		t.Fatalf("basic credential = %+v", basic)
	}
}

func TestCredentialSelectsClientCertWithRootCA(t *testing.T) {
	rootPEM := []byte("-----BEGIN CERTIFICATE-----\nroot\n-----END CERTIFICATE-----")
	certPEM := []byte("-----BEGIN CERTIFICATE-----\nclient\n-----END CERTIFICATE-----")
	keyPEM := []byte("-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----")
	c := &Client{cfg: Config{
		CredentialID:  "gw-1",
		RootCAPEM:     rootPEM,
		ClientCertPEM: certPEM,
		ClientKeyPEM:  keyPEM,
	}}
	cred, err := c.credential()
	if err != nil {
		t.Fatalf("credential() unexpected error: %v", err)
	}
	cert, ok := cred.(device.CertCredential)
	if !ok {
		t.Fatalf("credential type = %T, want device.CertCredential", cred)
	}
	if cert.ID != "gw-1" {
		t.Fatalf("cert credential ID = %q, want gw-1", cert.ID)
	}
	if string(cert.CertPEM) != string(certPEM) || string(cert.KeyPEM) != string(keyPEM) {
		t.Fatal("client cert/key PEM was not passed through")
	}
	if string(cert.RootCAPEM) != string(rootPEM) {
		t.Fatal("mTLS credential must carry the pinned server RootCAPEM")
	}
}

func TestCredentialRejectsHalfClientCertPair(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{name: "cert without key", cfg: Config{ClientCertPEM: []byte("cert")}},
		{name: "key without cert", cfg: Config{ClientKeyPEM: []byte("key")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{cfg: tc.cfg}
			if _, err := c.credential(); err == nil {
				t.Fatal("credential() must reject a half-configured client cert pair")
			}
		})
	}
}

func TestMQTTOptionsCarriesTLSMaterials(t *testing.T) {
	rootPEM := []byte("root-ca-pem")
	c := &Client{
		edgeID: "gw-1",
		cfg: Config{
			BrokerURL:          "tls://nereidlink.example:8883",
			CredentialID:       "gw-1",
			CredentialSecret:   "secret",
			RootCAPEM:          rootPEM,
			InsecureSkipVerify: true,
		},
	}
	cred, err := c.credential()
	if err != nil {
		t.Fatalf("credential() unexpected error: %v", err)
	}
	opts := c.mqttOptions(cred, &atomic.Bool{})

	if opts.BrokerURL != c.cfg.BrokerURL || opts.ClientID != "gw-1" {
		t.Fatalf("identity fields wrong: %+v", opts)
	}
	if !opts.AutoReconnect || opts.Will == nil {
		t.Fatal("reconnect policy and goodbye will must stay enabled")
	}
	if string(opts.RootCAPEM) != string(rootPEM) {
		t.Fatalf("RootCAPEM not wired: %q", string(opts.RootCAPEM))
	}
	if !opts.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify not wired")
	}
	if _, ok := opts.Credential.(device.BasicCredential); !ok {
		t.Fatalf("credential type = %T, want BasicCredential", opts.Credential)
	}
}

// TestConnectRequiresCredentialIDEvenForMTLS locks the rule that a client
// certificate never replaces the NereidLink credential ID: mTLS still needs
// CredentialID to name the registered X509 credential. The check runs before
// any network dial, so no broker is required.
func TestConnectRequiresCredentialIDEvenForMTLS(t *testing.T) {
	c := New(Config{
		BrokerURL:     "tls://nereidlink.example:8883",
		ClientCertPEM: []byte("client-cert"),
		ClientKeyPEM:  []byte("client-key"),
	})
	err := c.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect must fail without CredentialID, even with a full mTLS pair")
	}
	if !strings.Contains(err.Error(), "CredentialID") {
		t.Fatalf("error must name CredentialID, got: %v", err)
	}
}
