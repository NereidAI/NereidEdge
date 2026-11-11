// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package devicemqtt

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// TestTLSConfigForInfersServerName pins the rule that every non-nil TLS
// config carries a ServerName derived from the broker host: openDirect uses
// tls.Client directly, which rejects a config without ServerName or
// InsecureSkipVerify with "either ServerName or InsecureSkipVerify must be
// specified in the tls.Config".
func TestTLSConfigForInfersServerName(t *testing.T) {
	const broker = "tls://nereidlink.example:8883"
	caPEM, keyPEM, _, _ := generateTestCertificate(t, net.ParseIP("127.0.0.1"))

	cases := []struct {
		name      string
		opts      Options
		wantCerts int
		wantRoots bool
		insecure  bool
	}{
		{
			name: "public CA default config",
			opts: Options{BrokerURL: broker, Credential: device.Basic("gw", "secret")},
		},
		{
			name:      "private CA pinned bundle",
			opts:      Options{BrokerURL: broker, Credential: device.Basic("gw", "secret"), RootCAPEM: caPEM},
			wantRoots: true,
		},
		{
			name: "mTLS client certificate",
			opts: Options{
				BrokerURL:  broker,
				Credential: device.ClientCert("gw", caPEM, keyPEM, caPEM),
				RootCAPEM:  caPEM,
				ClientID:   "gw",
			},
			wantCerts: 1,
			wantRoots: true,
		},
		{
			name:     "insecure lab opt-in still sends SNI",
			opts:     Options{BrokerURL: broker, Credential: device.Basic("gw", "secret"), InsecureSkipVerify: true},
			insecure: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := tlsConfigFor(tc.opts)
			if err != nil {
				t.Fatalf("tlsConfigFor() error: %v", err)
			}
			if cfg == nil {
				t.Fatal("tlsConfigFor() returned nil for a tls:// broker")
			}
			if cfg.ServerName != "nereidlink.example" {
				t.Fatalf("ServerName = %q, want nereidlink.example", cfg.ServerName)
			}
			if cfg.InsecureSkipVerify != tc.insecure {
				t.Fatalf("InsecureSkipVerify = %v, want %v", cfg.InsecureSkipVerify, tc.insecure)
			}
			if tc.wantRoots && cfg.RootCAs == nil {
				t.Fatal("RootCAs must be populated from RootCAPEM")
			}
			if tc.wantCerts != len(cfg.Certificates) {
				t.Fatalf("Certificates len = %d, want %d", len(cfg.Certificates), tc.wantCerts)
			}
		})
	}
}

func TestTLSConfigForPlaintextReturnsNil(t *testing.T) {
	cfg, err := tlsConfigFor(Options{
		BrokerURL:  "tcp://nereidlink.example:1883",
		Credential: device.Basic("gw", "secret"),
	})
	if err != nil || cfg != nil {
		t.Fatalf("plaintext must yield nil TLS config, got %v (err %v)", cfg, err)
	}
}

// TestOpenDirectTLSHandshake reproduces the field failure where tls.Client
// rejected the SDK config for missing ServerName. A self-signed listener
// with a 127.0.0.1 SAN completes the handshake when the CA bundle is pinned,
// and is rejected by verification without it.
func TestOpenDirectTLSHandshake(t *testing.T) {
	caPEM, _, serverCert, serverKey := generateTestCertificate(t, net.ParseIP("127.0.0.1"))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	tlsListener := tls.NewListener(listener, &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}},
	})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			conn, acceptErr := tlsListener.Accept()
			if acceptErr != nil {
				return
			}
			// Serve every subtest connection in its own goroutine; a
			// verification failure aborts the handshake, which still
			// closes the connection cleanly on the server side.
			go func(c net.Conn) {
				if tlsConn, ok := c.(*tls.Conn); ok {
					_ = tlsConn.Handshake()
				}
				_ = c.Close()
			}(conn)
		}
	}()
	t.Cleanup(func() {
		_ = tlsListener.Close()
		<-serverDone
	})

	brokerURL := "tls://" + listener.Addr().String()
	uri, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatalf("parse broker URL: %v", err)
	}

	t.Run("pinned CA completes the handshake", func(t *testing.T) {
		tlsCfg, err := tlsConfigFor(Options{
			BrokerURL:  brokerURL,
			Credential: device.Basic("gw", "secret"),
			RootCAPEM:  caPEM,
		})
		if err != nil {
			t.Fatalf("tlsConfigFor() error: %v", err)
		}
		conn, err := openDirect(uri, tlsCfg)
		if err != nil {
			t.Fatalf("openDirect() handshake failed: %v", err)
		}
		_ = conn.Close()
	})

	t.Run("missing trust fails verification, not ServerName", func(t *testing.T) {
		tlsCfg, err := tlsConfigFor(Options{
			BrokerURL:  brokerURL,
			Credential: device.Basic("gw", "secret"),
		})
		if err != nil {
			t.Fatalf("tlsConfigFor() error: %v", err)
		}
		conn, err := openDirect(uri, tlsCfg)
		if err == nil {
			_ = conn.Close()
			t.Fatal("handshake must fail for an untrusted self-signed certificate")
		}
		if strings.Contains(err.Error(), "ServerName") {
			t.Fatalf("regression: missing ServerName error returned: %v", err)
		}
		if !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("expected a certificate verification error, got: %v", err)
		}
	})
}

// generateTestCertificate returns the certificate PEM, its EC private key
// PEM, parsed certificate and key for a loopback TLS listener.
func generateTestCertificate(t *testing.T, ip net.IP) (certPEM, keyPEM []byte, leaf *x509.Certificate, key any) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "devicemqtt-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{ip},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, parsed, priv
}
