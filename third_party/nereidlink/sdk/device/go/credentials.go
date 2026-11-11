// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"
)

// Credential authenticates a device or gateway against an ingress.
//
// The platform accepts three shapes, mirroring the registry credential
// types: a lone access token (TOKEN credentials with an empty secret),
// an id/secret pair (PASSWORD credentials; also accepted as MQTT
// username/password and HTTP Basic), or an X.509 client certificate
// (X509 credentials over TLS/mTLS).
//
// Credential values are created with Token, Basic and ClientCert; custom
// implementations are intentionally not supported.
type Credential interface {
	isCredential()
}

// TokenCredential is a passwordless device access token. Over HTTP it is
// sent as "Authorization: Bearer <token>"; over MQTT it becomes the
// CONNECT username with an empty password.
type TokenCredential string

// BasicCredential is a credentials id plus its secret. Over HTTP it is
// sent as "Authorization: Basic base64(id:secret)"; over MQTT the pair
// becomes the CONNECT username/password.
type BasicCredential struct {
	// ID is the credentials_id presented to the registry.
	ID string

	// Secret is the credential value/password.
	Secret string
}

// CertCredential is an X.509 client certificate used on a TLS listener.
// The ID still names the X509 credential during registry authentication;
// the proof of possession is the certificate itself.
type CertCredential struct {
	// ID is the credentials_id naming the X509 credential.
	ID string

	// CertPEM is the client certificate (or chain leaf first), PEM encoded.
	CertPEM []byte

	// KeyPEM is the private key matching CertPEM, PEM encoded.
	KeyPEM []byte

	// RootCAPEM optionally pins the server trust bundle; empty uses the
	// host root store.
	RootCAPEM []byte

	// InsecureSkipVerify disables server certificate verification. It must
	// never be set in production deployments.
	InsecureSkipVerify bool
}

// Token returns a passwordless access-token credential.
func Token(token string) Credential { return TokenCredential(token) }

// Basic returns an id/secret credential.
func Basic(id, secret string) Credential { return BasicCredential{ID: id, Secret: secret} }

// ClientCert returns an X.509 client-certificate credential.
func ClientCert(id string, certPEM, keyPEM, rootCAPEM []byte) Credential {
	return CertCredential{ID: id, CertPEM: certPEM, KeyPEM: keyPEM, RootCAPEM: rootCAPEM}
}

// isCredential implements Credential.
func (TokenCredential) isCredential() {}

// isCredential implements Credential.
func (BasicCredential) isCredential() {}

// isCredential implements Credential.
func (CertCredential) isCredential() {}

// SetAuthorization writes the HTTP Authorization header for cred.
// Certificate credentials carry no header because the proof of possession
// is the TLS client certificate.
func SetAuthorization(req *http.Request, cred Credential) {
	switch c := cred.(type) {
	case TokenCredential:
		req.Header.Set("Authorization", "Bearer "+string(c))
	case BasicCredential:
		{
			token := base64.StdEncoding.EncodeToString([]byte(c.ID + ":" + c.Secret))
			req.Header.Set("Authorization", "Basic "+token)
		}
	case CertCredential:
		// Authentication happens in the TLS handshake; the id names the
		// credential but no Authorization header is required.
	}
}

// MQTTUsernamePassword returns the CONNECT username/password pair for cred.
// Certificate credentials return the credential id with an empty password.
func MQTTUsernamePassword(cred Credential) (string, string) {
	switch c := cred.(type) {
	case TokenCredential:
		return string(c), ""
	case BasicCredential:
		return c.ID, c.Secret
	case CertCredential:
		return c.ID, ""
	default:
		return "", ""
	}
}

// TLSConfig builds the *tls.Config for a certificate credential. It returns
// nil for token/basic credentials, which can run over a plain listener or
// over a server-authenticated TLS listener that needs no client cert.
func TLSConfig(cred Credential) (*tls.Config, error) {
	cert, ok := cred.(CertCredential)
	if !ok {
		return nil, nil
	}
	pair, err := tls.X509KeyPair(cert.CertPEM, cert.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load device client key pair: %w", err)
	}
	cfg := &tls.Config{
		Certificates:       []tls.Certificate{pair},
		InsecureSkipVerify: cert.InsecureSkipVerify, //nolint:gosec // explicit opt-in for test setups
	}
	if len(cert.RootCAPEM) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cert.RootCAPEM) {
			return nil, fmt.Errorf("parse device root CA bundle")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// ServerTLSConfig builds a server-authenticated TLS config without a client
// certificate, pinning rootCAPEM when provided. It is used by devices that
// hold a token/basic credential but connect to an MQTTS/HTTPS listener with
// a private CA.
func ServerTLSConfig(rootCAPEM []byte, insecureSkipVerify bool) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: insecureSkipVerify} //nolint:gosec // explicit opt-in
	if len(rootCAPEM) == 0 {
		return cfg, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rootCAPEM) {
		return nil, fmt.Errorf("parse server root CA bundle")
	}
	cfg.RootCAs = pool
	return cfg, nil
}
