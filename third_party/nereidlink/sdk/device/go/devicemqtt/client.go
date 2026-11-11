// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package devicemqtt is the MQTT device ingress client for NereidLink.
//
// It is a thin wrapper over the Eclipse Paho v1 MQTT client: it binds the
// frozen v1/me and v1/gateway topics to typed frame structs, sets the
// CONNECT credential pair, registers the gateway Last Will and exposes
// typed publish/subscribe helpers. The package owns no goroutines beyond
// Paho's own network loop: reconnection policy, poll cadence and state
// machines stay in firmware.
//
// Downlink topics are all delivered QoS 0 and non-retained by the server,
// so device subscriptions may use QoS 0; uplink QoS is the publisher's
// choice (QoS 1 is recommended for telemetry).
package devicemqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/eclipse/paho.mqtt.golang"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

// Default protocol values suggested by docs/protocols/nereid-edge/v1.md.
const (
	// DefaultKeepAlive is the suggested MQTT transport keepalive; the
	// server applies the standard 1.5x PINGRESP timeout.
	DefaultKeepAlive = 60 * time.Second

	// DefaultConnectTimeout bounds the CONNECT handshake.
	DefaultConnectTimeout = 10 * time.Second
)

// Options configures one MQTT connection.
type Options struct {
	// BrokerURL is the MQTT listener URL, e.g. tls://host:8883 or
	// tcp://host:1883. Additional servers can be set with ExtraBrokers.
	BrokerURL string

	// ExtraBrokers lists failover broker URLs passed to Paho.
	ExtraBrokers []string

	// Credential is the CONNECT identity. Token -> (token, ""); Basic ->
	// (id, secret); Cert -> (id, "") with a client TLS certificate.
	Credential device.Credential

	// ClientID is the MQTT client id. The platform does not derive device
	// identity from it, but it must be unique per active connection.
	ClientID string

	// KeepAlive defaults to DefaultKeepAlive when zero.
	KeepAlive time.Duration

	// PersistentSession opts into MQTT clean session=false. The default
	// (false) is a clean session; gateway desired state does not rely on
	// broker persistence because it is re-driven after every hello.
	PersistentSession bool

	// AutoReconnect enables Paho's built-in reconnect loop. When enabled
	// subscriptions made before a drop are re-established by Paho; the
	// gateway hello handshake must still be re-driven by firmware because
	// the server state is UNIDENTIFIED until a new hello arrives.
	AutoReconnect bool

	// RootCAPEM optionally pins the server certificate for token/basic
	// credentials; Cert credentials already carry their own bundle.
	RootCAPEM []byte

	// InsecureSkipVerify disables server certificate verification.
	InsecureSkipVerify bool

	// Will optionally sets an MQTT Last Will; use GoodbyeWill() for the
	// gateway goodbye LWT.
	Will *Will

	// ConnectTimeout defaults to DefaultConnectTimeout when zero.
	ConnectTimeout time.Duration

	// OnConnectHook fires after every (re)connect, matching Paho's
	// OnConnect. It is the place to re-drive a gateway hello.
	OnConnectHook func(client mqtt.Client)

	// OnConnectionLostHook reports an unexpected disconnect.
	OnConnectionLostHook func(client mqtt.Client, err error)
}

// Client is the connected device MQTT client.
type Client struct {
	paho mqtt.Client
}

// Connect opens the MQTT connection and waits for CONNACK.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	if opts.BrokerURL == "" {
		return nil, fmt.Errorf("devicemqtt: BrokerURL is required")
	}
	if opts.Credential == nil {
		return nil, fmt.Errorf("devicemqtt: Credential is required")
	}
	keepAlive := opts.KeepAlive
	if keepAlive == 0 {
		keepAlive = DefaultKeepAlive
	}
	connectTimeout := opts.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = DefaultConnectTimeout
	}

	tlsCfg, err := tlsConfigFor(opts)
	if err != nil {
		return nil, err
	}

	username, password := device.MQTTUsernamePassword(opts.Credential)
	po := mqtt.NewClientOptions().
		AddBroker(opts.BrokerURL)
	for _, broker := range opts.ExtraBrokers {
		po.AddBroker(broker)
	}
	po.SetClientID(opts.ClientID)
	po.SetUsername(username)
	if password != "" {
		po.SetPassword(password)
	}
	po.SetKeepAlive(keepAlive)
	po.SetAutoReconnect(opts.AutoReconnect)
	po.SetCleanSession(!opts.PersistentSession)
	if tlsCfg != nil {
		po.SetTLSConfig(tlsCfg)
	}
	po.SetCustomOpenConnectionFn(func(uri *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		return openDirect(uri, tlsCfg)
	})
	if opts.Will != nil {
		payload, err := opts.Will.Encode()
		if err != nil {
			return nil, err
		}
		po.SetWill(opts.Will.Topic, payload, opts.Will.QoS, opts.Will.Retained)
	}
	if opts.OnConnectHook != nil {
		po.OnConnect = opts.OnConnectHook
	}
	if opts.OnConnectionLostHook != nil {
		po.OnConnectionLost = opts.OnConnectionLostHook
	}
	if deadline, ok := ctx.Deadline(); ok {
		po.SetConnectTimeout(time.Until(deadline))
	} else {
		po.SetConnectTimeout(connectTimeout)
	}

	pahoClient := mqtt.NewClient(po)
	token := pahoClient.Connect()
	if err := waitConnect(ctx, token, connectTimeout); err != nil {
		return nil, err
	}
	return &Client{paho: pahoClient}, nil
}

// waitConnect blocks for the CONNACK token, honoring ctx and the timeout.
func waitConnect(ctx context.Context, token mqtt.Token, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		token.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		if token.Error() != nil {
			return fmt.Errorf("devicemqtt connect: %w", token.Error())
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("devicemqtt connect timed out after %s", timeout)
	}
}

// tlsConfigFor builds the TLS configuration from either a client
// certificate credential or a pinned server CA bundle.
func tlsConfigFor(opts Options) (*tls.Config, error) {
	var tlsCfg *tls.Config
	if certCfg, err := device.TLSConfig(opts.Credential); err != nil {
		return nil, err
	} else if certCfg != nil {
		if certCfg.RootCAs == nil && len(opts.RootCAPEM) > 0 {
			root, err := device.ServerTLSConfig(opts.RootCAPEM, opts.InsecureSkipVerify)
			if err != nil {
				return nil, err
			}
			certCfg.RootCAs = root.RootCAs
		}
		if opts.InsecureSkipVerify {
			certCfg.InsecureSkipVerify = true
		}
		tlsCfg = certCfg
	} else if len(opts.RootCAPEM) > 0 || opts.InsecureSkipVerify {
		root, err := device.ServerTLSConfig(opts.RootCAPEM, opts.InsecureSkipVerify)
		if err != nil {
			return nil, err
		}
		tlsCfg = root
	} else if strings.HasPrefix(opts.BrokerURL, "ssl://") || strings.HasPrefix(opts.BrokerURL, "tls://") {
		// System roots are the correct default for a public certificate.
		tlsCfg = &tls.Config{}
	} else {
		return nil, nil
	}

	// openDirect dials raw TCP itself and wraps the connection with
	// tls.Client, which cannot infer the SNI/verification name from a dial
	// address the way tls.Dial does. Pin ServerName to the broker host so
	// certificate verification works for public CAs, private RootCAPEM
	// bundles and mTLS client certificates without InsecureSkipVerify.
	if tlsCfg.ServerName == "" {
		uri, err := url.Parse(opts.BrokerURL)
		if err != nil {
			return nil, fmt.Errorf("devicemqtt: parse broker URL for TLS ServerName: %w", err)
		}
		tlsCfg.ServerName = uri.Hostname()
	}
	return tlsCfg, nil
}

// dialContext dials without consulting HTTP(S)_PROXY. Device brokers live
// on local/field networks; a developer workstation proxy exported in the
// environment would otherwise capture MQTT TCP connections (Paho's default
// dialer honors proxy variables).
func dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := &net.Dialer{}
	return dialer.DialContext(ctx, network, address)
}

// openDirect establishes the broker connection bypassing environment
// proxies. It supports the plain schemes tcp/mqtt and the TLS schemes
// tls/ssl/mqtts/mqtt+ssl/tcps; websocket transports are not part of the
// device SDK surface.
func openDirect(uri *url.URL, tlsCfg *tls.Config) (net.Conn, error) {
	switch uri.Scheme {
	case "tcp", "mqtt":
		return dialContext(context.Background(), "tcp", uri.Host)
	case "tls", "ssl", "mqtts", "mqtt+ssl", "tcps":
		raw, err := dialContext(context.Background(), "tcp", uri.Host)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(raw, tlsCfg)
		if err := tlsConn.HandshakeContext(context.Background()); err != nil {
			_ = raw.Close()
			return nil, err
		}
		return tlsConn, nil
	default:
		return nil, fmt.Errorf("devicemqtt: unsupported broker scheme %q (use tcp:// or tls://)", uri.Scheme)
	}
}

// Paho exposes the underlying client for operations the thin wrapper does
// not cover.
func (c *Client) Paho() mqtt.Client { return c.paho }

// Disconnect closes the session gracefully. A quiesce of 0 is typical for
// constrained devices.
func (c *Client) Disconnect(quiesce uint) {
	c.paho.Disconnect(quiesce)
}
