// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package gateway provides the NereidLink gateway client lifecycle
// integrated with the EdgeX Application Service.
package gateway

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	device "github.com/nereidai/nereidlink/sdk/device/go"
	"github.com/nereidai/nereidlink/sdk/device/go/devicemqtt"
	nlgateway "github.com/nereidai/nereidlink/sdk/device/go/gateway"
)

// Config is the gateway connection configuration.
type Config struct {
	BrokerURL        string
	CredentialID     string
	CredentialSecret string
	// EdgeID is the optional MQTT client id / hello edge_id. When empty it
	// defaults to CredentialID so each gateway gets a unique stable id from
	// its credential alone.
	EdgeID string

	// OnReconnect runs after the MQTT client re-establishes a dropped
	// connection (it does not run for the initial connect). The broker
	// forgets session state after a restart, so the handler must re-drive
	// hello and replay presence, connector status and profiles.
	OnReconnect func()

	// OnConnectionLost reports an unexpected transport disconnect. Paho
	// keeps reconnecting and then invokes OnReconnect on success.
	OnConnectionLost func(err error)

	// MQTTDebug enables the underlying paho client's verbose DEBUG trace
	// (reconnect attempts, packet flow). Wire it only when the service log
	// level is debug; paho logs every packet otherwise.
	MQTTDebug bool

	// RootCAPEM optionally pins the broker CA bundle for server-authenticated
	// TLS over a tls:// BrokerURL. Nil leaves the SDK to use the system root
	// store, which is correct for a public CA certificate.
	RootCAPEM []byte
	// ClientCertPEM and ClientKeyPEM switch the MQTT CONNECT identity from
	// id/secret to X.509 client-certificate (mTLS) authentication. They must
	// be set together; CredentialID is still required and must name the
	// matching X509 credential registered in NereidLink.
	ClientCertPEM []byte
	ClientKeyPEM  []byte
	// InsecureSkipVerify disables broker certificate verification. Lab use
	// only; production deployments must pin RootCAPEM instead.
	InsecureSkipVerify bool

	// EnableSecrets advertises the "secrets" feature in the hello optional
	// set. The platform only pushes the desired secret set once the feature
	// is negotiated, so it must track the local secret-delivery config.
	EnableSecrets bool

	// SecretTypeAdverts are the credential-type descriptors advertised in
	// hello alongside the "secrets" feature (ADR-0034). Only types with a
	// real device-service binder should be announced, so the platform secret
	// catalog stays honest; ignored when EnableSecrets is false.
	SecretTypeAdverts []device.SecretTypeAdvert

	// OperationAdverts are the whole-gateway operation descriptors advertised
	// in hello (ADR-0035). NereidLink stores them in its global catalog and
	// derives callable operations and risk solely from them; only operations
	// with a real firmware implementation should be announced.
	OperationAdverts []device.OperationAdvert

	// EnableProfiles advertises the optional "profiles" feature in hello
	// (ADR-0038 device-profile downlink). The platform only pushes spec
	// frames after the feature is echoed in the ack intersection.
	EnableProfiles bool

	// EnableInference advertises the optional "inference" feature in hello
	// (ADR-0039). The gateway itself is a neutral channel: the capability
	// belongs to the deployed inference device service whose model
	// inventory this gateway bridges into the manifest models segment.
	EnableInference bool

	// InferenceBackends are the protocol families advertised with the
	// inference capability block, in preference order. Empty defaults to
	// ["kserve"]; values are protocol families, never vendor names.
	InferenceBackends []string

	// InferenceAvailable reports at hello time whether the deployed
	// inference device service (NI manager device) actually exists on this
	// node. The gateway is a neutral bridge: it must not advertise the
	// inference feature or register edge-inference on a node that has no NI
	// installed, otherwise NL opens binding entry points that can only
	// fail. Nil means always available (tests/legacy static behavior).
	// It is consulted on every (re)hello, so runtime NI install/uninstall
	// converges on the next negotiation.
	InferenceAvailable func() bool

	// Log receives paho internal diagnostics when non-nil.
	Log PahoLogger
}

// defaultInferenceBackends is used when the feature is enabled without an
// explicit backend list.
var defaultInferenceBackends = []string{"kserve", "openai"}

// PahoLogger is the minimal structured logging surface used to route paho's
// Printf-style diagnostics into the service logger.
type PahoLogger interface {
	Debugf(format string, v ...any)
	Infof(format string, v ...any)
	Warnf(format string, v ...any)
	Errorf(format string, v ...any)
}

// pahoLogAdapter turns the structured PahoLogger into paho's Logger
// interface, tagging each stream with a severity.
type pahoLogAdapter struct {
	lc    PahoLogger
	level string
	err   bool
}

func (p pahoLogAdapter) Printf(format string, v ...any) {
	msg := "mqtt[" + p.level + "] " + format
	switch {
	case p.err:
		p.lc.Errorf(msg, v...)
	case p.level == "WARN":
		p.lc.Warnf(msg, v...)
	default:
		p.lc.Debugf(msg, v...)
	}
}

func (p pahoLogAdapter) Println(v ...any) {
	p.Printf("%v", v)
}

// wirePahoLogging redirects paho's global silent loggers into the service
// logger. paho uses package-global log variables, so this affects the whole
// process, which is acceptable because the edge holds one broker link.
func wirePahoLogging(lc PahoLogger, debug bool) {
	if lc == nil {
		return
	}
	pahomqtt.ERROR = pahoLogAdapter{lc: lc, level: "ERROR", err: true}
	pahomqtt.CRITICAL = pahoLogAdapter{lc: lc, level: "CRIT", err: true}
	pahomqtt.WARN = pahoLogAdapter{lc: lc, level: "WARN"}
	if debug {
		pahomqtt.DEBUG = pahoLogAdapter{lc: lc, level: "DBG"}
	}
}

// Build/protocol identity advertised in hello and reported by sys.info.
const (
	// AgentVersion is the edge agent build string.
	AgentVersion = "nereid-edge-m1"
	// ProtoMin and ProtoMax bound the protocol versions the edge supports.
	ProtoMin = "1.0"
	ProtoMax = "1.4"
)

// Client wraps the NL gateway client and its lifecycle state.
type Client struct {
	mq *devicemqtt.Client
	gw *nlgateway.Client

	cfg Config
	// edgeID is the effective MQTT client id / hello edge_id: the explicit
	// Config.EdgeID when set, otherwise the CredentialID. Resolved once in
	// Connect so initial connect and every reconnect re-hello stay stable.
	edgeID string
}

// New creates a new gateway client.
func New(cfg Config) *Client {
	return &Client{cfg: cfg}
}

// resolveEdgeID picks the effective edge identifier: an explicit EdgeID wins
// for backward compatibility; when it is empty the CredentialID is reused.
// CredentialID is globally unique in NereidLink (one credential per gateway),
// so it also yields a unique MQTT client id without extra per-host config.
func resolveEdgeID(cfg Config) string {
	if id := strings.TrimSpace(cfg.EdgeID); id != "" {
		return id
	}
	return strings.TrimSpace(cfg.CredentialID)
}

// Connect establishes MQTT connection to NereidLink.
func (c *Client) Connect(ctx context.Context) error {
	if c.cfg.BrokerURL == "" {
		return errors.New("gateway: incomplete configuration: BrokerURL is required")
	}
	// CredentialID is mandatory for every auth mode: it is the CONNECT
	// username for password auth and names the X509 credential for mTLS, so
	// a client certificate without a registered NL credential ID is invalid.
	if c.cfg.CredentialID == "" {
		return errors.New("gateway: incomplete configuration: CredentialID is required, including mTLS, and must match a credential ID registered in NereidLink")
	}

	c.edgeID = resolveEdgeID(c.cfg)

	cred, err := c.credential()
	if err != nil {
		return err
	}

	// Paho restores the TCP connection, but the broker has forgotten the
	// negotiated session; firmware re-drives hello on reconnect.
	var connected atomic.Bool
	wirePahoLogging(c.cfg.Log, c.cfg.MQTTDebug)
	mq, err := devicemqtt.Connect(ctx, c.mqttOptions(cred, &connected))
	if err != nil {
		return err
	}

	c.mq = mq
	c.gw = nlgateway.Wrap(mq)
	if c.cfg.Log != nil {
		c.cfg.Log.Infof("gateway identity: edge_id=%s credential=%s broker=%s",
			c.edgeID, c.cfg.CredentialID, c.cfg.BrokerURL)
	}
	return nil
}

// credential selects the MQTT CONNECT identity: an X.509 client certificate
// when both PEM halves are configured, otherwise the id/secret pair. The
// cert credential carries the same RootCAPEM so an mTLS client can also pin
// a private server CA.
func (c *Client) credential() (device.Credential, error) {
	hasCert := len(c.cfg.ClientCertPEM) > 0
	hasKey := len(c.cfg.ClientKeyPEM) > 0
	if hasCert != hasKey {
		return nil, errors.New("gateway: ClientCertPEM and ClientKeyPEM must be configured together")
	}
	if hasCert {
		return device.ClientCert(c.cfg.CredentialID, c.cfg.ClientCertPEM, c.cfg.ClientKeyPEM, c.cfg.RootCAPEM), nil
	}
	return device.Basic(c.cfg.CredentialID, c.cfg.CredentialSecret), nil
}

// mqttOptions builds the SDK MQTT options, including TLS trust materials and
// the reconnect hooks. It is split out so the TLS/credential wiring is
// unit-testable without opening a broker connection.
func (c *Client) mqttOptions(cred device.Credential, connected *atomic.Bool) devicemqtt.Options {
	return devicemqtt.Options{
		BrokerURL:          c.cfg.BrokerURL,
		Credential:         cred,
		ClientID:           c.edgeID,
		KeepAlive:          devicemqtt.DefaultKeepAlive,
		Will:               devicemqtt.GoodbyeWill("transport_down"),
		AutoReconnect:      true,
		RootCAPEM:          c.cfg.RootCAPEM,
		InsecureSkipVerify: c.cfg.InsecureSkipVerify,
		OnConnectionLostHook: func(_ pahomqtt.Client, err error) {
			if c.cfg.Log != nil {
				c.cfg.Log.Warnf("NereidLink MQTT connection lost: %s; paho will auto-reconnect", err.Error())
			}
			if c.cfg.OnConnectionLost != nil {
				c.cfg.OnConnectionLost(err)
			}
		},
		OnConnectHook: func(_ pahomqtt.Client) {
			if !connected.Load() {
				connected.Store(true)
				return
			}
			if c.cfg.OnReconnect != nil {
				c.cfg.OnReconnect()
			}
		},
	}
}

// featureSecrets is the protocol feature name for ADR-0034 Edge Secret
// delivery. The device SDK predates the feature and defines no constant, so
// the gateway pins the wire string it advertises in hello.
const featureSecrets = "secrets"

// Negotiate performs hello negotiation with NereidLink.
func (c *Client) Negotiate(ctx context.Context, connectors ...device.ConnectorAdvert) (*device.HelloAck, error) {
	if c.gw == nil {
		return nil, errors.New("gateway: not connected")
	}

	hello := c.buildHello(connectors)
	ack, err := c.gw.NegotiateHello(ctx, hello, 10*time.Second)
	if err != nil {
		return nil, err
	}
	for _, result := range ack.SecretTypes {
		if result.Accepted {
			c.cfg.Log.Infof("secret type %s v%s registered", result.Type, result.TypeVersion)
		} else {
			c.cfg.Log.Warnf("secret type %s v%s rejected: %s", result.Type, result.TypeVersion, result.Reason)
		}
	}
	for _, result := range ack.Operations {
		if result.Accepted {
			c.cfg.Log.Infof("operation %s v%s registered", result.Operation, result.TypeVersion)
		} else {
			c.cfg.Log.Warnf("operation %s v%s rejected: %s", result.Operation, result.TypeVersion, result.Reason)
		}
	}
	if c.cfg.EnableProfiles {
		// Non-empty echoed intersection is the go/no-go signal: on an older
		// platform the feature is downgraded away and this stays empty.
		c.cfg.Log.Infof("profile downlink formats negotiated: %v", ack.ProfileFormats)
	}
	if c.inferenceAdvertised() {
		// The echoed block is the go/no-go signal, exactly like the profile
		// formats: an older platform leaves it nil and the manifest models
		// segment stays omitted.
		if ack.Inference != nil {
			c.cfg.Log.Infof("inference contract %s negotiated, backends %v",
				ack.Inference.Contract, ack.Inference.Backends)
		} else {
			c.cfg.Log.Warnf("inference feature not accepted by platform; models segment disabled")
		}
	}
	return ack, nil
}

// buildHello assembles the negotiation frame. It is split out so the secret
// feature wiring is unit-testable without a broker.
func (c *Client) buildHello(connectors []device.ConnectorAdvert) device.Hello {
	optional := []string{device.FeatureShadow}
	if c.cfg.EnableSecrets {
		optional = append(optional, featureSecrets)
	}
	if c.cfg.EnableProfiles {
		optional = append(optional, device.FeatureProfiles)
	}
	if c.inferenceAdvertised() {
		optional = append(optional, device.FeatureInference)
		// The evidence JPEGs travel as MQTT binary media markers on the
		// existing telemetry topic. Advertise the relay capability only on
		// inference gateways so gateways without binary media do not claim
		// it; it stays optional because an older platform must still accept
		// the base inference/telemetry features.
		optional = append(optional, device.FeatureBlobRelay)
	}
	hello := device.Hello{
		EdgeID:       c.edgeID,
		AgentVersion: AgentVersion,
		Proto:        device.ProtoRange{Min: ProtoMin, Max: ProtoMax},
		Features: device.FeatureSet{
			Required: []string{device.FeatureTelemetry, device.FeaturePresence, device.FeatureCommand},
			Optional: optional,
		},
	}
	if len(connectors) > 0 {
		hello.Connectors = connectors
	}
	if c.cfg.EnableSecrets && len(c.cfg.SecretTypeAdverts) > 0 {
		hello.SecretTypes = c.cfg.SecretTypeAdverts
	}
	if len(c.cfg.OperationAdverts) > 0 {
		hello.Operations = c.cfg.OperationAdverts
	}
	if c.cfg.EnableProfiles {
		// Preference order must match formats the agent truly lands:
		// EdgeX documents are upserted into local core-metadata, the
		// neutral document is the guaranteed fallback.
		hello.ProfileFormats = []string{device.ProfileFormatEdgeX, device.ProfileFormatNeutral}
	}
	if c.inferenceAdvertised() {
		// ADR-0039: describe the deployed inference plane. The gateway
		// itself performs no inference; the block is the contract/backends
		// of the NereidInference device service bridged by this gateway.
		backends := c.cfg.InferenceBackends
		if len(backends) == 0 {
			backends = defaultInferenceBackends
		}
		hello.Inference = &device.InferenceAdvert{
			Contract: device.InferenceContractModel1,
			Backends: backends,
		}
	}
	return hello
}

// inferenceAdvertised reports whether this hello must carry the inference
// capability: enabled in config AND the NI plane was proven present at
// negotiation time.
func (c *Client) inferenceAdvertised() bool {
	return c.cfg.EnableInference &&
		(c.cfg.InferenceAvailable == nil || c.cfg.InferenceAvailable())
}

// Gateway returns the wrapped gateway client.
func (c *Client) Gateway() *nlgateway.Client {
	return c.gw
}

// MQTT returns the underlying MQTT client.
func (c *Client) MQTT() *devicemqtt.Client {
	return c.mq
}

// Disconnect closes the MQTT connection.
func (c *Client) Disconnect() {
	if c.mq != nil {
		c.mq.Disconnect(500)
	}
}
