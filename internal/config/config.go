// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package config defines custom configuration structures for NereidEdge.
package config

// NereidEdgeConfig holds NereidLink gateway connection configuration.
type NereidEdgeConfig struct {
	// BrokerURL is the MQTT broker URL to NereidLink (tcp://host:1883 or tls://host:8883).
	BrokerURL string
	// CredentialID is the gateway credential ID registered in NereidLink.
	// It is required for every authentication mode: it is the MQTT CONNECT
	// username for password auth, and it names the X509 credential during
	// certificate (mTLS) auth. The value must match the credential ID created
	// on the NereidLink side; a certificate alone is not enough.
	CredentialID string
	// CredentialSecret is the gateway credentials secret (password/token).
	// Unused for X509 certificate authentication.
	CredentialSecret string
	// EdgeID is the optional edge instance identifier used as the MQTT
	// client id and hello edge_id. Leave empty to reuse CredentialID: each
	// gateway credential is already globally unique, so per-host deployment
	// only needs its own credential and never collides on client id.
	EdgeID string
	// DeviceMap maps EdgeX device names to NL sub-device UUIDs.
	// Format: "edgex-device1:nl-uuid1,edgex-device2:nl-uuid2"
	DeviceMap string
	// ForwardUnmapped controls whether events from EdgeX devices absent from
	// DeviceMap are forwarded using the raw EdgeX name (NL will classify them
	// as unregistered). Default false: skip unbound devices.
	ForwardUnmapped bool
	// Operations supplies the local sources some whole-gateway operations
	// (ADR-0035) need. Unset sources degrade that one operation only.
	Operations OperationsConfig
	// ConnectorObserve makes the edgex-device connector applier log intended
	// changes without mutating EdgeX core-metadata. Default false (apply).
	ConnectorObserve bool
	// CoreCommandURL is the base URL of the EdgeX core-command service.
	// Empty means the EdgeX docker-network default
	// (http://edgex-core-command:59882).
	CoreCommandURL string
	// MQTTTrace enables the underlying paho client's per-packet DEBUG trace.
	// It is intentionally separate from the service log level: at DEBUG the
	// edge emits many events and paho would log ~10 lines per event. Leave
	// false for normal operation, enable only for transport forensics.
	MQTTTrace bool
	// RootCAFile is the PEM file holding the trusted CA bundle for the
	// NereidLink MQTT connection. BrokerURL must still use a tls:// URL.
	// Empty selects the container/system trust store, which is correct for
	// a broker certificate issued by a public CA.
	RootCAFile string
	// RootCAPEM carries the same CA bundle as inline PEM text instead of a
	// file. File delivery is preferred: with EDGEX_OVERWRITE_CONFIG=true the
	// EdgeX config provider stores env-overridden values, and process env is
	// visible via docker inspect and /proc/<pid>/environ. When both are set,
	// the inline PEM wins.
	RootCAPEM string
	// ClientCertFile and ClientKeyFile select X.509 client-certificate
	// (mTLS) authentication. They must be set together; CredentialID is
	// still required and must name the matching X509 credential registered
	// in NereidLink in the MQTT CONNECT. Both empty keeps id/secret
	// authentication.
	ClientCertFile string
	ClientKeyFile  string
	// ClientCertPEM and ClientKeyPEM carry the mTLS identity as inline PEM
	// text. The certificate and key must use the same delivery mode (both
	// inline or both files); mixing modes is rejected at startup.
	ClientCertPEM string
	ClientKeyPEM  string
	// InsecureSkipVerify disables verification of the broker certificate.
	// It is for isolated lab brokers only and must never be enabled in a
	// production deployment; private CAs should use RootCAFile instead.
	InsecureSkipVerify bool
	// ProfileWatch configures the EdgeX message-bus device profile watcher.
	// When enabled, profile changes are pushed to NereidLink as soon as
	// core-metadata reports them; the periodic full scan stays as fallback.
	ProfileWatch ProfileWatchConfig
	// HealthCheck configures the sub-device reachability monitor.
	HealthCheck HealthCheckConfig
	// LinkWatch configures the local link-health watchdog (ADR-0026).
	LinkWatch LinkWatchConfig
	// Secrets configures the ADR-0034 Edge Secret delivery feature.
	Secrets SecretsConfig
	// ProfileDelivery configures the ADR-0038 device-profile downlink:
	// library profiles NereidLink pushes to this gateway are landed locally
	// and their apply state is reported back.
	ProfileDelivery ProfileDeliveryConfig
	// Inference configures the ADR-0039 edge-inference feature. NereidEdge
	// itself stays a neutral channel: the actual media/inference plane is a
	// separate edge service (NereidInference device service) treated like
	// any other southbound service; this switch only advertises the feature
	// and enables the manifest models-segment bridge.
	Inference InferenceConfig
}

// InferenceConfig controls the ADR-0039 "inference" hello feature. When
// Enabled, the gateway advertises contract model/1 and bridges state owned
// by the NereidInference device service. NereidEdge never acquires model
// packages, media frames or tensors; Backends lists protocol families only,
// never silicon/vendor names.
type InferenceConfig struct {
	// Enabled advertises the optional "inference" feature. Default false;
	// turn it on only when an inference device service is deployed.
	Enabled bool
	// Backends are the advertised inference protocol families in preference
	// order. Empty selects ["kserve", "openai"].
	Backends []string
	// DeviceService is the EdgeX service name of NereidInference.
	DeviceService string
	// ManagerDevice is its singleton service-management EdgeX device.
	ManagerDevice string
	// ModelsResource is the manager reading that returns the full inventory.
	ModelsResource string
	// InventoryPollS controls model-inventory refresh. Non-positive selects 30.
	InventoryPollS int
	// ShadowPollS controls runtime shadow refresh. Non-positive selects 30.
	ShadowPollS int
	// BindingPollS controls edge-inference job status refresh. Non-positive
	// selects 10 seconds.
	BindingPollS int
	// ManagerStoreDir persists manager-entry ownership and delete tombstones so
	// offline removals converge after restart. Opaque definitions stay in NI.
	// Empty selects "/app/data".
	ManagerStoreDir string
	// RuntimeEndpoint is the inference server address reported in the shadow
	// runtime block for diagnostics; it is display-only in Phase A.
	RuntimeEndpoint string
	// ServiceURL is the common REST API base URL of NereidInference. Stream
	// bindings POST the camera credential here under the same secret name
	// the onvif device service uses. Empty selects the docker default.
	ServiceURL string
	// KVSBase is the core-keeper config stem of NereidInference. Empty
	// selects "edgex/v4/device-inference".
	KVSBase string
}

// ProfileDeliveryConfig controls the ADR-0038 device-profile downlink. The
// "profiles" feature is advertised in the hello optional set when Enabled;
// an older platform that lacks the feature simply drops it. EdgeX-format
// documents are upserted into local core-metadata; neutral documents are
// persisted under StoreDir. The applied-revision ledger is always stored so
// re-delivered sets stay idempotent across restarts.
type ProfileDeliveryConfig struct {
	// Enabled advertises the "profiles" feature and processes spec pushes.
	// Default false unless the deployed configuration.yaml turns it on.
	Enabled bool
	// StoreDir is the directory holding the applied-revision ledger
	// (profiles-down.json) and neutral documents. Empty selects "/app/data".
	StoreDir string
}

// SecretsConfig controls the ADR-0034 Edge Secret delivery feature. The
// gateway keeps an encrypted copy of each platform secret; the encryption key
// never lives next to the store. When KeyFile is empty the gateway generates
// an ephemeral in-memory key, so the encrypted store is discarded on restart
// and the platform re-delivers the full set on reconnect.
type SecretsConfig struct {
	// Enabled advertises the "secrets" feature in hello and processes the
	// downlink sync set. Default false.
	Enabled bool
	// StoreDir is the directory holding the encrypted store file. Empty
	// selects "/app/data".
	StoreDir string
	// KeyFile is the path to an operator-provisioned 32-byte (raw or base64)
	// AES-256 key. Empty selects an ephemeral in-memory key. The key is only
	// read, never written, so secret plaintext never rests on disk.
	KeyFile string
	// OnvifServiceURL is the base URL of the EdgeX device-onvif-camera
	// service that receives onvif-basic credentials. Empty selects
	// http://edgex-device-onvif-camera:59984.
	OnvifServiceURL string
	// KeeperURL is the base URL of the EdgeX core-keeper service that holds
	// the onvif device service's AppCustom config. Empty selects
	// http://edgex-core-keeper:59890.
	KeeperURL string
	// OnvifKVSBase is the core-keeper config stem of the onvif device
	// service. Empty selects "edgex/v4/device-onvif-camera".
	OnvifKVSBase string
}

// OperationsConfig supplies the local sources the gateway operations need
// (ADR-0035). An unset source leaves the operation advertised but failing with
// a clear error; an empty Services map suppresses service.restart entirely.
type OperationsConfig struct {
	// LogFile is the agent log file sys.logs_tail tails. Empty makes the
	// operation fail.
	LogFile string
	// IdentityCertFile is the PEM northbound mTLS identity certificate
	// sys.cert_info reports. Empty makes the operation fail.
	IdentityCertFile string
	// Services maps an allowlisted service name to its systemd unit for
	// service.restart. Empty suppresses the operation.
	Services map[string]string
}

// ProfileWatchConfig controls the EdgeX message-bus watcher for device profile
// lifecycle events. The watcher shortens profile propagation from one scan
// interval to near-instant, which matters when many gateways would otherwise
// each poll NereidLink on a short timer. Empty fields fall back to the EdgeX
// reference message bus (MQTT edgex-mqtt-broker:1883, base topic "edgex").
type ProfileWatchConfig struct {
	// Enabled turns the message-bus watcher on. Default false.
	Enabled bool
	// Type is the message bus implementation. Empty selects "mqtt".
	Type string
	// Protocol is the broker protocol. Empty selects "tcp".
	Protocol string
	// Host is the broker host. Empty selects "edgex-mqtt-broker".
	Host string
	// Port is the broker port. Zero selects 1883.
	Port int
	// BaseTopic is the EdgeX base topic prefix. Empty selects "edgex".
	BaseTopic string
}

// HealthCheckConfig controls the sub-device reachability monitor. The
// monitor verifies reachability with a real read through core-command
// (ds-pushevent=false keeps the probe off the message bus), so devices
// that are physically unreachable are reported offline instead of
// staying online merely because a connector spec exists.
type HealthCheckConfig struct {
	// Enabled turns the monitor on. Default false (spec attach announces
	// presence unconditionally, preserving the prior behavior).
	Enabled bool
	// IntervalS is the time between reachability checks. Zero/negative
	// selects 30s.
	IntervalS int
	// FailureThreshold is the number of consecutive failed checks after
	// which an online device is reported offline. Zero/negative selects 3.
	FailureThreshold int
	// ProbeTimeoutS bounds one probe read attempt. Zero/negative selects 10s.
	ProbeTimeoutS int
}

// LinkWatchConfig controls the local link-health watchdog (ADR-0026). The
// watcher tracks transport episodes locally (the platform only sees a DOWN
// after ~90s of missed heartbeats), excludes planned shutdowns and detects
// flapping; the episode report uplink to NL rides on the future
// v1/gateway/linkhealth protocol extension.
type LinkWatchConfig struct {
	// Enabled turns the watchdog on. Default false.
	Enabled bool
	// FlapWindowS is the sliding window for flap detection in seconds.
	// Zero/negative selects 600 (10m).
	FlapWindowS int
	// FlapThreshold is the number of unplanned episodes inside the window
	// that constitute flapping. Zero/negative selects 5.
	FlapThreshold int
}

// ServiceConfig wraps custom structured configuration for the application service.
type ServiceConfig struct {
	// NereidEdge is the NereidLink gateway configuration section.
	NereidEdge NereidEdgeConfig
}

// UpdateFromRaw updates the service's full configuration from raw data received from
// the Service Provider.
func (c *ServiceConfig) UpdateFromRaw(rawConfig interface{}) bool {
	configuration, ok := rawConfig.(*ServiceConfig)
	if !ok {
		return false
	}
	*c = *configuration
	return true
}
