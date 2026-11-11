// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import "encoding/json"

// Hello negotiation statuses returned in Ack.Status.
const (
	HelloStatusOK                   = "ok"
	HelloStatusAuthFailed           = "auth_failed"
	HelloStatusIncompatibleVersion  = "incompatible_version"
	HelloStatusIncompatibleFeatures = "incompatible_features"
	HelloStatusQuotaExceeded        = "quota_exceeded"
)

// Session feature names negotiated in hello.
const (
	FeatureTelemetry = "telemetry"
	FeaturePresence  = "presence"
	FeatureOTA       = "ota"
	FeatureShadow    = "shadow"
	FeatureBlobRelay = "blob-relay"
	FeatureCommand   = "command"
	// FeatureProfiles advertises device-profile downlink (ADR-0038). A
	// gateway that does not negotiate it never receives profile spec frames,
	// so old firmware is unaffected.
	FeatureProfiles = "profiles"
	// FeatureInference advertises edge model inference support (ADR-0039):
	// the gateway reports its local model inventory in the manifest models
	// segment and accepts the generic edge-inference connector type. It is
	// always optional; a gateway that does not negotiate it takes no part in
	// any inference semantics, so old firmware is unaffected.
	FeatureInference = "inference"
)

// InferenceContractModel1 is the only edge model-inference contract version
// defined today (ADR-0039): model/1.
const InferenceContractModel1 = "model/1"

// InferenceAdvert is the optional hello/ack "inference" capability block
// (ADR-0039). Backends names protocol families the edge can speak
// ("kserve", "openai"), never silicon/vendor names, and does not imply any
// concrete model server instance is online.
type InferenceAdvert struct {
	// Contract is the model/inference contract version the edge implements,
	// e.g. InferenceContractModel1 ("model/1").
	Contract string `json:"contract"`
	// Backends lists, in preference order, the inference protocol families
	// the edge supports, e.g. ["kserve","openai"].
	Backends []string `json:"backends,omitempty"`
}

// HelloVersion is the only hello frame format version understood today.
const HelloVersion = 1

// ProtoRange declares an inclusive major.minor protocol-version range.
type ProtoRange struct {
	// Min is the lowest version the edge speaks.
	Min string `json:"min"`

	// Max is the highest version the edge speaks.
	Max string `json:"max"`
}

// FeatureSet splits the hello feature advertisement.
type FeatureSet struct {
	// Required features must all be supported or negotiation fails.
	Required []string `json:"required"`

	// Optional features are downgraded away when the server lacks them.
	Optional []string `json:"optional"`
}

// ConnectorAdvert advertises one connector type and the self-contained
// draft 2020-12 JSON Schema its config document must satisfy. External
// $ref/schema URLs are not accepted.
type ConnectorAdvert struct {
	// Type is the connector type key, e.g. "modbus-tcp".
	Type string `json:"type"`

	// TypeVersion is the plugin/type version; empty means unversioned.
	TypeVersion string `json:"type_version"`

	// ConfigSchema is the inline JSON Schema.
	ConfigSchema json.RawMessage `json:"config_schema,omitempty"`
}

// SecretTypeAdvert advertises one edge credential type and the
// self-contained draft 2020-12 JSON Schema its document must satisfy. NL
// compiles and registers it in the tenant secret catalog on a successful
// hello; a malformed schema never fails the session. External
// $ref/schema URLs are not accepted.
type SecretTypeAdvert struct {
	// Type is the credential type key, e.g. "onvif-basic".
	Type string `json:"type"`

	// TypeVersion is the descriptor version; empty means unversioned.
	TypeVersion string `json:"type_version"`

	// FieldsSchema is the inline credential document JSON Schema.
	FieldsSchema json.RawMessage `json:"fields_schema,omitempty"`

	// DisplayName optionally carries bilingual display names,
	// e.g. {"en":"…","zh":"…"}.
	DisplayName map[string]string `json:"display_name,omitempty"`
}

// SecretTypeResult reports the per-entry outcome of registering secret
// type descriptors advertised in hello.
type SecretTypeResult struct {
	// Type and TypeVersion echo the advertisement.
	Type string `json:"type"`

	// TypeVersion is the advertised descriptor version.
	TypeVersion string `json:"type_version"`

	// Accepted reports whether the schema compiled and was stored.
	Accepted bool `json:"accepted"`

	// Reason explains a rejected entry (compile error).
	Reason string `json:"reason,omitempty"`
}

// ResumeRequest asks the server to resume a prior in-memory session.
type ResumeRequest struct {
	// SessionID is the prior session identifier.
	SessionID string `json:"session_id,omitempty"`

	// LastCursor is accepted for forward compatibility; the server does
	// not replay a cursor window today because desired state is full-state
	// and resent idempotently after every (re)negotiation.
	LastCursor map[string]int64 `json:"last_cursor,omitempty"`
}

// Hello is the Edge -> NL negotiation frame on v1/gateway/hello.
type Hello struct {
	// V is the hello format version; use HelloVersion.
	V int `json:"v"`

	// EdgeID identifies this gateway/edge instance.
	EdgeID string `json:"edge_id"`

	// AgentVersion is the edge agent build, independent of proto version.
	AgentVersion string `json:"agent_version"`

	// Proto is the supported protocol-version range.
	Proto ProtoRange `json:"proto"`

	// Features advertises required and optional session features.
	Features FeatureSet `json:"features"`

	// Connectors optionally registers connector config schemas.
	Connectors []ConnectorAdvert `json:"connectors,omitempty"`

	// SecretTypes optionally registers edge credential type descriptors.
	SecretTypes []SecretTypeAdvert `json:"secret_types,omitempty"`

	// Operations optionally registers whole-gateway operation descriptors
	// (ADR-0035). NL compiles and stores each accepted entry globally; a
	// malformed entry is rejected per-entry, never fails the handshake.
	Operations []OperationAdvert `json:"operations,omitempty"`

	// ProfileFormats lists, in preference order, the rendered device-profile
	// document formats this gateway can apply (ADR-0038), e.g.
	// "edgex-device-profile/3" followed by "nereid-neutral/1". Empty means
	// the neutral format only. It is meaningful only when FeatureProfiles is
	// advertised; on an older platform it is ignored.
	ProfileFormats []string `json:"profile_formats,omitempty"`

	// Inference optionally advertises edge model-inference support
	// (ADR-0039). It is meaningful only when FeatureInference is advertised;
	// nil keeps the field off the wire (older peers). The ack echoes the
	// contract/backends intersection the platform accepted.
	Inference *InferenceAdvert `json:"inference,omitempty"`

	// Resume optionally carries a prior-session resume request.
	Resume ResumeRequest `json:"resume,omitempty"`

	// ClientTime is the edge clock in epoch milliseconds.
	ClientTime int64 `json:"client_time"`

	// ResponseTopic optionally overrides the topic the ack is sent on;
	// empty selects v1/gateway/hello/ack.
	ResponseTopic string `json:"response_topic,omitempty"`
}

// ServerInfo reports the server protocol range and build.
type ServerInfo struct {
	// Min is the lowest server-supported protocol version.
	Min string `json:"min"`

	// Max is the highest server-supported protocol version.
	Max string `json:"max"`

	// Version is the server build version.
	Version string `json:"server_version"`
}

// UploadLimits describes blob-relay upload constraints.
type UploadLimits struct {
	// MaxMB caps one upload size.
	MaxMB int `json:"max_mb"`

	// QPS caps upload requests per second.
	QPS int `json:"qps"`
}

// HelloLimits reports the negotiated session operating limits.
type HelloLimits struct {
	// HeartbeatS is the application heartbeat interval in seconds.
	HeartbeatS int `json:"heartbeat_s"`

	// HelloTimeoutS is the bootstrap timeout in seconds.
	HelloTimeoutS int `json:"hello_timeout_s"`

	// Upload carries blob-relay limits.
	Upload UploadLimits `json:"upload"`
}

// TimeInfo answers the hello with the server clock and measured skew.
type TimeInfo struct {
	// ServerTime is the server clock in epoch milliseconds.
	ServerTime int64 `json:"server_time"`

	// OffsetMS is server_time minus client_time in milliseconds.
	OffsetMS int64 `json:"offset_ms"`

	// ClockWarning flags excessive skew; it never rejects negotiation.
	ClockWarning bool `json:"clock_warning"`
}

// HelloResumeResult reports the resume decision.
type HelloResumeResult struct {
	// Accepted reports whether the prior in-memory session was resumed.
	Accepted bool `json:"accepted"`
}

// ConnectorSchemaResult reports one connector schema registration result.
type ConnectorSchemaResult struct {
	// Type is the advertised connector type.
	Type string `json:"type"`

	// TypeVersion is the advertised type version.
	TypeVersion string `json:"type_version"`

	// Accepted reports schema compilation/registration success.
	Accepted bool `json:"accepted"`

	// Reason explains a rejected schema.
	Reason string `json:"reason,omitempty"`
}

// HelloAck is the NL -> Edge negotiation response.
type HelloAck struct {
	// SessionID is the negotiated session identifier.
	SessionID string `json:"session_id"`

	// GatewayID is the platform device UUID resolved from the presented
	// credential. It lets an edge fill LinkEpisodeReport.EdgeID without
	// knowing the credential -> device mapping; older servers leave it
	// empty, so edge code must treat zero as "unknown".
	GatewayID string `json:"gateway_id,omitempty"`

	// Status is one of the HelloStatus* constants.
	Status string `json:"status"`

	// Reason carries an explanation when Status is not ok.
	Reason string `json:"reason,omitempty"`

	// Selected is the chosen protocol version.
	Selected string `json:"selected,omitempty"`

	// Server reports the server range and build.
	Server ServerInfo `json:"server"`

	// Features is the actually enabled feature intersection.
	Features []string `json:"features"`

	// Limits reports the session operating limits.
	Limits HelloLimits `json:"limits"`

	// Time reports the server clock and skew.
	Time TimeInfo `json:"time"`

	// Resume reports the resume decision.
	Resume HelloResumeResult `json:"resume"`

	// ConnectorSchemas reports per-entry schema registration results.
	ConnectorSchemas []ConnectorSchemaResult `json:"connector_schemas,omitempty"`

	// SecretTypes reports per-entry credential-type registration results.
	SecretTypes []SecretTypeResult `json:"secret_types,omitempty"`

	// Operations reports per-entry gateway-operation registration results.
	Operations []OperationAdvertResult `json:"operations,omitempty"`

	// ProfileFormats echoes the formats actually enabled for this session:
	// the gateway preference intersected with the formats the platform can
	// render (ADR-0038). It is populated only when FeatureProfiles was
	// negotiated; on an older platform it stays empty, meaning neutral-only.
	ProfileFormats []string `json:"profile_formats,omitempty"`

	// Inference echoes the inference capability the platform accepted for
	// this session (ADR-0039): the contract version and the backend protocol
	// family intersection. It is populated only when FeatureInference was
	// negotiated; nil/absent means the edge must not enable inference (it
	// omits the models segment and expects no edge-inference specs).
	Inference *InferenceAdvert `json:"inference,omitempty"`
}
