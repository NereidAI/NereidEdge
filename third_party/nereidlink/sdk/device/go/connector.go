// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Connector reported lifecycle states on v1/gateway/connectors/status.
const (
	ConnectorStateCreating = "CREATING"
	ConnectorStateApplied  = "APPLIED"
	ConnectorStateFailed   = "FAILED"
	ConnectorStateDegraded = "DEGRADED"
)

// Downlink connector frame kinds.
const (
	ConnectorKindSpec   = "spec"
	ConnectorKindDelete = "delete"
)

// ConnectorStatusStats contains independently optional job measurements.
// Pointer-to-zero is a reported zero; nil means unavailable.
type ConnectorStatusStats struct {
	SuccessTotal *uint64  `json:"success_total,omitempty"`
	ErrorTotal   *uint64  `json:"error_total,omitempty"`
	P50MS        *float64 `json:"p50_ms,omitempty"`
	P95MS        *float64 `json:"p95_ms,omitempty"`
}

// ConnectorStatus is the uplink frame reporting how one desired spec was
// applied.
type ConnectorStatus struct {
	// SpecID is the desired spec id, echoed back verbatim.
	SpecID string `json:"spec_id"`

	// State is one of the ConnectorState* constants.
	State string `json:"state"`

	// Revision is the applied desired revision.
	Revision int64 `json:"revision"`

	// Reason explains FAILED/DEGRADED.
	Reason string `json:"reason,omitempty"`

	// Stats is omitted when no valid job measurement is available.
	Stats *ConnectorStatusStats `json:"stats,omitempty"`

	// ObservedRevision/ObservedHash describe manager GET readback, not the
	// manifest's reported_config_hash/config_hash.
	ObservedRevision *int64 `json:"observed_revision,omitempty"`
	ObservedHash     string `json:"observed_hash,omitempty"`
}

// ConnectorPush is the downlink frame on v1/gateway/connectors/spec or
// .../delete (subscribe once with FilterGatewayConnectors).
//
// For KindSpec the edge applies the merged effective Config/Mappings and
// reports status keyed by (SpecID, Revision). For KindDelete it removes
// the instance. Desired is full-state and idempotently re-sent after
// every (re)hello, so de-duplicate by revision.
type ConnectorPush struct {
	// Kind is ConnectorKindSpec or ConnectorKindDelete.
	Kind string `json:"kind"`

	// TenantID owns the gateway.
	TenantID string `json:"tenant_id"`

	// GatewayID is the authenticated gateway device UUID.
	GatewayID string `json:"gateway_id"`

	// SpecID identifies the instance on every frame kind.
	SpecID string `json:"spec_id"`

	// DeviceID identifies the bound sub-device; empty for a gateway-level
	// instance.
	DeviceID string `json:"device_id,omitempty"`

	// ConnectorType is present on spec frames.
	ConnectorType string `json:"connector_type,omitempty"`

	// TypeVersion is present on spec frames.
	TypeVersion string `json:"type_version,omitempty"`

	// Config is the effective (template merged with overrides) config.
	Config json.RawMessage `json:"config,omitempty"`

	// Mappings is the effective point mapping list.
	Mappings json.RawMessage `json:"mappings,omitempty"`

	// HealthCheck, when present, requests the edge-side liveness probe for
	// the bound sub-device: periodically READ Resource, discard the value,
	// and autonomously assert connect/disconnect on FailThreshold
	// consecutive failures. Absent (or omitted entirely) means probing is
	// disabled; the edge must stop any running probe for this spec.
	HealthCheck *ConnectorHealthCheck `json:"health_check,omitempty"`

	// Revision starts at 1 and increments on every desired update.
	Revision int64 `json:"revision"`
}

// ConnectorHealthCheck is the protocol-agnostic probe policy for one bound
// device. See ConnectorPush.HealthCheck.
type ConnectorHealthCheck struct {
	// Resource names the south resource/command to read. It lives in the
	// connector's neutral resource-name space (the same names Mappings use).
	Resource string `json:"resource"`

	// IntervalMS is the probe cadence in milliseconds.
	IntervalMS int `json:"interval_ms"`

	// TimeoutMS bounds a single probe read.
	TimeoutMS int `json:"timeout_ms"`

	// FailThreshold is the consecutive-failure count that flips the bound
	// device to disconnected; the first successful probe reconnects it.
	FailThreshold int `json:"fail_threshold"`
}

// CanonicalConfigHash reproduces the manifest config_hash byte for byte:
// lowercase-hex sha256 over the compact, key-sorted JSON document
// {"config":<effective config>,"mappings":<effective mappings>}. Both
// arguments are already-decoded Go values (or json.RawMessage); the edge
// must hash the same effective document it applied after merging profile
// template and instance overrides.
func CanonicalConfigHash(config, mappings any) (string, error) {
	canonical := struct {
		Config any `json:"config"`
		Maps   any `json:"mappings"`
	}{Config: normalizeHashValue(config), Maps: normalizeHashValue(mappings)}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode canonical connector document: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// normalizeHashValue decodes json.RawMessage inputs so map keys are sorted
// during the final marshal like the server implementation does.
func normalizeHashValue(v any) any {
	raw, ok := v.(json.RawMessage)
	if !ok {
		return v
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil
	}
	return decoded
}
