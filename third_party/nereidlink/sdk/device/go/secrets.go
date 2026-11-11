// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// secrets.go defines the edge secret delivery frames (ADR-0034): the full
// desired set on v1/gateway/secrets/sync and the edge-reported states on
// v1/gateway/secrets/status.

package device

// Edge-reported secret binding states on v1/gateway/secrets/status.
const (
	SecretStateApplied = "APPLIED"
	SecretStateFailed  = "FAILED"
	SecretStateRemoved = "REMOVED"
)

// SecretSelector is one generic target selector on a binding.
type SecretSelector struct {
	// Kind is the open selector vocabulary the edge interprets; "mac"
	// binds the secret to the device with that MAC.
	Kind string `json:"kind"`
	// Value is the selector value; the platform never interprets it.
	Value string `json:"value"`
}

// SecretEntry is one secret in a delivery frame.
type SecretEntry struct {
	// Name identifies the vault secret.
	Name string `json:"name"`
	// Type and Version carry the secret type descriptor coordinates.
	Type string `json:"type,omitempty"`
	// Version is the descriptor version.
	Version string `json:"version,omitempty"`
	// Revision starts at 1 and increments on every desired update.
	Revision int64 `json:"revision"`
	// Removing, when true, asks the edge to erase this secret.
	Removing bool `json:"removing,omitempty"`
	// Selectors is the generic target list.
	Selectors []SecretSelector `json:"selectors,omitempty"`
	// Document is the plaintext credential document at the MQTT last mile.
	// Over Core NATS it arrives as secret:// references; the edge receives
	// resolved values. Empty on a removing entry.
	Document map[string]any `json:"document,omitempty"`
}

// SecretSync is the full desired set on v1/gateway/secrets/sync. It is
// idempotently re-sent after every (re)hello, so de-duplicate per entry by
// revision.
type SecretSync struct {
	// TenantID owns the gateway.
	TenantID string `json:"tenant_id"`
	// GatewayID is the authenticated gateway device UUID.
	GatewayID string `json:"gateway_id"`
	// Entries is the complete desired set (removing entries included).
	Entries []SecretEntry `json:"entries"`
}

// SecretStatus is one edge-reported binding state.
type SecretStatus struct {
	// Name identifies the secret binding.
	Name string `json:"name"`
	// Revision echoes the delivered revision the report refers to.
	Revision int64 `json:"revision,omitempty"`
	// Status is one of the SecretState* constants.
	Status string `json:"status"`
	// Reason explains a failed apply/removal.
	Reason string `json:"reason,omitempty"`
}

// SecretStatusFrame is the uplink on v1/gateway/secrets/status: one or more
// reports in a single publish.
type SecretStatusFrame struct {
	// Reports contains the per-secret states.
	Reports []SecretStatus `json:"reports"`
}
