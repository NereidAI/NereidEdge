// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// operations.go defines gateway operation catalog advertising and the
// invocation frames exchanged on v1/gateway/operations/invoke and
// .../result (ADR-0035). The edge advertises the operations it supports in
// hello and then executes only those described by its catalog descriptors.

package device

import "encoding/json"

// Operation risk levels as carried in an OperationAdvert.
const (
	OperationRiskRead      = "read"
	OperationRiskNormal    = "normal"
	OperationRiskDangerous = "dangerous"
)

// Operation interaction kinds.
const (
	OperationInteractionRequestResponse = "request-response"
	OperationInteractionOneWay          = "one-way"
)

// OperationAdvert is one gateway-level operation descriptor an edge
// registers in hello (operations[], ADR-0035). All fields are data: the
// platform has no built-in operation names and derives risk only from this
// descriptor. NL compiles and upserts it into the global catalog on a
// successful hello; a malformed descriptor is rejected per-entry and never
// fails the session.
type OperationAdvert struct {
	// Operation is the operation key the edge understands.
	Operation string `json:"operation"`

	// TypeVersion is the descriptor version; empty means unversioned.
	TypeVersion string `json:"type_version,omitempty"`

	// DisplayName optionally carries bilingual display names,
	// e.g. {"en":"Ping host","zh":"Ping 探测"}.
	DisplayName map[string]string `json:"display_name,omitempty"`

	// Description optionally carries bilingual explanatory text.
	Description map[string]string `json:"description,omitempty"`

	// Interaction is OperationInteractionRequestResponse (default) or
	// OperationInteractionOneWay.
	Interaction string `json:"interaction,omitempty"`

	// TimeoutMs bounds the edge-side execution; NL requires 100..60000 and
	// defaults to 15000 when zero.
	TimeoutMs int `json:"timeout_ms,omitempty"`

	// Risk is one of the OperationRisk* constants (default normal).
	Risk string `json:"risk,omitempty"`

	// ParamSchema is the self-contained draft 2020-12 parameters JSON
	// Schema. External $ref/schema URLs are not accepted.
	ParamSchema json.RawMessage `json:"param_schema,omitempty"`

	// ResultSchema is the self-contained result JSON Schema.
	ResultSchema json.RawMessage `json:"result_schema,omitempty"`
}

// OperationAdvertResult reports the per-entry outcome of registering
// operation descriptors advertised in hello. It is distinct from
// OperationResult, which is the frame returned when an operation runs.
type OperationAdvertResult struct {
	// Operation and TypeVersion echo the advertisement.
	Operation   string `json:"operation"`
	TypeVersion string `json:"type_version"`

	// Accepted reports whether the descriptor compiled and was stored.
	Accepted bool `json:"accepted"`

	// Reason explains a rejected entry (validation/compile error).
	Reason string `json:"reason,omitempty"`
}

// OperationInvoke is the NL -> Edge request on
// v1/gateway/operations/invoke.
type OperationInvoke struct {
	// ID is the task id; it must be echoed in the result.
	ID string `json:"id"`

	// Operation is the catalog key to execute.
	Operation string `json:"operation"`

	// TypeVersion optionally identifies the descriptor version.
	TypeVersion string `json:"type_version,omitempty"`

	// Params is the resolved parameter document.
	Params map[string]any `json:"params"`

	// TimeoutMs is the edge-side execution budget.
	TimeoutMs int `json:"timeout_ms"`
}

// OperationResult is the Edge -> NL frame on
// v1/gateway/operations/result. By protocol agreement the result must not
// contain passwords: the platform does not inspect its content, so the edge
// is responsible for redacting them.
type OperationResult struct {
	// ID echoes the invoke task id.
	ID string `json:"id"`

	// OK reports edge-level success.
	OK bool `json:"ok"`

	// Result is any JSON document (the platform caps it at 64KB).
	Result json.RawMessage `json:"result,omitempty"`

	// Error carries a short, transport-safe failure explanation.
	Error string `json:"error,omitempty"`
}
