// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import "encoding/json"

// Probe modes accepted by the platform. The platform only validates and
// forwards them; semantics are defined by the gateway.
const (
	ProbeModeCached = "cached"
	ProbeModeFresh  = "fresh"
	ProbeModeStrict = "strict"
)

// ProbeRequest is the aggregated downlink frame on v1/gateway/probe. One
// frame covers every probed sub-device sharing the gateway.
type ProbeRequest struct {
	// ProbeID equals the platform task id and correlates every response.
	ProbeID string `json:"probe_id"`

	// Devices lists the target sub-device UUIDs.
	Devices []string `json:"devices"`

	// Mode is one of the ProbeMode* constants.
	Mode string `json:"mode"`

	// TimeoutMS bounds the whole probe.
	TimeoutMS int64 `json:"timeout_ms"`
}

// ProbeResponse is one per-device uplink frame on
// v1/gateway/probe/response. Exactly one of Result/Error should carry a
// value; Result is opaque JSON passed through to the application.
type ProbeResponse struct {
	// ProbeID equals the request id.
	ProbeID string `json:"probe_id"`

	// Device is the probed sub-device UUID.
	Device string `json:"device"`

	// Result is the opaque successful probe result.
	Result json.RawMessage `json:"result,omitempty"`

	// Error explains a failed probe.
	Error string `json:"error,omitempty"`
}
