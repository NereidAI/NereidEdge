// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

// ReportedPatch is the device shadow report body posted to
// /v1/me/shadow/report (HTTP) or published to v1/me/shadow/report (MQTT).
type ReportedPatch struct {
	// State is the reported-state patch merged server-side.
	State map[string]any `json:"state"`

	// Version optionally carries the desired version the device applied.
	// Nil omits the field, meaning the current desired version.
	Version *int64 `json:"version,omitempty"`
}

// NewReportedPatch builds a report for state.
func NewReportedPatch(state map[string]any) ReportedPatch {
	return ReportedPatch{State: state}
}

// NewReportedPatchForVersion builds a report acknowledging an applied
// desired version.
func NewReportedPatchForVersion(state map[string]any, desiredVersion int64) ReportedPatch {
	return ReportedPatch{State: state, Version: &desiredVersion}
}

// DesiredPush is the complete desired state pushed on
// v1/me/shadow/desired when desired changes or a device reconnects. The
// payload carries no tenant/device internals: the device knows its own
// identity from the connection.
type DesiredPush struct {
	// Version is the monotonically increasing desired version.
	Version int64 `json:"version"`

	// Desired is the complete desired state, not a delta.
	Desired map[string]any `json:"desired"`
}

// GatewayShadowReport is one sub-device reported-state entry inside a
// gateway batch report.
type GatewayShadowReport struct {
	// Device names the attached sub-device.
	Device string `json:"device"`

	// State is the reported patch; a JSON null value deletes that key.
	State map[string]any `json:"state"`

	// Version optionally carries the desired version applied.
	Version *int64 `json:"version,omitempty"`
}

// NewGatewayShadowReport builds one sub-device report.
func NewGatewayShadowReport(device string, state map[string]any) GatewayShadowReport {
	return GatewayShadowReport{Device: device, State: state}
}

// NewGatewayShadowReportForVersion builds a report acknowledging an applied
// desired version.
func NewGatewayShadowReportForVersion(device string, state map[string]any, desiredVersion int64) GatewayShadowReport {
	return GatewayShadowReport{Device: device, State: state, Version: &desiredVersion}
}

// GatewayShadowReportFrame is the batched payload published on
// v1/gateway/shadow/report. A single-device report is a length-one batch.
type GatewayShadowReportFrame struct {
	Reports []GatewayShadowReport `json:"reports"`
}

// GatewayShadowDesired is one sub-device full desired-state entry inside a
// gateway batch push.
type GatewayShadowDesired struct {
	// Device names the target sub-device behind the gateway.
	Device string `json:"device"`

	// Version is the desired version for this sub-device.
	Version int64 `json:"version"`

	// Desired is the complete desired state, not a delta.
	Desired map[string]any `json:"desired"`
}

// GatewayShadowDesiredFrame is the batched payload received on
// v1/gateway/shadow/desired; one subscription serves every sub-device and the
// gateway demuxes each entry by Device.
type GatewayShadowDesiredFrame struct {
	Desired []GatewayShadowDesired `json:"desired"`
}
