// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// profiles_downlink.go defines the device-profile delivery frames (ADR-0038):
// the full desired set on v1/gateway/profiles/spec and the edge-reported
// states on v1/gateway/profiles/status. It is deliberately separate from
// profiles.go, which carries the EdgeX profile uplink on the unrelated
// v1/gateway/profiles topic; the two directions use different frames.

package device

// Profile document render format identifiers. The platform renders each
// document in the first format both sides declared in hello negotiation.
const (
	// ProfileFormatNeutral is the vendor-neutral profile document
	// (ADR-0032). It is always renderable platform-side, so every gateway
	// must support it.
	ProfileFormatNeutral = "nereid-neutral/1"
	// ProfileFormatEdgeX is the EdgeX DeviceProfile shape
	// (deviceResources/deviceCommands). Declare it only when the gateway
	// can genuinely ingest EdgeX DeviceProfiles.
	ProfileFormatEdgeX = "edgex-device-profile/3"
)

// Edge-reported profile delivery states on v1/gateway/profiles/status. The
// platform-side PENDING/DISPATCHED/REMOVING states must never be reported.
const (
	ProfileStateApplied  = "APPLIED"
	ProfileStateFailed   = "FAILED"
	ProfileStateRejected = "REJECTED"
	ProfileStateRemoved  = "REMOVED"
)

// ProfileEntry is one profile in a delivery frame.
type ProfileEntry struct {
	// ProfileID identifies the tenant library profile; correlation is
	// nevertheless by Name, never by this id.
	ProfileID string `json:"profile_id"`
	// Name is the tenant-unique library profile name and the key the
	// platform matches status reports by.
	Name string `json:"name"`
	// Revision is the monotonic content revision; de-duplicate against the
	// locally applied revision.
	Revision int64 `json:"revision"`
	// Format names the render format of Document, e.g.
	// "edgex-device-profile/3". Select the parser from this field; never
	// infer the format from document contents.
	Format string `json:"format"`
	// Removing, when true, asks the gateway to delete its local copy. No
	// Document is carried in that case.
	Removing bool `json:"removing,omitempty"`
	// Target is an optional opaque delivery hint (e.g.
	// {"service":"device-modbus"}). The kernel never interprets it and the
	// gateway may ignore it entirely.
	Target map[string]any `json:"target,omitempty"`
	// Document is the platform-rendered, ready-to-apply profile document in
	// Format. Absent on a removing entry.
	Document map[string]any `json:"document,omitempty"`
}

// ProfileSpecFrame is the full desired profile set for one gateway on
// v1/gateway/profiles/spec. The frame is idempotently re-sent after every
// (re)hello, so de-duplicate each entry by (name, revision); entries absent
// from the frame are NOT implicitly removed, only explicit Removing entries
// delete a local profile.
type ProfileSpecFrame struct {
	// TenantID owns the gateway and library profiles.
	TenantID string `json:"tenant_id"`
	// GatewayID is the authenticated gateway device UUID.
	GatewayID string `json:"gateway_id"`
	// Entries is the complete desired set, removing entries included.
	Entries []ProfileEntry `json:"entries"`
}

// ProfileStatus is one edge-reported delivery state.
type ProfileStatus struct {
	// Name matches ProfileEntry.Name; the platform correlates reports by
	// name, not by profile_id.
	Name string `json:"name"`
	// Revision echoes the entry revision the report refers to.
	Revision int64 `json:"revision"`
	// Status is one of the ProfileState* constants.
	Status string `json:"status"`
	// Reason carries a short, transport-safe explanation for
	// FAILED/REJECTED.
	Reason string `json:"reason,omitempty"`
}

// ProfileStatusFrame is the uplink on v1/gateway/profiles/status: one or
// more reports in a single fire-and-forget QoS1 publish.
type ProfileStatusFrame struct {
	// Reports contains the per-profile states.
	Reports []ProfileStatus `json:"reports"`
}
