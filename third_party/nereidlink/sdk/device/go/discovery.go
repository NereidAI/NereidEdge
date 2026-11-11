// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

// Discovery result statuses (also includes a pending review state that
// may arrive asynchronously on the same result topic).
const (
	DiscoveryStatusCreated  = "created"
	DiscoveryStatusExisting = "existing"
	DiscoveryStatusRejected = "rejected"
	DiscoveryStatusPending  = "pending"
)

// DiscoveryCandidate is one gateway-local device reported for
// auto-provisioning.
type DiscoveryCandidate struct {
	// LocalID is the stable key in the gateway's southbound domain.
	LocalID string `json:"local_id"`

	// Name optionally overrides the generated "<gateway>/<local>" name.
	Name string `json:"name,omitempty"`

	// Profile references a tenant profile by UUID or name.
	Profile string `json:"profile,omitempty"`
}

// DiscoveryFrame is the uplink payload on v1/gateway/discovery. The
// platform derives tenant/gateway from the authenticated connection, so
// the frame carries neither.
type DiscoveryFrame struct {
	// Type optionally identifies the discovery source/bus for audit.
	Type string `json:"type,omitempty"`

	// Candidates lists the local devices seen by the gateway.
	Candidates []DiscoveryCandidate `json:"candidates"`
}

// DiscoveryItemResult reports the provisioning outcome for one candidate.
type DiscoveryItemResult struct {
	// LocalID correlates the result with the uplink candidate.
	LocalID string `json:"local_id"`

	// DeviceID is the platform UUID for created/existing entries.
	DeviceID string `json:"device_id,omitempty"`

	// CanonicalName is the deterministic device name.
	CanonicalName string `json:"canonical_name,omitempty"`

	// ProfileID is the resolved profile UUID.
	ProfileID string `json:"profile_id,omitempty"`

	// Status is one of the DiscoveryStatus* constants.
	Status string `json:"status"`

	// Reason explains a rejected candidate.
	Reason string `json:"reason,omitempty"`
}

// DiscoveryResult is downlink payload on v1/gateway/discovery/result.
type DiscoveryResult struct {
	// Results carries one entry per candidate; partial success is normal.
	Results []DiscoveryItemResult `json:"results"`
}
