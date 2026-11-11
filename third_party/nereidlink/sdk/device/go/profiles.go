// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// EdgeDeviceResource is the minimal EdgeX deviceResource subset reported on
// v1/gateway/profiles. It mirrors the fields NL needs to build measurement
// descriptors; the richer EdgeX shape (properties nesting, attributes) is not
// carried in the first version.
type EdgeDeviceResource struct {
	// Name is the resource name and equals the telemetry measurement key.
	Name string `json:"name"`

	// ValueType is the EdgeX value type (Bool, Int32, Float64, String, ...).
	ValueType string `json:"valueType"`

	// Units is the optional engineering unit.
	Units string `json:"units,omitempty"`

	// Description is the optional human description.
	Description string `json:"description,omitempty"`
}

// EdgeProfile is one EdgeX device profile as reported by the edge. Only the
// name and deviceResources subset is carried.
type EdgeProfile struct {
	// Name is the EdgeX device profile name, matching connector profile_name.
	Name string `json:"name"`

	// DeviceResources maps resource name/value type to measurement descriptors.
	DeviceResources []EdgeDeviceResource `json:"deviceResources"`
}

// EdgeProfileFrame is the uplink payload of v1/gateway/profiles: a batch of
// edge profiles, QoS1, non-retained, fire-and-forget (no ack topic).
type EdgeProfileFrame struct {
	// Profiles lists every profile in this report; hello/reconnect replays the
	// full set. The server de-duplicates by a per-profile content hash.
	Profiles []EdgeProfile `json:"profiles"`
}

// CanonicalProfileHash returns the lowercase-hex sha256 over the compact,
// key-sorted JSON of one reported profile. It is the de-duplication key the
// server uses so hello replays and unchanged re-reports are no-ops. Go's
// encoding/json sorts map keys, matching the connector config-hash convention.
func CanonicalProfileHash(profile EdgeProfile) (string, error) {
	raw, err := json.Marshal(profile)
	if err != nil {
		return "", fmt.Errorf("encode canonical edge profile: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
