// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import "encoding/json"

// Attribute scopes. Devices write only CLIENT scope; SERVER/SHARED
// scopes are managed by the platform and arrive as pushes. The stored
// scope literal for the device-owned scope is "CLIENT" (verified live
// against the dev server and matching web/src AttributesTab), while
// SERVER/SHARED carry the "_SCOPE" suffix.
const (
	// AttributeScopeClient is the device-owned scope reported on uplink;
	// the platform stores/filters it as the literal "CLIENT".
	AttributeScopeClient = "CLIENT"

	// AttributeScopeServer is the platform-managed scope.
	AttributeScopeServer = "SERVER_SCOPE"

	// AttributeScopeShared is the shared scope.
	AttributeScopeShared = "SHARED_SCOPE"
)

// AttributeUpdate is a direct-device CLIENT-scope uplink payload on
// v1/me/attributes: a flat {key: value} object. Values may be any JSON
// value (nested documents allowed, unlike telemetry frames).
type AttributeUpdate map[string]any

// GatewayAttributeUpdate is the v1/gateway/attributes uplink payload:
// {device: {key: value}} for one or more attached sub-devices. Entries
// for devices not attached to this connection are skipped server-side.
type GatewayAttributeUpdate map[string]map[string]any

// AttributesDown is the v1/me/attributes/down frame for a directly
// connected device after a SERVER/SHARED attribute upsert. Only the
// changed key is carried.
type AttributesDown struct {
	// Scope is SERVER_SCOPE or SHARED_SCOPE.
	Scope string `json:"scope"`

	// Data carries the changed key/value pair. A JSON null value means the
	// key was deleted: remove it from any local cache.
	Data map[string]json.RawMessage `json:"data"`
}

// AttributesGetRequest is the v1/me/attributes/get uplink a device sends
// to pull platform-managed attributes after a reconnect (closing the gap
// when it missed an attributes/down push while offline). An empty Scopes
// list means both SERVER_SCOPE and SHARED_SCOPE.
type AttributesGetRequest struct {
	// Scopes lists SERVER_SCOPE/SHARED_SCOPE; empty means both.
	Scopes []string `json:"scopes,omitempty"`
}

// AttributesGetAck is the v1/me/attributes/get/ack reply: one key/value
// map per requested scope.
type AttributesGetAck struct {
	// Scopes maps each requested scope to its key/value pairs.
	Scopes map[string]map[string]json.RawMessage `json:"scopes"`
}

// GatewayAttributesPush is the v1/gateway/attributes/push frame for a
// gateway-attached sub-device. It is the gateway counterpart of
// AttributesDown and names the target sub-device explicitly.
type GatewayAttributesPush struct {
	// Device is the target sub-device UUID.
	Device string `json:"device"`

	// Scope is SERVER_SCOPE or SHARED_SCOPE.
	Scope string `json:"scope"`

	// Data carries the changed key/value pair. A JSON null value means the
	// key was deleted.
	Data map[string]json.RawMessage `json:"data"`
}

// GatewayAttributesGetRequest is the v1/gateway/attributes/get uplink a
// gateway sends to fetch one attached sub-device's platform-managed
// attributes. An empty Scopes list means both platform scopes.
type GatewayAttributesGetRequest struct {
	// Device names the attached sub-device.
	Device string `json:"device"`

	// Scopes lists SERVER_SCOPE/SHARED_SCOPE; empty means both.
	Scopes []string `json:"scopes,omitempty"`
}

// GatewayAttributesGetAck is the v1/gateway/attributes/get/ack reply,
// naming the sub-device.
type GatewayAttributesGetAck struct {
	// Device is the sub-device the attributes belong to.
	Device string `json:"device"`

	// Scopes maps each requested scope to its key/value pairs.
	Scopes map[string]map[string]json.RawMessage `json:"scopes"`
}
