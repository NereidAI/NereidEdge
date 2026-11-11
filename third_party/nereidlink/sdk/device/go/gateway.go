// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import "time"

// LinkEpisodeReasonTransportDown is the documented reason of an edge-local
// link watchdog episode: the transport went down outside a planned stop.
const LinkEpisodeReasonTransportDown = "transport_down"

// LinkEpisodeReport is the uplink frame on v1/gateway/linkhealth. The edge
// local watchdog opens an unplanned episode when the link drops and closes
// it when re-hello succeeds; it then reports the episode so the platform can
// correct its lease-guessed episode boundaries. DownAt/UpAt are the edge
// local ground truth and always take precedence over server lease times.
type LinkEpisodeReport struct {
	// EdgeID is the gateway device UUID. It must equal the authenticated
	// connection's device id; the server validates it but never derives
	// identity from the frame.
	EdgeID string `json:"edge_id"`

	// SessionID optionally names the platform session (hello ack session_id)
	// that preceded the episode. Informational only; may be empty.
	SessionID string `json:"session_id,omitempty"`

	// DownAt is the edge-local time the link went down, RFC3339.
	DownAt time.Time `json:"down_at"`

	// UpAt is the edge-local time the re-hello completed, RFC3339. It must
	// not be before DownAt.
	UpAt time.Time `json:"up_at"`

	// Reason explains the episode. The documented value is
	// LinkEpisodeReasonTransportDown; unknown future values are still
	// accepted.
	Reason string `json:"reason"`

	// ResponseTopic optionally overrides the default ack topic
	// (v1/gateway/linkhealth/ack), mirroring hello.
	ResponseTopic string `json:"response_topic,omitempty"`
}

// Link episode ack status values returned by the platform.
const (
	// LinkEpisodeAckAccepted means the episode was applied (a lease row was
	// corrected or a new closed episode was inserted).
	LinkEpisodeAckAccepted = "accepted"

	// LinkEpisodeAckDuplicate means this (gateway, down_at) was already
	// recorded; the edge must stop retransmitting it.
	LinkEpisodeAckDuplicate = "duplicate"

	// LinkEpisodeAckRejected means the frame failed validation.
	LinkEpisodeAckRejected = "rejected"
)

// LinkEpisodeAck is the platform response on v1/gateway/linkhealth/ack.
type LinkEpisodeAck struct {
	// EdgeID echoes the gateway device UUID.
	EdgeID string `json:"edge_id"`

	// DownAt echoes the reported episode start.
	DownAt time.Time `json:"down_at"`

	// Status is one of the LinkEpisodeAck* constants.
	Status string `json:"status"`

	// Reason explains a rejected frame when Status is rejected.
	Reason string `json:"reason,omitempty"`
}

// Heartbeat is the optional payload on v1/gateway/heartbeat. An empty
// payload is legal and also renews the application watchdog lease.
type Heartbeat struct {
	// Seq is the edge heartbeat sequence number.
	Seq int64 `json:"seq"`

	// ClientTime is the edge clock at send, epoch milliseconds.
	ClientTime int64 `json:"client_time,omitempty"`
}

// Goodbye is the graceful-shutdown payload on v1/gateway/goodbye. The
// same topic is pre-registered as the MQTT LWT; the broker delivers it
// with Planned left false when the transport dies unexpectedly.
type Goodbye struct {
	// Reason optionally explains the stop (upgrade, restart, ...).
	Reason string `json:"reason,omitempty"`

	// Planned reports intentional downtime. It must be false in the LWT.
	Planned bool `json:"planned,omitempty"`
}

// DeviceList is the payload of v1/gateway/connect and
// v1/gateway/disconnect: a batch of attached sub-device UUIDs.
type DeviceList struct {
	// Devices lists platform device UUIDs.
	Devices []string `json:"devices"`
}

// ManifestConnectorInstance is one applied connector instance reported in
// the manifest.
type ManifestConnectorInstance struct {
	// SpecID is the desired spec id the edge applied.
	SpecID string `json:"spec_id"`

	// Revision is the applied desired revision.
	Revision int64 `json:"revision"`

	// ConfigHash is the optional sha256 of canonical config+mappings.
	ConfigHash string `json:"config_hash,omitempty"`
}

// Edge-reported model inventory states on the manifest models segment
// (ADR-0039). They describe the local lifecycle of one model on a gateway.
const (
	// ModelStateExtracting means the model package is being unpacked.
	ModelStateExtracting = "EXTRACTING"
	// ModelStateValidating means checksum/manifest validation is running.
	ModelStateValidating = "VALIDATING"
	// ModelStateReady means the model passed validation and the runtime
	// accepted it (its ready probe succeeded), so it may be bound.
	ModelStateReady = "READY"
	// ModelStateFailed means extraction/validation/activation failed; the
	// Reason field carries a transport-safe code.
	ModelStateFailed = "FAILED"
	// ModelStateRemoved is carried only as a transient tombstone; under
	// full-replacement reconciliation a removed model normally just
	// disappears from the list and the cloud prunes it.
	ModelStateRemoved = "REMOVED"
)

// ManifestModel is one model present on a gateway, reported in the optional
// full-replacement models segment of v1/gateway/manifest (ADR-0039). The
// inventory key is (ID, Version); Revision is the build id and is a string
// (e.g. "76937"), unlike connector/profile int revisions.
type ManifestModel struct {
	// ID is the globally meaningful model identifier, e.g.
	// "yiqisoft-yolov8n".
	ID string `json:"id"`
	// Version is the semantic model version, e.g. "1.0.0".
	Version string `json:"version"`
	// Revision is the build/optimization job id; empty means unpinned.
	Revision string `json:"revision,omitempty"`
	// Task is the task family, e.g. "detection"; it is an open enum.
	Task string `json:"task"`
	// Family is the in-task result/post-process family, e.g. "yolov8".
	Family string `json:"family,omitempty"`
	// Backend is the protocol family used to call the runtime, one of
	// "kserve"/"openai"/"task-runtime"; it is never a vendor name.
	Backend string `json:"backend"`
	// ModelName is the name used when requesting the runtime server; the
	// platform never calls it, it is for display/diagnostics only.
	ModelName string `json:"model_name,omitempty"`
	// State is one of the ModelState* constants.
	State string `json:"state"`
	// Reason carries a transport-safe failure code; empty on success.
	Reason string `json:"reason,omitempty"`
	// Labels are the class names whose slice index is the class id; the
	// kernel stores/displays them verbatim and never interprets or
	// internationalizes them.
	Labels []string `json:"labels,omitempty"`
	// ActiveBindings counts edge-inference connector instances on this
	// gateway that reference this model; the edge is authoritative.
	ActiveBindings int `json:"active_bindings,omitempty"`
}

// Manifest is the full-state payload of v1/gateway/manifest. Devices is
// the complete online sub-device list for the cycle; every bound device
// absent from it is treated as missing.
type Manifest struct {
	// Devices is the full online sub-device UUID list.
	Devices []string `json:"devices"`

	// Connectors is the full applied connector-instance list. A nil slice
	// omits the segment (skip connector reconciliation); an explicit empty
	// non-nil slice reports zero instances.
	Connectors *[]ManifestConnectorInstance `json:"connectors,omitempty"`

	// Models is the full local model inventory (ADR-0039). As with
	// Connectors, a nil pointer omits the segment so an older edge keeps
	// any stored inventory untouched (unsupported-edge), whereas an
	// explicit empty non-nil slice is an authoritative zero-inventory
	// report that prunes every row for the gateway. Full replacement: a
	// model absent from a present list is deleted.
	Models *[]ManifestModel `json:"models,omitempty"`

	// Revision is the observed desired revision, recorded for drift.
	Revision int64 `json:"revision,omitempty"`

	// MissedCycles reports how many aligned boundaries were skipped.
	MissedCycles int `json:"missed_cycles,omitempty"`
}
