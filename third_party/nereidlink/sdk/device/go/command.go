// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import "encoding/json"

// Command is one downlink command delivered to a device.
//
// The platform publishes it to v1/me/cmd/<name> over MQTT or returns it
// from GET /v1/me/commands over HTTP. The topic suffix is the command
// Name while the response topic/endpoint uses the ID; never exchange them.
type Command struct {
	// ID is the command identifier used on the response path.
	ID string `json:"id"`

	// Name is the catalog command name; it also suffixes the downlink topic.
	Name string `json:"name"`

	// Params is the catalog-validated JSON document delivered verbatim.
	Params json.RawMessage `json:"params"`

	// Direction names the requested south-side operation: "read" (return
	// a value) or "write" (apply Params). Empty preserves the edge's
	// legacy capability-based inference.
	Direction string `json:"direction,omitempty"`
}

// TelemetryAck is the response of POST /v1/me/telemetry.
type TelemetryAck struct {
	// Published is the number of measurement Envelopes the frame produced.
	Published int `json:"published"`
}

// CommandsResponse is the long-poll envelope. Command is nil when no
// command became available within the wait window; the device simply polls
// again.
type CommandsResponse struct {
	// Command carries the next queued command, if any.
	Command *Command `json:"command"`
}

// Command interaction and timeliness modes. They name the platform
// command catalog vocabulary; gateway-relayed sub-device commands carry
// the values explicitly.
const (
	// InteractionOneWay requires no response.
	InteractionOneWay = "one-way"

	// InteractionRequestResponse requires a correlated response.
	InteractionRequestResponse = "request-response"

	// TimelinessRealtime fails fast when the target is offline.
	TimelinessRealtime = "realtime"

	// TimelinessPersistent queues for delivery after reconnect.
	TimelinessPersistent = "persistent"
)

// Command direction names the south-side operation the edge performs.
const (
	// DirectionRead asks the edge to read and return a value.
	DirectionRead = "read"

	// DirectionWrite asks the edge to apply Params to the device.
	DirectionWrite = "write"
)

// GatewayCommand is the downlink frame on v1/gateway/command for one
// sub-device attached to the subscribing gateway. Unlike the direct
// device frame, the target sub-device is named explicitly and the
// command name rides the Method field (protocol v1 spelling).
type GatewayCommand struct {
	// CommandID correlates the response.
	CommandID string `json:"command_id"`

	// Device is the target sub-device UUID.
	Device string `json:"device"`

	// Interaction is one-way or request-response.
	Interaction string `json:"interaction"`

	// Timeliness is realtime or persistent.
	Timeliness string `json:"timeliness"`

	// Method is the catalog command name.
	Method string `json:"method"`

	// Params is the catalog-validated JSON document.
	Params json.RawMessage `json:"params"`

	// Direction names the requested south-side operation: "read" (return
	// a value) or "write" (apply Params). Empty preserves the edge's
	// legacy capability-based inference.
	Direction string `json:"direction,omitempty"`
}

// GatewayCommandResponse is the uplink frame on
// v1/gateway/command/response for one sub-device command. It is accepted
// only from the negotiated gateway that attached the named device.
type GatewayCommandResponse struct {
	// CommandID echoes the downlink command id.
	CommandID string `json:"command_id"`

	// Device names the sub-device that produced the result.
	Device string `json:"device"`

	// Data is the arbitrary JSON result document.
	Data json.RawMessage `json:"data"`
}
