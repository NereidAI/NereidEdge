// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import "encoding/json"

// Device domain-event severities accepted on v1/me/events. Empty
// severity defaults to info server-side.
const (
	EventSeverityInfo     = "info"
	EventSeverityWarning  = "warning"
	EventSeverityError    = "error"
	EventSeverityCritical = "critical"
)

// EventFrame is one device-published domain event on v1/me/events.
// The topic accepts a single object or an array of objects. The
// platform carries no business logic: EventType, Severity and Payload
// are tenant-defined opaque data; payload may be nested JSON.
type EventFrame struct {
	// EventType is the required tenant-named signal label.
	EventType string `json:"event_type"`

	// Severity is one of the EventSeverity* constants; empty = info.
	Severity string `json:"severity,omitempty"`

	// TSMS optionally supplies the event time in epoch milliseconds;
	// zero means the server stamps it.
	TSMS int64 `json:"ts_ms,omitempty"`

	// Payload is the opaque JSON document.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// NewEvent builds one event frame. payload is marshaled to JSON; pass
// nil to omit the field.
func NewEvent(eventType string, payload any) (EventFrame, error) {
	frame := EventFrame{EventType: eventType}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return EventFrame{}, err
		}
		frame.Payload = raw
	}
	return frame, nil
}
