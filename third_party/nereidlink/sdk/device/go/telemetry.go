// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Frame is one device telemetry uplink: a flat JSON object whose keys are
// measurement identifiers and whose values are numbers, booleans or
// strings.
//
// The server publishes one normalized Envelope per key. Null and nested
// object/array values are rejected by the platform, so the client Build
// rejects them before a byte leaves the device; this keeps malformed frames
// visible in firmware logs instead of being silently skipped server-side.
type Frame map[string]any

// NewFrame starts an empty telemetry frame.
func NewFrame() Frame { return Frame{} }

// SetFloat adds a numeric measurement.
func (f Frame) SetFloat(key string, value float64) Frame { f[key] = value; return f }

// SetBool adds a boolean measurement.
func (f Frame) SetBool(key string, on bool) Frame { f[key] = on; return f }

// SetString adds a string measurement (for example an enumerated mode).
func (f Frame) SetString(key, value string) Frame { f[key] = value; return f }

// Set adds a measurement. It returns an error when value is nil, a slice,
// a map or a pointer: the wire frame accepts only JSON scalars.
func (f Frame) Set(key string, value any) error {
	switch value.(type) {
	case nil:
		return fmt.Errorf("telemetry key %q: null values are not accepted", key)
	case []any, map[string]any, json.RawMessage:
		return fmt.Errorf("telemetry key %q: structured values are not accepted", key)
	default:
		switch v := value.(type) {
		case string, bool,
			float32, float64,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			f[key] = v
			return nil
		default:
			return fmt.Errorf("telemetry key %q: unsupported value type %T", key, value)
		}
	}
}

// EncodeJSON renders the frame exactly as the ingests expect: a compact
// flat JSON object.
func (f Frame) EncodeJSON() ([]byte, error) {
	if len(f) == 0 {
		return nil, fmt.Errorf("telemetry frame is empty")
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(f); err != nil {
		return nil, fmt.Errorf("encode telemetry frame: %w", err)
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}

// Validate performs the server-side scalar classification locally so that
// a firmware bug is reported at the call site.
func (f Frame) Validate() error {
	for key, value := range f {
		switch value.(type) {
		case nil:
			return fmt.Errorf("telemetry key %q: null values are not accepted", key)
		case json.RawMessage:
			return fmt.Errorf("telemetry key %q: raw values must be added decoded", key)
		case string, bool:
			continue
		}
		switch v := value.(type) {
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			_ = v
		default:
			if _, isMap := value.(map[string]any); isMap {
				return fmt.Errorf("telemetry key %q: nested objects are not accepted", key)
			}
			if _, isSlice := value.([]any); isSlice {
				return fmt.Errorf("telemetry key %q: arrays are not accepted", key)
			}
			return fmt.Errorf("telemetry key %q: unsupported value type %T", key, value)
		}
	}
	return nil
}

// GatewayFrame is one sub-device telemetry block inside a gateway batch.
// TS is optional and sets the event time in epoch milliseconds; zero sends
// without the reserved "ts" key so the server stamps arrival.
type GatewayFrame struct {
	// DeviceID is the platform UUID of the attached sub-device.
	DeviceID string

	// TS is the optional event time in epoch milliseconds.
	TS int64

	// Measurements is the flat scalar frame for the sub-device.
	Measurements Frame
}

// EncodeGatewayBatch renders one v1/gateway/telemetry payload: a map of
// sub-device UUID to a flat frame that may carry the reserved "ts" key.
// Every block must name a device and carry at least one measurement.
func EncodeGatewayBatch(blocks ...GatewayFrame) ([]byte, error) {
	if len(blocks) == 0 {
		return nil, fmt.Errorf("gateway telemetry batch is empty")
	}
	out := make(map[string]map[string]any, len(blocks))
	for _, block := range blocks {
		if block.DeviceID == "" {
			return nil, fmt.Errorf("gateway telemetry block is missing device_id")
		}
		if err := block.Measurements.Validate(); err != nil {
			return nil, err
		}
		values := map[string]any(block.Measurements)
		if block.TS != 0 {
			values["ts"] = block.TS
		}
		out[block.DeviceID] = values
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode gateway telemetry batch: %w", err)
	}
	return raw, nil
}
