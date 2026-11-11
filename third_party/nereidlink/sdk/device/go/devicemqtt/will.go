// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package devicemqtt

import (
	"encoding/json"
	"fmt"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

// Will is an MQTT Last Will and Testament message.
type Will struct {
	// Topic receives the will when the broker detects transport death.
	Topic string

	// Payload is the will body. It may be a string, []byte,
	// json.RawMessage or any JSON-encodable value.
	Payload any

	// QoS for the will delivery.
	QoS byte

	// Retained marks the will retained.
	Retained bool
}

// GoodbyeWill builds the standard gateway LWT on v1/gateway/goodbye with
// Planned left false. A planned application shutdown should also publish
// a real goodbye frame before Disconnect; the LWT only fires on crashes.
func GoodbyeWill(reason string) *Will {
	return &Will{
		Topic:   device.TopicGatewayGoodbye,
		Payload: device.Goodbye{Reason: reason},
		QoS:     QoSAtLeastOnce,
	}
}

// Encode renders the will payload as a JSON string as expected by Paho's
// SetWill. It returns an error for unsupported payload types.
func (w *Will) Encode() (string, error) {
	if w == nil {
		return "", fmt.Errorf("nil will")
	}
	switch p := w.Payload.(type) {
	case string:
		return p, nil
	case []byte:
		return string(p), nil
	case json.RawMessage:
		return string(p), nil
	default:
		raw, err := json.Marshal(p)
		if err != nil {
			return "", fmt.Errorf("encode will payload: %w", err)
		}
		return string(raw), nil
	}
}
