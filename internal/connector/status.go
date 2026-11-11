// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"fmt"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// InferenceJobStats is the optional health extension on connector-status.
// Nil fields mean NI did not provide a usable reading; they never mean zero.
type InferenceJobStats struct {
	SuccessTotal *uint64  `json:"success_total,omitempty"`
	ErrorTotal   *uint64  `json:"error_total,omitempty"`
	P50MS        *float64 `json:"p50_ms,omitempty"`
	P95MS        *float64 `json:"p95_ms,omitempty"`
}

// connectorStatusFrame is wire-compatible with the SDK's ConnectorStatus.
// Unknown optional fields are ignored by older NL readers.
type connectorStatusFrame struct {
	device.ConnectorStatus
	Stats            *InferenceJobStats `json:"stats,omitempty"`
	ObservedRevision int64              `json:"observed_revision,omitempty"`
	ObservedHash     string             `json:"observed_hash,omitempty"`
}

func (f connectorStatusFrame) extended() bool {
	return f.Stats != nil || f.ObservedRevision != 0 || f.ObservedHash != ""
}

func (f connectorStatusFrame) key() string {
	raw, _ := json.Marshal(f)
	return string(raw)
}

// reportStatusFrame keeps legacy frames on the typed SDK path. Extended frames
// use the same MQTT topic with a locally encoded compatible JSON envelope.
func (a *Applier) reportStatusFrame(frame connectorStatusFrame) error {
	if !frame.extended() {
		return a.reportStatus(frame.ConnectorStatus)
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		a.lc.Errorf("Failed to encode extended connector status for spec %s: %s", frame.SpecID, err.Error())
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.statusTimeout)
	defer cancel()
	if a.reportRawStatus != nil {
		err = a.reportRawStatus(ctx, raw)
	} else if a.gw != nil && a.gw.MQTT() != nil {
		err = a.gw.MQTT().PublishRaw(ctx, device.TopicGatewayConnectorStatus, raw, 1)
	} else {
		// Unit tests and legacy embedders without an MQTT client retain the
		// original typed lifecycle fields rather than dropping the whole frame.
		return a.reportStatus(frame.ConnectorStatus)
	}
	if err != nil {
		a.lc.Errorf("Failed to report extended %s for spec %s: %s", frame.State, frame.SpecID, err.Error())
	}
	return err
}

func (s *InferenceJobStats) key() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("%v/%v/%v/%v",
		optionalUint64(s.SuccessTotal), optionalUint64(s.ErrorTotal),
		optionalFloat(s.P50MS), optionalFloat(s.P95MS))
}

func optionalUint64(v *uint64) any {
	if v == nil {
		return nil
	}
	return *v
}

func optionalFloat(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}
