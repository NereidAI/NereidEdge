// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// ExportMQTTHandler is a minimal node-level handler for "edge-export-mqtt" (MVP).
// Node-level instances have empty device_id.
type ExportMQTTHandler struct {
	applier *Applier
}

// NewExportMQTTHandler creates a new export-mqtt handler.
func NewExportMQTTHandler(applier *Applier) *ExportMQTTHandler {
	return &ExportMQTTHandler{
		applier: applier,
	}
}

// Type returns the connector type this handler implements.
func (h *ExportMQTTHandler) Type() string {
	return "edge-export-mqtt"
}

// Apply validates the export config and records the node-level spec.
func (h *ExportMQTTHandler) Apply(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	// Defensive validation beyond the advertised schema: a spec that can never
	// route events is FAILED immediately instead of being tracked healthy.
	var cfg struct {
		TopicTemplate string `json:"topic_template"`
	}
	if len(push.Config) > 0 {
		if err := json.Unmarshal(push.Config, &cfg); err != nil {
			h.applier.reportFailed(ctx, push, "invalid edge-export-mqtt config: "+err.Error())
			return nil
		}
	}
	if cfg.TopicTemplate == "" {
		h.applier.reportFailed(ctx, push, "config requires topic_template")
		return nil
	}
	h.applier.applyNodeSpec(ctx, push, h.applier.exports)
	return nil
}

// Delete removes the connector spec.
func (h *ExportMQTTHandler) Delete(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	h.applier.applyDelete(ctx, push)
	return nil
}

var _ Handler = (*ExportMQTTHandler)(nil)
