// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// EdgeXHandler adapts the existing Applier logic for the edgex-device connector.
type EdgeXHandler struct {
	applier *Applier
}

// NewEdgeXHandler creates a new handler for edgex-device connectors.
func NewEdgeXHandler(applier *Applier) *EdgeXHandler {
	return &EdgeXHandler{
		applier: applier,
	}
}

// Type returns the connector type this handler implements.
func (h *EdgeXHandler) Type() string {
	return TypeEdgeXConnector
}

// Apply applies the connector spec.
func (h *EdgeXHandler) Apply(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	if push.DeviceID == "" {
		h.applier.reportFailed(ctx, push, "spec carries no device_id")
		return nil
	}
	h.applier.applySpec(ctx, push)
	return nil
}

// Delete removes the connector spec.
func (h *EdgeXHandler) Delete(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	h.applier.applyDelete(ctx, push)
	return nil
}
