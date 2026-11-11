// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

const TypeNorthboundFilter = "edge-northbound-filter"

var northboundFilterSchema = json.RawMessage(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"object",
  "additionalProperties":false,
  "properties":{
    "enabled":{"type":"boolean"},
    "default_action":{"type":"string","enum":["allow","deny"],"default":"allow"},
    "rules":{"type":"array","items":{
      "type":"object",
      "required":["action"],
      "additionalProperties":false,
      "properties":{
        "action":{"type":"string","enum":["allow","deny"]},
        "device_names":{"type":"array","items":{"type":"string","minLength":1},"uniqueItems":true},
        "profile_names":{"type":"array","items":{"type":"string","minLength":1},"uniqueItems":true},
        "resource_names":{"type":"array","items":{"type":"string","minLength":1},"uniqueItems":true}
      }
    }}
  }
}`)

// NorthboundFilterHandler owns the singleton policy consumed by the primary
// v1/gateway/telemetry publisher. It never parses reading values.
type NorthboundFilterHandler struct {
	applier *Applier
}

func NewNorthboundFilterHandler(applier *Applier) *NorthboundFilterHandler {
	return &NorthboundFilterHandler{applier: applier}
}

func (h *NorthboundFilterHandler) Type() string { return TypeNorthboundFilter }

func (h *NorthboundFilterHandler) Apply(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	if push.DeviceID != "" {
		h.applier.reportFailed(ctx, push, "edge-northbound-filter requires an empty device_id")
		return nil
	}
	h.applier.applyNodeSpec(ctx, push, h.applier.uplinkFilters)
	return nil
}

func (h *NorthboundFilterHandler) Delete(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	h.applier.applyDelete(ctx, push)
	return nil
}

var _ Handler = (*NorthboundFilterHandler)(nil)
