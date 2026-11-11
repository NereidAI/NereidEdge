// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nereidai/nereidedge/internal/cameraprofiles"
)

// cameraProfilesParams is the camera.profiles v1 invoke document.
type cameraProfilesParams struct {
	Source string `json:"source"`
}

// runCameraProfiles enumerates ONVIF media profiles of one managed camera
// sub-device for the cloud binding wizard's stream-profile selector. It only
// reads through core-command; no URL or credential leaves the edge.
func (app *myApp) runCameraProfiles(ctx context.Context, in map[string]any) (json.RawMessage, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("invalid camera.profiles params: %w", err)
	}
	var params cameraProfilesParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("invalid camera.profiles params: %w", err)
	}
	if params.Source == "" {
		return nil, fmt.Errorf("source is required")
	}
	if app.inferenceCore == nil || app.applier == nil {
		return nil, fmt.Errorf("edge plane not ready")
	}
	edgexName, ok := app.applier.ResolveManagedName(params.Source)
	if !ok {
		return nil, fmt.Errorf("camera %s is not attached to this gateway", params.Source)
	}
	profiles, err := cameraprofiles.Enumerate(ctx, app.inferenceCore, edgexName)
	if err != nil {
		// Transport-safe surface: local error text stays in the agent log.
		app.service.LoggingClient().Errorf("camera.profiles %s failed: %s", edgexName, err.Error())
		return nil, fmt.Errorf("could not enumerate camera profiles")
	}
	return json.Marshal(map[string]any{"profiles": profiles})
}
