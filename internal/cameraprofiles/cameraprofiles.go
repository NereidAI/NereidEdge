// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package cameraprofiles enumerates ONVIF media profiles of a managed
// camera through ordinary core-command reads. It is read-only, carries no
// credential handling (core-command authenticates against the camera itself)
// and returns transport-safe metadata only: ProfileToken, display name,
// encoding, resolution and frame rate. No URL, address or vendor path is
// surfaced.
package cameraprofiles

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
)

// Profile is one ONVIF media profile (main/sub/third stream).
type Profile struct {
	Token    string  `json:"token"`
	Name     string  `json:"name,omitempty"`
	Encoding string  `json:"encoding,omitempty"`
	Width    int     `json:"width,omitempty"`
	Height   int     `json:"height,omitempty"`
	FPS      float64 `json:"fps,omitempty"`
}

// CommandReader is the core-command subset used here.
type CommandReader interface {
	Read(ctx context.Context, edgexName, commandName, rawQuery string) (responses.EventResponse, int, error)
}

// profileCommandNames lists the device resources that expose ONVIF media
// profiles across device-onvif-camera releases, richest payload first:
// device-onvif-camera 4.x names the read resource "MediaProfiles" (full
// profile documents carrying VideoEncoderConfiguration); releases built
// around the classic ONVIF operation name expose "GetProfiles" or the
// simplified "Profiles"; "Media2Profiles" (Media2 slim list, token/name
// only) is the last resort as it carries no encoding or resolution.
var profileCommandNames = []string{"MediaProfiles", "GetProfiles", "Profiles", "Media2Profiles"}

// Enumerate calls the camera's profiles command (trying every known device
// resource name) and parses the reading payload.
func Enumerate(ctx context.Context, reader CommandReader, edgexName string) ([]Profile, error) {
	var lastStatus int
	for _, cmd := range profileCommandNames {
		resp, status, err := reader.Read(ctx, edgexName, cmd, "ds-pushevent=false&ds-returnevent=true")
		if err != nil || status != 200 {
			lastStatus = status
			continue
		}
		profiles, err := ParseEvent(resp)
		if err != nil {
			continue
		}
		return profiles, nil
	}
	if lastStatus != 0 {
		return nil, fmt.Errorf("camera profiles command returned HTTP %d", lastStatus)
	}
	return nil, fmt.Errorf("camera profiles command unavailable")
}

// ParseEvent extracts profiles from a core-command event response. The ONVIF
// payload shape varies across device service releases, so both string
// readings (JSON-encoded) and object readings are searched recursively.
func ParseEvent(resp responses.EventResponse) ([]Profile, error) {
	for _, r := range resp.Event.Readings {
		if r.ObjectValue != nil {
			if profiles := parseAny(r.ObjectValue); len(profiles) > 0 {
				return profiles, nil
			}
		}
		s := strings.TrimSpace(r.Value)
		if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
			var nested any
			if err := json.Unmarshal([]byte(s), &nested); err == nil {
				if profiles := parseAny(nested); len(profiles) > 0 {
					return profiles, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("no profiles in command response")
}

// parseAny walks arbitrary JSON and returns every object that looks like an
// ONVIF media profile (carries a token/Token string).
func parseAny(v any) []Profile {
	switch t := v.(type) {
	case []any:
		var out []Profile
		for _, item := range t {
			out = append(out, parseAny(item)...)
		}
		return out
	case map[string]any:
		if token := stringField(t, "token", "Token"); token != "" {
			return []Profile{parseProfile(token, t)}
		}
		var out []Profile
		for _, child := range t {
			out = append(out, parseAny(child)...)
		}
		return out
	default:
		return nil
	}
}

func parseProfile(token string, m map[string]any) Profile {
	p := Profile{
		Token:    token,
		Name:     stringField(m, "name", "Name"),
		Encoding: strings.ToUpper(stringField(m, "encoding", "Encoding")),
	}
	// Encoding/resolution/rate live under the video encoder configuration in
	// ONVIF; some flattened payloads put them on the profile itself.
	encoder := mapField(m, "videoEncoderConfiguration", "VideoEncoderConfiguration")
	if encoder == nil {
		encoder = m
	}
	if p.Encoding == "" {
		p.Encoding = strings.ToUpper(stringField(encoder, "encoding", "Encoding"))
	}
	if res := mapField(encoder, "resolution", "Resolution"); res != nil {
		p.Width = intField(res, "width", "Width")
		p.Height = intField(res, "height", "Height")
	}
	if p.Width == 0 {
		p.Width = intField(encoder, "width", "Width")
	}
	if p.Height == 0 {
		p.Height = intField(encoder, "height", "Height")
	}
	if rate := mapField(encoder, "rateControl", "RateControl"); rate != nil {
		p.FPS = floatField(rate, "frameRateLimit", "FrameRateLimit", "frameRate", "FrameRate")
	}
	if p.FPS == 0 {
		p.FPS = floatField(encoder, "frameRateLimit", "FrameRateLimit", "fps", "FPS")
	}
	return p
}

func mapField(m map[string]any, keys ...string) map[string]any {
	for _, k := range keys {
		if v, ok := m[k].(map[string]any); ok {
			return v
		}
	}
	return nil
}

func stringField(m map[string]any, keys ...string) string {
	for _, k := range keys {
		switch v := m[k].(type) {
		case string:
			if v != "" {
				return v
			}
		case float64:
			// Some serializations emit the token as a numeric-ish value.
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

func intField(m map[string]any, keys ...string) int {
	v := floatField(m, keys...)
	return int(v)
}

func floatField(m map[string]any, keys ...string) float64 {
	for _, k := range keys {
		switch v := m[k].(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case json.Number:
			if f, err := v.Float64(); err == nil {
				return f
			}
		}
	}
	return 0
}
