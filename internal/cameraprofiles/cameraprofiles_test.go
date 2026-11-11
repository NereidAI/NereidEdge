// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package cameraprofiles

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
)

func newProfilesEvent(object any, value string) responses.EventResponse {
	reading := dtos.BaseReading{
		ResourceName: "GetProfiles",
		ProfileName:  "onvif",
		DeviceName:   "camera-1",
		ValueType:    common.ValueTypeObject,
	}
	if object != nil {
		reading.ObjectReading = dtos.ObjectReading{ObjectValue: object}
	}
	if value != "" {
		reading.ValueType = common.ValueTypeString
		reading.SimpleReading = dtos.SimpleReading{Value: value}
	}
	return responses.NewEventResponse("req", "", 200, dtos.Event{Readings: []dtos.BaseReading{reading}})
}

func TestParseNestedObjectProfiles(t *testing.T) {
	// Canonical ONVIF shape nested under Profiles[].
	payload := map[string]any{
		"Profiles": []any{
			map[string]any{
				"token": "Profile_1",
				"name":  "main stream",
				"videoEncoderConfiguration": map[string]any{
					"encoding": "H264",
					"resolution": map[string]any{
						"width": float64(1920), "height": float64(1080),
					},
					"rateControl": map[string]any{"frameRateLimit": float64(25)},
				},
			},
			map[string]any{
				"Token": "Profile_2",
				"Name":  "sub",
				"VideoEncoderConfiguration": map[string]any{
					"Encoding": "H265",
					"Resolution": map[string]any{
						"Width": float64(640), "Height": float64(480),
					},
					"RateControl": map[string]any{"FrameRateLimit": float64(15)},
				},
			},
		},
	}
	got, err := ParseEvent(newProfilesEvent(payload, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("profiles=%d, want 2: %+v", len(got), got)
	}
	want := []Profile{
		{Token: "Profile_1", Name: "main stream", Encoding: "H264", Width: 1920, Height: 1080, FPS: 25},
		{Token: "Profile_2", Name: "sub", Encoding: "H265", Width: 640, Height: 480, FPS: 15},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("profile %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestParseFlatArrayAndJSONString(t *testing.T) {
	// Some firmware flattens the array straight into the reading value.
	flat := []any{
		map[string]any{"token": "Profile_1", "encoding": "h264",
			"width": float64(1280), "height": float64(720), "frameRateLimit": float64(20)},
	}
	raw, _ := json.Marshal(flat)
	got, err := ParseEvent(newProfilesEvent(nil, string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Token != "Profile_1" || got[0].Encoding != "H264" ||
		got[0].Width != 1280 || got[0].Height != 720 || got[0].FPS != 20 {
		t.Fatalf("flat parse = %+v", got)
	}

	if _, err := ParseEvent(newProfilesEvent(map[string]any{"unrelated": true}, "")); err == nil {
		t.Fatal("payload without profile tokens must fail")
	}
}

type fakeReader struct {
	profiles responses.EventResponse
	notFound map[string]bool
	calls    []string
}

func (f *fakeReader) Read(_ context.Context, _, command, _ string) (responses.EventResponse, int, error) {
	f.calls = append(f.calls, command)
	if f.notFound[command] {
		return responses.EventResponse{}, 404, errors.New("no such command")
	}
	return f.profiles, 200, nil
}

func TestEnumeratePrefersMediaProfilesResource(t *testing.T) {
	// device-onvif-camera 4.x names the resource "MediaProfiles"; its
	// profile documents carry their own Token plus nested configurations
	// that also carry Token fields (AudioEncoderConfiguration), which must
	// not be mistaken for additional media profiles.
	payload := map[string]any{
		"Profiles": []any{
			map[string]any{
				"Token": "Profile_1", "Name": "mainStream",
				"AudioEncoderConfiguration": map[string]any{"Token": "MainAudioEncoderToken"},
				"VideoEncoderConfiguration": map[string]any{
					"Encoding": "H264",
					"Resolution": map[string]any{
						"Width": float64(1920), "Height": float64(1080),
					},
					"RateControl": map[string]any{"FrameRateLimit": float64(25)},
				},
			},
			map[string]any{
				"Token": "Profile_2", "Name": "subStream",
				"VideoEncoderConfiguration": map[string]any{
					"Encoding": "H264",
					"Resolution": map[string]any{
						"Width": float64(640), "Height": float64(480),
					},
					"RateControl": map[string]any{"FrameRateLimit": float64(25)},
				},
			},
		},
	}
	reader := &fakeReader{
		notFound: map[string]bool{"GetProfiles": true, "Profiles": true, "Media2Profiles": true},
		profiles: newProfilesEvent(payload, ""),
	}
	got, err := Enumerate(context.Background(), reader, "camera-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.calls) != 1 || reader.calls[0] != "MediaProfiles" {
		t.Fatalf("commands tried = %v, want only MediaProfiles", reader.calls)
	}
	want := []Profile{
		{Token: "Profile_1", Name: "mainStream", Encoding: "H264", Width: 1920, Height: 1080, FPS: 25},
		{Token: "Profile_2", Name: "subStream", Encoding: "H264", Width: 640, Height: 480, FPS: 25},
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("profiles = %+v, want %+v", got, want)
	}
}

func TestEnumerateFallsBackToProfilesCommand(t *testing.T) {
	reader := &fakeReader{
		notFound: map[string]bool{"MediaProfiles": true, "GetProfiles": true, "Media2Profiles": true},
		profiles: newProfilesEvent(map[string]any{"Profiles": []any{map[string]any{"token": "Profile_1"}}}, ""),
	}
	got, err := Enumerate(context.Background(), reader, "camera-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Token != "Profile_1" {
		t.Fatalf("enumerate = %+v", got)
	}
	wantCalls := []string{"MediaProfiles", "GetProfiles", "Profiles"}
	if len(reader.calls) != len(wantCalls) {
		t.Fatalf("commands tried = %v, want %v", reader.calls, wantCalls)
	}
	for i := range wantCalls {
		if reader.calls[i] != wantCalls[i] {
			t.Fatalf("commands tried = %v, want %v", reader.calls, wantCalls)
		}
	}
}

// TestEnumerateFallsBackToMedia2Profiles covers the Media2 slim list shape
// ({Profiles:[{Fixed,Name,Token}]}) used when no classic profiles resource
// is registered.
func TestEnumerateFallsBackToMedia2Profiles(t *testing.T) {
	reader := &fakeReader{
		notFound: map[string]bool{"MediaProfiles": true, "GetProfiles": true, "Profiles": true},
		profiles: newProfilesEvent(map[string]any{"Profiles": []any{
			map[string]any{"Fixed": true, "Name": "mainStream", "Token": "Profile_1"},
			map[string]any{"Fixed": true, "Name": "subStream", "Token": "Profile_2"},
		}}, ""),
	}
	got, err := Enumerate(context.Background(), reader, "camera-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Token != "Profile_1" || got[1].Token != "Profile_2" ||
		got[1].Name != "subStream" {
		t.Fatalf("enumerate = %+v", got)
	}
}
