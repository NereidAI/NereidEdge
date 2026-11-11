// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package corecmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func TestDeviceCoreCommands(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/device/name/camera-109" {
			t.Errorf("path = %s", r.URL.Path)
		}
		writeJSON(w, http.StatusOK, responses.DeviceCoreCommandResponse{
			BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		})
	}))
	defer srv.Close()

	c := New(srv.URL, 0)
	dcc, status, err := c.DeviceCoreCommands(context.Background(), "camera-109")
	if err != nil || status != http.StatusOK {
		t.Fatalf("err=%v status=%d", err, status)
	}
	if dcc.DeviceName != "" {
		t.Errorf("unexpected device name %q", dcc.DeviceName)
	}
}

func TestReadAppendsRawQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RawQuery; got != "ds-returnevent=false" {
			t.Errorf("raw query = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, 0)
	_, status, err := c.Read(context.Background(), "camera-109", "DeviceInformation", "ds-returnevent=false")
	if err != nil || status != http.StatusOK {
		t.Fatalf("err=%v status=%d", err, status)
	}
}

func TestSetWithQuerySuppressesManagerBusEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v3/device/name/nereid-inference/PipelinesJSON" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.RawQuery; got != "ds-pushevent=false&ds-returnevent=false" {
			t.Errorf("raw query = %q", got)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["PipelinesJSON"] != `[{"pipelineId":"p1"}]` {
			t.Errorf("body = %+v", body)
		}
		writeJSON(w, http.StatusOK, dtoCommon.NewBaseResponse("", "", http.StatusOK))
	}))
	defer srv.Close()

	client := New(srv.URL, 0)
	_, status, err := client.SetWithQuery(context.Background(), "nereid-inference", "PipelinesJSON",
		"ds-pushevent=false&ds-returnevent=false", map[string]string{"PipelinesJSON": `[{"pipelineId":"p1"}]`})
	if err != nil || status != http.StatusOK {
		t.Fatalf("err=%v status=%d", err, status)
	}
}

func TestReadDecodesEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, responses.EventResponse{
			BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		})
	}))
	defer srv.Close()

	c := New(srv.URL, 0)
	ev, status, err := c.Read(context.Background(), "camera-109", "DeviceInformation", "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("err=%v status=%d", err, status)
	}
	if ev.StatusCode != http.StatusOK {
		t.Errorf("envelope status = %d", ev.StatusCode)
	}
}

func TestNon200ReturnsStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusInternalServerError, responses.DeviceCoreCommandResponse{
			BaseResponse: dtoCommon.NewBaseResponse("", "boom", http.StatusInternalServerError),
		})
	}))
	defer srv.Close()

	c := New(srv.URL, 0)
	_, _, err := c.DeviceCoreCommands(context.Background(), "camera-109")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want message \"boom\"", err)
	}
}
