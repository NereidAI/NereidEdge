// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package devicehttp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

// newTestServer records the last request and answers with reply, wiring
// the standard device error envelope on errorReply.
func newTestServer(t *testing.T, reply map[string]any, status int, errorReply *device.ErrorBody) (*httptest.Server, *http.Request, *string) {
	t.Helper()
	var lastReq http.Request
	var lastBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastReq = *r
		raw, _ := io.ReadAll(r.Body)
		lastBody = string(raw)
		if errorReply != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": errorReply})
			return
		}
		if status != 0 {
			w.WriteHeader(status)
			if reply != nil {
				_ = json.NewEncoder(w).Encode(reply)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	}))
	t.Cleanup(srv.Close)
	return srv, &lastReq, &lastBody
}

func TestBearerAuthAndTelemetry(t *testing.T) {
	srv, lastReq, lastBody := newTestServer(t, map[string]any{"published": 2}, 0, nil)
	c, err := New(Options{BaseURL: srv.URL, Credential: device.Token("dev-token")})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ack, err := c.PublishTelemetry(context.Background(),
		device.NewFrame().SetFloat("temperature", 21.5).SetBool("online", true))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ack.Published != 2 {
		t.Fatalf("ack: %+v", ack)
	}
	if got := lastReq.Header.Get("Authorization"); got != "Bearer dev-token" {
		t.Fatalf("auth header: %q", got)
	}
	if lastReq.URL.Path != PathTelemetry {
		t.Fatalf("path: %s", lastReq.URL.Path)
	}
	if !strings.Contains(*lastBody, "temperature") {
		t.Fatalf("body: %s", *lastBody)
	}
}

func TestBasicAuth(t *testing.T) {
	srv, lastReq, _ := newTestServer(t, map[string]any{"command": nil}, 0, nil)
	c, _ := New(Options{BaseURL: srv.URL, Credential: device.Basic("cid", "secret")})
	if _, err := c.PollCommands(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if got := lastReq.Header.Get("Authorization"); !strings.HasPrefix(got, "Basic ") {
		t.Fatalf("auth header: %q", got)
	}
}

func TestPollCommandsIdleAndBusy(t *testing.T) {
	srv, _, _ := newTestServer(t, map[string]any{"command": nil}, 0, nil)
	c, _ := New(Options{BaseURL: srv.URL, Credential: device.Token("t")})
	cmd, err := c.PollCommands(context.Background())
	if err != nil {
		t.Fatalf("idle poll: %v", err)
	}
	if cmd != nil {
		t.Fatalf("idle poll must return nil command: %+v", cmd)
	}

	reply := map[string]any{"command": map[string]any{
		"id": "cmd-1", "name": "set-led", "params": map[string]any{"color": "green"}}}
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	}))
	t.Cleanup(srv2.Close)
	c2, _ := New(Options{BaseURL: srv2.URL, Credential: device.Token("t")})
	cmd, err = c2.PollCommands(context.Background())
	if err != nil || cmd == nil || cmd.ID != "cmd-1" || cmd.Name != "set-led" {
		t.Fatalf("busy poll: cmd=%+v err=%v", cmd, err)
	}
}

func TestRespondCommand(t *testing.T) {
	srv, lastReq, lastBody := newTestServer(t, nil, http.StatusNoContent, nil)
	c, _ := New(Options{BaseURL: srv.URL, Credential: device.Token("t")})
	err := c.RespondCommand(context.Background(), "cmd-1", map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("respond: %v", err)
	}
	if lastReq.URL.Path != PathCommandReply+"cmd-1" {
		t.Fatalf("path: %s", lastReq.URL.Path)
	}
	if !strings.Contains(*lastBody, `"ok":true`) {
		t.Fatalf("body: %s", *lastBody)
	}
}

func TestReportShadow(t *testing.T) {
	srv, lastReq, _ := newTestServer(t, nil, http.StatusNoContent, nil)
	c, _ := New(Options{BaseURL: srv.URL, Credential: device.Token("t")})
	v := int64(3)
	err := c.ReportShadow(context.Background(), device.ReportedPatch{State: map[string]any{"led": "on"}, Version: &v})
	if err != nil {
		t.Fatalf("shadow: %v", err)
	}
	if lastReq.URL.Path != PathShadowReport {
		t.Fatalf("path: %s", lastReq.URL.Path)
	}
}

func TestErrorEnvelope(t *testing.T) {
	errBody := &device.ErrorBody{Code: "UNAUTHORIZED", Message: "missing or invalid access token"}
	srv, _, _ := newTestServer(t, nil, http.StatusUnauthorized, errBody)
	c, _ := New(Options{BaseURL: srv.URL, Credential: device.Token("bad")})
	_, err := c.PollCommands(context.Background())
	var apiErr *device.APIError
	if err == nil {
		t.Fatal("expected error")
	}
	if !asError(err, &apiErr) {
		t.Fatalf("expected *device.APIError, got %T: %v", err, err)
	}
	if apiErr.HTTPStatus != http.StatusUnauthorized || apiErr.Body.Code != "UNAUTHORIZED" {
		t.Fatalf("bad error: %+v", apiErr)
	}
}

func asError(err error, target **device.APIError) bool {
	for err != nil {
		if e, ok := err.(*device.APIError); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestOTADownloadRange(t *testing.T) {
	packageBytes := []byte("0123456789abcdef")
	var lastReq http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastReq = *r
		if r.URL.Path == "/v1/me/ota/task-1/package" {
			w.Header().Set("X-Firmware-Sha256", "abc")
			rangeHdr := r.Header.Get("Range")
			if rangeHdr == "" {
				w.Header().Set("Content-Length", "16")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(packageBytes)
				return
			}
			w.Header().Set("Content-Range", "bytes 4-9/16")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(packageBytes[4:10])
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	c, _ := New(Options{BaseURL: srv.URL, Credential: device.Token("t")})

	var sb strings.Builder
	res, err := c.DownloadOTAPackage(context.Background(), "task-1",
		DownloadOptions{Offset: 4, Length: 6, Destination: &sb})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if res.Status != http.StatusPartialContent || res.SHA256 != "abc" || sb.String() != "456789" {
		t.Fatalf("bad ranged download: %+v body=%q", res, sb.String())
	}
	if lastReq.Header.Get("Range") != "bytes=4-9" {
		t.Fatalf("range header: %q", lastReq.Header.Get("Range"))
	}
}

func TestOTAFlow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/me/ota/tasks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
			"task_id": "task-1", "version": "1.2.3",
			"url":    "/v1/me/ota/task-1/package",
			"sha256": strings.Repeat("a", 64), "size": 16, "state": "downloading",
		}}})
	})
	var progress string
	mux.HandleFunc("POST /v1/me/ota/{task}/progress", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		progress = string(raw)
		if task := r.PathValue("task"); task != "task-1" {
			t.Fatalf("task: %s", task)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c, _ := New(Options{BaseURL: srv.URL, Credential: device.Token("t")})
	list, err := c.ListOTAAssignments(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Assignments) != 1 || list.Assignments[0].TaskID != "task-1" {
		t.Fatalf("list: %+v", list)
	}
	if _, err := url.Parse(list.Assignments[0].URL); err != nil {
		t.Fatalf("url: %v", err)
	}
	err = c.ReportOTAProgress(context.Background(), "task-1",
		device.OTAProgress{State: device.OTAStateDownloading, ProgressPct: 50, Bytes: 8})
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if !strings.Contains(progress, `"state":"downloading"`) {
		t.Fatalf("progress body: %s", progress)
	}
}

// TestEmptyAssignmentsNeverNull pins the API arrays rule on the client
// side for defensive consumers.
func TestEmptyAssignmentsNormalized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"assignments":null}`))
	}))
	t.Cleanup(srv.Close)
	c, _ := New(Options{BaseURL: srv.URL, Credential: device.Token("t")})
	list, err := c.ListOTAAssignments(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if list.Assignments == nil {
		t.Fatal("assignments must be non-nil slice")
	}
}
