// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package probe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"

	"github.com/nereidai/nereidedge/internal/binding"
	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// deviceUUID is the NL sub-device UUID bound to the fake EdgeX device.
const deviceUUID = "07704cf9-f0fc-4876-8955-64e6141540c7"

// fakeGateway records the per-device probe responses published by the handler.
type fakeGateway struct {
	mu        sync.Mutex
	responses []device.ProbeResponse
	onProbe   func(device.ProbeRequest)
}

func (f *fakeGateway) SubscribeProbes(_ context.Context, _ byte, on func(device.ProbeRequest)) error {
	f.onProbe = on
	return nil
}

func (f *fakeGateway) ProbeResult(_ context.Context, probeID, deviceID string, result any, _ byte) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	f.record(device.ProbeResponse{ProbeID: probeID, Device: deviceID, Result: raw})
	return nil
}

func (f *fakeGateway) ProbeError(_ context.Context, probeID, deviceID, reason string, _ byte) error {
	f.record(device.ProbeResponse{ProbeID: probeID, Device: deviceID, Error: reason})
	return nil
}

func (f *fakeGateway) record(resp device.ProbeResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses = append(f.responses, resp)
}

func (f *fakeGateway) all() []device.ProbeResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]device.ProbeResponse, len(f.responses))
	copy(out, f.responses)
	return out
}

// coreStub is a fake EdgeX core-command service: it serves the device core
// command list and delegates single-command reads to a callback.
type coreStub struct {
	server *httptest.Server
	mu     sync.Mutex
	reads  int
}

func (c *coreStub) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

// newCoreStub builds the fake service. read maps a command name to an HTTP
// status and JSON body.
func newCoreStub(t *testing.T, dc responses.DeviceCoreCommandResponse, read func(command string) (int, any)) *coreStub {
	t.Helper()
	stub := &coreStub{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/device/name/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/device/name/"), "/")
		switch {
		case r.Method == http.MethodGet && len(parts) == 1:
			writeJSON(w, http.StatusOK, dc)
		case r.Method == http.MethodGet && len(parts) == 2:
			stub.mu.Lock()
			stub.reads++
			stub.mu.Unlock()
			status, body := read(parts[1])
			writeJSON(w, status, body)
		default:
			http.NotFound(w, r)
		}
	})
	stub.server = httptest.NewServer(mux)
	t.Cleanup(stub.server.Close)
	return stub
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// coreCommands builds a device core command response with the given readable
// command names.
func coreCommands(names ...string) responses.DeviceCoreCommandResponse {
	cmds := make([]dtos.CoreCommand, 0, len(names))
	for _, name := range names {
		cmds = append(cmds, dtos.CoreCommand{Name: name, Get: true})
	}
	return responses.DeviceCoreCommandResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		DeviceCoreCommand: dtos.DeviceCoreCommand{
			DeviceName:   "camera-109",
			CoreCommands: cmds,
		},
	}
}

// eventWithReadings builds a 200 event response with one reading per pair.
func eventWithReadings(pairs ...string) responses.EventResponse {
	readings := make([]dtos.BaseReading, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		readings = append(readings, dtos.BaseReading{
			ResourceName:  pairs[i],
			ValueType:     common.ValueTypeString,
			SimpleReading: dtos.SimpleReading{Value: pairs[i+1]},
		})
	}
	return responses.EventResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		Event:        dtos.Event{DeviceName: "camera-109", Readings: readings},
	}
}

func emptyEvent() responses.EventResponse {
	return responses.EventResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		Event:        dtos.Event{DeviceName: "camera-109"},
	}
}

func newHandler(t *testing.T, gw Gateway, resolver *binding.Resolver, coreURL string) *Handler {
	t.Helper()
	return New(Config{
		Gateway:        gw,
		Resolver:       resolver,
		CoreCommandURL: coreURL,
		Logger:         logger.NewMockClient(),
	})
}

func TestHandleFreshReportsResult(t *testing.T) {
	stub := newCoreStub(t, coreCommands("Status"), func(string) (int, any) {
		return http.StatusOK, eventWithReadings("vendor", "ONVIF-ACME", "uptime", "42")
	})
	gw := &fakeGateway{}
	h := newHandler(t, gw, binding.NewResolver(map[string]string{"camera-109": deviceUUID}), stub.server.URL)

	h.Handle(device.ProbeRequest{ProbeID: "p1", Devices: []string{deviceUUID}, Mode: "fresh", TimeoutMS: 5000})

	all := gw.all()
	if len(all) != 1 {
		t.Fatalf("expected 1 response, got %d: %+v", len(all), all)
	}
	resp := all[0]
	if resp.ProbeID != "p1" || resp.Device != deviceUUID {
		t.Fatalf("correlation lost: %+v", resp)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	var result probeResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("result not JSON: %v", err)
	}
	if !result.Reachable || result.Resource != "Status" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.Values["vendor"] != "ONVIF-ACME" {
		t.Fatalf("reading missing: %+v", result.Values)
	}
}

func TestProbeUnattachedReportsError(t *testing.T) {
	stub := newCoreStub(t, coreCommands("Status"), func(string) (int, any) {
		return http.StatusOK, eventWithReadings("vendor", "ONVIF-ACME")
	})
	gw := &fakeGateway{}
	h := newHandler(t, gw, binding.NewResolver(nil), stub.server.URL)

	h.Handle(device.ProbeRequest{ProbeID: "p1", Devices: []string{deviceUUID}, Mode: "fresh", TimeoutMS: 5000})

	all := gw.all()
	if len(all) != 1 || all[0].Error != reasonNotAttached {
		t.Fatalf("expected %q, got %+v", reasonNotAttached, all)
	}
	if stub.readCount() != 0 {
		t.Fatalf("unattached device must not hit core-command, reads=%d", stub.readCount())
	}
}

func TestProbeUnreachableReportsNoResponse(t *testing.T) {
	stub := newCoreStub(t, coreCommands("Status"), func(string) (int, any) {
		return http.StatusServiceUnavailable, dtoCommon.NewBaseResponse("", "boom", http.StatusServiceUnavailable)
	})
	gw := &fakeGateway{}
	h := newHandler(t, gw, binding.NewResolver(map[string]string{"camera-109": deviceUUID}), stub.server.URL)

	h.Handle(device.ProbeRequest{ProbeID: "p1", Devices: []string{deviceUUID}, Mode: "fresh", TimeoutMS: 5000})

	all := gw.all()
	if len(all) != 1 || all[0].Error != reasonNoResponse {
		t.Fatalf("expected %q, got %+v", reasonNoResponse, all)
	}
}

func TestProbeInconclusiveIsError(t *testing.T) {
	stub := newCoreStub(t, coreCommands("Status"), func(string) (int, any) {
		return http.StatusOK, emptyEvent()
	})
	gw := &fakeGateway{}
	h := newHandler(t, gw, binding.NewResolver(map[string]string{"camera-109": deviceUUID}), stub.server.URL)

	h.Handle(device.ProbeRequest{ProbeID: "p1", Devices: []string{deviceUUID}, Mode: "fresh", TimeoutMS: 5000})

	all := gw.all()
	if len(all) != 1 || all[0].Error != reasonNoReadings {
		t.Fatalf("expected %q, got %+v", reasonNoReadings, all)
	}
}

func TestProbeStrictNoFallback(t *testing.T) {
	read := func(command string) (int, any) {
		if command == "First" {
			return http.StatusOK, emptyEvent()
		}
		return http.StatusOK, eventWithReadings("vendor", "ONVIF-ACME")
	}

	t.Run("strict", func(t *testing.T) {
		stub := newCoreStub(t, coreCommands("First", "Second"), read)
		gw := &fakeGateway{}
		h := newHandler(t, gw, binding.NewResolver(map[string]string{"camera-109": deviceUUID}), stub.server.URL)

		h.Handle(device.ProbeRequest{ProbeID: "p1", Devices: []string{deviceUUID}, Mode: device.ProbeModeStrict, TimeoutMS: 5000})

		all := gw.all()
		if len(all) != 1 || all[0].Error == "" {
			t.Fatalf("strict must not fall back, got %+v", all)
		}
		if stub.readCount() != 1 {
			t.Fatalf("strict should try exactly one command, reads=%d", stub.readCount())
		}
	})

	t.Run("fresh", func(t *testing.T) {
		stub := newCoreStub(t, coreCommands("First", "Second"), read)
		gw := &fakeGateway{}
		h := newHandler(t, gw, binding.NewResolver(map[string]string{"camera-109": deviceUUID}), stub.server.URL)

		h.Handle(device.ProbeRequest{ProbeID: "p1", Devices: []string{deviceUUID}, Mode: device.ProbeModeFresh, TimeoutMS: 5000})

		all := gw.all()
		if len(all) != 1 || all[0].Error != "" {
			t.Fatalf("fresh must fall back to a readable command, got %+v", all)
		}
		var result probeResult
		if err := json.Unmarshal(all[0].Result, &result); err != nil {
			t.Fatalf("result not JSON: %v", err)
		}
		if result.Resource != "Second" {
			t.Fatalf("expected fallback to Second, got %q", result.Resource)
		}
	})
}

func TestProbeCachedUsesCache(t *testing.T) {
	stub := newCoreStub(t, coreCommands("Status"), func(string) (int, any) {
		return http.StatusOK, eventWithReadings("vendor", "ONVIF-ACME")
	})
	gw := &fakeGateway{}
	h := newHandler(t, gw, binding.NewResolver(map[string]string{"camera-109": deviceUUID}), stub.server.URL)

	h.Handle(device.ProbeRequest{ProbeID: "p1", Devices: []string{deviceUUID}, Mode: device.ProbeModeFresh, TimeoutMS: 5000})
	if stub.readCount() != 1 {
		t.Fatalf("expected one live read, got %d", stub.readCount())
	}

	h.Handle(device.ProbeRequest{ProbeID: "p2", Devices: []string{deviceUUID}, Mode: device.ProbeModeCached, TimeoutMS: 5000})
	if stub.readCount() != 1 {
		t.Fatalf("cached probe must reuse the cache, reads=%d", stub.readCount())
	}

	all := gw.all()
	if len(all) != 2 {
		t.Fatalf("expected two responses, got %d", len(all))
	}
	var result probeResult
	if err := json.Unmarshal(all[1].Result, &result); err != nil {
		t.Fatalf("result not JSON: %v", err)
	}
	if !result.Cached || result.AgeMS < 0 {
		t.Fatalf("expected a cached result, got %+v", result)
	}
}

func TestProbeCachedExpiryFallsBackLive(t *testing.T) {
	stub := newCoreStub(t, coreCommands("Status"), func(string) (int, any) {
		return http.StatusOK, eventWithReadings("vendor", "ONVIF-ACME")
	})
	gw := &fakeGateway{}
	h := newHandler(t, gw, binding.NewResolver(map[string]string{"camera-109": deviceUUID}), stub.server.URL)
	h.cacheTTL = time.Nanosecond

	h.Handle(device.ProbeRequest{ProbeID: "p1", Devices: []string{deviceUUID}, Mode: device.ProbeModeCached, TimeoutMS: 5000})
	h.Handle(device.ProbeRequest{ProbeID: "p2", Devices: []string{deviceUUID}, Mode: device.ProbeModeCached, TimeoutMS: 5000})

	if stub.readCount() != 2 {
		t.Fatalf("expired cache must re-read live, reads=%d", stub.readCount())
	}
}

func TestStartSubscribes(t *testing.T) {
	stub := newCoreStub(t, coreCommands("Status"), func(string) (int, any) {
		return http.StatusOK, eventWithReadings("vendor", "ONVIF-ACME")
	})
	gw := &fakeGateway{}
	h := newHandler(t, gw, binding.NewResolver(map[string]string{"camera-109": deviceUUID}), stub.server.URL)

	if err := h.Start(context.Background(), 1); err != nil {
		t.Fatalf("start: %v", err)
	}
	if gw.onProbe == nil {
		t.Fatal("expected a probe subscription")
	}
}
