// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"

	"github.com/nereidai/nereidedge/internal/binding"
	device "github.com/nereidai/nereidlink/sdk/device/go"
	nlgateway "github.com/nereidai/nereidlink/sdk/device/go/gateway"
)

// deviceUUID is the NL sub-device UUID bound to the fake EdgeX device.
const deviceUUID = "07704cf9-f0fc-4876-8955-64e6141540c7"

// fakeGateway records RespondCommand calls for request-response commands.
type fakeGateway struct {
	mu        sync.Mutex
	responses []device.GatewayCommandResponse
	onCommand func(device.GatewayCommand)
}

func (f *fakeGateway) SubscribeCommands(_ context.Context, _ byte, on nlgateway.GatewayCommandHandler) error {
	f.onCommand = on
	return nil
}

func (f *fakeGateway) RespondCommand(_ context.Context, commandID, subDeviceID string, result any, _ byte) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses = append(f.responses, device.GatewayCommandResponse{
		CommandID: commandID,
		Device:    subDeviceID,
		Data:      raw,
	})
	return nil
}

func (f *fakeGateway) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.responses)
}

func (f *fakeGateway) last() device.GatewayCommandResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.responses) == 0 {
		return device.GatewayCommandResponse{}
	}
	return f.responses[len(f.responses)-1]
}

// stubCoreCommand builds a core-command fake serving the device/command
// routes used by the handler and returns the sent PUT bodies.
func stubCoreCommand(deviceCoreCommands responses.DeviceCoreCommandResponse, read responses.EventResponse) (*httptest.Server, *[]map[string]any) {
	writes := make([]map[string]any, 0, 2)
	mux := http.NewServeMux()
	writeMu := &sync.Mutex{}
	mux.HandleFunc("/api/v3/device/name/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/device/name/"), "/")
		switch {
		case r.Method == http.MethodGet && len(parts) == 1:
			writeJSON(w, http.StatusOK, deviceCoreCommands)
		case r.Method == http.MethodGet && len(parts) == 2:
			writeJSON(w, http.StatusOK, read)
		case r.Method == http.MethodPut && len(parts) == 2:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			writeMu.Lock()
			writes = append(writes, body)
			writeMu.Unlock()
			writeJSON(w, http.StatusOK, dtoCommon.NewBaseResponse("", "", http.StatusOK))
		default:
			http.NotFound(w, r)
		}
	})
	server := httptest.NewServer(mux)
	return server, &writes
}

// stubCoreCommandError serves a fixed status for every /api/v3/device/name/ call.
func stubCoreCommandError(status int) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/device/name/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, status, dtoCommon.NewBaseResponse("", "boom", status))
	})
	return httptest.NewServer(mux)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
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

func commandFrame(commandID, deviceUUID, method string, params string) device.GatewayCommand {
	var raw json.RawMessage
	if params != "" {
		raw = json.RawMessage(params)
	}
	return device.GatewayCommand{
		CommandID:   commandID,
		Device:      deviceUUID,
		Interaction: device.InteractionRequestResponse,
		Timeliness:  device.TimelinessRealtime,
		Method:      method,
		Params:      raw,
	}
}

func TestHandleReadRequestResponse(t *testing.T) {
	dc := responses.DeviceCoreCommandResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		DeviceCoreCommand: dtos.DeviceCoreCommand{
			DeviceName: "camera-109",
			CoreCommands: []dtos.CoreCommand{{
				Name: "DeviceInformation",
				Get:  true,
			}},
		},
	}
	ev := responses.EventResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		Event: dtos.Event{
			DeviceName: "camera-109",
			Readings: []dtos.BaseReading{{
				ResourceName:  "vendor",
				ValueType:     common.ValueTypeString,
				SimpleReading: dtos.SimpleReading{Value: "ONVIF-ACME"},
			}, {
				ResourceName:  "uptime",
				ValueType:     common.ValueTypeInt32,
				SimpleReading: dtos.SimpleReading{Value: "42"},
			}},
		},
	}
	core, _ := stubCoreCommand(dc, ev)
	defer core.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, core.URL)

	cmd := commandFrame("cmd-1", deviceUUID, "DeviceInformation", "")
	h.Handle(cmd)

	if gw.count() != 1 {
		t.Fatalf("expected 1 response, got %d", gw.count())
	}
	resp := gw.last()
	if resp.CommandID != "cmd-1" {
		t.Fatalf("command_id not echoed: %q", resp.CommandID)
	}
	if resp.Device != deviceUUID {
		t.Fatalf("device not echoed: %q", resp.Device)
	}
	var data map[string]any
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("data not JSON: %v", err)
	}
	if data["vendor"] != "ONVIF-ACME" {
		t.Fatalf("vendor reading missing: %v", data)
	}
	if data["uptime"] != float64(42) {
		t.Fatalf("uptime reading wrong: %v", data["uptime"])
	}
}

func TestHandleOneWayNoResponse(t *testing.T) {
	dc := responses.DeviceCoreCommandResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		DeviceCoreCommand: dtos.DeviceCoreCommand{
			DeviceName: "camera-109",
			CoreCommands: []dtos.CoreCommand{{
				Name: "DeviceInformation",
				Get:  true,
			}},
		},
	}
	ev := responses.EventResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		Event:        dtos.Event{DeviceName: "camera-109", Readings: []dtos.BaseReading{}},
	}
	core, _ := stubCoreCommand(dc, ev)
	defer core.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, core.URL)

	cmd := commandFrame("cmd-2", deviceUUID, "DeviceInformation", "")
	cmd.Interaction = device.InteractionOneWay
	h.Handle(cmd)

	if gw.count() != 0 {
		t.Fatalf("one-way command must not respond, got %d response(s)", gw.count())
	}
}

func TestHandleUnmappedDevice(t *testing.T) {
	core, _ := stubCoreCommand(responses.DeviceCoreCommandResponse{}, responses.EventResponse{})
	defer core.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(nil)
	h := newHandler(t, gw, resolver, core.URL)

	cmd := commandFrame("cmd-3", "unknown-uuid", "DeviceInformation", "")
	h.Handle(cmd)

	if gw.count() != 1 {
		t.Fatalf("expected unmapped error response, got %d", gw.count())
	}
	resp := gw.last()
	if resp.CommandID != "cmd-3" {
		t.Fatalf("command_id not echoed: %q", resp.CommandID)
	}
	var data map[string]any
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("data not JSON: %v", err)
	}
	if data["error"] != "device not attached to this gateway" {
		t.Fatalf("unexpected error: %v", data["error"])
	}
}

func TestHandleCoreCommand404(t *testing.T) {
	core := stubCoreCommandError(http.StatusNotFound)
	defer core.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, core.URL)

	cmd := commandFrame("cmd-4", deviceUUID, "DeviceInformation", "")
	h.Handle(cmd)

	if gw.count() != 1 {
		t.Fatalf("expected error response, got %d", gw.count())
	}
	var data map[string]any
	if err := json.Unmarshal(gw.last().Data, &data); err != nil {
		t.Fatalf("data not JSON: %v", err)
	}
	if data["error"] != "boom" {
		t.Fatalf("unexpected error: %v", data["error"])
	}
	if data["edgex_status"] != float64(http.StatusNotFound) {
		t.Fatalf("unexpected edgex_status: %v", data["edgex_status"])
	}
}

func TestHandleCoreCommand500(t *testing.T) {
	core := stubCoreCommandError(http.StatusInternalServerError)
	defer core.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, core.URL)

	cmd := commandFrame("cmd-5", deviceUUID, "DeviceInformation", "")
	h.Handle(cmd)

	if gw.count() != 1 {
		t.Fatalf("expected error response, got %d", gw.count())
	}
	var data map[string]any
	if err := json.Unmarshal(gw.last().Data, &data); err != nil {
		t.Fatalf("data not JSON: %v", err)
	}
	if data["edgex_status"] != float64(http.StatusInternalServerError) {
		t.Fatalf("unexpected edgex_status: %v", data["edgex_status"])
	}
}

func TestHandleWriteCommand(t *testing.T) {
	dc := responses.DeviceCoreCommandResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		DeviceCoreCommand: dtos.DeviceCoreCommand{
			DeviceName: "camera-109",
			CoreCommands: []dtos.CoreCommand{{
				Name: "SetMode",
				Set:  true,
				Parameters: []dtos.CoreCommandParameter{{
					ResourceName: "Mode",
					ValueType:    common.ValueTypeString,
				}},
			}},
		},
	}
	core, writes := stubCoreCommand(dc, responses.EventResponse{})
	defer core.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, core.URL)

	cmd := commandFrame("cmd-6", deviceUUID, "SetMode", `{"Mode":"night"}`)
	h.Handle(cmd)

	if len(*writes) != 1 {
		t.Fatalf("expected 1 PUT, got %d", len(*writes))
	}
	body := (*writes)[0]
	if body["Mode"] != "night" {
		t.Fatalf("object params must be forwarded verbatim, got: %v", body)
	}

	if gw.count() != 1 {
		t.Fatalf("expected 1 response, got %d", gw.count())
	}
	resp := gw.last()
	if resp.CommandID != "cmd-6" {
		t.Fatalf("command_id not echoed: %q", resp.CommandID)
	}
	var data map[string]any
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("data not JSON: %v", err)
	}
	if data["status"] != "ok" {
		t.Fatalf("unexpected write status: %v", data)
	}
}

func TestHandleWriteScalarParamWrappedUnderResource(t *testing.T) {
	dc := responses.DeviceCoreCommandResponse{
		BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
		DeviceCoreCommand: dtos.DeviceCoreCommand{
			DeviceName: "camera-109",
			CoreCommands: []dtos.CoreCommand{{
				Name: "SetMode",
				Set:  true,
				Parameters: []dtos.CoreCommandParameter{{
					ResourceName: "Mode",
					ValueType:    common.ValueTypeString,
				}},
			}},
		},
	}
	core, writes := stubCoreCommand(dc, responses.EventResponse{})
	defer core.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, core.URL)

	h.Handle(commandFrame("cmd-7", deviceUUID, "SetMode", `"night"`))

	if len(*writes) != 1 {
		t.Fatalf("expected 1 PUT, got %d", len(*writes))
	}
	if (*writes)[0]["Mode"] != "night" {
		t.Fatalf("scalar param must be wrapped under the resource, got: %v", (*writes)[0])
	}
}

func TestStartRegistersCommandSubscription(t *testing.T) {
	gw := &fakeGateway{}
	resolver := binding.NewResolver(nil)
	h := newHandler(t, gw, resolver, "http://127.0.0.1:1")

	if err := h.Start(context.Background(), 1); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if gw.onCommand == nil {
		t.Fatal("subscription callback not registered")
	}
}

// captureServer records each request's method and decoded query, serving a
// caller-provided response writer.
type captureServer struct {
	mu       sync.Mutex
	methods  []string
	queries  []map[string]string
	commands []dtos.CoreCommand
	onRead   func(w http.ResponseWriter)
}

func (c *captureServer) serve() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/device/name/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/device/name/"), "/")
		c.mu.Lock()
		c.methods = append(c.methods, r.Method)
		query := make(map[string]string)
		for k := range r.URL.Query() {
			query[k] = r.URL.Query().Get(k)
		}
		c.queries = append(c.queries, query)
		commands := c.commands
		onRead := c.onRead
		c.mu.Unlock()

		switch {
		case r.Method == http.MethodGet && len(parts) == 1:
			writeJSON(w, http.StatusOK, responses.DeviceCoreCommandResponse{
				BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
				DeviceCoreCommand: dtos.DeviceCoreCommand{
					DeviceName:   parts[0],
					CoreCommands: commands,
				},
			})
		case r.Method == http.MethodGet && len(parts) == 2:
			onRead(w)
		case r.Method == http.MethodPut && len(parts) == 2:
			writeJSON(w, http.StatusOK, dtoCommon.NewBaseResponse("", "", http.StatusOK))
		default:
			http.NotFound(w, r)
		}
	})
	return httptest.NewServer(mux)
}

func (c *captureServer) calls() ([]string, []map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	methods := make([]string, len(c.methods))
	copy(methods, c.methods)
	queries := make([]map[string]string, len(c.queries))
	copy(queries, c.queries)
	return methods, queries
}

func TestHandleReadWithParamsQuery(t *testing.T) {
	capd := &captureServer{
		commands: []dtos.CoreCommand{{Name: "Events", Get: true}},
		onRead: func(w http.ResponseWriter) {
			writeJSON(w, http.StatusOK, responses.EventResponse{BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK)})
		},
	}
	srv := capd.serve()
	defer srv.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, srv.URL)

	cmd := commandFrame("cmd-rp", deviceUUID, "Events", `{"limit":"10","active":true}`)
	cmd.Direction = device.DirectionRead
	h.Handle(cmd)

	methods, queries := capd.calls()
	if methods[len(methods)-1] != http.MethodGet {
		t.Fatalf("read command method = %q, want GET", methods[len(methods)-1])
	}
	last := queries[len(queries)-1]
	if last["limit"] != "10" || last["active"] != "true" {
		t.Fatalf("read params not encoded into query: %v", last)
	}
}

func TestHandleForcedWriteOverridesReadWriteCapability(t *testing.T) {
	capd := &captureServer{
		commands: []dtos.CoreCommand{{Name: "Events", Get: true, Set: true}},
		onRead: func(w http.ResponseWriter) {
			writeJSON(w, http.StatusOK, responses.EventResponse{BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK)})
		},
	}
	srv := capd.serve()
	defer srv.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, srv.URL)

	cmd := commandFrame("cmd-wr", deviceUUID, "Events", `{"ack":true}`)
	cmd.Direction = device.DirectionWrite
	h.Handle(cmd)

	methods, _ := capd.calls()
	last := methods[len(methods)-1]
	if last != http.MethodPut {
		t.Fatalf("forced write on RW command method = %q, want PUT", last)
	}
}

func TestHandleReadRejectedForWriteOnly(t *testing.T) {
	capd := &captureServer{
		commands: []dtos.CoreCommand{{Name: "Reboot", Set: true}},
	}
	srv := capd.serve()
	defer srv.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, srv.URL)

	cmd := commandFrame("cmd-ro", deviceUUID, "Reboot", "")
	cmd.Direction = device.DirectionRead
	h.Handle(cmd)

	var data map[string]any
	if err := json.Unmarshal(gw.last().Data, &data); err != nil {
		t.Fatalf("data not JSON: %v", err)
	}
	if data["error"] == nil {
		t.Fatalf("expected capability error, got %v", data)
	}
}

func TestHandleWriteRejectedForReadOnly(t *testing.T) {
	capd := &captureServer{
		commands: []dtos.CoreCommand{{Name: "DeviceInformation", Get: true}},
	}
	srv := capd.serve()
	defer srv.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, srv.URL)

	cmd := commandFrame("cmd-wo", deviceUUID, "DeviceInformation", `{"ack":true}`)
	cmd.Direction = device.DirectionWrite
	h.Handle(cmd)

	var data map[string]any
	if err := json.Unmarshal(gw.last().Data, &data); err != nil {
		t.Fatalf("data not JSON: %v", err)
	}
	if data["error"] == nil {
		t.Fatalf("expected capability error, got %v", data)
	}
	methods, _ := capd.calls()
	for _, m := range methods {
		if m == http.MethodPut {
			t.Fatalf("read-only command must not be written, methods=%v", methods)
		}
	}
}

func TestEmptyDirectionLegacyInference(t *testing.T) {
	capd := &captureServer{
		commands: []dtos.CoreCommand{{Name: "DeviceInformation", Get: true, Set: true}},
		onRead: func(w http.ResponseWriter) {
			writeJSON(w, http.StatusOK, responses.EventResponse{BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK)})
		},
	}
	srv := capd.serve()
	defer srv.Close()

	gw := &fakeGateway{}
	resolver := binding.NewResolver(map[string]string{"camera-109": deviceUUID})
	h := newHandler(t, gw, resolver, srv.URL)

	cmd := commandFrame("cmd-legacy", deviceUUID, "DeviceInformation", "")
	h.Handle(cmd)

	methods, _ := capd.calls()
	if last := methods[len(methods)-1]; last != http.MethodGet {
		t.Fatalf("legacy inference on RW command method = %q, want GET", last)
	}
}
