// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package command executes NereidLink gateway sub-device command downlinks
// against EdgeX core-command and reports the results.
//
// NL relays catalog commands addressed to an attached sub-device on
// v1/gateway/command (device.GatewayCommand). The method names the EdgeX
// command, the device names the NL sub-device UUID, and the interaction
// flag decides whether a response frame on v1/gateway/command/response is
// expected: one-way commands are executed silently, request-response
// commands always get a response with the command id and device echoed.
package command

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	"github.com/nereidai/nereidedge/internal/binding"
	"github.com/nereidai/nereidedge/internal/corecmd"
	device "github.com/nereidai/nereidlink/sdk/device/go"
	nlgateway "github.com/nereidai/nereidlink/sdk/device/go/gateway"
)

// DefaultCoreCommandURL is retained as an alias of corecmd.DefaultURL.
const DefaultCoreCommandURL = corecmd.DefaultURL

// httpTimeout bounds each core-command HTTP request.
const httpTimeout = 10 * time.Second

// Gateway is the subset of the NereidLink gateway client surface the
// command handler needs. *nlgateway.Client satisfies it; tests substitute a
// recording fake.
type Gateway interface {
	SubscribeCommands(ctx context.Context, qos byte, onCommand nlgateway.GatewayCommandHandler) error
	RespondCommand(ctx context.Context, commandID, subDeviceID string, result any, qos byte) error
}

// Config carries the dependencies for the command Handler.
type Config struct {
	// Gateway is the connected NereidLink gateway client.
	Gateway Gateway
	// Resolver maps NL sub-device UUIDs to EdgeX device names.
	Resolver *binding.Resolver
	// CoreCommandURL is the base URL of the EdgeX core-command service.
	// Defaults to DefaultCoreCommandURL when empty.
	CoreCommandURL string
	// Logger is the application logging client.
	Logger logger.LoggingClient
	// Timeout bounds the whole command execution, default 10s.
	Timeout time.Duration
}

// Handler executes NL gateway command downlinks and reports their results.
type Handler struct {
	gw       Gateway
	resolver *binding.Resolver
	core     *corecmd.Client
	timeout  time.Duration
	qos      byte
	lc       logger.LoggingClient
}

// New creates a command handler.
func New(cfg Config) *Handler {
	if cfg.Timeout <= 0 {
		cfg.Timeout = httpTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewMockClient()
	}
	return &Handler{
		gw:       cfg.Gateway,
		resolver: cfg.Resolver,
		core:     corecmd.New(cfg.CoreCommandURL, 0),
		timeout:  cfg.Timeout,
		lc:       cfg.Logger,
	}
}

// Start subscribes to NL gateway command downlinks. Each command is handled
// on its own goroutine so a slow EdgeX read never blocks the MQTT dispatch.
func (h *Handler) Start(ctx context.Context, qos byte) error {
	lc := h.lc
	if h.gw == nil {
		lc.Warn("command handler has no gateway; skipping subscription")
		return nil
	}
	h.qos = qos
	return h.gw.SubscribeCommands(ctx, qos, func(cmd device.GatewayCommand) {
		go h.Handle(cmd)
	})
}

// Handle processes one gateway command downlink: reverse-resolve the NL
// sub-device UUID to an EdgeX device name, run the named command via
// core-command, and (for request-response commands) publish the result.
func (h *Handler) Handle(cmd device.GatewayCommand) {
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	edgexName, ok := h.resolver.Name(cmd.Device)
	if !ok {
		h.lc.Warnf("command %s for unattached device %s: device not attached to this gateway",
			cmd.CommandID, cmd.Device)
		h.respond(cmd, map[string]any{"error": "device not attached to this gateway"})
		return
	}
	if cmd.Method == "" {
		h.respond(cmd, errorResult("empty command method", http.StatusBadRequest))
		return
	}

	dc, status, err := h.core.DeviceCoreCommands(ctx, edgexName)
	if err != nil {
		h.lc.Errorf("command %s on device %s: core-command device probe failed: %s (http %d)",
			cmd.CommandID, edgexName, err.Error(), status)
		h.respond(cmd, errorResult(err.Error(), status))
		return
	}
	var core *dtos.CoreCommand
	for i := range dc.CoreCommands {
		if dc.CoreCommands[i].Name == cmd.Method {
			core = &dc.CoreCommands[i]
			break
		}
	}
	if core == nil {
		msg := fmt.Sprintf("command %q not found on device %s", cmd.Method, edgexName)
		h.lc.Errorf("command %s on device %s: %s (http %d)", cmd.CommandID, edgexName, msg, http.StatusNotFound)
		h.respond(cmd, errorResult(msg, http.StatusNotFound))
		return
	}

	result, status, err := h.execute(ctx, edgexName, *core, cmd)
	if err != nil {
		h.lc.Errorf("command %s on device %s failed: %s (http %d)", cmd.CommandID, edgexName, err.Error(), status)
		h.respond(cmd, errorResult(err.Error(), status))
		return
	}
	h.respond(cmd, result)
}

// execute runs a read or write command against core-command, returning the
// result payload. The direction comes from cmd.Direction when the platform
// provides it; an empty direction preserves the legacy capability-based
// inference (Get wins over Set). The HTTP status of a failed call is
// returned so Handle can surface it in the response frame.
func (h *Handler) execute(ctx context.Context, edgexName string, core dtos.CoreCommand, cmd device.GatewayCommand) (any, int, error) {
	direction := cmd.Direction
	if direction == "" {
		switch {
		case core.Get:
			direction = device.DirectionRead
		case core.Set:
			direction = device.DirectionWrite
		default:
			return nil, http.StatusBadRequest, fmt.Errorf("command %q on device %s has neither read nor write capability", core.Name, edgexName)
		}
	}

	switch direction {
	case device.DirectionRead:
		return h.executeRead(ctx, edgexName, cmd.Method, core, cmd.Params)
	case device.DirectionWrite:
		return h.executeWrite(ctx, edgexName, cmd.Method, core, cmd.Params)
	default:
		return nil, http.StatusBadRequest, fmt.Errorf("unknown direction %q for command %q", direction, cmd.Method)
	}
}

// executeRead forces the read path when the core command advertises Get.
// Read params are query parameters on the south-bound request, encoded from
// the JSON object rather than assumed absent.
func (h *Handler) executeRead(ctx context.Context, edgexName, commandName string, core dtos.CoreCommand, params json.RawMessage) (any, int, error) {
	if !core.Get {
		return nil, http.StatusBadRequest, fmt.Errorf("command %q on device %s does not support read", commandName, edgexName)
	}
	query, err := encodeReadParams(params)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	envelope, status, err := h.core.Read(ctx, edgexName, commandName, query)
	if err != nil {
		return nil, status, err
	}
	return corecmd.ReadingsToMap(envelope.Event.Readings), status, nil
}

// executeWrite forces the write path when the core command advertises Set.
//
// EdgeX core-command expects a settings body keyed by resource name, so an
// object params document is forwarded verbatim ({"heartbeat":0} writes the
// heartbeat resource). A non-object document is treated as the value of the
// command's single resource and wrapped as {resource: value}. Wrapping an
// object under the resource name would hand EdgeX a map where it expects a
// scalar, which is the failure this avoids.
func (h *Handler) executeWrite(ctx context.Context, edgexName, commandName string, core dtos.CoreCommand, params json.RawMessage) (any, int, error) {
	if !core.Set {
		return nil, http.StatusBadRequest, fmt.Errorf("command %q on device %s does not support write", commandName, edgexName)
	}
	body := map[string]any{}
	if len(params) > 0 {
		var v any
		if err := json.Unmarshal(params, &v); err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("invalid params for command %q: %w", commandName, err)
		}
		if obj, ok := v.(map[string]any); ok {
			body = obj
		} else {
			body[setResourceName(core)] = v
		}
	}
	if _, status, err := h.core.Set(ctx, edgexName, commandName, body); err != nil {
		return nil, status, err
	}
	return map[string]any{"status": "ok"}, http.StatusOK, nil
}

// encodeReadParams converts a read command's JSON params object into an
// encoded URL query string. A non-object JSON document is rejected; nested
// values are JSON-encoded into their query value.
func encodeReadParams(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return "", fmt.Errorf("read params must be a JSON object: %w", err)
	}
	values := url.Values{}
	for key, value := range params {
		values.Set(key, queryValue(value))
	}
	return values.Encode(), nil
}

// queryValue renders one read parameter value for query encoding.
func queryValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	case []any, map[string]any:
		raw, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return string(raw)
	default:
		return fmt.Sprint(t)
	}
}

// setResourceName picks the EdgeX resource name a write command body is
// keyed by: the first command parameter when present, else the command name.
func setResourceName(core dtos.CoreCommand) string {
	for _, p := range core.Parameters {
		if p.ResourceName != "" {
			return p.ResourceName
		}
	}
	return core.Name
}

// respond publishes the result for a request-response command; one-way
// commands never produce a response frame.
func (h *Handler) respond(cmd device.GatewayCommand, data any) {
	if cmd.Interaction != device.InteractionRequestResponse {
		return
	}
	if h.gw == nil {
		h.lc.Warnf("command %s: no gateway to respond through", cmd.CommandID)
		return
	}
	h.lc.Debugf("responding to command %s for device %s", cmd.CommandID, cmd.Device)
	if err := h.gw.RespondCommand(context.Background(), cmd.CommandID, cmd.Device, data, h.qos); err != nil {
		h.lc.Errorf("respond to command %s failed: %s", cmd.CommandID, err.Error())
	}
}

// errorResult builds the arbitrary JSON error payload. NL marks the command
// SUCCEEDED as long as a response arrives; failures are carried in data.
func errorResult(msg string, edgexStatus int) map[string]any {
	out := map[string]any{"error": msg}
	if edgexStatus > 0 {
		out["edgex_status"] = edgexStatus
	}
	return out
}
