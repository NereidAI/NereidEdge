// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gatewayoperation

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

const (
	// publishTimeout bounds publishing one result. It is independent of the
	// operation deadline so the failure frame still goes out when a routine
	// exhausts timeout_ms.
	publishTimeout = 5 * time.Second

	// maxResultBytes caps a result frame well under the 64KB protocol limit.
	maxResultBytes = 64 * 1024

	// seenTTL is how long an invoke id is remembered for duplicate
	// suppression.
	seenTTL = 10 * time.Minute

	// defaultPingCount is the diag.ping probe count when the caller omits it.
	defaultPingCount = 4

	// maxPingCount caps the diag.ping probe count.
	maxPingCount = 10
)

// Gateway is the subset of the NereidLink gateway client surface the handler
// needs. *nlgateway.Client satisfies it; tests substitute a recording fake.
type Gateway interface {
	SubscribeOperations(ctx context.Context, qos byte, onInvoke func(device.OperationInvoke)) error
	RespondOperation(ctx context.Context, frame device.OperationResult, qos byte) error
}

// Options carries the local sources some operations need. The zero value is
// valid: operations whose source is unset are still advertised but fail with a
// clear error at invoke time. Services unset suppresses service.restart
// entirely (it is only offered when a managed-service allowlist exists).
type Options struct {
	// LogFile is the agent log file sys.logs_tail tails. Empty makes
	// sys.logs_tail fail.
	LogFile string
	// IdentityCertFile is the PEM northbound mTLS identity certificate
	// sys.cert_info reports. Empty makes sys.cert_info fail.
	IdentityCertFile string
	// Services maps an allowlisted service name to its systemd unit for
	// service.restart. Empty suppresses the operation entirely.
	Services map[string]string
	// Restart triggers a supervised restart of the agent process, used by the
	// one-way sys.agent_restart. Nil disables the restart action.
	Restart func()
	// InferenceDiag executes the ADR-0039 inference diagnostic through the
	// deployed NereidInference device service. Nil suppresses inference.diag.
	InferenceDiag func(context.Context, map[string]any) (json.RawMessage, error)
	// CameraProfiles enumerates ONVIF media profiles of one managed camera
	// sub-device (read-only). Nil suppresses the camera.profiles operation.
	CameraProfiles func(context.Context, map[string]any) (json.RawMessage, error)
}

// Config carries the dependencies for the operation Handler.
type Config struct {
	// Gateway is the connected NereidLink gateway client.
	Gateway Gateway
	// Logger is the application logging client.
	Logger logger.LoggingClient
	// Options carries the local sources the operations need.
	Options Options
}

// Handler executes the firmware's advertised gateway operations.
type Handler struct {
	gw      Gateway
	lc      logger.LoggingClient
	opts    Options
	qos     byte
	catalog map[string]device.OperationAdvert

	mu   sync.Mutex
	seen map[string]time.Time
}

// New creates a gateway-operation handler.
func New(cfg Config) *Handler {
	if cfg.Logger == nil {
		cfg.Logger = logger.NewMockClient()
	}
	catalog := make(map[string]device.OperationAdvert)
	for _, advert := range Adverts(cfg.Options) {
		catalog[advert.Operation] = advert
	}
	return &Handler{
		gw:      cfg.Gateway,
		lc:      cfg.Logger,
		opts:    cfg.Options,
		catalog: catalog,
		seen:    make(map[string]time.Time),
	}
}

// Start subscribes to the NL operation downlink. It must run before hello and
// again after every clean-session reconnect, because the broker drops every
// subscription on a transport break. Each frame runs on its own goroutine so a
// slow routine never blocks the MQTT dispatch.
func (h *Handler) Start(ctx context.Context, qos byte) error {
	if h.gw == nil {
		h.lc.Warn("gateway operation handler has no gateway; skipping subscription")
		return nil
	}
	h.qos = qos
	return h.gw.SubscribeOperations(ctx, qos, func(inv device.OperationInvoke) {
		go h.Handle(inv)
	})
}

// Handle runs one operation invocation. Only operations advertised in hello
// are executed. One-way descriptors run their routine but never produce a
// result frame. Param values are never logged: a secret:// reference arrives
// here as plaintext and must not be written to logs.
func (h *Handler) Handle(inv device.OperationInvoke) {
	if !h.claim(inv.ID) {
		h.lc.Debugf("gateway operation %s ignored: duplicate or late id=%s", inv.Operation, inv.ID)
		return
	}
	advert, ok := h.catalog[inv.Operation]
	if !ok {
		h.respond(device.OperationResult{ID: inv.ID, OK: false, Error: "unknown operation"})
		return
	}

	result, err := h.safeExecute(inv, advert)
	if advert.Interaction == device.OperationInteractionOneWay {
		if err != nil {
			h.lc.Warnf("gateway operation %s v%s failed: %s", inv.Operation, inv.TypeVersion, err.Error())
			return
		}
		h.lc.Infof("gateway operation %s v%s executed (one-way)", inv.Operation, inv.TypeVersion)
		return
	}
	if err != nil {
		h.lc.Warnf("gateway operation %s v%s failed: %s", inv.Operation, inv.TypeVersion, err.Error())
		h.respond(device.OperationResult{ID: inv.ID, OK: false, Error: err.Error()})
		return
	}
	h.lc.Infof("gateway operation %s v%s succeeded", inv.Operation, inv.TypeVersion)
	h.respond(device.OperationResult{ID: inv.ID, OK: true, Result: result})
}

// safeExecute runs execute, converting a panic in an operation routine into an
// error so a bug can never crash the gateway.
func (h *Handler) safeExecute(inv device.OperationInvoke, advert device.OperationAdvert) (result json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			h.lc.Errorf("gateway operation %s panicked: %v", inv.Operation, r)
			result = nil
			err = fmt.Errorf("operation %s internal error", inv.Operation)
		}
	}()
	return h.execute(inv, advert)
}

// execute dispatches an advertised operation to its routine.
func (h *Handler) execute(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	switch inv.Operation {
	case OperationDiagPing:
		return runPing(inv, advert)
	case OperationDiagDNSResolve:
		return runDNSResolve(inv, advert)
	case OperationDiagTCPConnect:
		return runTCPConnect(inv, advert)
	case OperationDiagTLSInspect:
		return runTLSInspect(inv, advert)
	case OperationDiagHTTPCheck:
		return runHTTPCheck(inv, advert)
	case OperationSysInfo:
		return runSysInfo(inv, advert)
	case OperationSysResources:
		return runSysResources(inv, advert)
	case OperationSysNetwork:
		return runSysNetwork(inv, advert)
	case OperationSysProcessesTop:
		return runSysProcessesTop(inv, advert)
	case OperationSysNTPStatus:
		return runSysNTPStatus(inv, advert)
	case OperationSysNTPSync:
		return runSysNTPSync(inv, advert)
	case OperationDiagUplinkStatus:
		return runDiagUplinkStatus(inv, advert)
	case OperationNetworkReconnect:
		return runNetworkReconnect(inv, advert)
	case OperationSysReboot:
		return runSysReboot(inv, advert)
	case OperationSysAgentRestart:
		return h.runAgentRestart(inv, advert)
	case OperationSysLogLevel:
		return h.runLogLevelGet(inv, advert)
	case OperationSysLogsTail:
		return h.runLogsTail(inv, advert)
	case OperationSysCertInfo:
		return h.runCertInfo(inv, advert)
	case OperationServiceRestart:
		return h.runServiceRestart(inv, advert)
	case OperationInferenceDiag:
		if h.opts.InferenceDiag == nil {
			return nil, fmt.Errorf("operation %q not implemented", inv.Operation)
		}
		diagCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		return h.opts.InferenceDiag(diagCtx, inv.Params)
	case OperationCameraProfiles:
		if h.opts.CameraProfiles == nil {
			return nil, fmt.Errorf("operation %q not implemented", inv.Operation)
		}
		profCtx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		return h.opts.CameraProfiles(profCtx, inv.Params)
	default:
		return nil, fmt.Errorf("operation %q not implemented", inv.Operation)
	}
}

// respond publishes one result frame under a short deadline, downgrading an
// oversized payload to an error rather than emitting a truncated frame.
func (h *Handler) respond(frame device.OperationResult) {
	if len(frame.Result) > maxResultBytes {
		frame = device.OperationResult{ID: frame.ID, OK: false, Error: "result too large"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()
	if err := h.gw.RespondOperation(ctx, frame, h.qos); err != nil {
		h.lc.Errorf("respond gateway operation id=%s failed: %s", frame.ID, err.Error())
	}
}

// claim records an invoke id, returning false if it was already seen. It keeps
// the map bounded by dropping entries older than seenTTL. An empty id cannot
// be correlated, so it is always executed.
func (h *Handler) claim(id string) bool {
	if id == "" {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.seen[id]; ok {
		return false
	}
	h.seen[id] = time.Now()
	if len(h.seen) > 4096 {
		cutoff := time.Now().Add(-seenTTL)
		for k, t := range h.seen {
			if t.Before(cutoff) {
				delete(h.seen, k)
			}
		}
	}
	return true
}

// opTimeout resolves the operation deadline: the smaller of the descriptor
// default and the per-invocation override, clamped to the protocol range.
func opTimeout(advert device.OperationAdvert, invokeMs int) time.Duration {
	ms := advert.TimeoutMs
	if invokeMs > 0 && (ms <= 0 || invokeMs < ms) {
		ms = invokeMs
	}
	if ms < 100 {
		ms = 100
	}
	if ms > 60000 {
		ms = 60000
	}
	return time.Duration(ms) * time.Millisecond
}

// paramInt coerces a JSON number param to int; JSON numbers decode as float64.
func paramInt(v any) (int, error) {
	switch n := v.(type) {
	case float64:
		if n != math.Trunc(n) {
			return 0, fmt.Errorf("not an integer")
		}
		return int(n), nil
	case int:
		return n, nil
	case json.Number:
		i, err := n.Int64()
		return int(i), err
	default:
		return 0, fmt.Errorf("not an integer")
	}
}

// stringParam returns a string param and whether it was present and a string.
func stringParam(params map[string]any, key string) (string, bool) {
	v, ok := params[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return s, true
}
