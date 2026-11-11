// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package connector applies NereidLink connector desired specs to the local
// EdgeX core-metadata registry.
//
// The registered connector type "nereid-edge" is opaque to NereidLink: its
// config carries everything needed to create or update one EdgeX device.
// Specs are declarative and fully re-sent after every (re)hello, so the
// applier is idempotent and keeps only in-memory state.
package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/requests"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
	edgexErrors "github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	device "github.com/nereidai/nereidlink/sdk/device/go"
	nlgateway "github.com/nereidai/nereidlink/sdk/device/go/gateway"

	"github.com/nereidai/nereidedge/internal/binding"
	"github.com/nereidai/nereidedge/internal/pipeline"
)

// pushQueueCapacity bounds how many connector pushes may wait for the
// single serial apply worker. The paho downlink callback MUST NOT block
// (it runs on the MQTT read loop, where blocking also stalls PINGRESP
// decoding); an overfull queue drops the push and relies on the next hello
// redrive to reconcile desired state.
const pushQueueCapacity = 64

// metadataCallTimeout bounds every blocking EdgeX core-metadata HTTP call.
// The generated EdgeX metadata client sets no overall http.Client timeout,
// so without a deadline an unreachable core-metadata hangs the apply path
// forever.
const metadataCallTimeout = 10 * time.Second

// statusReportTimeout bounds the status publish acknowledgement wait. A QoS1
// paho publish normally returns immediately, but while the transport is
// degraded its token may never complete; the apply worker must never wait on
// that indefinitely. The publish itself is not cancelled (paho ignores
// contexts), the worker only stops waiting.
const statusReportTimeout = 5 * time.Second

// TypeEdgeXConnector is the canonical connector type agreed with NereidLink.
// Earlier naming drafts are accepted as aliases so in-flight specs keep
// working.
const (
	TypeEdgeXConnector = "nereid-edge"
	aliasEdgeConnector = "edge-connector"
	aliasEdgeXDevice   = "edgex-device"
	aliasEdgeXConn     = "edgex-connector"
)

// SpecConfig is the effective config contract of an edgex-device connector.
type SpecConfig struct {
	// DSName is the target EdgeX device service name.
	DSName string `json:"ds_name"`
	// ProfileName references an EdgeX device profile that must already exist.
	ProfileName string `json:"profile_name"`
	// DeviceName is the EdgeX device name and the binding resolver key.
	DeviceName string `json:"device_name"`
	// Description is optional free text.
	Description string `json:"description,omitempty"`
	// Labels are EdgeX device labels.
	Labels []string `json:"labels,omitempty"`
	// Protocols maps EdgeX protocol names to protocol properties.
	Protocols map[string]map[string]any `json:"protocols"`
	// AutoEvents declares periodic readings.
	AutoEvents []dtos.AutoEvent `json:"auto_events,omitempty"`
	// Tags and Properties are passed through verbatim.
	Tags       map[string]any `json:"tags,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
	// ProbeResource is the readable command used by the sub-device health
	// monitor, explicitly chosen by the spec author. Empty falls back to
	// profile markers/auto-detection.
	ProbeResource string `json:"probe_resource,omitempty"`
}

// isLocalTag reports whether the spec tags mark a gateway-local resource,
// which skips reachability probing and follows the gateway lifecycle.
func isLocalTag(tags map[string]any) bool {
	v, ok := tags["local"].(bool)
	return ok && v
}

// AttachFunc is invoked after a spec device is successfully applied. local
// reports whether the device is a gateway-local resource (tags.local=true),
// which skips reachability probing.
type AttachFunc func(edgexName, nlDeviceUUID, profileName, probeResource string, local bool, hc *device.ConnectorHealthCheck)

// healthCheckLog renders the top-level health_check policy for logs. It is
// the only log line that reveals whether NL actually sent a per-instance
// probe policy (the field lives outside the spec config JSON).
func healthCheckLog(hc *device.ConnectorHealthCheck) string {
	if hc == nil {
		return "<none>"
	}
	return fmt.Sprintf("{resource=%q interval_ms=%d timeout_ms=%d fail_threshold=%d}",
		hc.Resource, hc.IntervalMS, hc.TimeoutMS, hc.FailThreshold)
}

// sameHealthCheck compares two per-instance probe policies, treating two nil
// pointers as equal.
func sameHealthCheck(a, b *device.ConnectorHealthCheck) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Resource == b.Resource &&
		a.IntervalMS == b.IntervalMS &&
		a.TimeoutMS == b.TimeoutMS &&
		a.FailThreshold == b.FailThreshold
}

// DetachFunc is invoked after a spec device has been removed.
type DetachFunc func(edgexName, nlDeviceUUID string)

// Applier subscribes to connector desired pushes and reconciles EdgeX devices.
type Applier struct {
	service  interfaces.ApplicationService
	gw       *nlgateway.Client
	reporter statusReporter
	// reportRawStatus is a test seam for optional status fields. Production
	// publishes through gw.MQTT on the unchanged connector-status topic.
	reportRawStatus func(context.Context, []byte) error
	resolver        *binding.Resolver
	lc              logger.LoggingClient

	// observe only logs and updates bindings; it never mutates EdgeX metadata.
	observe bool

	onAttach AttachFunc
	onDetach DetachFunc

	mu             sync.Mutex
	revisions      map[string]int64
	specDeviceOf   map[string]string // specID -> EdgeX device name
	applied        map[string]appliedSpec
	broken         map[string]struct{} // applied specs whose EdgeX device is gone and could not be restored
	inferenceState map[string]string

	registry *Registry
	edge     *EdgeXHandler

	// exports and uplinkFilters own gateway-level connector runtimes. They are
	// never nil after NewApplier. The configurable uplink policy guards the main
	// NL telemetry path, while the mandatory raw-NI privacy guard
	// (PipelineJSON/Embedding*) is enforced on both paths: FilterReadings for
	// the uplink and matchingReadings inside every edge-export-mqtt fanout.
	exports          *pipeline.ExportManager
	uplinkFilters    *pipeline.UplinkFilterManager
	managerResources *managerResourceManager

	// pushQ decouples the paho downlink callback (which runs on the MQTT
	// read loop and must never block) from EdgeX HTTP calls. One worker
	// drains it serially, preserving per-connection push ordering.
	pushQ chan device.ConnectorPush

	// metadataTimeout bounds each core-metadata call. It is a field only so
	// tests can shorten it; production uses metadataCallTimeout.
	metadataTimeout time.Duration
	// statusTimeout bounds the status publish wait; tests shorten it.
	statusTimeout time.Duration

	inferenceAdvert     bool
	inferenceDSName     string
	inferenceProfile    string
	inferenceCommands   CommandReader
	inferencePoll       time.Duration
	inferenceNegotiated func() bool
	// inferencePresent reports whether the deployed inference device
	// service/manager device currently exists in EdgeX. It gates the hello
	// self-registration so a gateway without NI never claims the capability.
	// A nil predicate means "present" (unit tests that do not wire NI).
	inferencePresent func() bool

	// inferenceCreds resolves onvif-basic camera secret names for stream
	// bindings and converges the credential replica set inside NI. Nil keeps
	// snapshot bindings unchanged and stream jobs without an NI secret.
	inferenceCreds InferenceCredentialManager
	// inferenceCredPing coalesces credential-reconcile triggers (spec
	// apply/delete, onvif secret delivery, periodic manifest reconciliation).
	inferenceCredPing chan struct{}

	// settledAt marks the start of the desired-state redrive after (re)hello
	// or process start; the orphan-job sweep waits one grace period so it
	// cannot delete a live job whose spec simply has not replayed yet.
	settledAt time.Time
	// lastSweep throttles the metadata orphan scan.
	lastSweep time.Time
}

// statusReporter is the subset of *nlgateway.Client used to report connector
// lifecycle state; tests substitute a recorder.
type statusReporter interface {
	ReportConnectorStatus(ctx context.Context, status device.ConnectorStatus, qos byte) error
}

// appliedSpec is one connector instance the edge has applied, reported back
// to NL in every gateway manifest for desired/observed reconciliation.
type nodeRuntime interface {
	Apply(specID string, revision int64, hash string, raw json.RawMessage) (state, reason string, err error)
	Delete(specID string)
	NeedsRestart(specID string) bool
}

type appliedSpec struct {
	revision      int64
	deviceID      string
	deviceName    string
	profileName   string
	connectorType string
	config        json.RawMessage
	mappings      json.RawMessage
	hash          string
	hc            *device.ConnectorHealthCheck
	// node-level instance has no EdgeX device
	node bool
}

// AppliedInstance represents an applied connector instance snapshot.
type AppliedInstance struct {
	Revision    int64
	DeviceID    string
	DeviceName  string
	ProfileName string
	Hash        string
}

// NewApplier creates a connector spec applier.
func NewApplier(
	service interfaces.ApplicationService,
	gw *nlgateway.Client,
	resolver *binding.Resolver,
	observe bool,
	onAttach AttachFunc,
	onDetach DetachFunc,
) *Applier {
	a := &Applier{
		service:           service,
		gw:                gw,
		reporter:          gw,
		resolver:          resolver,
		lc:                service.LoggingClient(),
		observe:           observe,
		onAttach:          onAttach,
		onDetach:          onDetach,
		revisions:         make(map[string]int64),
		specDeviceOf:      make(map[string]string),
		applied:           make(map[string]appliedSpec),
		broken:            make(map[string]struct{}),
		inferenceState:    make(map[string]string),
		pushQ:             make(chan device.ConnectorPush, pushQueueCapacity),
		inferenceCredPing: make(chan struct{}, 1),
		metadataTimeout:   metadataCallTimeout,
		statusTimeout:     statusReportTimeout,
		settledAt:         time.Now(),
	}
	a.registry = NewRegistry()
	a.edge = NewEdgeXHandler(a)
	a.registry.Register(NewInferenceHandler(a))
	export := NewExportMQTTHandler(a)
	a.registry.Register(a.edge)
	a.registry.Register(export)
	a.registry.Register(&aliasHandler{h: a.edge, t: aliasEdgeConnector})
	a.registry.Register(&aliasHandler{h: a.edge, t: aliasEdgeXDevice})
	a.registry.Register(&aliasHandler{h: a.edge, t: aliasEdgeXConn})
	a.exports = pipeline.NewExportManager(a.lc)
	a.exports.SetStateCallback(a.reportExportStatus)
	a.uplinkFilters = pipeline.NewUplinkFilterManager()
	a.registry.Register(NewNorthboundFilterHandler(a))
	a.managerResources = newManagerResourceManager(a.lc)
	a.managerResources.report = a.reportManagerResult
	a.registry.Register(NewManagerResourceHandler(a))
	return a
}

// Exports returns the node-level export manager for pipeline stage wiring.
func (a *Applier) Exports() *pipeline.ExportManager {
	return a.exports
}

// UplinkFilters returns the policy manager applied by TelemetryPublisher.
func (a *Applier) UplinkFilters() *pipeline.UplinkFilterManager {
	return a.uplinkFilters
}

// ReconcileAndReport verifies every applied spec against the real EdgeX
// registry before building the manifest segment. Devices deleted out-of-band
// are recreated from the stored desired config; specs that cannot be
// restored are excluded so NL classifies them missing and keeps alerting.
func (a *Applier) ReconcileAndReport(ctx context.Context) []device.ManifestConnectorInstance {
	// Full-state convergence for NI Job devices survives reconnects and
	// out-of-band deletes. Throttled and replay-grace guarded internally;
	// run detached so the metadata scan never delays the manifest frame.
	if a.inferenceAdvert {
		go a.SweepOrphanInferenceJobs(context.Background())
		a.NotifyInferenceCredentials()
	}

	a.mu.Lock()
	snapshot := make(map[string]appliedSpec, len(a.applied))
	for id, sp := range a.applied {
		snapshot[id] = sp
	}
	a.mu.Unlock()

	for specID, sp := range snapshot {
		if a.observe {
			continue
		}
		if sp.node {
			a.markHealthy(specID)
			continue
		}
		_, err := a.deviceByName(ctx, sp.deviceName)
		if err == nil {
			a.markHealthy(specID)
			continue
		}
		if edgexErrors.Kind(err) != edgexErrors.KindEntityDoesNotExist {
			a.lc.Warnf("Cannot verify EdgeX device %s for spec %s: %s", sp.deviceName, specID, err.Error())
			reason := "cannot verify EdgeX device " + sp.deviceName + ": " + err.Error()
			if sp.connectorType == TypeEdgeInference {
				reason = "runtime-unreachable"
			}
			a.reportDegraded(ctx, pushFor(specID, sp), reason)
			continue
		}
		a.lc.Warnf("EdgeX device %s missing, restoring from spec %s", sp.deviceName, specID)
		push := pushFor(specID, sp)
		cfg, err := a.restoreConfig(specID, sp)
		if err != nil {
			if sp.connectorType == TypeEdgeInference {
				// The source-camera mapping may be re-attaching; keep retrying
				// instead of permanently marking the spec broken.
				a.lc.Warnf("Cannot translate edge-inference spec %s yet: %v", specID, err)
				a.reportDegraded(ctx, push, inferenceFailureReason(err))
				continue
			}
			a.lc.Errorf("Cannot restore device %s: stored config invalid: %s", sp.deviceName, err.Error())
			a.markBroken(specID)
			a.reportDegraded(ctx, push, "restore failed: stored config invalid")
			continue
		}
		if err := a.upsertDevice(ctx, cfg); err != nil {
			a.lc.Errorf("Cannot restore device %s: %s", sp.deviceName, err.Error())
			reason := "restore failed: " + err.Error()
			if sp.connectorType == TypeEdgeInference {
				reason = inferenceFailureReason(err)
			} else {
				a.markBroken(specID)
			}
			a.reportDegraded(ctx, pushFor(specID, sp), reason)
			continue
		}
		a.resolver.SetSpec(specID, sp.deviceName, sp.deviceID)
		a.markHealthy(specID)
		a.lc.Infof("Restored EdgeX device %s from spec %s", sp.deviceName, specID)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]device.ManifestConnectorInstance, 0, len(a.applied))
	for specID, sp := range a.applied {
		if _, bad := a.broken[specID]; bad {
			continue
		}
		out = append(out, device.ManifestConnectorInstance{
			SpecID:     specID,
			Revision:   sp.revision,
			ConfigHash: sp.hash,
		})
	}
	return out
}

// Reset clears all in-memory applied/dedupe state after a gateway session
// was lost (broker restart). Paho restores the TCP connection and
// subscriptions, but the platform has forgotten the negotiated session and
// re-drives full desired state after the next hello; dropping the local
// cache lets those pushes fully re-apply and re-report APPLIED instead of
// being skipped as "already applied".
func (a *Applier) Reset() {
	a.mu.Lock()
	for specID, name := range a.specDeviceOf {
		a.resolver.ReleaseSpec(specID, name)
	}
	a.revisions = make(map[string]int64)
	a.specDeviceOf = make(map[string]string)
	a.applied = make(map[string]appliedSpec)
	a.broken = make(map[string]struct{})
	a.inferenceState = make(map[string]string)
	// Desired state re-uploads after the upcoming hello; reopen the replay
	// grace window so the orphan sweep cannot race spec re-application.
	a.settledAt = time.Now()
	a.mu.Unlock()
	a.managerResources.prepareReplay()
}

func (a *Applier) markBroken(specID string) {
	a.mu.Lock()
	a.broken[specID] = struct{}{}
	a.mu.Unlock()
}

func (a *Applier) markHealthy(specID string) {
	a.mu.Lock()
	delete(a.broken, specID)
	a.mu.Unlock()
}

// Start registers the downlink handler and launches the serial apply
// worker. It must be called BEFORE hello negotiation because the server
// re-sends full desired state after hello.
//
// The paho callback only enqueues the push and returns: paho invokes it
// synchronously on the MQTT read loop, so any blocking work here (EdgeX
// metadata HTTP in particular) would also block packet decoding and lead
// to pingresp timeouts.
func (a *Applier) Start(ctx context.Context) error {
	if err := a.Resubscribe(ctx); err != nil {
		return err
	}
	a.managerResources.prepareReplay()
	go a.runPushWorker(ctx)
	go a.runInferenceCredentialReconciler(ctx)
	go a.managerResources.run(ctx)
	return nil
}

// Resubscribe re-issues the connector downlink subscription without starting
// another push worker. The SDK uses a clean session, so a transport reconnect
// drops the broker-side subscription; the caller re-issues it after every
// fresh hello while the worker started by Start keeps draining the queue.
func (a *Applier) Resubscribe(ctx context.Context) error {
	return a.gw.SubscribeConnectors(ctx, 1, a.enqueuePush)
}

// enqueuePush is the paho downlink callback. It must never block: when the
// queue is full the push is dropped and logged; the next hello redrive
// re-sends full desired state, so strict reconcile eventually heals it.
func (a *Applier) enqueuePush(push device.ConnectorPush) {
	select {
	case a.pushQ <- push:
	default:
		a.lc.Errorf("connector push queue full (%d); dropping spec=%s kind=%s rev=%d; next hello redrive will reconcile",
			pushQueueCapacity, push.SpecID, push.Kind, push.Revision)
	}
}

// runPushWorker drains pushed specs serially so per-connection ordering
// from the broker is preserved. A single bad spec (e.g. unreachable
// core-metadata) must never stop the loop.
func (a *Applier) runPushWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case push := <-a.pushQ:
			a.dispatchPush(ctx, push)
		}
	}
}

// dispatchPush routes one push: delete frames bypass the type registry
// because NL sends them without connector_type; spec frames go to the
// registered handler for their connector type. It runs on the apply worker,
// never on the MQTT read loop.
func (a *Applier) dispatchPush(ctx context.Context, push device.ConnectorPush) {
	a.lc.Debugf("connector push received: kind=%s spec=%s type=%q rev=%d device=%s health_check=%s config=%s mappings=%s",
		push.Kind, push.SpecID, push.ConnectorType, push.Revision, push.DeviceID,
		healthCheckLog(push.HealthCheck),
		sanitizeSecrets(string(push.Config)), sanitizeSecrets(string(push.Mappings)))

	// Delete frames intentionally omit connector_type (NL sends only
	// kind/spec_id/device_id/revision), so they cannot be routed through the
	// type registry. applyDelete is type-independent bookkeeping and handles
	// both node exports and EdgeX devices.
	if push.Kind == device.ConnectorKindDelete {
		a.applyDelete(ctx, push)
		return
	}

	h, ok := a.registry.Get(push.ConnectorType)
	if !ok {
		a.lc.Debugf("Ignoring connector spec %s of type %q", push.SpecID, push.ConnectorType)
		return
	}
	switch push.Kind {
	case device.ConnectorKindSpec:
		_ = h.Apply(ctx, push)
	default:
		a.lc.Warnf("Unknown connector push kind %q for spec %s", push.Kind, push.SpecID)
	}
}

func (a *Applier) applySpec(ctx context.Context, push device.ConnectorPush) {
	// Node-level specs (empty device_id) never create an EdgeX device; route
	// them to the dedicated bookkeeping path before any device-only checks.
	if push.DeviceID == "" {
		a.applyNodeSpec(ctx, push, a.exports)
		return
	}

	var cfg SpecConfig
	if err := json.Unmarshal(push.Config, &cfg); err != nil {
		a.reportFailed(ctx, push, "invalid edgex-device config: "+err.Error())
		return
	}
	if cfg.DSName == "" || cfg.ProfileName == "" || cfg.DeviceName == "" {
		a.reportFailed(ctx, push, "config requires ds_name, profile_name and device_name")
		return
	}
	a.applyDeviceSpec(ctx, push, cfg)
}

// applyDeviceSpec contains the type-independent lifecycle for one spec-backed
// EdgeX device. Hash and reconciliation always use the original NL config,
// while cfg supplies the translated EdgeX device representation.
func (a *Applier) applyDeviceSpec(ctx context.Context, push device.ConnectorPush, cfg SpecConfig) {
	hash, err := device.CanonicalConfigHash(push.Config, push.Mappings)
	if err != nil {
		a.reportApplyFailed(ctx, push, fmt.Errorf("cannot hash config: %w", err))
		return
	}

	a.mu.Lock()
	prev, known := a.applied[push.SpecID]
	if owner, taken := a.specDeviceOf[cfg.DeviceName]; taken && owner != push.SpecID {
		a.mu.Unlock()
		a.reportApplyFailed(ctx, push, fmt.Errorf("edgex device name %s already claimed by spec %s", cfg.DeviceName, owner))
		return
	}
	a.mu.Unlock()

	// The applied decision is based on the desired target (device name +
	// canonical config), not revision alone: a revision can rename the target
	// device, and strict reconcile re-pushes the same revision to heal drift.
	if known && prev.deviceName == cfg.DeviceName && prev.hash == hash &&
		prev.revision >= push.Revision {
		if _, err := a.deviceByName(ctx, cfg.DeviceName); err == nil {
			// The EdgeX device is intact. A revision may still change the
			// top-level health_check policy, which is not part of the config
			// hash and must re-drive the attach hook to replace the probe
			// loop without touching the device.
			if !sameHealthCheck(prev.hc, push.HealthCheck) {
				a.lc.Infof("health_check changed for spec %s: %s -> %s; re-attaching probe",
					push.SpecID, healthCheckLog(prev.hc), healthCheckLog(push.HealthCheck))
				a.mu.Lock()
				cur := a.applied[push.SpecID]
				cur.hc = push.HealthCheck
				cur.revision = push.Revision
				a.applied[push.SpecID] = cur
				a.mu.Unlock()
				if a.onAttach != nil {
					a.onAttach(cfg.DeviceName, push.DeviceID, cfg.ProfileName,
						cfg.ProbeResource, isLocalTag(cfg.Tags), push.HealthCheck)
				}
				return
			}
			a.lc.Debugf("Skipping already-applied spec %s revision %d", push.SpecID, push.Revision)
			return
		}
		a.lc.Warnf("Spec %s already at rev %d but EdgeX device %s is missing, reapplying",
			push.SpecID, push.Revision, cfg.DeviceName)
	}

	oldName := ""
	if known && prev.deviceName != cfg.DeviceName {
		oldName = prev.deviceName
	}

	// State machine entry: from here on the spec is being built, reported as
	// CREATING until finishApply marks it APPLIED or a validation/apply error
	// reports FAILED. The idempotent skip above never reaches this point.
	a.reportCreating(ctx, push)

	if a.observe {
		a.lc.Infof("[observe] would apply edgex-device spec=%s rev=%d device=%s ds=%s",
			push.SpecID, push.Revision, cfg.DeviceName, cfg.DSName)
		a.finishApply(push, cfg, hash, oldName)
		a.reportApplied(ctx, push)
		return
	}

	if err := a.upsertDevice(ctx, cfg); err != nil {
		a.reportApplyFailed(ctx, push, err)
		return
	}

	// The target device was renamed: release the old binding and remove the
	// old EdgeX device unless another live spec still owns that name.
	if oldName != "" {
		a.resolver.ReleaseSpec(push.SpecID, oldName)
		if !a.nameClaimedByOther(push.SpecID, oldName) {
			if _, err := a.deviceByName(ctx, oldName); err == nil {
				if err := a.deviceDelete(ctx, oldName); err != nil {
					a.lc.Errorf("Failed to delete renamed-away EdgeX device %s: %s", oldName, err.Error())
				}
			}
		}
	}

	a.finishApply(push, cfg, hash, oldName)
	a.lc.Infof("edgex-device spec applied: spec=%s rev=%d edgex=%s nl=%s",
		push.SpecID, push.Revision, cfg.DeviceName, push.DeviceID)
	a.reportApplied(ctx, push)
}

// applyNodeSpec applies a node-level connector spec (empty device_id). No
// EdgeX device is created and no NL device binding is registered; the spec
// is only tracked in the applied set so manifests keep reconciling it.
func (a *Applier) applyNodeSpec(ctx context.Context, push device.ConnectorPush, runtime nodeRuntime) {
	if !json.Valid(push.Config) {
		a.reportFailed(ctx, push, "config is not valid JSON")
		return
	}
	hash, err := device.CanonicalConfigHash(push.Config, push.Mappings)
	if err != nil {
		a.reportFailed(ctx, push, "cannot hash config: "+err.Error())
		return
	}

	a.mu.Lock()
	prev, known := a.applied[push.SpecID]
	a.mu.Unlock()

	// Same idempotency rule as the device path: identical desired state at an
	// equal or newer revision must not rebuild anything (hello redrive),
	// unless the previous runtime is terminally FAILED and needs a rebuild.
	if known && prev.hash == hash && prev.revision >= push.Revision &&
		(a.observe || !runtime.NeedsRestart(push.SpecID)) {
		a.lc.Debugf("Skipping already-applied node spec %s revision %d", push.SpecID, push.Revision)
		return
	}

	a.reportCreating(ctx, push)

	var state, reason string
	if !a.observe {
		state, reason, err = runtime.Apply(push.SpecID, push.Revision, hash, push.Config)
		if err != nil {
			// Invalid desired config: report FAILED and do not record it as
			// applied, so a later fixed revision retries cleanly.
			a.reportFailed(ctx, push, err.Error())
			return
		}
	}

	a.finishNodeApply(push, hash)

	a.lc.Infof("node connector spec applied: spec=%s rev=%d type=%q",
		push.SpecID, push.Revision, push.ConnectorType)
	if state == device.ConnectorStateDegraded {
		a.reportDegraded(ctx, push, reason)
		return
	}
	a.reportApplied(ctx, push)
}

func (a *Applier) finishNodeApply(push device.ConnectorPush, hash string) {
	a.mu.Lock()
	a.revisions[push.SpecID] = push.Revision
	a.applied[push.SpecID] = appliedSpec{
		revision:      push.Revision,
		deviceID:      push.DeviceID,
		connectorType: push.ConnectorType,
		config:        push.Config,
		mappings:      push.Mappings,
		hash:          hash,
		node:          true,
	}
	delete(a.broken, push.SpecID)
	a.mu.Unlock()
}

// reportExportStatus relays export runtime state transitions (reconnect,
// backoff, hot swap) to NL. It is the ExportManager state callback.
func (a *Applier) reportExportStatus(specID string, revision int64, state, reason string) {
	var st string
	switch state {
	case pipeline.ExportStateApplied:
		st = device.ConnectorStateApplied
	case pipeline.ExportStateDegraded:
		st = device.ConnectorStateDegraded
	case pipeline.ExportStateFailed:
		st = device.ConnectorStateFailed
	default:
		return
	}
	_ = a.reportStatus(device.ConnectorStatus{
		SpecID:   specID,
		State:    st,
		Revision: revision,
		Reason:   reason,
	})
}

// finishApply updates all in-memory state after a spec was applied and fires
// presence hooks. oldName is non-empty when this revision renamed the target.
func (a *Applier) finishApply(push device.ConnectorPush, cfg SpecConfig, hash, oldName string) {
	var oldUUID string
	if oldName != "" {
		oldUUID, _ = a.resolver.Resolve(oldName)
	}

	a.mu.Lock()
	a.specDeviceOf[push.SpecID] = cfg.DeviceName
	a.revisions[push.SpecID] = push.Revision
	a.applied[push.SpecID] = appliedSpec{
		revision:      push.Revision,
		deviceID:      push.DeviceID,
		deviceName:    cfg.DeviceName,
		profileName:   cfg.ProfileName,
		connectorType: push.ConnectorType,
		config:        push.Config,
		mappings:      push.Mappings,
		hash:          hash,
		hc:            push.HealthCheck,
		node:          push.DeviceID == "",
	}
	delete(a.broken, push.SpecID)
	if push.ConnectorType == TypeEdgeInference {
		// Device creation/config application is APPLIED; the poller changes it
		// to DEGRADED/FAILED only when the translated DS job reports that fact.
		a.inferenceState[push.SpecID] = device.ConnectorStateApplied + "\x00"
	}
	a.mu.Unlock()

	a.resolver.SetSpec(push.SpecID, cfg.DeviceName, push.DeviceID)

	if oldName != "" && a.onDetach != nil && oldUUID != "" {
		a.onDetach(oldName, oldUUID)
	}
	if a.onAttach != nil {
		a.lc.Infof("attach edgex=%s nl=%s probe_resource=%q local=%v health_check=%s",
			cfg.DeviceName, push.DeviceID, cfg.ProbeResource, isLocalTag(cfg.Tags),
			healthCheckLog(push.HealthCheck))
		a.onAttach(cfg.DeviceName, push.DeviceID, cfg.ProfileName,
			cfg.ProbeResource, isLocalTag(cfg.Tags), push.HealthCheck)
	}
	if push.ConnectorType == TypeEdgeInference {
		// Replicate the stream credential into NI / prune replicas no
		// longer referenced; coalesced off the apply hot path.
		a.NotifyInferenceCredentials()
	}
}

// nameClaimedByOther reports whether an EdgeX device name is currently owned
// by any applied spec other than excludeSpec.
func (a *Applier) nameClaimedByOther(excludeSpec, name string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for specID, deviceName := range a.specDeviceOf {
		if specID != excludeSpec && deviceName == name {
			return true
		}
	}
	return false
}

func (a *Applier) applyDelete(ctx context.Context, push device.ConnectorPush) {
	// Delete frames omit connector_type. Fence a delayed manager delete before
	// clearing the generic applied ledger, not only before touching NI.
	if a.managerResources.superseded(push.SpecID, push.Revision) {
		return
	}
	a.mu.Lock()
	sp, existed := a.applied[push.SpecID]
	node := existed && sp.node
	deviceName := a.specDeviceOf[push.SpecID]
	delete(a.specDeviceOf, push.SpecID)
	delete(a.revisions, push.SpecID)
	delete(a.applied, push.SpecID)
	delete(a.broken, push.SpecID)
	delete(a.inferenceState, push.SpecID)
	a.mu.Unlock()

	if node {
		switch sp.connectorType {
		case TypeNorthboundFilter:
			a.uplinkFilters.Delete(push.SpecID)
		case TypeManagerResource:
			result := a.managerResources.delete(ctx, push.SpecID, push.Revision)
			if result.state != device.ConnectorStateApplied {
				reportPush := pushFor(push.SpecID, sp)
				reportPush.Revision = push.Revision
				a.managerResources.publishResult(push.SpecID, reportPush, result)
			}
		default:
			a.exports.Delete(push.SpecID)
		}
		a.lc.Infof("node connector spec removed: spec=%s type=%s", push.SpecID, sp.connectorType)
		return
	}
	if deviceName == "" {
		// Delete frames omit connector_type. A reconnect Reset or an initial
		// terminal manager rejection can leave a live node runtime without an
		// applied ledger entry, so probe each node owner before giving up.
		switch {
		case a.managerResources.has(push.SpecID):
			result := a.managerResources.delete(ctx, push.SpecID, push.Revision)
			if result.state != device.ConnectorStateApplied {
				a.managerResources.publishResult(push.SpecID, push, result)
			}
		case !a.uplinkFilters.NeedsRestart(push.SpecID):
			a.uplinkFilters.Delete(push.SpecID)
		case !a.exports.NeedsRestart(push.SpecID):
			a.exports.Delete(push.SpecID)
		case push.DeviceID == "":
			// A node-level delete may race ahead of its first spec frame. Keep a
			// non-writing manager revision fence so the delayed spec cannot revive
			// an already-deleted definition after this process or a restart.
			result := a.managerResources.delete(ctx, push.SpecID, push.Revision)
			if result.state != device.ConnectorStateApplied {
				a.managerResources.publishResult(push.SpecID, push, result)
			}
		default:
			a.lc.Warnf("Delete for unknown spec %s, nothing to remove", push.SpecID)
		}
		return
	}

	// Prefer the virtual output sub-device UUID remembered at apply time:
	// the resolver mapping may already be gone after a reconnect Reset, and
	// the detach must withdraw presence of exactly the device this spec
	// owned so it never lingers in manifest devices[].
	uuid := sp.deviceID
	if resolved, ok := a.resolver.Resolve(deviceName); ok && resolved != "" {
		uuid = resolved
	}
	if !a.observe {
		if _, err := a.deviceByName(ctx, deviceName); err == nil {
			if err := a.deviceDelete(ctx, deviceName); err != nil {
				a.lc.Errorf("Failed to delete EdgeX device %s: %s", deviceName, err.Error())
			}
		}
	}
	a.resolver.ReleaseSpec(push.SpecID, deviceName)
	a.lc.Infof("edgex-device spec removed: spec=%s edgex=%s", push.SpecID, deviceName)
	if existed && sp.connectorType == TypeEdgeInference {
		a.NotifyInferenceCredentials()
	}
	if a.onDetach != nil {
		a.onDetach(deviceName, uuid)
	}
}

// upsertDevice creates the EdgeX device, or updates it when the name exists.
// Every core-metadata call is individually bounded by metadataTimeout so
// an unreachable metadata service fails the spec instead of hanging the
// apply worker (the generated metadata client sets no HTTP timeout).
func (a *Applier) upsertDevice(ctx context.Context, cfg SpecConfig) error {
	deviceDTO := dtos.Device{
		Name:           cfg.DeviceName,
		Description:    cfg.Description,
		ServiceName:    cfg.DSName,
		ProfileName:    cfg.ProfileName,
		Labels:         cfg.Labels,
		AdminState:     "UNLOCKED",
		OperatingState: "UP",
		Protocols:      toEdgeXProtocols(cfg.Protocols),
		AutoEvents:     cfg.AutoEvents,
		Tags:           cfg.Tags,
		Properties:     cfg.Properties,
	}

	_, err := a.deviceByName(ctx, cfg.DeviceName)
	if err != nil {
		if edgexErrors.Kind(err) != edgexErrors.KindEntityDoesNotExist {
			return fmt.Errorf("core-metadata unreachable: %w", err)
		}
		// Create path.
		req := requests.AddDeviceRequest{
			BaseRequest: dtoCommon.NewBaseRequest(),
			Device:      deviceDTO,
		}
		if err := a.deviceAdd(ctx, req); err != nil {
			if edgexErrors.Kind(err) == edgexErrors.KindDuplicateName {
				return a.updateDevice(ctx, deviceDTO)
			}
			if errors.Is(err, errDeviceRejected) {
				return err
			}
			return fmt.Errorf("core-metadata unreachable: %w", err)
		}
		return nil
	}
	return a.updateDevice(ctx, deviceDTO)
}

// deviceByName calls core-metadata DeviceByName with metadataTimeout.
func (a *Applier) deviceByName(ctx context.Context, name string) (responses.DeviceResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, a.metadataTimeout)
	defer cancel()
	return a.service.DeviceClient().DeviceByName(ctx, name)
}

// errDeviceRejected marks a device add/update the device service refused
// through its synchronous validation callback (EdgeX multi-status 207 with
// a non-2xx per-item status). Unlike a metadata transport failure, the
// desired document itself is invalid; edge-inference reports it as
// config-invalid rather than runtime-unreachable.
var errDeviceRejected = errors.New("device service rejected device")

// deviceAdd calls core-metadata Add with metadataTimeout.
func (a *Applier) deviceAdd(ctx context.Context, req requests.AddDeviceRequest) error {
	ctx, cancel := context.WithTimeout(ctx, a.metadataTimeout)
	defer cancel()
	res, err := a.service.DeviceClient().Add(ctx, []requests.AddDeviceRequest{req})
	if err != nil {
		return err
	}
	for _, item := range res {
		if item.StatusCode >= 300 {
			return fmt.Errorf("%w: %s", errDeviceRejected, item.Message)
		}
	}
	return nil
}

// deviceDelete calls core-metadata DeleteDeviceByName with metadataTimeout.
func (a *Applier) deviceDelete(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, a.metadataTimeout)
	defer cancel()
	_, err := a.service.DeviceClient().DeleteDeviceByName(ctx, name)
	return err
}

func (a *Applier) updateDevice(ctx context.Context, deviceDTO dtos.Device) error {
	name := deviceDTO.Name
	adminState := deviceDTO.AdminState
	operatingState := deviceDTO.OperatingState
	patch := dtos.UpdateDevice{
		Name:           &name,
		Description:    &deviceDTO.Description,
		ServiceName:    &deviceDTO.ServiceName,
		ProfileName:    &deviceDTO.ProfileName,
		Labels:         deviceDTO.Labels,
		AdminState:     &adminState,
		OperatingState: &operatingState,
		Protocols:      deviceDTO.Protocols,
		AutoEvents:     deviceDTO.AutoEvents,
		Tags:           deviceDTO.Tags,
		Properties:     deviceDTO.Properties,
	}
	req := requests.UpdateDeviceRequest{
		BaseRequest: dtoCommon.NewBaseRequest(),
		Device:      patch,
	}
	ctx, cancel := context.WithTimeout(ctx, a.metadataTimeout)
	defer cancel()
	res, err := a.service.DeviceClient().Update(ctx, []requests.UpdateDeviceRequest{req})
	if err != nil {
		return fmt.Errorf("core-metadata unreachable: %w", err)
	}
	for _, item := range res {
		if item.StatusCode >= 300 {
			return fmt.Errorf("%w: %s", errDeviceRejected, item.Message)
		}
	}
	return err
}

// reportStatus publishes one lifecycle frame with a bounded acknowledgement
// wait. It is best-effort: failures are logged and never stop the apply
// worker or block subsequent pushes. A fresh timeout context is used instead
// of the (long-lived) worker context so a stalled paho publish token cannot
// hold the worker beyond statusReportTimeout.
func (a *Applier) reportStatus(status device.ConnectorStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.statusTimeout)
	defer cancel()
	if err := a.reporter.ReportConnectorStatus(ctx, status, 1); err != nil {
		a.lc.Errorf("Failed to report %s for spec %s: %s", status.State, status.SpecID, err.Error())
		return err
	}
	return nil
}

func (a *Applier) reportApplied(ctx context.Context, push device.ConnectorPush) {
	a.lc.Infof("Reporting APPLIED: spec=%s rev=%d", push.SpecID, push.Revision)
	_ = a.reportStatus(device.ConnectorStatus{
		SpecID:   push.SpecID,
		State:    device.ConnectorStateApplied,
		Revision: push.Revision,
	})
}

func (a *Applier) reportFailed(ctx context.Context, push device.ConnectorPush, reason string) {
	a.lc.Errorf("edgex-device spec failed: spec=%s rev=%d reason=%s", push.SpecID, push.Revision, reason)
	_ = a.reportStatus(device.ConnectorStatus{
		SpecID:   push.SpecID,
		State:    device.ConnectorStateFailed,
		Revision: push.Revision,
		Reason:   reason,
	})
}

// reportApplyFailed keeps raw local error text for generic connector types but
// maps edge-inference failures onto the transport-safe §15.6 reason table, so
// metadata/DS implementation details never reach NL.
func (a *Applier) reportApplyFailed(ctx context.Context, push device.ConnectorPush, err error) {
	reason := err.Error()
	if push.ConnectorType == TypeEdgeInference {
		reason = inferenceFailureReason(err)
	}
	a.reportFailed(ctx, push, reason)
}

// toEdgeXProtocols converts the config map form to EdgeX ProtocolProperties.
func toEdgeXProtocols(in map[string]map[string]any) map[string]dtos.ProtocolProperties {
	if len(in) == 0 {
		// EdgeX rejects devices without a protocols entry; use the standard
		// "other" empty protocol used by async-only devices.
		return map[string]dtos.ProtocolProperties{"other": {}}
	}
	out := make(map[string]dtos.ProtocolProperties, len(in))
	for proto, props := range in {
		out[proto] = dtos.ProtocolProperties(props)
	}
	return out
}

// GetApplied returns a snapshot of currently applied connector instances.
func (a *Applier) GetApplied() []AppliedInstance {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]AppliedInstance, 0, len(a.applied))
	for _, sp := range a.applied {
		out = append(out, AppliedInstance{
			Revision:    sp.revision,
			DeviceID:    sp.deviceID,
			DeviceName:  sp.deviceName,
			ProfileName: sp.profileName,
			Hash:        sp.hash,
		})
	}
	return out
}

// ConnectorAdverts returns the connector types and their config schemas to
// advertise during hello negotiation.
func (a *Applier) ConnectorAdverts() []device.ConnectorAdvert {
	schema := json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["ds_name","profile_name","device_name"],"properties":{"ds_name":{"type":"string"},"profile_name":{"type":"string"},"device_name":{"type":"string"},"description":{"type":"string"},"labels":{"type":"array","items":{"type":"string"}},"protocols":{"type":"object"},"auto_events":{"type":"array"},"tags":{"type":"object"},"properties":{"type":"object"}}}`)
	expSchema := json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["broker_url"],"properties":{"enabled":{"type":"boolean"},"broker_url":{"type":"string"},"client_id":{"type":"string"},"qos":{"type":"integer","minimum":0,"maximum":2},"retained":{"type":"boolean"},"keep_alive_s":{"type":"integer"},"username":{"type":"string"},"password":{"type":"string"},"ca_cert":{"type":"string"},"client_cert":{"type":"string"},"client_key":{"type":"string"},"insecure_skip_verify":{"type":"boolean"},"topic_template":{"type":"string"},"payload_format":{"type":"string","enum":["json","raw"]},"filter":{"type":"object","properties":{"device_names":{"type":"array","items":{"type":"string"}},"profile_names":{"type":"array","items":{"type":"string"}},"resource_names":{"type":"array","items":{"type":"string"}},"min_readings":{"type":"integer"}}},"buffer":{"type":"object","properties":{"max_batch":{"type":"integer"},"flush_ms":{"type":"integer"}}},"reconnect":{"type":"object","properties":{"min_backoff_ms":{"type":"integer"},"max_backoff_ms":{"type":"integer"},"max_attempts":{"type":"integer"}}},"tags":{"type":"object"}}}`)
	adverts := []device.ConnectorAdvert{{
		Type:         TypeEdgeXConnector,
		TypeVersion:  "",
		ConfigSchema: schema,
	}, {
		Type:         "edge-export-mqtt",
		TypeVersion:  "",
		ConfigSchema: expSchema,
	}, {
		Type:         TypeNorthboundFilter,
		TypeVersion:  "1",
		ConfigSchema: northboundFilterSchema,
	}}
	if a.inferenceAdvert && (a.inferencePresent == nil || a.inferencePresent()) {
		adverts = append(adverts, device.ConnectorAdvert{
			Type:         TypeEdgeInference,
			TypeVersion:  inferenceTypeVersion,
			ConfigSchema: inferenceSchema,
		})
		if a.managerResources.configured() {
			adverts = append(adverts, device.ConnectorAdvert{
				Type:         TypeManagerResource,
				TypeVersion:  "1",
				ConfigSchema: managerResourceSchema,
			})
		}
	}
	return adverts
}

func (a *Applier) reportDegraded(ctx context.Context, push device.ConnectorPush, reason string) {
	a.lc.Warnf("edgex-device spec degraded: spec=%s rev=%d reason=%s", push.SpecID, push.Revision, reason)
	_ = a.reportStatus(device.ConnectorStatus{
		SpecID:   push.SpecID,
		State:    device.ConnectorStateDegraded,
		Revision: push.Revision,
		Reason:   reason,
	})
}
func (a *Applier) reportCreating(ctx context.Context, push device.ConnectorPush) {
	a.lc.Debugf("Reporting CREATING: spec=%s rev=%d", push.SpecID, push.Revision)
	_ = a.reportStatus(device.ConnectorStatus{
		SpecID:   push.SpecID,
		State:    device.ConnectorStateCreating,
		Revision: push.Revision,
	})
}

func (a *Applier) restoreConfig(specID string, sp appliedSpec) (SpecConfig, error) {
	if sp.connectorType == TypeEdgeInference {
		return a.inferenceDeviceConfig(device.ConnectorPush{
			SpecID:        specID,
			ConnectorType: TypeEdgeInference,
			DeviceID:      sp.deviceID,
			Config:        sp.config,
		})
	}
	var cfg SpecConfig
	err := json.Unmarshal(sp.config, &cfg)
	return cfg, err
}

// pushFor reconstructs the minimal ConnectorPush for a stored applied spec so
// status helpers (reportDegraded) can be used outside the original push
// context, e.g. during manifest reconciliation self-heal.
func pushFor(specID string, sp appliedSpec) device.ConnectorPush {
	return device.ConnectorPush{
		SpecID:        specID,
		Revision:      sp.revision,
		ConnectorType: sp.connectorType,
		DeviceID:      sp.deviceID,
		Config:        sp.config,
	}
}
