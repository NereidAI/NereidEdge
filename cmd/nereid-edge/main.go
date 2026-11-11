// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package main is the entry point for the NereidEdge application service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg"
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/labstack/echo/v4"

	"github.com/nereidai/nereidedge/internal/attributes"
	"github.com/nereidai/nereidedge/internal/binding"
	"github.com/nereidai/nereidedge/internal/command"
	"github.com/nereidai/nereidedge/internal/config"
	"github.com/nereidai/nereidedge/internal/connector"
	"github.com/nereidai/nereidedge/internal/corecmd"
	"github.com/nereidai/nereidedge/internal/gateway"
	"github.com/nereidai/nereidedge/internal/gatewayoperation"
	"github.com/nereidai/nereidedge/internal/healthmonitor"
	"github.com/nereidai/nereidedge/internal/inferencebridge"
	"github.com/nereidai/nereidedge/internal/onvifbind"
	"github.com/nereidai/nereidedge/internal/pipeline"
	"github.com/nereidai/nereidedge/internal/probe"
	"github.com/nereidai/nereidedge/internal/profile"
	"github.com/nereidai/nereidedge/internal/secrets"
	"github.com/nereidai/nereidedge/internal/settings"
	"github.com/nereidai/nereidedge/internal/shadow"

	"github.com/nereidai/nereidedge/internal/linkwatch"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

const (
	// serviceKey is the unique service name used by EdgeX registration.
	serviceKey = "nereid-edge"
)

// Version and GitSHA are injected by the Makefile/Dockerfile ldflags. The
// fallback is the rolling dev channel version; release builds overwrite it
// with the bare SemVer derived from the git tag (v0.0.1 -> 0.0.1).
var (
	Version = "0.0.0-dev"
	GitSHA  = "dev"
)

// myApp is a minimal wrapper to separate app service setup from main for clarity.
type myApp struct {
	service           interfaces.ApplicationService
	gwClient          *gateway.Client
	svcConfig         *config.ServiceConfig
	telemetry         *pipeline.TelemetryPublisher
	manager           *gateway.Manager
	mapper            *gateway.DeviceMapper
	resolver          *binding.Resolver
	applier           *connector.Applier
	profileFetcher    *profile.Fetcher
	profilePub        *profile.Publisher
	profileSyncer     *profile.Syncer
	attrFiller        *attributes.Filler
	settingsApplier   *settings.Applier
	shadowHandler     *shadow.Handler
	cmdHandler        *command.Handler
	probeHandler      *probe.Handler
	secretStore       *secrets.Store
	secretHandler     *secrets.Handler
	profileDownStore  *profile.DownlinkStore
	profileDown       *profile.DownlinkHandler
	opHandler         *gatewayoperation.Handler
	opOpts            gatewayoperation.Options
	healthMon         *healthmonitor.Monitor
	inferenceBridge   *inferencebridge.Bridge
	inferenceCore     *corecmd.Client
	inferencePresence *inferencePresence
	// inferenceHelloMu serializes presence-triggered re-hellos so NI
	// install/uninstall flaps cannot pile up overlapping negotiations.
	inferenceHelloMu sync.Mutex
	reconnectGate    *gateway.ReconnectGate
	linkWatch        *linkwatch.Watch
	gatewayDeviceID  string
}

// restartAgent asks the supervisor to restart the agent by raising a clean
// shutdown signal. Under `restart: unless-stopped` (or a systemd Restart=)
// policy the process comes straight back; the host is never touched.
func restartAgent() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
}

func (app *myApp) initProfile() {
	if app.service == nil {
		return
	}
	app.profileFetcher = profile.NewFetcher(app.service)
	app.profilePub = profile.NewPublisher()
	app.profileSyncer = profile.NewSyncer(
		app.profileFetcher,
		app.profilePub,
		func(ctx context.Context, profiles []device.EdgeProfile) error {
			return app.gwClient.PublishProfiles(ctx, profiles)
		},
	)
	app.attrFiller = attributes.New(
		app.gwClient.MQTT(),
		app.gwClient.Gateway(),
		app.resolver,
		app.service,
		app.manager.GetDevices,
		app.service.LoggingClient(),
	)
	app.shadowHandler = shadow.New(app.gwClient.MQTT(), app.service.LoggingClient(), app.gatewayShadowState)
}

// gatewayShadowState builds the gateway's real reported shadow state: the
// agent's own runtime facts (build, effective knobs, link counters, attached
// devices), never an echo of platform desired state. The negotiated protocol
// and enabled features are filled in by the handler from the hello ack.
func (app *myApp) gatewayShadowState() map[string]any {
	cfg := &app.svcConfig.NereidEdge
	hc := cfg.HealthCheck
	intervalS := hc.IntervalS
	if intervalS <= 0 {
		intervalS = 30
	}

	link := map[string]any{
		"enabled":     app.linkWatch != nil,
		"up":          false,
		"down_events": int64(0),
		"unplanned":   int64(0),
		"flap_events": int64(0),
	}
	if app.linkWatch != nil {
		s := app.linkWatch.Snapshot()
		link["up"] = s.Up
		link["down_events"] = s.DownEvents
		link["unplanned"] = s.Unplanned
		link["flap_events"] = s.FlapEvents
	}

	state := map[string]any{
		"agent_version": gateway.AgentVersion,
		"settings": map[string]any{
			"log_level":        app.service.LoggingClient().LogLevel(),
			"forward_unmapped": cfg.ForwardUnmapped,
			"mqtt_trace":       cfg.MQTTTrace,
		},
		"health": map[string]any{
			"enabled":    hc.Enabled,
			"interval_s": intervalS,
		},
		"link": link,
		"devices": map[string]any{
			"attached": len(app.manager.GetDevices()),
		},
	}
	if app.inferenceBridge != nil && app.inferencePlaneReady() {
		runtime := app.inferenceBridge.Status()
		// The DS aggregate includes hand-built/orphan Job devices; the
		// gateway shadow must report the count of live cloud bindings only.
		if app.applier != nil {
			runtime.ActiveBindings = app.applier.ActiveInferenceBindings()
		}
		runtimeState := runtime.State
		if app.applier != nil && app.applier.DegradedInferenceBindings() > 0 && runtimeState == "ONLINE" {
			runtimeState = "DEGRADED"
		}
		block := map[string]any{
			"state":            runtimeState,
			"models_ready":     runtime.ModelsReady,
			"models_total":     runtime.ModelsTotal,
			"active_bindings":  runtime.ActiveBindings,
			"checked_at":       runtime.CheckedAt.Format(time.RFC3339),
			"probe_interval_s": app.inferenceBridge.IntervalS(),
		}
		if endpoint := app.svcConfig.NereidEdge.Inference.RuntimeEndpoint; endpoint != "" {
			block["endpoint"] = endpoint
		}
		if runtime.Protocol != "" {
			block["protocol"] = runtime.Protocol
		}
		// latency_ms is the manager-device read RTT; NI exposes no runtime
		// probe resource in this slice, so omit it until a real probe exists.
		if runtime.ReadLatencyMS > 0 {
			block["latency_ms"] = runtime.ReadLatencyMS
		}
		state["inference"] = block
	}
	return state
}

// main starts the NereidEdge application service.
func main() {
	app := &myApp{}
	code := app.createAndRunAppService(serviceKey, pkg.NewAppService)
	os.Exit(code)
}

// createAndRunAppService initializes and runs the EdgeX Application Service.
func (app *myApp) createAndRunAppService(
	serviceKey string,
	newServiceFactory func(string) (interfaces.ApplicationService, bool),
) int {
	var ok bool
	app.service, ok = newServiceFactory(serviceKey)
	if !ok {
		return -1
	}

	lc := app.service.LoggingClient()
	lc.Infof("NereidEdge app service initializing (version=%s git_sha=%s)", Version, GitSHA)

	// Load custom configuration
	app.svcConfig = &config.ServiceConfig{}
	if err := app.service.LoadCustomConfig(app.svcConfig, "NereidEdge"); err != nil {
		lc.Errorf("failed load custom configuration: %s", err.Error())
		return -1
	}

	// Edge Secret delivery (ADR-0034) must fail fast on a bad key or store
	// directory before connecting, so a misconfigured encryption key never
	// leaves the gateway silently dropping platform secrets.
	if app.svcConfig.NereidEdge.Secrets.Enabled {
		key, ephemeral, kerr := secrets.LoadKey(app.svcConfig.NereidEdge.Secrets.KeyFile)
		if kerr != nil {
			lc.Errorf("secret store key: %s", kerr.Error())
			return -1
		}
		codec, cerr := secrets.NewCodec(key)
		if cerr != nil {
			lc.Errorf("secret store codec: %s", cerr.Error())
			return -1
		}
		store, serr := secrets.NewStore(app.svcConfig.NereidEdge.Secrets.StoreDir, codec, ephemeral)
		if serr != nil {
			lc.Errorf("secret store open: %s", serr.Error())
			return -1
		}
		if ephemeral {
			lc.Warn("secret store uses an ephemeral in-memory key; copies will not survive a restart")
		}
		app.secretStore = store
	}

	// ADR-0038 device-profile downlink opens its applied-revision ledger up
	// front for the same fail-fast reason: a bad data directory must not
	// leave the gateway advertising the feature and dropping every spec.
	if app.svcConfig.NereidEdge.ProfileDelivery.Enabled {
		downStore, derr := profile.NewDownlinkStore(app.svcConfig.NereidEdge.ProfileDelivery.StoreDir)
		if derr != nil {
			lc.Errorf("profile store open: %s", derr.Error())
			return -1
		}
		app.profileDownStore = downStore
	}

	// TLS materials for the NereidLink MQTT connection are read once up
	// front so a bad PEM path fails before the gateway connects with an
	// unverifiable or missing identity.
	rootPEM, clientCertPEM, clientKeyPEM, tlsErr := loadGatewayTLSMaterials(app.svcConfig.NereidEdge)
	if tlsErr != nil {
		lc.Errorf("gateway TLS materials: %s", tlsErr.Error())
		return -1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Binding resolution: static DeviceMap (escape hatch) + dynamic sources
	// (connector specs, discovery results).
	app.mapper = gateway.NewDeviceMapper(app.svcConfig.NereidEdge.DeviceMap)
	app.resolver = binding.NewResolver(app.mapper.StaticMap())
	app.manager = gateway.NewManager(app.mapper)

	// Connect to NereidLink MQTT. The OnReconnect hook re-drives hello and
	// replays presence/profiles because the broker forgets session state
	// after a restart while Paho silently restores the TCP connection. The
	// reconnect gate serializes the replay so rapid flaps cannot pile up
	// concurrent hello loops, and a fresh disconnect aborts one in flight.
	app.reconnectGate = gateway.NewReconnectGate()
	lwCfg := app.svcConfig.NereidEdge.LinkWatch
	if lwCfg.Enabled {
		app.linkWatch = linkwatch.New(linkwatch.Config{
			FlapWindow:    time.Duration(lwCfg.FlapWindowS) * time.Second,
			FlapThreshold: lwCfg.FlapThreshold,
			Logger:        lc,
			OnEpisode:     app.reportLinkEpisode,
		})
	}
	// Whole-gateway operation sources (ADR-0035).
	opCfg := app.svcConfig.NereidEdge.Operations
	app.opOpts = gatewayoperation.Options{
		LogFile:          opCfg.LogFile,
		IdentityCertFile: opCfg.IdentityCertFile,
		Services:         opCfg.Services,
		Restart:          restartAgent,
	}
	if app.svcConfig.NereidEdge.Inference.Enabled {
		// The closure is lazy: the bridge/applier are constructed later but
		// exist by the time NL invokes the operation after negotiation.
		app.opOpts.InferenceDiag = app.runInferenceDiag
		app.opOpts.CameraProfiles = app.runCameraProfiles
		// Runtime NI existence gate: inference is advertised only while the
		// singleton manager device is actually deployed on this node.
		app.inferencePresence = newInferencePresence(
			app.svcConfig.NereidEdge.Inference.ManagerDevice, app.service, lc)
	}

	app.gwClient = gateway.New(gateway.Config{
		BrokerURL:        app.svcConfig.NereidEdge.BrokerURL,
		CredentialID:     app.svcConfig.NereidEdge.CredentialID,
		CredentialSecret: app.svcConfig.NereidEdge.CredentialSecret,
		EdgeID:           app.svcConfig.NereidEdge.EdgeID,
		OnReconnect:      app.onGatewayReconnect,
		OnConnectionLost: func(_ error) {
			app.reconnectGate.Invalidate()
			if app.linkWatch != nil {
				app.linkWatch.ReportDown(linkwatch.ReasonTransportDown, false)
			}
		},
		// Per-packet paho traces are opt-in (MQTTTrace) because they are far
		// noisier than the service DEBUG level.
		MQTTDebug:          app.svcConfig.NereidEdge.MQTTTrace,
		RootCAPEM:          rootPEM,
		ClientCertPEM:      clientCertPEM,
		ClientKeyPEM:       clientKeyPEM,
		InsecureSkipVerify: app.svcConfig.NereidEdge.InsecureSkipVerify,
		EnableSecrets:      app.svcConfig.NereidEdge.Secrets.Enabled,
		SecretTypeAdverts:  append(onvifbind.SecretTypeAdverts(), secrets.SecretTypeAdverts()...),
		OperationAdverts:   gatewayoperation.Adverts(app.opOpts),
		EnableProfiles:     app.svcConfig.NereidEdge.ProfileDelivery.Enabled,
		EnableInference:    app.svcConfig.NereidEdge.Inference.Enabled,
		InferenceBackends:  app.svcConfig.NereidEdge.Inference.Backends,
		InferenceAvailable: func() bool {
			return app.inferencePresence == nil || app.inferencePresence.Present()
		},
		Log: lc,
	})

	if err := app.gwClient.Connect(ctx); err != nil {
		lc.Errorf("gateway connect failed: %s", err.Error())
		return -1
	}
	defer func() {
		// Planned goodbye (ADR-0026): publish before disconnect so NL sees an
		// intentional stop instead of waiting ~90s for the lease to expire.
		// linkwatch swallows the transport-down that the local disconnect
		// triggers, keeping the break out of unplanned episode accounting.
		if app.linkWatch != nil {
			app.linkWatch.BeginPlannedShutdown()
		}
		if app.manager != nil && app.gwClient != nil && app.gwClient.Gateway() != nil {
			if err := app.gwClient.Gateway().Goodbye(context.Background(),
				device.Goodbye{Reason: "app shutdown", Planned: true}, 1); err != nil {
				lc.Errorf("planned goodbye publish failed: %s", err.Error())
			}
			app.manager.DisconnectAll(context.Background(), app.gwClient.Gateway())
		}
		app.gwClient.Disconnect()
	}()

	lc.Infof("Connected to NereidLink MQTT at %s", app.svcConfig.NereidEdge.BrokerURL)

	// Profile plumbing and the health monitor MUST be constructed before the
	// connector applier subscribes: spec pushes can arrive immediately after
	// the subscription (initial hello redrive, or an automatic-reconnect
	// goroutine that completes a fresh hello before startup finishes here),
	// and onSpecAttached dereferences the monitor. Constructing it later left
	// a window where an early spec nil-pointer panicked the process.
	app.initProfile()

	hcCfg := app.svcConfig.NereidEdge.HealthCheck
	// The monitor always exists: per-instance health_check specs drive their
	// own probe loops regardless of the global switch; the global switch only
	// controls the legacy hc==nil regime.
	app.healthMon = healthmonitor.New(healthmonitor.Config{
		Core:             corecmd.New(app.svcConfig.NereidEdge.CoreCommandURL, 0),
		LookupProfile:    app.profileFetcher.GetByName,
		Connect:          app.connectPresence,
		Disconnect:       app.disconnectPresence,
		Notify:           app.manager.NotifyManifestChange,
		GlobalEnabled:    hcCfg.Enabled,
		Interval:         time.Duration(hcCfg.IntervalS) * time.Second,
		FailureThreshold: hcCfg.FailureThreshold,
		ProbeTimeout:     time.Duration(hcCfg.ProbeTimeoutS) * time.Second,
		Logger:           lc,
	})
	go app.healthMon.Start(ctx)
	lc.Infof("Health monitor ready: global_probing=%v interval=%ds threshold=%d",
		hcCfg.Enabled, hcCfg.IntervalS, hcCfg.FailureThreshold)

	inferenceCore := app.initInferenceBridge(ctx)

	// Downlink subscriptions MUST be registered before hello: the server
	// re-sends full desired connector state after every successful hello.
	app.applier = connector.NewApplier(
		app.service, app.gwClient.Gateway(), app.resolver,
		app.svcConfig.NereidEdge.ConnectorObserve,
		app.onSpecAttached, app.onSpecDetached,
	)
	if app.inferenceBridge != nil {
		bindingPoll := time.Duration(app.svcConfig.NereidEdge.Inference.BindingPollS) * time.Second
		app.applier.SetInferenceCredentials(newInferenceCredentialManager(app, lc))
		app.applier.ConfigureInference(
			app.svcConfig.NereidEdge.Inference.DeviceService,
			"nereid-inference-detection",
			inferenceCore,
			bindingPoll,
			app.manager.InferenceNegotiated,
		)
		if err := app.applier.ConfigureManagerResources(
			inferenceCore,
			app.svcConfig.NereidEdge.Inference.ManagerDevice,
			app.svcConfig.NereidEdge.Inference.ManagerStoreDir,
			bindingPoll,
		); err != nil {
			lc.Errorf("manager resource store open failed: %s", err.Error())
			return -1
		}
		app.applier.SetInferencePresence(func() bool {
			return app.inferencePresence == nil || app.inferencePresence.Present()
		})
		go app.applier.StartInferenceStatusPoller(ctx)
	}
	if err := app.applier.Start(ctx); err != nil {
		lc.Errorf("subscribe connector specs failed: %s", err.Error())
		return -1
	}
	if app.attrFiller != nil {
		if err := app.attrFiller.Start(ctx, 1); err != nil {
			lc.Errorf("subscribe attribute pushes failed: %s", err.Error())
			return -1
		}
	}
	if app.shadowHandler != nil {
		if err := app.shadowHandler.Start(ctx, 1); err != nil {
			lc.Errorf("subscribe gateway shadow failed: %s", err.Error())
			return -1
		}
	}
	app.manager.SetConnectorProvider(app.applier.ReconcileAndReport)

	// Edge Secret delivery subscribes before hello: NereidLink pushes the
	// full desired set immediately after negotiating the "secrets" feature,
	// so the subscription must already be active to receive the first set.
	if app.secretStore != nil {
		sc := app.svcConfig.NereidEdge.Secrets
		onvifBinder := onvifbind.New(onvifbind.Config{
			DeviceServiceURL: sc.OnvifServiceURL,
			KeeperURL:        sc.KeeperURL,
			KVSBase:          sc.OnvifKVSBase,
			Devices: metadataDeviceReader{
				service: app.service,
			},
			// Selectors may carry an NL sub-device UUID; resolve it to the
			// EdgeX device name before the metadata MAC lookup.
			LookupName: func(ref string) (string, bool) {
				return app.resolver.Name(ref)
			},
			Logger: lc,
		})
		app.secretHandler = secrets.New(secrets.Config{
			Gateway: app.gwClient.Gateway(),
			Store:   app.secretStore,
			// Any onvif-basic delivery/removal re-runs the stream-credential
			// reconciler so a secret landing after a stream binding exists
			// retroactively delivers it to NI and rebuilds the job property.
			Binder: notifyingSecretBinder{
				inner:  onvifBinder,
				notify: app.applier.NotifyInferenceCredentials,
			},
			Logger: lc,
		})
		if err := app.secretHandler.Start(ctx, 1); err != nil {
			lc.Errorf("subscribe gateway secrets failed: %s", err.Error())
			return -1
		}
		lc.Infof("secret delivery ready: store_dir=%s key_file=%q",
			app.svcConfig.NereidEdge.Secrets.StoreDir, app.svcConfig.NereidEdge.Secrets.KeyFile)
	}

	// ADR-0038 device-profile downlink subscribes before hello, exactly like
	// secret delivery: NL re-sends the full desired set right after the
	// profiles feature is negotiated. EdgeX documents land in local
	// core-metadata; neutral documents land in the durable ledger.
	if app.profileDownStore != nil {
		app.profileDown = profile.NewDownlinkHandler(profile.DownlinkConfig{
			Gateway: app.gwClient.Gateway(),
			Store:   app.profileDownStore,
			Appliers: []profile.ProfileApplier{
				profile.NewEdgeXApplier(app.service.DeviceProfileClient()),
			},
			Logger: lc,
		})
		if err := app.profileDown.Start(ctx, 1); err != nil {
			lc.Errorf("subscribe profile specs failed: %s", err.Error())
			return -1
		}
		lc.Infof("profile downlink ready: store_dir=%s",
			app.svcConfig.NereidEdge.ProfileDelivery.StoreDir)
	}

	// Whole-gateway operations (ADR-0035): subscribe before hello so NL can
	// invoke as soon as the advertised catalog is registered. The subscription
	// is event-driven and re-issued after every reconnect.
	app.opHandler = gatewayoperation.New(gatewayoperation.Config{
		Gateway: app.gwClient.Gateway(),
		Logger:  lc,
		Options: app.opOpts,
	})
	if err := app.opHandler.Start(ctx, 1); err != nil {
		lc.Errorf("subscribe gateway operations failed: %s", err.Error())
		return -1
	}

	// Probe NI existence before the first hello so a node without an
	// inference device service never advertises the capability; the probe
	// is definitive for 404 and inconclusive on metadata outages (which
	// keep the optimistic default).
	if app.inferencePresence != nil {
		app.inferencePresence.Probe(ctx)
	}

	var adverts []device.ConnectorAdvert
	if app.applier != nil {
		adverts = app.applier.ConnectorAdverts()
	}
	ack, err := app.gwClient.Negotiate(ctx, adverts...)
	if err != nil {
		lc.Errorf("gateway hello negotiation failed: %s", err.Error())
		return -1
	}
	lc.Infof("Hello negotiated: session=%s proto=%s features=%v", ack.SessionID, ack.Selected, ack.Features)
	// Profile downlink is gated on the negotiated intersection: a platform
	// that dropped the optional feature sends no spec frames anyway, but
	// the handler keeps an explicit gate on the ack.
	if app.profileDown != nil {
		app.profileDown.SetNegotiation(
			slices.Contains(ack.Features, device.FeatureProfiles),
			ack.ProfileFormats)
	}
	app.manager.SetAck(ack)
	app.armInferenceAfterHello(ctx)
	if app.inferencePresence != nil {
		app.inferencePresence.SetOnAppear(app.redriveHelloForInference)
		go app.inferencePresence.Start(ctx)
	}
	if ack.GatewayID != "" {
		app.gatewayDeviceID = ack.GatewayID
	} else {
		lc.Warnf("hello ack carries no gateway device id; link-health episode reports will be disabled")
	}
	app.subscribeLinkHealthAcks()

	// Recover platform-managed attributes missed while offline: pull the
	// gateway's own set and one per attached sub-device now that this session
	// is negotiated.
	if app.attrFiller != nil {
		app.attrFiller.RequestAttributes()
	}

	if app.linkWatch != nil {
		app.linkWatch.ReportUp()
	}

	// Publish the gateway's real reported state for this fresh session, after
	// ReportUp so the link counters reflect the now-live transport.
	if app.shadowHandler != nil {
		app.shadowHandler.SetIdentity(ack.Selected, ack.Features)
		app.shadowHandler.Report(true)
	}

	// Sub-device command downlinks ride a dedicated subscription. The SDK
	// runs a clean session, so the reconnect handler re-issues it after each
	// fresh hello.
	app.cmdHandler = command.New(command.Config{
		Gateway:        app.gwClient.Gateway(),
		Resolver:       app.resolver,
		CoreCommandURL: app.svcConfig.NereidEdge.CoreCommandURL,
		Logger:         lc,
	})
	if err := app.cmdHandler.Start(ctx, 1); err != nil {
		lc.Errorf("subscribe gateway commands failed: %s", err.Error())
		return -1
	}

	// On-demand probes ride their own subscription; like commands, the
	// reconnect handler re-issues it after each fresh hello.
	app.probeHandler = probe.New(probe.Config{
		Gateway:        app.gwClient.Gateway(),
		Resolver:       app.resolver,
		CoreCommandURL: app.svcConfig.NereidEdge.CoreCommandURL,
		Logger:         lc,
	})
	if err := app.probeHandler.Start(ctx, 1); err != nil {
		lc.Errorf("subscribe gateway probes failed: %s", err.Error())
		return -1
	}

	go app.runInitialProfileSync()

	// Event-driven profile propagation: subscribe to core-metadata device
	// profile lifecycle events on the EdgeX message bus so new or changed
	// profiles reach NereidLink immediately instead of waiting for the
	// periodic full scan, which stays as the fallback when the bus is
	// unreachable.
	if pw := app.svcConfig.NereidEdge.ProfileWatch; pw.Enabled {
		watcher, werr := profile.NewWatcher(profile.WatchConfig{
			Type:      pw.Type,
			Protocol:  pw.Protocol,
			Host:      pw.Host,
			Port:      pw.Port,
			BaseTopic: pw.BaseTopic,
		}, app.profileSyncer, lc)
		if werr != nil {
			lc.Warnf("profile watcher disabled: %s", werr.Error())
		} else {
			go watcher.Start(ctx)
			lc.Infof("EdgeX profile watcher enabled")
		}
	}

	// Announce statically mapped devices. They carry no health_check (legacy
	// regime): probed when the global switch is on, announced directly when
	// it is off.
	if static := app.mapper.StaticMap(); len(static) > 0 {
		for edgexName, uuid := range static {
			app.healthMon.OnAttach(edgexName, uuid, "", "", false, nil)
		}
	}

	app.telemetry = &pipeline.TelemetryPublisher{
		Gateway:         app.gwClient.Gateway(),
		Resolver:        app.resolver,
		Manager:         app.manager,
		ForwardUnmapped: app.svcConfig.NereidEdge.ForwardUnmapped,
		UplinkFilters:   app.applier.UplinkFilters(),
	}

	// Wire the gateway's well-known SHARED_SCOPE attributes (currently just
	// log.level: the process logger level) to the running knobs. The applier
	// only fires when a value differs from the startup default, so it is safe
	// to reconcile on every attribute change; re-emitting the cache now applies
	// anything pulled during the post-hello sweep before the applier existed.
	app.settingsApplier = settings.New(settings.Knobs{
		SetLogLevel: lc.SetLogLevel,
		Logger:      lc,
	}, settings.Defaults{
		LogLevel: lc.LogLevel(),
	})
	if app.attrFiller != nil {
		app.attrFiller.SetSelfObserver(func(self map[string]map[string]any) {
			app.settingsApplier.Apply(self)
			// A gateway attribute may have changed an effective knob (e.g.
			// log.level), so re-publish the observed state.
			if app.shadowHandler != nil {
				app.shadowHandler.Report(false)
			}
		})
		app.attrFiller.ReapplySelf()
	}

	// The export stage fans EdgeX events out to node-level connectors first
	// and always returns (true, data), so it can never stall or short-circuit
	// the telemetry stage that follows.
	if err := app.service.SetDefaultFunctionsPipeline(app.applier.Exports().ExportStage(), app.telemetry.Publish); err != nil {
		lc.Errorf("SetDefaultFunctionsPipeline failed: %s", err.Error())
		return -1
	}

	if err := app.service.AddCustomRoute("/api/v4/ping", interfaces.Unauthenticated, func(c echo.Context) error {
		c.Response().WriteHeader(http.StatusOK)
		_, _ = c.Response().Write([]byte("pong"))
		return nil
	}, http.MethodGet); err != nil {
		lc.Errorf("AddCustomRoute failed: %s", err.Error())
		return -1
	}

	go app.heartbeatLoop(ctx, lc, ack.Limits.HeartbeatS)
	go app.manager.StartManifestLoop(ctx, app.gwClient, 5*time.Minute, lc)
	go app.profileSyncLoop(ctx, 5*time.Minute)

	if err := app.service.Run(); err != nil {
		lc.Errorf("Run failed: %s", err.Error())
		return -1
	}

	return 0
}

// initInferenceBridge starts the NereidInference manager-device poller and
// connects its cached inventory to the manifest models provider. The bridge
// reads nothing before the hello ack accepts "inference", and BuildManifest
// still gates the segment on negotiation, so old-platform sessions behave
// exactly like firmware without the feature.
func (app *myApp) initInferenceBridge(ctx context.Context) *corecmd.Client {
	cfg := app.svcConfig.NereidEdge.Inference
	if !cfg.Enabled {
		return nil
	}
	core := corecmd.New(app.svcConfig.NereidEdge.CoreCommandURL, 0)
	interval := time.Duration(cfg.InventoryPollS) * time.Second
	if cfg.InventoryPollS <= 0 {
		interval = 30 * time.Second
	}
	shadowInterval := time.Duration(cfg.ShadowPollS) * time.Second
	if cfg.ShadowPollS <= 0 {
		shadowInterval = 30 * time.Second
	}
	app.inferenceCore = core
	app.inferenceBridge = inferencebridge.New(inferencebridge.Config{
		Core:          core,
		ManagerDevice: cfg.ManagerDevice,
		Resource:      cfg.ModelsResource,
		Interval:      interval,
		Negotiated:    app.inferencePlaneReady,
		OnChange:      app.onInferenceInventoryChange,
		Logger:        app.service.LoggingClient(),
	})
	app.manager.SetModelProvider(func(context.Context) []device.ManifestModel {
		// Omit on a read failure OR a missing NI plane; an explicit [] only
		// reaches the wire when the manager read itself returned zero rows.
		if !app.inferencePlaneReady() {
			return nil
		}
		models, ok := app.inferenceBridge.Models()
		if !ok {
			return nil
		}
		// The DS counts every Job device it hosts (hand-built test jobs,
		// orphans). active_bindings must reflect only the live edge-inference
		// specs this NE still owns.
		if app.applier != nil {
			models = inferencebridge.ApplyBindingCounts(models, app.applier.InferenceBindingCounts())
		}
		return models
	})
	go app.inferenceBridge.Start(ctx)
	go app.inferenceShadowLoop(ctx, shadowInterval)
	return core
}

// inferencePlaneReady is the combined gate for all inference uplinks: the
// session must have negotiated the feature AND the deployed NI plane must
// currently exist. Before negotiation/without NI the edge behaves byte
// identically to firmware without the capability.
func (app *myApp) inferencePlaneReady() bool {
	return app.manager != nil && app.manager.InferenceNegotiated() &&
		(app.inferencePresence == nil || app.inferencePresence.Present())
}

// armInferenceAfterHello forces an immediate inventory read once the ack
// accepts the inference feature: the bridge goroutine started pre-hello but
// skipped its first tick while un-negotiated, so without this the models
// segment would wait up to InventoryPollS for its first data point.
func (app *myApp) armInferenceAfterHello(ctx context.Context) {
	if app.inferenceBridge == nil || !app.inferencePlaneReady() {
		return
	}
	go app.inferenceBridge.Refresh(ctx)
}

// onInferenceInventoryChange pushes a debounced manifest when the inventory
// changes and refreshes the shadow when runtime reachability changes.
func (app *myApp) onInferenceInventoryChange() {
	app.manager.NotifyManifestChange()
	if app.manager.InferenceNegotiated() && app.shadowHandler != nil {
		app.shadowHandler.Report(false)
	}
}

func (app *myApp) inferenceShadowLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Keep checked_at fresh inside NL's 3 x interval silence
			// watchdog and re-poll the manager device independently of the
			// inventory interval.
			if !app.inferencePlaneReady() {
				continue
			}
			app.inferenceBridge.Refresh(ctx)
			if app.shadowHandler != nil {
				app.shadowHandler.Report(false)
			}
		}
	}
}

// inferenceDiagParams is the §15.7 inference.diag request document.
type inferenceDiagParams struct {
	ModelRef struct {
		ID      string `json:"id"`
		Version string `json:"version"`
	} `json:"model_ref"`
	// Source optionally narrows the diagnostic to one bound acquisition
	// source: the §15.3 source.device camera sub-device NL UUID.
	Source string `json:"source"`
}

// inferenceDiagResult is the §15.7 diagnostic response. It carries no
// addresses or vendor detail beyond the transport-safe reason vocabulary.
type inferenceDiagResult struct {
	Reachable   bool    `json:"reachable"`
	ModelReady  bool    `json:"model_ready"`
	InferOK     bool    `json:"infer_ok"`
	LatencyMS   float64 `json:"latency_ms,omitempty"`
	FrameSource string  `json:"frame_source"`
	Reason      string  `json:"reason,omitempty"`
}

// runInferenceDiag distinguishes runtime/model/source failures for one bound
// inference path without adding any MQTT topic: all evidence comes from
// ordinary core-command reads and one Trigger write.
func (app *myApp) runInferenceDiag(ctx context.Context, in map[string]any) (json.RawMessage, error) {
	if !app.inferencePlaneReady() {
		return nil, fmt.Errorf("inference feature not negotiated in this session")
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("invalid inference.diag params: %w", err)
	}
	var params inferenceDiagParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("invalid inference.diag params: %w", err)
	}
	if params.ModelRef.ID == "" || params.ModelRef.Version == "" {
		return nil, fmt.Errorf("model_ref.id and model_ref.version are required")
	}
	result := inferenceDiagResult{FrameSource: "snapshot_jpeg"}

	// Force a live manager-device read so the verdict reflects this instant
	// rather than the 30s cache; reachability is judged solely on that read.
	app.inferenceBridge.Refresh(ctx)
	runtime := app.inferenceBridge.Status()
	result.Reachable = runtime.Available
	if !result.Reachable {
		result.Reason = "runtime-unreachable"
		return json.Marshal(result)
	}

	_, modelReady := app.inferenceBridge.LookupModel(params.ModelRef.ID, params.ModelRef.Version)
	result.ModelReady = modelReady
	if !modelReady {
		result.Reason = "model-not-ready"
		return json.Marshal(result)
	}

	edgexName, mode, ok := app.applier.FindInferenceBindingWithMode(params.ModelRef.ID, params.ModelRef.Version, params.Source)
	if !ok {
		// Inventory is healthy but no edge-inference binding serves this
		// model/source combination.
		result.Reason = "config-invalid"
		return json.Marshal(result)
	}
	if mode == "stream" {
		result.FrameSource = "rtsp"
	}

	// Best-effort synchronous path: poke Trigger once and briefly watch the
	// job state. NI jobs run asynchronously, so success means a healthy read
	// back within the window; otherwise the last job reason is translated.
	result.InferOK, result.LatencyMS, result.Reason = app.probeInferenceOnce(ctx, edgexName)
	return json.Marshal(result)
}

// probeInferenceOnce writes Trigger and polls the job for at most ~8s for a
// healthy inference verdict.
func (app *myApp) probeInferenceOnce(ctx context.Context, edgexName string) (bool, float64, string) {
	if _, status, err := app.inferenceCore.Set(ctx, edgexName, "Trigger", map[string]any{"Trigger": "diag"}); err != nil || status != http.StatusOK {
		// NI answers a synchronous Trigger with non-200 when the attempt
		// itself failed (e.g. 503 after the stream frame-acquisition
		// timeout); the authoritative §15.6 reason has already landed on
		// LastError. Prefer it over a blanket runtime-unreachable so diag
		// keeps its §15.7 fault-domain distinction (source vs runtime).
		// No LastError means we could not get any DS-side evidence, which
		// is the genuine unreachable case.
		if raw := readJobReason(ctx, app.inferenceCore, edgexName); raw != "" {
			return false, 0, connector.SanitizeReason(raw)
		}
		return false, 0, "runtime-unreachable"
	}
	deadline := time.Now().Add(8 * time.Second)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		stateEvent, status, err := app.inferenceCore.Read(ctx, edgexName, "State",
			"ds-pushevent=false&ds-returnevent=true")
		if err != nil || status != http.StatusOK {
			return false, 0, "runtime-unreachable"
		}
		readings := corecmd.ReadingsToMap(stateEvent.Event.Readings)
		state, _ := readings["State"].(string)
		reason := readJobReason(ctx, app.inferenceCore, edgexName)
		switch state {
		case device.ConnectorStateFailed:
			if reason == "" {
				reason = "runtime-error"
			}
			return false, 0, connector.SanitizeReason(reason)
		case device.ConnectorStateApplied:
			if reason == "" {
				if latency := readJobLatency(ctx, app.inferenceCore, edgexName); latency > 0 {
					return true, latency, ""
				}
			}
		}
		if !time.Now().Before(deadline) {
			if reason != "" {
				return false, 0, connector.SanitizeReason(reason)
			}
			// Triggered but no fresh result arrived in the synchronous window.
			return false, 0, "source-no-frame"
		}
		select {
		case <-ctx.Done():
			return false, 0, "inference-timeout"
		case <-ticker.C:
		}
	}
}

func readJobReason(ctx context.Context, core *corecmd.Client, edgexName string) string {
	event, status, err := core.Read(ctx, edgexName, "LastError", "ds-pushevent=false&ds-returnevent=true")
	if err != nil || status != http.StatusOK {
		return ""
	}
	reason, _ := corecmd.ReadingsToMap(event.Event.Readings)["LastError"].(string)
	return reason
}

func readJobLatency(ctx context.Context, core *corecmd.Client, edgexName string) float64 {
	event, status, err := core.Read(ctx, edgexName, "InferLatencyMs", "ds-pushevent=false&ds-returnevent=true")
	if err != nil || status != http.StatusOK {
		return 0
	}
	latency, _ := corecmd.ReadingsToMap(event.Event.Readings)["InferLatencyMs"].(float64)
	return latency
}

// onSpecAttached announces a spec-driven device presence to NereidLink and
// publishes its EdgeX profile the first time this gateway sees it. Presence
// is announced by the health monitor: after a reachability probe for probed
// devices, immediately otherwise.
func (app *myApp) onSpecAttached(edgexName, nlUUID, profileName, probeResource string, local bool, hc *device.ConnectorHealthCheck) {
	app.healthMon.OnAttach(edgexName, nlUUID, profileName, probeResource, local, hc)

	// Make a profile referenced by a newly applied spec visible immediately,
	// even when the periodic full scan has not run yet. Best-effort, never
	// block the spec callback.
	if profileName != "" {
		go func() {
			pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer pcancel()
			if err := app.profileSyncer.SyncOne(pctx, profileName); err != nil {
				app.service.LoggingClient().Warnf("profile publish failed for %s: %v", profileName, err)
			}
		}()
	}

	// Reconcile the manifest immediately (debounced inside the manager):
	// without this, a health_check-only revision or an apply to an already
	// online device would leave NL showing Stale until the periodic report.
	app.manager.NotifyManifestChange()
}

// onSpecDetached withdraws presence of a removed spec device.
func (app *myApp) onSpecDetached(edgexName string, nlUUID string) {
	app.healthMon.OnDetach(edgexName)
	// Drop the instance from the observed manifest right away instead of
	// waiting for the periodic report.
	app.manager.NotifyManifestChange()
}

// connectPresence is the health monitor hook that tracks a device and
// announces its attachment.
func (app *myApp) connectPresence(ctx context.Context, nlUUID string) error {
	app.manager.AddDevice(nlUUID)
	if app.shadowHandler != nil {
		app.shadowHandler.Report(false)
	}
	gw := app.gwClient.Gateway()
	if gw == nil {
		return nil
	}
	if err := gw.ConnectDevices(ctx, []string{nlUUID}, 1); err != nil {
		return err
	}
	// The platform pushes only changed attribute keys, so a sub-device that
	// attaches mid-session needs an explicit pull to seed its cache. Pull only
	// after the attachment is announced.
	if app.attrFiller != nil {
		app.attrFiller.RequestDevice(nlUUID)
	}
	return nil
}

// disconnectPresence is the health monitor hook that withdraws a device and
// removes it from the manifest device set.
func (app *myApp) disconnectPresence(ctx context.Context, nlUUID string) error {
	gw := app.gwClient.Gateway()
	var err error
	if gw != nil {
		err = gw.DisconnectDevices(ctx, []string{nlUUID}, 1)
	}
	app.manager.RemoveDevice(nlUUID)
	if app.shadowHandler != nil {
		app.shadowHandler.Report(false)
	}
	return err
}

func (app *myApp) heartbeatLoop(ctx context.Context, lc logger.LoggingClient, intervalSec int) {
	if intervalSec <= 0 {
		intervalSec = 30
	}
	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()
	seq := int64(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			seq++
			gw := app.gwClient.Gateway()
			if gw == nil {
				continue
			}
			_ = gw.Heartbeat(ctx, device.Heartbeat{Seq: seq, ClientTime: time.Now().UnixMilli()}, 1)
		}
	}
}

// onGatewayReconnect is invoked by the MQTT client after an automatic
// reconnect (e.g. a broker restart). The new broker session is
// unnegotiated and has forgotten all edge-reported state, so we reset local
// de-dupe caches, re-drive hello (which makes NL replay desired connector
// state), replay presence, profiles and manifest. The reconnect gate flattens
// bursts of OnConnect callbacks into a single in-flight replay and aborts it
// if the transport drops again mid-way.
// subscribeLinkHealthAcks registers the v1/gateway/linkhealth/ack handler.
// The SDK uses a clean session (devicemqtt.PersistentSession defaults off),
// so the subscription is dropped on every transport reconnect and must be
// re-issued after each fresh hello.
func (app *myApp) subscribeLinkHealthAcks() {
	lc := app.service.LoggingClient()
	if app.gwClient == nil || app.gwClient.Gateway() == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := app.gwClient.Gateway().SubscribeLinkEpisodeAcks(ctx, 1,
		func(resp device.LinkEpisodeAck) {
			lc.Debugf("linkhealth ack: edge_id=%s down_at=%s status=%s",
				resp.EdgeID, resp.DownAt.Format(time.RFC3339), resp.Status)
		})
	if err != nil {
		lc.Errorf("subscribe link-health episode acks failed: %s", err.Error())
	}
}

// resubscribeDesiredState re-issues the downlink subscriptions NL replays full
// desired state on right after a successful hello: connector specs, attribute
// pushes and secret syncs. It must run BEFORE re-driving hello: the SDK uses a
// clean session, so a transport reconnect drops every subscription broker-side
// and paho does not restore them; re-issuing after hello would miss the
// post-hello redrive and leave the edge out of sync until the next restart.
func (app *myApp) resubscribeDesiredState(ctx context.Context) {
	lc := app.service.LoggingClient()
	if app.applier != nil {
		if err := app.applier.Resubscribe(ctx); err != nil {
			lc.Errorf("resubscribe connector specs failed: %s", err.Error())
		}
	}
	if app.attrFiller != nil {
		if err := app.attrFiller.Start(ctx, 1); err != nil {
			lc.Errorf("resubscribe attribute pushes failed: %s", err.Error())
		}
	}
	if app.shadowHandler != nil {
		if err := app.shadowHandler.Start(ctx, 1); err != nil {
			lc.Errorf("resubscribe gateway shadow failed: %s", err.Error())
		}
	}
	if app.secretHandler != nil {
		if err := app.secretHandler.Start(ctx, 1); err != nil {
			lc.Errorf("resubscribe gateway secrets failed: %s", err.Error())
		}
	}
	if app.profileDown != nil {
		if err := app.profileDown.Start(ctx, 1); err != nil {
			lc.Errorf("resubscribe profile specs failed: %s", err.Error())
		}
	}
}

// resubscribeEventDriven re-issues the user-triggered downlink subscriptions
// after a fresh hello. Unlike desired state they are not replayed by hello,
// but the clean-session reconnect drops them just the same, so without this a
// network blip would silently stop commands and probes until a restart.
func (app *myApp) resubscribeEventDriven(ctx context.Context) {
	lc := app.service.LoggingClient()
	app.subscribeLinkHealthAcks()
	if app.cmdHandler != nil {
		if err := app.cmdHandler.Start(ctx, 1); err != nil {
			lc.Errorf("resubscribe gateway commands failed: %s", err.Error())
		}
	}
	if app.probeHandler != nil {
		if err := app.probeHandler.Start(ctx, 1); err != nil {
			lc.Errorf("resubscribe gateway probes failed: %s", err.Error())
		}
	}
	if app.opHandler != nil {
		if err := app.opHandler.Start(ctx, 1); err != nil {
			lc.Errorf("resubscribe gateway operations failed: %s", err.Error())
		}
	}
}

// v1/gateway/linkhealth (ADR-0026). The platform validates edge_id against
// the authenticated gateway device id and answers with an ack; duplicate
// acks are tolerated (the server dedups on (gateway, down_at)).
func (app *myApp) reportLinkEpisode(ep linkwatch.Episode) {
	lc := app.service.LoggingClient()
	if app.gatewayDeviceID == "" {
		lc.Warnf("linkwatch: link episode closed but gateway device id unknown; cannot report down=%s up=%s",
			ep.Start.Format(time.RFC3339), ep.End.Format(time.RFC3339))
		return
	}
	if app.gwClient == nil || app.gwClient.Gateway() == nil {
		lc.Warnf("linkwatch: gateway not connected; skipping episode report down=%s up=%s",
			ep.Start.Format(time.RFC3339), ep.End.Format(time.RFC3339))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	report := device.LinkEpisodeReport{
		// EdgeID must equal the authenticated gateway device uuid; it is
		// echoed in the hello ack (ack.GatewayID) at negotiate time.
		EdgeID: app.gatewayDeviceID,
		DownAt: ep.Start.UTC(),
		UpAt:   ep.End.UTC(),
		Reason: string(linkwatch.ReasonTransportDown),
	}
	if err := app.gwClient.Gateway().ReportLinkEpisode(ctx, report, 1); err != nil {
		lc.Errorf("linkwatch: report link episode failed: %s", err.Error())
		return
	}
	lc.Infof("linkwatch: reported link episode down=%s up=%s edge_id=%s",
		ep.Start.Format(time.RFC3339), ep.End.Format(time.RFC3339), report.EdgeID)
}

func (app *myApp) onGatewayReconnect() {
	app.reconnectGate.Run(func(gen int64) {
		lc := app.service.LoggingClient()
		lc.Warn("gateway transport reconnected; re-driving hello and replaying state")

		// Clear de-dupe state before hello so the desired-state replay
		// following the new hello fully re-applies specs and re-reports
		// APPLIED instead of being skipped.
		if app.applier != nil {
			app.applier.Reset()
		}
		if app.profilePub != nil {
			app.profilePub.Reset()
		}

		// The clean-session reconnect dropped every broker-side subscription.
		// Re-issue the desired-state ones BEFORE re-hello so NL's post-hello
		// redrive (connector specs, secrets, attribute pushes) is not published
		// into a dead window.
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			app.resubscribeDesiredState(ctx)
			cancel()
		}

		var ack *device.HelloAck
		var err error
		for attempt := 0; attempt < 10; attempt++ {
			if app.reconnectGate.Stale(gen) {
				lc.Warn("connection dropped again during reconnect replay; aborting")
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			// Re-register connector schemas/capabilities just like the
			// initial hello, otherwise the broker forgets them and NL
			// validates new specs against a stale catalog.
			var adverts []device.ConnectorAdvert
			if app.applier != nil {
				adverts = app.applier.ConnectorAdverts()
			}
			ack, err = app.gwClient.Negotiate(ctx, adverts...)
			cancel()
			if err == nil {
				break
			}
			if app.reconnectGate.Stale(gen) {
				return
			}
			lc.Errorf("reconnect hello attempt %d failed: %s", attempt+1, err.Error())
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if err != nil {
			lc.Errorf("reconnect hello gave up: %s", err.Error())
			return
		}
		lc.Infof("Re-hello negotiated after reconnect: session=%s proto=%s", ack.SessionID, ack.Selected)
		app.manager.SetAck(ack)
		app.armInferenceAfterHello(context.Background())
		if app.profileDown != nil {
			app.profileDown.SetNegotiation(
				slices.Contains(ack.Features, device.FeatureProfiles),
				ack.ProfileFormats)
		}
		if ack.GatewayID != "" && app.gatewayDeviceID != ack.GatewayID {
			app.gatewayDeviceID = ack.GatewayID
			lc.Infof("gateway device id refreshed by re-hello: %s", ack.GatewayID)
		}
		// The SDK runs a clean session, so every reconnect drops these
		// subscriptions too; re-issue the user-triggered ones now that this
		// session is negotiated.
		{
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			app.resubscribeEventDriven(ctx)
			cancel()
		}
		// Re-pull the platform-managed attributes missed while offline now
		// that this session is negotiated (the pull sweep is detached from
		// this short-lived ctx).
		if app.attrFiller != nil {
			app.attrFiller.RequestAttributes()
		}
		if app.linkWatch != nil {
			// The re-negotiated session marks the transport break closed; an
			// unplanned episode (if the loss was not planned) is now counted,
			// logged and delivered to the uplink hook.
			app.linkWatch.ReportUp()
		}
		// Re-publish the gateway's real reported state for the new session so
		// the platform sees the refreshed link counters and feature set.
		if app.shadowHandler != nil {
			app.shadowHandler.SetIdentity(ack.Selected, ack.Features)
			app.shadowHandler.Report(true)
		}

		// Replay presence for every device we still track.
		if devs := app.manager.GetDevices(); len(devs) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := app.gwClient.Gateway().ConnectDevices(ctx, devs, 1); err != nil {
				lc.Errorf("reconnect presence replay failed: %s", err.Error())
			}
			cancel()
		}

		// Replay the profile inventory after device services have a moment.
		go app.runInitialProfileSync()

		// Push an immediate manifest so reconcile converges quickly.
		app.manager.NotifyManifestChange()
	})
}

// runInitialProfileSync waits briefly for device services to register with
// core-metadata, then publishes the full EdgeX profile inventory. Profiles
// are de-duplicated by content hash, so replaying the full set after hello
// only costs the core-metadata scan when nothing changed.
func (app *myApp) runInitialProfileSync() {
	lc := app.service.LoggingClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Give device services a short grace period to finish registration.
	select {
	case <-ctx.Done():
		return
	case <-time.After(3 * time.Second):
	}
	n, err := app.profileSyncer.SyncAll(ctx)
	if err != nil {
		lc.Warnf("initial profile sync failed: %v", err)
		return
	}
	lc.Infof("initial profile sync published %d EdgeX profile(s)", n)
}

// profileSyncLoop periodically enumerates core-metadata and reports newly
// registered or changed profiles. Failures are logged and retried on the
// next tick; the de-duplication cache guarantees no duplicate wire frames.
func (app *myApp) profileSyncLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	lc := app.service.LoggingClient()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			n, err := app.profileSyncer.SyncAll(sctx)
			cancel()
			if err != nil {
				lc.Warnf("periodic profile sync failed: %v", err)
				continue
			}
			if n > 0 {
				lc.Infof("periodic profile sync published %d new/changed profile(s)", n)
			}
		}
	}
}

// metadataDeviceReader adapts the app service's core-metadata DeviceClient to
// the onvif binder's DeviceReader, dropping the envelope DTO.
type metadataDeviceReader struct {
	service interfaces.ApplicationService
}

// DeviceByName fetches one EdgeX device from core-metadata by name.
func (r metadataDeviceReader) DeviceByName(ctx context.Context, name string) (dtos.Device, error) {
	resp, err := r.service.DeviceClient().DeviceByName(ctx, name)
	if err != nil {
		return dtos.Device{}, err
	}
	return resp.Device, nil
}
