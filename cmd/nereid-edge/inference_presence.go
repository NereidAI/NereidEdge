// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"sync"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	edgexErrors "github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// presenceProbeInterval matches the inventory poll cadence; the rising edge
// is what matters, a 30s NI-install detection delay is harmless.
const presenceProbeInterval = 30 * time.Second

const presenceProbeTimeout = 5 * time.Second

// inferencePresence answers one question the static Inference.Enabled config
// cannot: whether the NereidInference plane (singleton manager device) is
// actually deployed on this node. A gateway without NI must not advertise
// the inference feature or self-register edge-inference; otherwise NL opens
// binding entry points that can only fail, and the manager poll spams 404s.
//
// Semantics:
//   - manager device found (200): present;
//   - definitive 404 (KindEntityDoesNotExist): absent;
//   - any other metadata error: previous state is retained so a transient
//     core-metadata outage neither withdraws a live plane nor advertises a
//     freshly installed one on inconclusive evidence. The initial state is
//     optimistic (present) to match the pre-gating firmware behavior.
//
// The falling edge does not tear down a negotiated session: the bridge's
// failed-read rules then apply (omit models segment, keep cached inventory).
// De-advertisement happens on the next (re)hello; the rising edge triggers
// one immediately.
type inferencePresence struct {
	deviceName string
	service    interfaces.ApplicationService
	lc         logger.LoggingClient

	mu      sync.Mutex
	present bool

	onAppear func()
}

func newInferencePresence(deviceName string, service interfaces.ApplicationService, lc logger.LoggingClient) *inferencePresence {
	if deviceName == "" {
		deviceName = "nereid-inference"
	}
	return &inferencePresence{
		deviceName: deviceName,
		service:    service,
		lc:         lc,
		present:    true,
	}
}

// Present reports the latest probe result; safe for concurrent hello build.
func (p *inferencePresence) Present() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.present
}

// SetOnAppear registers the rising-edge callback (re-hello registration).
func (p *inferencePresence) SetOnAppear(fn func()) {
	p.mu.Lock()
	p.onAppear = fn
	p.mu.Unlock()
}

// Probe performs one core-metadata existence check and applies the rules
// above. It returns the resulting presence state.
func (p *inferencePresence) Probe(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, presenceProbeTimeout)
	defer cancel()
	_, err := p.service.DeviceClient().DeviceByName(probeCtx, p.deviceName)

	next, definitive := p.Present(), true
	switch {
	case err == nil:
		next = true
	case edgexErrors.Kind(err) == edgexErrors.KindEntityDoesNotExist:
		next = false
	default:
		definitive = false
	}

	p.mu.Lock()
	was := p.present
	if definitive {
		p.present = next
	}
	cb := p.onAppear
	p.mu.Unlock()

	if !definitive {
		return was
	}
	if next && !was {
		p.lc.Infof("inference plane discovered: manager device %s now registered; re-registering capability",
			p.deviceName)
		if cb != nil {
			cb()
		}
	}
	if !next && was {
		p.lc.Warnf("inference plane absent: manager device %s not found; inference stays active for this session and is withdrawn on next (re)hello",
			p.deviceName)
	}
	return next
}

// Start runs periodic presence probes until ctx is cancelled.
func (p *inferencePresence) Start(ctx context.Context) {
	ticker := time.NewTicker(presenceProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.Probe(ctx)
		}
	}
}

// redriveHelloForInference re-negotiates the live MQTT session when the NI
// plane appears mid-session. Unlike the transport-reconnect replay, the
// broker session and all subscriptions stay alive: no Reset (specs ignored
// while NI was absent were never applied and arrive in NL's post-hello
// desired-state redrive) and no subscription re-issue (which would double
// register handlers on the still-open session). Coalesced: a flap storm
// results in at most one negotiation at a time.
func (app *myApp) redriveHelloForInference() {
	if !app.inferenceHelloMu.TryLock() {
		return
	}
	go func() {
		defer app.inferenceHelloMu.Unlock()
		lc := app.service.LoggingClient()
		if app.gwClient == nil || app.gwClient.Gateway() == nil {
			lc.Warn("inference plane appeared but gateway session is down; next reconnect will negotiate")
			return
		}
		var ack *device.HelloAck
		var err error
		for attempt := 0; attempt < 5; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			var adverts []device.ConnectorAdvert
			if app.applier != nil {
				adverts = app.applier.ConnectorAdverts()
			}
			ack, err = app.gwClient.Negotiate(ctx, adverts...)
			cancel()
			if err == nil {
				break
			}
			lc.Errorf("inference-capability re-hello attempt %d failed: %s", attempt+1, err.Error())
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if err != nil {
			lc.Errorf("inference-capability re-hello gave up: %s", err.Error())
			return
		}
		lc.Infof("hello re-negotiated with inference plane: session=%s backends=%v",
			ack.SessionID, ack.Inference)
		app.manager.SetAck(ack)
		app.armInferenceAfterHello(context.Background())
		if app.shadowHandler != nil {
			app.shadowHandler.SetIdentity(ack.Selected, ack.Features)
			app.shadowHandler.Report(true)
		}
		// Replay presence over the same session (idempotent) and force a
		// manifest so the models segment appears immediately.
		if devs := app.manager.GetDevices(); len(devs) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if perr := app.gwClient.Gateway().ConnectDevices(ctx, devs, 1); perr != nil {
				lc.Errorf("inference re-hello presence replay failed: %s", perr.Error())
			}
			cancel()
		}
		app.manager.NotifyManifestChange()
	}()
}
