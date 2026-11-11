// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Command gateway demonstrates a NereidEdge-style gateway: hello
// negotiation, application heartbeat with LWT, sub-device attachment,
// manifest reconciliation, batched telemetry and the connector/probe/OTA
// downlink handlers. The gateway owns the state machine; the SDK provides
// only the frames and topic bindings.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/nereidai/nereidlink/sdk/device/go"
	"github.com/nereidai/nereidlink/sdk/device/go/devicemqtt"
	"github.com/nereidai/nereidlink/sdk/device/go/gateway"
)

func main() {
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "MQTT broker URL")
	id := flag.String("credentials-id", "", "gateway credentials id")
	secret := flag.String("credentials-secret", "", "gateway credentials secret")
	edgeID := flag.String("edge-id", "edge-001", "stable edge instance id")
	flag.Parse()

	if *id == "" {
		log.Fatal("-credentials-id is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The LWT must be registered before CONNECT; its Planned bool stays
	// false so a crash is distinguishable from a published planned goodbye.
	mq, err := devicemqtt.Connect(ctx, devicemqtt.Options{
		BrokerURL:     *broker,
		Credential:    device.Basic(*id, *secret),
		ClientID:      *edgeID,
		KeepAlive:     60 * time.Second,
		Will:          devicemqtt.GoodbyeWill("transport_down"),
		AutoReconnect: true,
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = gwGoodbye(mq)
		mq.Disconnect(500)
	}()

	gw := gateway.Wrap(mq)
	registerDownlinkHandlers(ctx, gw)

	ack, err := gw.NegotiateHello(ctx, device.Hello{
		EdgeID:       *edgeID,
		AgentVersion: "example-1.0.0",
		Proto:        device.ProtoRange{Min: "1.0", Max: "1.4"},
		Features: device.FeatureSet{
			Required: []string{device.FeatureTelemetry, device.FeaturePresence},
			Optional: []string{device.FeatureOTA, device.FeatureShadow,
				device.FeatureCommand, device.FeatureProfiles, device.FeatureInference},
		},
		// Preference order for downlinked profile documents (ADR-0038):
		// ask for EdgeX first, always fall back to the neutral format.
		// Declare edgex only because this firmware really ingests it.
		ProfileFormats: []string{device.ProfileFormatEdgeX, device.ProfileFormatNeutral},
		// Advertise edge model inference (ADR-0039): the model/1 contract
		// and the backend protocol families this firmware can speak. The
		// platform echoes the accepted intersection in ack.Inference.
		Inference: &device.InferenceAdvert{
			Contract: device.InferenceContractModel1,
			Backends: []string{"kserve", "openai"},
		},
		// Advertise the whole-gateway operations this firmware supports.
		// NL compiles and upserts them globally; check ack.Operations for
		// per-entry accepted/reason. The platform ships no op vocabulary.
		Operations: advertisedOperations(),
	}, 10*time.Second)
	if err != nil {
		log.Fatalf("hello: %v", err)
	}
	for _, r := range ack.Operations {
		if !r.Accepted {
			log.Printf("operation rejected op=%s v=%s reason=%s",
				r.Operation, r.TypeVersion, r.Reason)
		}
	}
	log.Printf("negotiated session=%s proto=%s features=%v heartbeat=%ds",
		ack.SessionID, ack.Selected, ack.Features, ack.Limits.HeartbeatS)
	// The echoed intersection is the go/no-go signal for profile downlink:
	// non-empty means the platform will render in one of these formats.
	log.Printf("profile downlink formats: %v", ack.ProfileFormats)
	// A non-nil ack.Inference means the platform accepted edge model
	// inference for this session; nil means stay on the generic path.
	if ack.Inference != nil {
		log.Printf("inference enabled contract=%s backends=%v",
			ack.Inference.Contract, ack.Inference.Backends)
	}

	// Attach known sub-devices; ids must be registered and gateway-bound.
	subDevices := []string{"REPLACE-WITH-SUBDEVICE-UUID"}
	if err := gw.ConnectDevices(ctx, subDevices, 1); err != nil {
		log.Printf("connect devices: %v", err)
	}

	heartbeat := time.NewTicker(time.Duration(ack.Limits.HeartbeatS) * time.Second)
	defer heartbeat.Stop()
	manifest := time.NewTicker(5 * time.Minute)
	defer manifest.Stop()
	telemetry := time.NewTicker(15 * time.Second)
	defer telemetry.Stop()

	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			seq++
			if err := gw.Heartbeat(ctx,
				device.Heartbeat{Seq: seq, ClientTime: time.Now().UnixMilli()}, 0); err != nil {
				log.Printf("heartbeat: %v", err)
			}
		case <-manifest.C:
			connectors := []device.ManifestConnectorInstance{}
			// This example loads no models yet; an explicit empty list is
			// an authoritative zero-inventory report (the cloud prunes any
			// rows for this gateway). A nil pointer instead would omit the
			// segment and leave stored inventory untouched.
			models := []device.ManifestModel{}
			if err := gw.SendManifest(ctx, device.Manifest{
				Devices:    subDevices,
				Connectors: &connectors, // explicit empty slice = zero connectors
				Models:     &models,     // explicit empty slice = zero models
			}, 1); err != nil {
				log.Printf("manifest: %v", err)
			}
		case <-telemetry.C:
			err := gw.PublishTelemetry(ctx, device.GatewayFrame{
				DeviceID: subDevices[0],
				TS:       time.Now().UnixMilli(),
				Measurements: device.NewFrame().
					SetFloat("temperature", 21.5).
					SetBool("online", true),
			})
			if err != nil {
				log.Printf("gateway telemetry: %v", err)
			}
		}
	}
}

// registerDownlinkHandlers subscribes the gateway-specific downlinks.
// Handlers must return quickly; do protocol work on the gateway's own
// goroutines.
func registerDownlinkHandlers(ctx context.Context, gw *gateway.Client) {
	if err := gw.SubscribeConnectors(ctx, 0, func(push device.ConnectorPush) {
		switch push.Kind {
		case device.ConnectorKindSpec:
			log.Printf("apply connector spec=%s revision=%d device=%s config=%s",
				push.SpecID, push.Revision, push.DeviceID, compact(push.Config))
			// Apply desired here, then report APPLIED:
			_ = gw.ReportConnectorStatus(ctx, device.ConnectorStatus{
				SpecID: push.SpecID, State: device.ConnectorStateApplied,
				Revision: push.Revision,
			}, 1)
		case device.ConnectorKindDelete:
			log.Printf("remove connector spec=%s", push.SpecID)
		}
	}); err != nil {
		log.Fatalf("subscribe connectors: %v", err)
	}

	if err := gw.SubscribeProfiles(ctx, 1, func(spec device.ProfileSpecFrame) {
		reports := make([]device.ProfileStatus, 0, len(spec.Entries))
		for _, entry := range spec.Entries {
			if entry.Removing {
				log.Printf("remove profile name=%s revision=%d", entry.Name, entry.Revision)
				reports = append(reports, device.ProfileStatus{
					Name: entry.Name, Revision: entry.Revision,
					Status: device.ProfileStateRemoved,
				})
				continue
			}
			log.Printf("apply profile name=%s revision=%d format=%s",
				entry.Name, entry.Revision, entry.Format)
			// De-duplicate by (name, revision), persist the rendered
			// document, then report APPLIED (or FAILED/REJECTED with a
			// short, transport-safe reason).
			reports = append(reports, device.ProfileStatus{
				Name: entry.Name, Revision: entry.Revision,
				Status: device.ProfileStateApplied,
			})
		}
		if len(reports) > 0 {
			_ = gw.ReportProfileStatus(ctx,
				device.ProfileStatusFrame{Reports: reports}, 1)
		}
	}); err != nil {
		log.Fatalf("subscribe profiles: %v", err)
	}

	if err := gw.SubscribeDiscoveryResults(ctx, 1, func(r device.DiscoveryResult) {
		for _, item := range r.Results {
			log.Printf("discovery local=%s status=%s device=%s reason=%s",
				item.LocalID, item.Status, item.DeviceID, item.Reason)
		}
	}); err != nil {
		log.Fatalf("subscribe discovery: %v", err)
	}

	if err := gw.SubscribeProbes(ctx, 0, func(req device.ProbeRequest) {
		for _, id := range req.Devices {
			// Replace with the real south-bus probe result.
			_ = gw.ProbeResult(ctx, req.ProbeID, id,
				map[string]any{"reachable": true}, 1)
		}
	}); err != nil {
		log.Fatalf("subscribe probes: %v", err)
	}

	if err := gw.SubscribeOTATasks(ctx, 0, func(task device.GatewayOTATask) {
		log.Printf("ota task=%s version=%s devices=%d sha256=%s",
			task.TaskID, task.Version, len(task.Devices), task.SHA256)
		// Download task.URL, verify sha256, flash, then report progress
		// per device via gw.ReportOTAProgress(...).
	}); err != nil {
		log.Fatalf("subscribe ota: %v", err)
	}

	if err := gw.SubscribeCommands(ctx, 0, func(cmd device.GatewayCommand) {
		// Command targets an attached sub-device; send it south, then
		// correlate a request-response answer by command_id + device.
		log.Printf("sub-device command id=%s device=%s method=%s",
			cmd.CommandID, cmd.Device, cmd.Method)
		if cmd.Interaction == device.InteractionRequestResponse {
			_ = gw.RespondCommand(ctx, cmd.CommandID, cmd.Device,
				map[string]any{"ok": true}, 1)
		}
	}); err != nil {
		log.Fatalf("subscribe sub-device commands: %v", err)
	}

	if err := gw.SubscribeAttributePushes(ctx, 1, func(push device.GatewayAttributesPush) {
		log.Printf("attribute push device=%s scope=%s keys=%d",
			push.Device, push.Scope, len(push.Data))
	}); err != nil {
		log.Fatalf("subscribe attribute pushes: %v", err)
	}

	if err := gw.SubscribeOperations(ctx, 0, func(inv device.OperationInvoke) {
		// Execute only operations advertised in advertisedOperations();
		// answer request-response ops within inv.TimeoutMs. Do the work on
		// your own goroutine; keep this callback short.
		log.Printf("operation invoke id=%s op=%s v=%s timeout=%dms params=%s",
			inv.ID, inv.Operation, inv.TypeVersion, inv.TimeoutMs, fmt.Sprintf("%v", inv.Params))
		result, err := runOperation(inv)
		frame := device.OperationResult{ID: inv.ID}
		if err != nil {
			frame.OK = false
			frame.Error = err.Error()
		} else {
			frame.OK = true
			frame.Result = result
		}
		// One-way descriptors expect no result; reply only for
		// request-response operations.
		if err := gw.RespondOperation(ctx, frame, 1); err != nil {
			log.Printf("respond operation id=%s: %v", inv.ID, err)
		}
	}); err != nil {
		log.Fatalf("subscribe operations: %v", err)
	}
}

// advertisedOperations declares the operation catalog of this firmware.
func advertisedOperations() []device.OperationAdvert {
	return []device.OperationAdvert{{
		Operation:   "diag.ping",
		TypeVersion: "1",
		DisplayName: map[string]string{"en": "Ping host", "zh": "Ping 探测"},
		Description: map[string]string{
			"en": "Send ICMP echo probes and report RTT/loss.",
			"zh": "发送 ICMP 探测并返回时延与丢包率。",
		},
		Interaction: device.OperationInteractionRequestResponse,
		TimeoutMs:   10000,
		Risk:        device.OperationRiskRead,
		ParamSchema: json.RawMessage(`{
			"$schema": "https://json-schema.org/draft/2020-12/schema",
			"type": "object", "required": ["host"],
			"properties": {
				"host":  {"type": "string", "description": "target host or IP"},
				"count": {"type": "integer", "description": "probe count"}
			}
		}`),
		ResultSchema: json.RawMessage(`{
			"$schema": "https://json-schema.org/draft/2020-12/schema",
			"type": "object",
			"properties": {
				"ok":     {"type": "boolean"},
				"rtt_ms": {"type": "array", "items": {"type": "number"}},
				"loss":   {"type": "number"}
			}
		}`),
	}}
}

// runOperation is the firmware-side operation implementation. Replace the
// stub with the real diagnostic routine; never leak secrets in the result.
func runOperation(inv device.OperationInvoke) (json.RawMessage, error) {
	switch inv.Operation {
	case "diag.ping":
		host, _ := inv.Params["host"].(string)
		// Run the actual ICMP probes here. Demo returns a fixed result.
		return json.RawMessage(
			`{"ok":true,"host":"` + host + `","rtt_ms":[1.2,1.1],"loss":0}`), nil
	default:
		return nil, fmt.Errorf("unknown operation %q", inv.Operation)
	}
}

// gwGoodbye publishes a planned shutdown before the LWT would fire.
func gwGoodbye(mq *devicemqtt.Client) error {
	gw := gateway.Wrap(mq)
	return gw.Goodbye(context.Background(),
		device.Goodbye{Reason: "upgrade", Planned: true}, 1)
}

// compact renders raw JSON on one line for logs.
func compact(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}
	return string(raw)
}
