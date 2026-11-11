// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway_test

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/mochi-mqtt/server/v2"
	authal "github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/nereidai/nereidlink/sdk/device/go"
	"github.com/nereidai/nereidlink/sdk/device/go/devicemqtt"
	"github.com/nereidai/nereidlink/sdk/device/go/gateway"
)

// TestMain exempts loopback broker connections from workstation proxies
// for the observer clients.
func TestMain(m *testing.M) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
		"http_proxy", "https_proxy", "all_proxy"} {
		_ = os.Unsetenv(key)
	}
	_ = os.Setenv("NO_PROXY", "127.0.0.1,localhost")
	_ = os.Setenv("no_proxy", "127.0.0.1,localhost")
	os.Exit(m.Run())
}

func startBroker(t *testing.T) string {
	t.Helper()
	addr := "127.0.0.1:0"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr = ln.Addr().String()
	_ = ln.Close()

	broker := mqtt.New(&mqtt.Options{})
	if err := broker.AddHook(&authal.AllowHook{}, nil); err != nil {
		t.Fatalf("allow hook: %v", err)
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "gw-test", Address: addr})
	if err := broker.AddListener(tcp); err != nil {
		t.Fatalf("listener: %v", err)
	}
	go func() { _ = broker.Serve() }()
	t.Cleanup(func() { _ = broker.Close() })
	return "tcp://" + addr
}

func connectGateway(t *testing.T, brokerURL, clientID string) *gateway.Client {
	t.Helper()
	mq, err := devicemqtt.Connect(context.Background(), devicemqtt.Options{
		BrokerURL:  brokerURL,
		Credential: device.Basic("gw-cred", "secret"),
		ClientID:   clientID,
		Will:       devicemqtt.GoodbyeWill(""),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { mq.Disconnect(50) })
	return gateway.Wrap(mq)
}

func observer(t *testing.T, brokerURL, id string) paho.Client {
	t.Helper()
	opts := paho.NewClientOptions().AddBroker(brokerURL).SetClientID(id)
	client := paho.NewClient(opts)
	token := client.Connect()
	if token.WaitTimeout(5*time.Second) && token.Error() != nil {
		t.Fatalf("observer connect: %v", token.Error())
	}
	t.Cleanup(func() { client.Disconnect(50) })
	return client
}

func waitSettle() { time.Sleep(100 * time.Millisecond) }

// waitFor waits up to 2s for cond to become true.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// capture subscribes an observer to filter and stores received payloads.
func capture(t *testing.T, obs paho.Client, filter string) (*sync.Mutex, *[]string) {
	t.Helper()
	var mu sync.Mutex
	var got []string
	token := obs.Subscribe(filter, 1, func(_ paho.Client, msg paho.Message) {
		mu.Lock()
		got = append(got, msg.Topic()+":"+string(msg.Payload()))
		mu.Unlock()
	})
	token.Wait()
	return &mu, &got
}

// TestNegotiateHello verifies the one-shot hello/ack exchange and a
// rejected negotiation surfacing as *gateway.HelloError.
func TestNegotiateHello(t *testing.T) {
	brokerURL := startBroker(t)
	gw := connectGateway(t, brokerURL, "edge-hello")

	obs := observer(t, brokerURL, "obs-hello")
	ackToken := obs.Subscribe(device.TopicGatewayHello, 1, func(_ paho.Client, msg paho.Message) {
		// Emulate the platform answering on the default ack topic.
		obs.Publish(device.TopicGatewayHelloAck, 1, false, `{
		  "session_id":"s-1","status":"ok","selected":"1.4",
		  "server":{"min":"1.0","max":"2.0","server_version":"test"},
		  "features":["telemetry","presence"],
		  "limits":{"heartbeat_s":30,"hello_timeout_s":10,"upload":{"max_mb":256,"qps":20}},
		  "time":{"server_time":1727360000456,"offset_ms":1,"clock_warning":false},
		  "resume":{"accepted":false}}`)
		_ = msg
	})
	ackToken.Wait()
	waitSettle()

	hello := device.Hello{
		EdgeID:       "edge-001",
		AgentVersion: "2.3.1",
		Proto:        device.ProtoRange{Min: "1.0", Max: "1.4"},
		Features: device.FeatureSet{
			Required: []string{device.FeatureTelemetry, device.FeaturePresence},
		},
	}
	ack, err := gw.NegotiateHello(context.Background(), hello, 3*time.Second)
	if err != nil {
		t.Fatalf("negotiate: %v", err)
	}
	if ack.SessionID != "s-1" || ack.Limits.HeartbeatS != 30 || ack.Selected != "1.4" {
		t.Fatalf("ack: %+v", ack)
	}
}

// TestGatewayUplinks verifies the gateway-only uplink topics and frames.
func TestGatewayUplinks(t *testing.T) {
	brokerURL := startBroker(t)
	gw := connectGateway(t, brokerURL, "edge-up")
	obs := observer(t, brokerURL, "obs-up")

	mu, got := capture(t, obs, "v1/gateway/#")
	waitSettle()

	ctx := context.Background()
	if err := gw.Heartbeat(ctx, device.Heartbeat{Seq: 1, ClientTime: 123}, 1); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := gw.ConnectDevices(ctx, []string{"dev-a", "dev-b"}, 1); err != nil {
		t.Fatalf("connect: %v", err)
	}
	connectors := []device.ManifestConnectorInstance{{SpecID: "spec-1", Revision: 1}}
	if err := gw.SendManifest(ctx, device.Manifest{
		Devices:      []string{"dev-a", "dev-b"},
		Connectors:   &connectors,
		Revision:     1,
		MissedCycles: 0,
	}, 1); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if err := gw.PublishTelemetry(ctx,
		device.GatewayFrame{DeviceID: "dev-a", TS: 1727360000123,
			Measurements: device.NewFrame().SetFloat("temperature", 21.5)},
	); err != nil {
		t.Fatalf("telemetry: %v", err)
	}
	zero := uint64(0)
	p50 := 47.5
	observed := int64(1)
	if err := gw.ReportConnectorStatus(ctx, device.ConnectorStatus{
		SpecID: "spec-1", State: device.ConnectorStateApplied, Revision: 1,
		Stats:            &device.ConnectorStatusStats{SuccessTotal: &zero, P50MS: &p50},
		ObservedRevision: &observed, ObservedHash: "abc123",
	}, 1); err != nil {
		t.Fatalf("connector status: %v", err)
	}
	if err := gw.ReportDiscovery(ctx, device.DiscoveryFrame{
		Type: "modbus-tcp",
		Candidates: []device.DiscoveryCandidate{
			{LocalID: "unit-1", Profile: "Modbus-Temp"},
		},
	}, 1); err != nil {
		t.Fatalf("discovery: %v", err)
	}
	if err := gw.ProbeResult(ctx, "probe-1", "dev-a",
		map[string]any{"reachable": true}, 1); err != nil {
		t.Fatalf("probe result: %v", err)
	}
	if err := gw.ReportOTAProgress(ctx, device.GatewayOTAProgress{
		TaskID: "task-1", Device: "dev-a",
		State: device.OTAStateDownloading, ProgressPct: 40,
	}, 1); err != nil {
		t.Fatalf("ota: %v", err)
	}
	if err := gw.Goodbye(ctx, device.Goodbye{Reason: "upgrade", Planned: true}, 1); err != nil {
		t.Fatalf("goodbye: %v", err)
	}
	if err := gw.PublishProfiles(ctx, []device.EdgeProfile{{
		Name: "Modbus-Temp",
		DeviceResources: []device.EdgeDeviceResource{
			{Name: "temperature", ValueType: "Float64", Units: "C"},
		},
	}}); err != nil {
		t.Fatalf("profiles: %v", err)
	}

	want := []string{
		"v1/gateway/heartbeat",
		"v1/gateway/connect",
		"v1/gateway/manifest",
		"v1/gateway/telemetry",
		"v1/gateway/connectors/status",
		"v1/gateway/discovery",
		"v1/gateway/probe/response",
		"v1/gateway/ota/progress",
		"v1/gateway/goodbye",
		"v1/gateway/profiles",
	}
	for _, topic := range want {
		waitFor(t, topic, func() bool {
			mu.Lock()
			defer mu.Unlock()
			for _, line := range *got {
				if strings.HasPrefix(line, topic+":") {
					return true
				}
			}
			return false
		})
	}
	mu.Lock()
	joined := strings.Join(*got, "\n")
	mu.Unlock()
	if !strings.Contains(joined, `"ts":1727360000123`) {
		t.Fatalf("missing ts in gateway telemetry: %s", joined)
	}
	if !strings.Contains(joined, `"profiles":[{"name":"Modbus-Temp"`) {
		t.Fatalf("missing profiles frame: %s", joined)
	}
	for _, wantPayload := range []string{
		`"spec_id":"spec-1"`, `"success_total":0`, `"p50_ms":47.5`,
		`"observed_revision":1`, `"observed_hash":"abc123"`,
	} {
		if !strings.Contains(joined, wantPayload) {
			t.Fatalf("connector status payload missing %s: %s", wantPayload, joined)
		}
	}
}

// TestGatewayCommandRelay verifies the sub-device command downlink on
// v1/gateway/command and the correlated response on
// v1/gateway/command/response (matching the platform relay frame names).
func TestGatewayCommandRelay(t *testing.T) {
	brokerURL := startBroker(t)
	gw := connectGateway(t, brokerURL, "edge-cmdrelay")

	cmds := make(chan device.GatewayCommand, 1)
	ctx := context.Background()
	if err := gw.SubscribeCommands(ctx, 1, func(cmd device.GatewayCommand) { cmds <- cmd }); err != nil {
		t.Fatalf("subscribe commands: %v", err)
	}
	waitSettle()

	obs := observer(t, brokerURL, "obs-cmdrelay")
	var mu sync.Mutex
	var response string
	token := obs.Subscribe(device.TopicGatewayCommandResponse, 1, func(_ paho.Client, msg paho.Message) {
		mu.Lock()
		response = string(msg.Payload())
		mu.Unlock()
	})
	token.Wait()
	waitSettle()

	obs.Publish(device.TopicGatewayCommand, 1, false, `{
	  "command_id":"cmd-1","device":"dev-a",
	  "interaction":"request-response","timeliness":"realtime",
	  "method":"set-led","params":{"color":"green"}}`)

	select {
	case cmd := <-cmds:
		if cmd.CommandID != "cmd-1" || cmd.Device != "dev-a" ||
			cmd.Method != "set-led" || cmd.Interaction != device.InteractionRequestResponse {
			t.Fatalf("command: %+v", cmd)
		}
		if err := gw.RespondCommand(ctx, cmd.CommandID, cmd.Device,
			map[string]any{"ok": true}, 1); err != nil {
			t.Fatalf("respond: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no gateway command received")
	}

	waitFor(t, "command response uplink", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(response, `"command_id":"cmd-1"`) &&
			strings.Contains(response, `"device":"dev-a"`)
	})
}

// TestGatewayAttributes verifies the batched sub-device attribute uplink
// and the server/shared push downlink frames.
func TestGatewayAttributes(t *testing.T) {
	brokerURL := startBroker(t)
	gw := connectGateway(t, brokerURL, "edge-attrs")

	pushes := make(chan device.GatewayAttributesPush, 1)
	ctx := context.Background()
	if err := gw.SubscribeAttributePushes(ctx, 1, func(push device.GatewayAttributesPush) {
		pushes <- push
	}); err != nil {
		t.Fatalf("subscribe attribute pushes: %v", err)
	}
	waitSettle()

	obs := observer(t, brokerURL, "obs-attrs")
	var mu sync.Mutex
	var uplink string
	token := obs.Subscribe(device.TopicGatewayAttributes, 1, func(_ paho.Client, msg paho.Message) {
		mu.Lock()
		uplink = string(msg.Payload())
		mu.Unlock()
	})
	token.Wait()
	waitSettle()

	if err := gw.PublishAttributes(ctx, device.GatewayAttributeUpdate{
		"dev-a": {"firmware": "1.2.3", "retries": float64(2)},
	}, 1); err != nil {
		t.Fatalf("publish attributes: %v", err)
	}
	waitFor(t, "attribute uplink", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(uplink, `"dev-a"`)
	})

	obs.Publish(device.TopicGatewayAttributesPush, 1, false, `{
	  "device":"dev-a","scope":"SHARED_SCOPE","data":{"mode":"AUTO"}}`)
	select {
	case push := <-pushes:
		if push.Device != "dev-a" || push.Scope != device.AttributeScopeShared {
			t.Fatalf("push: %+v", push)
		}
		if string(push.Data["mode"]) != `"AUTO"` {
			t.Fatalf("push data: %+v", push.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no attribute push received")
	}
}

// TestGatewayProfileDelivery verifies the ADR-0038 profile downlink pair:
// spec frames arrive decoded on v1/gateway/profiles/spec and status frames
// are published on v1/gateway/profiles/status.
func TestGatewayProfileDelivery(t *testing.T) {
	brokerURL := startBroker(t)
	gw := connectGateway(t, brokerURL, "edge-profiles")

	specs := make(chan device.ProfileSpecFrame, 1)
	ctx := context.Background()
	if err := gw.SubscribeProfiles(ctx, 1, func(spec device.ProfileSpecFrame) { specs <- spec }); err != nil {
		t.Fatalf("sub profiles: %v", err)
	}
	waitSettle()

	obs := observer(t, brokerURL, "obs-profiles")
	mu, got := capture(t, obs, device.TopicGatewayProfilesStatus)
	waitSettle()

	obs.Publish(device.TopicGatewayProfilesSpec, 1, false, `{
	  "tenant_id":"t","gateway_id":"g","entries":[
	    {"profile_id":"p1","name":"A1","revision":2,"format":"edgex-device-profile/3",
	     "document":{"name":"A1","deviceResources":[]}},
	    {"profile_id":"p2","name":"Old","revision":1,"format":"edgex-device-profile/3","removing":true}
	  ]}`)

	select {
	case spec := <-specs:
		if spec.TenantID != "t" || len(spec.Entries) != 2 {
			t.Fatalf("spec: %+v", spec)
		}
		if spec.Entries[0].Name != "A1" || spec.Entries[0].Revision != 2 ||
			spec.Entries[0].Format != device.ProfileFormatEdgeX || !spec.Entries[1].Removing {
			t.Fatalf("entries: %+v", spec.Entries)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no profile spec")
	}

	if err := gw.ReportProfileStatus(ctx, device.ProfileStatusFrame{Reports: []device.ProfileStatus{
		{Name: "A1", Revision: 2, Status: device.ProfileStateApplied},
		{Name: "Old", Revision: 1, Status: device.ProfileStateRemoved},
	}}, 1); err != nil {
		t.Fatalf("report status: %v", err)
	}

	want := `"reports":[{"name":"A1","revision":2,"status":"APPLIED"},{"name":"Old","revision":1,"status":"REMOVED"}]`
	waitFor(t, "profile status publish", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, line := range *got {
			if strings.Contains(line, device.TopicGatewayProfilesStatus) &&
				strings.Contains(line, want) {
				return true
			}
		}
		return false
	})
}

// TestGatewayDownlinks verifies subscription routing for connector spec,
// discovery results, probe requests and OTA tasks.
func TestGatewayDownlinks(t *testing.T) {
	brokerURL := startBroker(t)
	gw := connectGateway(t, brokerURL, "edge-down")

	specs := make(chan device.ConnectorPush, 1)
	results := make(chan device.DiscoveryResult, 1)
	probes := make(chan device.ProbeRequest, 1)
	tasks := make(chan device.GatewayOTATask, 1)

	ctx := context.Background()
	if err := gw.SubscribeConnectors(ctx, 1, func(p device.ConnectorPush) { specs <- p }); err != nil {
		t.Fatalf("sub connectors: %v", err)
	}
	if err := gw.SubscribeDiscoveryResults(ctx, 1, func(r device.DiscoveryResult) { results <- r }); err != nil {
		t.Fatalf("sub discovery: %v", err)
	}
	if err := gw.SubscribeProbes(ctx, 1, func(p device.ProbeRequest) { probes <- p }); err != nil {
		t.Fatalf("sub probes: %v", err)
	}
	if err := gw.SubscribeOTATasks(ctx, 1, func(task device.GatewayOTATask) { tasks <- task }); err != nil {
		t.Fatalf("sub ota: %v", err)
	}
	waitSettle()

	obs := observer(t, brokerURL, "obs-down")
	obs.Publish(device.TopicGatewayConnectorSpec, 1, false, `{
	  "kind":"spec","tenant_id":"t","gateway_id":"g","spec_id":"spec-1",
	  "device_id":"dev-a","connector_type":"modbus-tcp","type_version":"2.1",
	  "config":{"host":"10.0.0.1"},"mappings":[],"revision":1}`)
	obs.Publish(device.TopicGatewayConnectorDelete, 1, false, `{
	  "kind":"delete","tenant_id":"t","gateway_id":"g","spec_id":"spec-2","revision":2}`)
	obs.Publish(device.TopicGatewayDiscoveryResult, 1, false, `{
	  "results":[{"local_id":"unit-1","device_id":"uuid-1","status":"created"}]}`)
	obs.Publish(device.TopicGatewayProbe, 1, false, `{
	  "probe_id":"probe-1","devices":["dev-a"],"mode":"fresh","timeout_ms":10000}`)
	obs.Publish(device.TopicGatewayOTATask, 1, false, `{
	  "task_id":"task-1","devices":["dev-a"],"version":"1.2.3",
	  "url":"https://example.test/fw.bin","sha256":"`+strings.Repeat("a", 64)+`","size":42}`)

	select {
	case s := <-specs:
		if s.Kind != device.ConnectorKindSpec || s.DeviceID != "dev-a" || s.Revision != 1 {
			t.Fatalf("spec: %+v", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no connector spec")
	}
	select {
	case s := <-specs:
		if s.Kind != device.ConnectorKindDelete || s.SpecID != "spec-2" {
			t.Fatalf("delete: %+v", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no connector delete")
	}
	select {
	case r := <-results:
		if r.Results[0].DeviceID != "uuid-1" || r.Results[0].Status != device.DiscoveryStatusCreated {
			t.Fatalf("discovery: %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no discovery result")
	}
	select {
	case p := <-probes:
		if p.ProbeID != "probe-1" || p.Mode != device.ProbeModeFresh {
			t.Fatalf("probe: %+v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no probe request")
	}
	select {
	case task := <-tasks:
		if task.TaskID != "task-1" || task.Version != "1.2.3" {
			t.Fatalf("ota: %+v", task)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no ota task")
	}
}
