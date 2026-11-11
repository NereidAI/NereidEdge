// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package devicemqtt_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/nereidai/nereidlink/sdk/device/go"
	"github.com/nereidai/nereidlink/sdk/device/go/devicemqtt"
)

// TestConnectAndPublishTelemetry verifies CONNECT credentials, uplink
// topic and flat frame shape through a real broker.
func TestConnectAndPublishTelemetry(t *testing.T) {
	brokerURL := startBroker(t)
	var mu sync.Mutex
	var gotTopic, gotPayload string
	observer := observerClient(t, brokerURL, "observer-pub")
	if token := observer.Subscribe(device.TopicTelemetry, 1, func(_ paho.Client, msg paho.Message) {
		mu.Lock()
		gotTopic = msg.Topic()
		gotPayload = string(msg.Payload())
		mu.Unlock()
	}); token.Wait() && token.Error() != nil {
		t.Fatalf("observer subscribe: %v", token.Error())
	}
	time.Sleep(100 * time.Millisecond)

	client, err := devicemqtt.Connect(context.Background(), devicemqtt.Options{
		BrokerURL:  brokerURL,
		Credential: device.Token("dev-token"),
		ClientID:   "dev-1",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(50) })

	err = client.PublishTelemetry(context.Background(),
		device.NewFrame().SetFloat("temperature", 21.5).SetBool("online", true), 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, "telemetry uplink", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotTopic == device.TopicTelemetry
	})
	if !strings.Contains(gotPayload, `"temperature":21.5`) {
		t.Fatalf("payload: %s", gotPayload)
	}
}

// TestCommandRoundTrip verifies the downlink command subscription and a
// request-response reply keyed by command ID, matching the platform relay:
// downlink on v1/me/cmd/<name>, response on v1/me/cmdres/<id>.
func TestCommandRoundTrip(t *testing.T) {
	brokerURL := startBroker(t)
	client, err := devicemqtt.Connect(context.Background(), devicemqtt.Options{
		BrokerURL:  brokerURL,
		Credential: device.Basic("cid", "secret"),
		ClientID:   "dev-cmd",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(50) })

	var mu sync.Mutex
	var replied string
	if err := client.SubscribeCommands(context.Background(), 1, func(cmd device.Command) {
		mu.Lock()
		defer mu.Unlock()
		_ = client.RespondCommand(context.Background(), cmd.ID,
			map[string]any{"ok": true, "command": cmd.Name}, 1)
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	observer := observerClient(t, brokerURL, "observer-cmd")
	done := make(chan struct{})
	token := observer.Subscribe(device.PrefixCommandResponse+"+", 1, func(_ paho.Client, msg paho.Message) {
		mu.Lock()
		replied = msg.Topic() + ":" + string(msg.Payload())
		mu.Unlock()
		close(done)
	})
	token.Wait()
	time.Sleep(100 * time.Millisecond)

	observer.Publish(device.PrefixCommands+"set-led", 1, false,
		`{"id":"cmd-uuid-1","name":"set-led","params":{"color":"green"}}`)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("no command response observed")
	}
	if !strings.Contains(replied, "cmd-uuid-1") || !strings.Contains(replied, `"ok":true`) {
		t.Fatalf("reply: %s", replied)
	}
}

// TestShadowDesired verifies the desired-state push subscription.
func TestShadowDesired(t *testing.T) {
	brokerURL := startBroker(t)
	client, err := devicemqtt.Connect(context.Background(), devicemqtt.Options{
		BrokerURL:  brokerURL,
		Credential: device.Token("t"),
		ClientID:   "dev-shadow",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(50) })

	got := make(chan device.DesiredPush, 1)
	if err := client.SubscribeShadowDesired(context.Background(), 1, func(push device.DesiredPush) {
		got <- push
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	observer := observerClient(t, brokerURL, "observer-shadow")
	observer.Publish(device.TopicShadowDesired, 1, false,
		`{"version":3,"desired":{"led":"on"}}`)

	select {
	case push := <-got:
		if push.Version != 3 || push.Desired["led"] != "on" {
			t.Fatalf("push: %+v", push)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no desired push received")
	}
}

// TestGoodbyeWill verifies the LWT payload is valid gateway goodbye JSON.
func TestGoodbyeWill(t *testing.T) {
	will := devicemqtt.GoodbyeWill("")
	encoded, err := will.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if will.Topic != device.TopicGatewayGoodbye {
		t.Fatalf("topic: %s", will.Topic)
	}
	if strings.Contains(encoded, `"planned":true`) {
		t.Fatalf("LWT must not be planned: %s", encoded)
	}
}

// TestAttributesAndEvents verifies attribute uplink/push and the single
// and batched domain-event uplinks.
func TestAttributesAndEvents(t *testing.T) {
	brokerURL := startBroker(t)
	client, err := devicemqtt.Connect(context.Background(), devicemqtt.Options{
		BrokerURL:  brokerURL,
		Credential: device.Token("t"),
		ClientID:   "dev-attr",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(50) })

	down := make(chan device.AttributesDown, 1)
	if err := client.SubscribeAttributesDown(context.Background(), 1, func(push device.AttributesDown) {
		down <- push
	}); err != nil {
		t.Fatalf("subscribe attributes down: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	var mu sync.Mutex
	got := map[string]string{}
	observer := observerClient(t, brokerURL, "obs-attr")
	for _, topic := range []string{device.TopicAttributes, device.TopicEvents} {
		topic := topic
		token := observer.Subscribe(topic, 1, func(_ paho.Client, msg paho.Message) {
			mu.Lock()
			got[topic] = string(msg.Payload())
			mu.Unlock()
		})
		token.Wait()
	}
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	if err := client.PublishAttributes(ctx,
		device.AttributeUpdate{"firmware": "1.0", "nested": map[string]any{"a": 1}}, 1); err != nil {
		t.Fatalf("attributes: %v", err)
	}

	ev1, err := device.NewEvent("boot", map[string]any{"cause": "power"})
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	ev1.Severity = device.EventSeverityInfo
	if err := client.PublishEvents(ctx, []device.EventFrame{ev1}, 1); err != nil {
		t.Fatalf("single event: %v", err)
	}
	ev2 := device.EventFrame{EventType: "door", Severity: device.EventSeverityWarning, TSMS: 42}
	if err := client.PublishEvents(ctx,
		[]device.EventFrame{ev1, ev2}, 1); err != nil {
		t.Fatalf("batch events: %v", err)
	}

	waitFor(t, "attributes uplink", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(got[device.TopicAttributes], `"firmware":"1.0"`)
	})
	waitFor(t, "events uplink", func() bool {
		mu.Lock()
		defer mu.Unlock()
		// Last publish is the batch: must be a JSON array.
		return strings.HasPrefix(strings.TrimSpace(got[device.TopicEvents]), "[")
	})

	observer.Publish(device.TopicAttributesDown, 1, false,
		`{"scope":"SERVER_SCOPE","data":{"mode":"AUTO"}}`)
	select {
	case push := <-down:
		if push.Scope != device.AttributeScopeServer || string(push.Data["mode"]) != `"AUTO"` {
			t.Fatalf("push: %+v", push)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no attribute push received")
	}
}

// TestConnectOptionsValidation fails fast on missing required fields.
func TestConnectOptionsValidation(t *testing.T) {
	if _, err := devicemqtt.Connect(context.Background(), devicemqtt.Options{
		Credential: device.Token("t"),
	}); err == nil {
		t.Fatal("expected BrokerURL validation error")
	}
	if _, err := devicemqtt.Connect(context.Background(), devicemqtt.Options{
		BrokerURL: "tcp://127.0.0.1:1",
	}); err == nil {
		t.Fatal("expected Credential validation error")
	}
}
