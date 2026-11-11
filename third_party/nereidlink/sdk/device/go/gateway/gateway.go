// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package gateway is the gateway (NereidEdge) surface of the device SDK.
//
// A gateway is an ordinary device that additionally holds the "gateway"
// capability. It uses the device topics (telemetry, commands, shadow) and,
// after the one-per-connection hello negotiation, the v1/gateway topics:
// application heartbeat/goodbye, edge-local link episode reports, sub-device
// attachment, manifest reconciliation, batched telemetry, connector
// desired/reported, edge profile reports, discovery auto-provisioning,
// on-demand probes and gateway OTA.
//
// Like devicemqtt this package is only a protocol binding: it frames JSON
// and binds topics. The hello state machine, heartbeat cadence, manifest
// wall-clock alignment, retry policy and connector application logic all
// live in the edge firmware.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/nereidai/nereidlink/sdk/device/go"
	"github.com/nereidai/nereidlink/sdk/device/go/devicemqtt"
)

// Default negotiation timeout; the server closes a connection that does
// not finish hello within the hello_timeout_s limit advertised in the ack
// (10s by default).
const DefaultHelloTimeout = 10 * time.Second

// Client binds the gateway topics to one MQTT connection.
type Client struct {
	mqtt *devicemqtt.Client
}

// Wrap returns the gateway surface for an already-connected device MQTT
// client authenticated with gateway credentials.
func Wrap(c *devicemqtt.Client) *Client { return &Client{mqtt: c} }

// MQTT exposes the underlying device client for the v1/me topics.
func (c *Client) MQTT() *devicemqtt.Client { return c.mqtt }

// publish is a small typed JSON publisher.
func (c *Client) publish(ctx context.Context, topic string, payload any, qos byte) error {
	return c.mqtt.PublishJSON(ctx, topic, payload, qos)
}

// SubscribeHelloAck registers a handler for the negotiation response. By
// default the server answers on v1/gateway/hello/ack; if Hello.ResponseTopic
// is set, subscribe that topic instead. Register before PublishHello.
func (c *Client) SubscribeHelloAck(ctx context.Context, responseTopic string, qos byte,
	onAck func(device.HelloAck)) error {
	topic := responseTopic
	if topic == "" {
		topic = device.TopicGatewayHelloAck
	}
	return c.mqtt.SubscribeDecoded(ctx, topic, qos, func(_ string, raw []byte) error {
		var ack device.HelloAck
		if err := json.Unmarshal(raw, &ack); err != nil {
			return err
		}
		onAck(ack)
		return nil
	})
}

// NegotiateHello is the one-shot request/response form: it subscribes the
// ack topic (or hello.ResponseTopic), publishes hello, waits up to timeout
// for the ack and removes the subscription. It starts no persistent loop;
// callers drive a retry themselves. A non-ok status returns *HelloError
// wrapping the ack reason.
func (c *Client) NegotiateHello(ctx context.Context, hello device.Hello, timeout time.Duration) (*device.HelloAck, error) {
	if timeout <= 0 {
		timeout = DefaultHelloTimeout
	}
	ackTopic := hello.ResponseTopic
	if ackTopic == "" {
		ackTopic = device.TopicGatewayHelloAck
	}
	acks := make(chan device.HelloAck, 1)
	handler := func(_ paho.Client, msg paho.Message) {
		var ack device.HelloAck
		if err := json.Unmarshal(msg.Payload(), &ack); err == nil {
			select {
			case acks <- ack:
			default:
			}
		}
	}
	token := c.mqtt.Paho().Subscribe(ackTopic, 1, handler)
	token.Wait()
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("subscribe hello ack: %w", err)
	}
	defer func() {
		t := c.mqtt.Paho().Unsubscribe(ackTopic)
		t.Wait()
	}()

	if err := c.PublishHello(ctx, hello); err != nil {
		return nil, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ack := <-acks:
		if ack.Status != device.HelloStatusOK {
			return &ack, &HelloError{Ack: ack}
		}
		return &ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, errors.New("gateway: hello negotiation timed out")
	}
}

// HelloError carries a hello ack whose status is not ok.
type HelloError struct {
	// Ack is the full negotiation response.
	Ack device.HelloAck
}

// Error implements error.
func (e *HelloError) Error() string {
	return fmt.Sprintf("gateway: hello rejected: %s: %s", e.Ack.Status, e.Ack.Reason)
}

// PublishHello publishes the negotiation frame to v1/gateway/hello.
func (c *Client) PublishHello(ctx context.Context, hello device.Hello) error {
	if hello.V == 0 {
		hello.V = device.HelloVersion
	}
	if hello.ClientTime == 0 {
		hello.ClientTime = time.Now().UnixMilli()
	}
	return c.publish(ctx, device.TopicGatewayHello, hello, 1)
}

// Heartbeat publishes one application liveness frame. An empty frame (zero
// Heartbeat) is legal and also renews the lease.
func (c *Client) Heartbeat(ctx context.Context, beat device.Heartbeat, qos byte) error {
	return c.publish(ctx, device.TopicGatewayHeartbeat, beat, qos)
}

// Goodbye publishes the planned-shutdown frame. Register the same topic as
// the MQTT LWT (devicemqtt.GoodbyeWill) so an unplanned transport death is
// reported with Planned false.
func (c *Client) Goodbye(ctx context.Context, bye device.Goodbye, qos byte) error {
	return c.publish(ctx, device.TopicGatewayGoodbye, bye, qos)
}

// ReportLinkEpisode publishes one edge-local link watchdog episode on
// v1/gateway/linkhealth. The platform answers with a LinkEpisodeAck on
// v1/gateway/linkhealth/ack (or report.ResponseTopic): "accepted" applies
// the episode, "duplicate" means it was already recorded, so do not retry.
func (c *Client) ReportLinkEpisode(ctx context.Context, report device.LinkEpisodeReport, qos byte) error {
	return c.publish(ctx, device.TopicGatewayLinkHealth, report, qos)
}

// ConnectDevices announces a batch of attached sub-devices coming online.
// Every id must already be registered and bound to this gateway.
func (c *Client) ConnectDevices(ctx context.Context, deviceIDs []string, qos byte) error {
	return c.publish(ctx, device.TopicGatewayConnect, device.DeviceList{Devices: deviceIDs}, qos)
}

// DisconnectDevices announces a batch of attached sub-device detachments.
// The gateway connection itself stays open.
func (c *Client) DisconnectDevices(ctx context.Context, deviceIDs []string, qos byte) error {
	return c.publish(ctx, device.TopicGatewayDisconnect, device.DeviceList{Devices: deviceIDs}, qos)
}

// SendManifest publishes the full-state online sub-device list (and, when
// manifest.Connectors is a non-nil slice, the full applied connector list)
// for one reconciliation cycle.
func (c *Client) SendManifest(ctx context.Context, manifest device.Manifest, qos byte) error {
	return c.publish(ctx, device.TopicGatewayManifest, manifest, qos)
}

// PublishTelemetry sends a batch of sub-device telemetry frames. The
// platform accepts frames only for sub-devices attached on this connection.
func (c *Client) PublishTelemetry(ctx context.Context, blocks ...device.GatewayFrame) error {
	raw, err := device.EncodeGatewayBatch(blocks...)
	if err != nil {
		return err
	}
	return c.mqtt.PublishRaw(ctx, device.TopicGatewayTelemetry, raw, 1)
}

// PublishProfiles reports a batch of EdgeX device profiles (name +
// deviceResources) on v1/gateway/profiles, QoS1. The edge replays the full
// set after each hello and sends immediately when it first sees a profile;
// the server de-duplicates by per-profile content hash.
func (c *Client) PublishProfiles(ctx context.Context, profiles []device.EdgeProfile) error {
	return c.publish(ctx, device.TopicGatewayProfiles, device.EdgeProfileFrame{Profiles: profiles}, 1)
}
