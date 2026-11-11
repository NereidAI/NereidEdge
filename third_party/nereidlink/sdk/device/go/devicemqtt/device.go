// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package devicemqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eclipse/paho.mqtt.golang"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

// QoS levels re-exported for callers that do not want to import Paho
// directly.
const (
	QoSAtMostOnce  = 0
	QoSAtLeastOnce = 1
	QoSExactlyOnce = 2
)

// waitToken blocks until the Paho token completes, ctx is canceled or
// timeout elapses (zero means unbounded). A timeout/cancel does not
// unblock the Paho operation itself; it only returns control.
func waitToken(ctx context.Context, token mqtt.Token, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		token.Wait()
		close(done)
	}()
	if timeout <= 0 {
		select {
		case <-done:
			return token.Error()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return token.Error()
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("mqtt operation timed out after %s", timeout)
	}
}

// PublishTelemetry publishes one flat telemetry frame to
// v1/me/telemetry. The server emits one Envelope per scalar key.
func (c *Client) PublishTelemetry(ctx context.Context, frame device.Frame, qos byte) error {
	if err := frame.Validate(); err != nil {
		return err
	}
	raw, err := frame.EncodeJSON()
	if err != nil {
		return err
	}
	return c.PublishRaw(ctx, device.TopicTelemetry, raw, qos)
}

// PublishRaw publishes an already-encoded JSON payload.
func (c *Client) PublishRaw(ctx context.Context, topic string, payload []byte, qos byte) error {
	token := c.paho.Publish(topic, qos, false, payload)
	if err := waitToken(ctx, token, 0); err != nil {
		return fmt.Errorf("publish %s: %w", topic, err)
	}
	return nil
}

// RespondCommand publishes the arbitrary JSON result of a request-response
// command to v1/me/cmdres/<id>. Note the suffix is the command ID while
// the downlink topic suffixes the command name.
func (c *Client) RespondCommand(ctx context.Context, commandID string, result any, qos byte) error {
	if commandID == "" {
		return fmt.Errorf("devicemqtt: command id is required")
	}
	return c.publishJSON(ctx, device.CommandResponseTopic(commandID), qos, false, result)
}

// ReportShadow publishes a reported-state patch on
// v1/me/shadow/report.
func (c *Client) ReportShadow(ctx context.Context, patch device.ReportedPatch, qos byte) error {
	return c.publishJSON(ctx, device.TopicShadowReport, qos, false, patch)
}

// PublishAttributes reports CLIENT-scope attributes on
// v1/me/attributes. Values may be nested JSON; this is the key/value
// attribute store, not telemetry.
func (c *Client) PublishAttributes(ctx context.Context, attrs device.AttributeUpdate, qos byte) error {
	return c.publishJSON(ctx, device.TopicAttributes, qos, false, attrs)
}

// PublishEvents publishes one or more domain events on v1/me/events.
// The server accepts a single event object or an array; the SDK always
// sends exactly what is passed (one element may be sent unwrapped).
func (c *Client) PublishEvents(ctx context.Context, events []device.EventFrame, qos byte) error {
	if len(events) == 0 {
		return fmt.Errorf("devicemqtt: at least one event is required")
	}
	var payload any = events
	if len(events) == 1 {
		payload = events[0]
	}
	return c.publishJSON(ctx, device.TopicEvents, qos, false, payload)
}

// AttributePushHandler processes one SERVER/SHARED attribute change
// delivered on v1/me/attributes/down.
type AttributePushHandler func(device.AttributesDown)

// SubscribeAttributesDown registers a handler for server/shared
// attribute pushes.
func (c *Client) SubscribeAttributesDown(ctx context.Context, qos byte,
	onPush AttributePushHandler) error {
	return c.SubscribeDecoded(ctx, device.TopicAttributesDown, qos, func(_ string, raw []byte) error {
		var push device.AttributesDown
		if err := json.Unmarshal(raw, &push); err != nil {
			return err
		}
		onPush(push)
		return nil
	})
}

// AttributeAckHandler processes the on-demand fetch reply on
// v1/me/attributes/get/ack.
type AttributeAckHandler func(device.AttributesGetAck)

// SubscribeAttributesAck registers a handler for on-demand attribute
// fetch replies. Subscribe before calling RequestAttributes (typically once
// at startup, then request after each connect).
func (c *Client) SubscribeAttributesAck(ctx context.Context, qos byte,
	onAck AttributeAckHandler) error {
	return c.SubscribeDecoded(ctx, device.TopicAttributesGetAck, qos, func(_ string, raw []byte) error {
		var ack device.AttributesGetAck
		if err := json.Unmarshal(raw, &ack); err != nil {
			return err
		}
		onAck(ack)
		return nil
	})
}

// RequestAttributes publishes an on-demand pull of platform-managed
// attributes on v1/me/attributes/get. An empty scopes list asks for both
// SERVER_SCOPE and SHARED_SCOPE. The reply arrives on the topic handled by
// SubscribeAttributesAck. Call it once after each (re)connect to recover
// pushes missed while offline.
func (c *Client) RequestAttributes(ctx context.Context, scopes []string, qos byte) error {
	return c.publishJSON(ctx, device.TopicAttributesGet, qos, false,
		device.AttributesGetRequest{Scopes: scopes})
}

// CommandHandler processes one downlink command.
type CommandHandler func(device.Command)

// MessageHandler is the raw-subscribe callback.
type MessageHandler func(topic string, payload []byte)

// SubscribeCommands registers a handler on v1/me/cmd/# and decodes each
// downlink frame. qos applies to the subscription.
func (c *Client) SubscribeCommands(ctx context.Context, qos byte, onCommand CommandHandler) error {
	return c.SubscribeDecoded(ctx, device.FilterCommands, qos, func(_ string, raw []byte) error {
		var cmd device.Command
		if err := json.Unmarshal(raw, &cmd); err != nil {
			return err
		}
		onCommand(cmd)
		return nil
	})
}

// SubscribeShadowDesired registers a handler for complete desired-state
// pushes on v1/me/shadow/desired.
func (c *Client) SubscribeShadowDesired(ctx context.Context, qos byte, onPush func(device.DesiredPush)) error {
	return c.SubscribeDecoded(ctx, device.TopicShadowDesired, qos, func(_ string, raw []byte) error {
		var push device.DesiredPush
		if err := json.Unmarshal(raw, &push); err != nil {
			return err
		}
		onPush(push)
		return nil
	})
}

// PublishJSON marshals payload and publishes it. It is shared by the
// device and gateway topic surfaces.
func (c *Client) PublishJSON(ctx context.Context, topic string, payload any, qos byte) error {
	return c.publishJSON(ctx, topic, qos, false, payload)
}

// publishJSON marshals payload and publishes it.
func (c *Client) publishJSON(ctx context.Context, topic string, qos byte, retained bool, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", topic, err)
	}
	token := c.paho.Publish(topic, qos, false, raw)
	if err := waitToken(ctx, token, 0); err != nil {
		return fmt.Errorf("publish %s: %w", topic, err)
	}
	_ = retained
	return nil
}

// SubscribeDecoded subscribes to filter and decodes each message through
// decode. A decode error is swallowed so one bad frame never kills the
// Paho callback; handlers that need observability should log inside decode.
func (c *Client) SubscribeDecoded(ctx context.Context, filter string, qos byte,
	decode func(topic string, payload []byte) error) error {
	handler := func(_ mqtt.Client, msg mqtt.Message) {
		_ = decode(msg.Topic(), msg.Payload())
	}
	token := c.paho.Subscribe(filter, qos, handler)
	if err := waitToken(ctx, token, 0); err != nil {
		return fmt.Errorf("subscribe %s: %w", filter, err)
	}
	return nil
}

// Subscribe registers a raw message handler on a filter.
func (c *Client) Subscribe(ctx context.Context, filter string, qos byte, onMessage MessageHandler) error {
	handler := func(_ mqtt.Client, msg mqtt.Message) {
		onMessage(msg.Topic(), msg.Payload())
	}
	token := c.paho.Subscribe(filter, qos, handler)
	if err := waitToken(ctx, token, 0); err != nil {
		return fmt.Errorf("subscribe %s: %w", filter, err)
	}
	return nil
}

// Unsubscribe removes the named subscription filters.
func (c *Client) Unsubscribe(ctx context.Context, filters ...string) error {
	if len(filters) == 0 {
		return nil
	}
	token := c.paho.Unsubscribe(filters...)
	if err := waitToken(ctx, token, 0); err != nil {
		return fmt.Errorf("unsubscribe: %w", err)
	}
	return nil
}
