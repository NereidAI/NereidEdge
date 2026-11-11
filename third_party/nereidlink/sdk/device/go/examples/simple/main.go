// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Command simple demonstrates a directly connected device over MQTT:
// connect with a device token, subscribe to commands, answer a
// request-response command and publish telemetry. All cadence decisions
// stay in main; the SDK only frames and transports messages.
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/nereidai/nereidlink/sdk/device/go"
	"github.com/nereidai/nereidlink/sdk/device/go/devicemqtt"
)

func main() {
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "MQTT broker URL")
	token := flag.String("token", "", "device access token (TOKEN credential)")
	clientID := flag.String("client-id", "example-device", "MQTT client id")
	flag.Parse()

	if *token == "" {
		log.Fatal("-token is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := devicemqtt.Connect(ctx, devicemqtt.Options{
		BrokerURL:  *broker,
		Credential: device.Token(*token),
		ClientID:   *clientID,
		KeepAlive:  devicemqtt.DefaultKeepAlive,
		Will:       nil,
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(200)

	// Commands arrive on v1/me/cmd/<name>; the reply goes to
	// v1/me/cmdres/<id> — note name versus id.
	if err := client.SubscribeCommands(ctx, 1, func(cmd device.Command) {
		log.Printf("command: id=%s name=%s params=%s", cmd.ID, cmd.Name, cmd.Params)
		if err := client.RespondCommand(ctx, cmd.ID,
			map[string]any{"ok": true, "command": cmd.Name}, 1); err != nil {
			log.Printf("respond: %v", err)
		}
	}); err != nil {
		log.Fatalf("subscribe commands: %v", err)
	}

	if err := client.SubscribeShadowDesired(ctx, 1, func(push device.DesiredPush) {
		log.Printf("desired v%d: %v", push.Version, push.Desired)
	}); err != nil {
		log.Fatalf("subscribe shadow: %v", err)
	}

	if err := client.SubscribeAttributesDown(ctx, 1, func(down device.AttributesDown) {
		log.Printf("attribute push scope=%s keys=%v", down.Scope, down.Data)
	}); err != nil {
		log.Fatalf("subscribe attributes: %v", err)
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	var eventSeq int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			frame := device.NewFrame().
				SetFloat("temperature", 21.5).
				SetBool("online", true).
				SetString("mode", "AUTO")
			if err := client.PublishTelemetry(ctx, frame, 1); err != nil {
				log.Printf("telemetry: %v", err)
			}
			eventSeq++
			event, _ := device.NewEvent("sample", map[string]any{"seq": eventSeq})
			if err := client.PublishEvents(ctx, []device.EventFrame{event}, 1); err != nil {
				log.Printf("event: %v", err)
			}
		}
	}
}
