// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package devicemqtt_test

import (
	"net"
	"os"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	mochimqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// TestMain keeps development-machine proxy variables (used to reach
// external services) from hijacking loopback broker connections. The SDK
// client bypasses proxies itself; the observer Paho clients in tests do
// not, so loopback is exempted here.
func TestMain(m *testing.M) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
		"http_proxy", "https_proxy", "all_proxy"} {
		_ = os.Unsetenv(key)
	}
	_ = os.Setenv("NO_PROXY", "127.0.0.1,localhost")
	_ = os.Setenv("no_proxy", "127.0.0.1,localhost")
	os.Exit(m.Run())
}

// startBroker launches an anonymous in-process mochi-mqtt v2 broker for the
// SDK integration tests and returns its tcp:// URL. The platform broker
// uses the same server family (internal/ingestmqtt), so wire behavior
// matches production.
func startBroker(t *testing.T) string {
	t.Helper()
	addr := freeAddr(t)
	broker := mochimqtt.New(nil)
	// The platform broker installs its registry-backed nl-auth hook; the
	// test broker admits any credential so SDK credential encoding can be
	// exercised without a registry.
	if err := broker.AddHook(&auth.AllowHook{}, nil); err != nil {
		t.Fatalf("allow hook: %v", err)
	}
	listener := listeners.NewTCP(listeners.Config{ID: "test-tcp", Address: addr})
	if err := broker.AddListener(listener); err != nil {
		t.Fatalf("add listener: %v", err)
	}
	go func() { _ = broker.Serve() }()
	t.Cleanup(func() {
		_ = broker.Close()
	})
	return "tcp://" + addr
}

// freeAddr returns a free loopback TCP address.
func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

// observerClient is a second plain Paho client used by tests to publish
// downlink frames and observe uplink frames.
func observerClient(t *testing.T, brokerURL, id string) paho.Client {
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
