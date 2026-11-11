// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

type publishRecord struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

type fakeConn struct {
	mu        sync.Mutex
	closed    bool
	connected bool
	losses    chan struct{}
	published []publishRecord
	pubErr    error
}

func newFakeConn() *fakeConn {
	return &fakeConn{connected: true, losses: make(chan struct{}, 1)}
}

func (c *fakeConn) Publish(topic string, qos byte, retained bool, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pubErr != nil {
		return c.pubErr
	}
	c.published = append(c.published, publishRecord{topic: topic, qos: qos, retained: retained, payload: payload})
	return nil
}

func (c *fakeConn) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

func (c *fakeConn) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
}

func (c *fakeConn) Lost() <-chan struct{} {
	return c.losses
}

func (c *fakeConn) snapshot() []publishRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]publishRecord(nil), c.published...)
	return out
}

// dialer returns a dialFunc whose connections and dial count can be observed.
type dialer struct {
	count atomic.Int64
	mu    sync.Mutex
	conn  *fakeConn
	fail  atomic.Bool
}

func newDialer() *dialer {
	d := &dialer{}
	return d
}

func (d *dialer) dial(ctx context.Context, cfg ExportConfig, specID string) (mqttConn, error) {
	d.count.Add(1)
	if d.fail.Load() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		return nil, errors.New("broker unreachable")
	}
	c := newFakeConn()
	d.mu.Lock()
	d.conn = c
	d.mu.Unlock()
	return c, nil
}

func (d *dialer) latest() *fakeConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.conn
}

func (d *dialer) dials() int64 { return d.count.Load() }

func testEvent() *dtos.Event {
	return &dtos.Event{
		Id: "e1", DeviceName: "d1", ProfileName: "p1", SourceName: "s1", Origin: 9,
		Readings: []dtos.BaseReading{{ResourceName: "temp", ValueType: "Float32", SimpleReading: dtos.SimpleReading{Value: "22.5"}}},
	}
}

func validSpec(t *testing.T) (string, int64, string, json.RawMessage) {
	t.Helper()
	raw := json.RawMessage(`{"broker_url":"mqtt://broker:1883","topic_template":"nereid/{deviceName}/{sourceName}"}`)
	cfg, err := ParseExportConfig(raw)
	if err != nil {
		t.Fatalf("parse spec: %s", err)
	}
	hash, err := canonicalHashForTest(raw)
	if err != nil {
		t.Fatalf("hash: %s", err)
	}
	_ = cfg
	return "spec-1", 1, hash, raw
}

// canonicalHashForTest is the idempotency key used in the applier; any stable
// value per config works for the manager.
func canonicalHashForTest(raw json.RawMessage) (string, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return "", err
	}
	b, err := json.Marshal(decoded)
	return string(b), err
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

func TestManagerApplyPublishesAndStops(t *testing.T) {
	m := NewExportManager(logger.NewMockClient())
	d := newDialer()
	m.SetDialer(d.dial)

	state, reason, err := m.Apply(validSpec(t))
	if err != nil {
		t.Fatalf("apply: %s", err)
	}
	if state != ExportStateApplied {
		t.Fatalf("first state = %s (%s)", state, reason)
	}
	if d.dials() != 1 {
		t.Fatalf("dial count = %d", d.dials())
	}

	ev := testEvent()
	m.Fanout(ev)
	conn := d.latest()
	waitFor(t, time.Second, func() bool { return len(conn.snapshot()) == 1 }, "publish")
	p := conn.snapshot()
	if p[0].topic != "nereid/d1/s1" {
		t.Errorf("topic = %q", p[0].topic)
	}
	var envelope map[string]any
	if err := json.Unmarshal(p[0].payload, &envelope); err != nil {
		t.Fatalf("payload json: %s", err)
	}
	if envelope["device_name"] != "d1" {
		t.Errorf("device in payload = %v", envelope["device_name"])
	}

	m.Delete("spec-1")
	waitFor(t, time.Second, func() bool { return d.latest() != nil && d.latest().closed }, "conn close")
	m.Fanout(ev) // after delete, must not publish to the closed instance
	if got := len(conn.snapshot()); got != 1 {
		t.Errorf("publishes after delete = %d", got)
	}
}

// TestManagerFanoutMandatoryRawGuard proves the privacy guard reaches the
// auxiliary edge-export-mqtt egress, not only the NL uplink: an export spec
// with an empty (match-all) filter must never publish PipelineJSON or
// Embedding* readings to the external broker, and a raw-only event produces
// no publish at all.
func TestManagerFanoutMandatoryRawGuard(t *testing.T) {
	m := NewExportManager(logger.NewMockClient())
	d := newDialer()
	m.SetDialer(d.dial)

	if _, _, err := m.Apply(validSpec(t)); err != nil {
		t.Fatalf("apply: %s", err)
	}
	defer m.StopAll()
	conn := d.latest()

	mixed := &dtos.Event{
		Id: "e-raw", DeviceName: "d1", ProfileName: "p1", SourceName: "s1", Origin: 9,
		Readings: []dtos.BaseReading{
			{ResourceName: "PipelineJSON", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: `{"embedding":[1]}`}},
			{ResourceName: "EmbeddingVector", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "[0.1]"}},
			{ResourceName: "matched", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "alice"}},
		},
	}
	m.Fanout(mixed)
	waitFor(t, time.Second, func() bool { return len(conn.snapshot()) == 1 }, "mixed-event publish")

	var envelope struct {
		Readings []struct {
			ResourceName string `json:"resource_name"`
		} `json:"readings"`
	}
	if err := json.Unmarshal(conn.snapshot()[0].payload, &envelope); err != nil {
		t.Fatalf("payload json: %s", err)
	}
	if len(envelope.Readings) != 1 || envelope.Readings[0].ResourceName != "matched" {
		t.Fatalf("export payload leaked edge-local readings: %+v", envelope.Readings)
	}

	rawOnly := &dtos.Event{
		Id: "e-local", DeviceName: "d1", ProfileName: "p1", SourceName: "s1", Origin: 10,
		Readings: []dtos.BaseReading{
			{ResourceName: "PipelineJSON", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "{}"}},
			{ResourceName: "Embedding", ValueType: "String", SimpleReading: dtos.SimpleReading{Value: "[]"}},
		},
	}
	m.Fanout(rawOnly)
	// No readiness signal exists for a skipped event; give the queue time to
	// prove nothing was enqueued or published.
	time.Sleep(100 * time.Millisecond)
	if got := len(conn.snapshot()); got != 1 {
		t.Fatalf("raw-only event must not be exported, got %d publishes", got)
	}
}

func TestManagerApplyIdempotent(t *testing.T) {
	m := NewExportManager(logger.NewMockClient())
	d := newDialer()
	m.SetDialer(d.dial)

	specID, rev, hash, raw := validSpec(t)
	if _, _, err := m.Apply(specID, rev, hash, raw); err != nil {
		t.Fatalf("apply: %s", err)
	}
	state, _, err := m.Apply(specID, rev, hash, raw)
	if err != nil {
		t.Fatalf("redo: %s", err)
	}
	if state != ExportStateApplied {
		t.Errorf("redo state = %s", state)
	}
	if d.dials() != 1 {
		t.Errorf("idempotent redo must not redial, got %d dials", d.dials())
	}
	m.StopAll()
}

func TestManagerApplyHotSwap(t *testing.T) {
	m := NewExportManager(logger.NewMockClient())
	d := newDialer()
	m.SetDialer(d.dial)

	specID, rev, hash, raw := validSpec(t)
	if _, _, err := m.Apply(specID, rev, hash, raw); err != nil {
		t.Fatalf("apply: %s", err)
	}
	oldConn := d.latest()

	// Revision bump with the same config hash must start a fresh instance.
	if _, _, err := m.Apply(specID, rev+1, hash, raw); err != nil {
		t.Fatalf("hot swap: %s", err)
	}
	newConn := d.latest()
	if newConn == oldConn {
		t.Fatal("hot swap reused the same connection")
	}
	if !oldConn.closed {
		t.Error("old connection not closed after hot swap")
	}
	if d.dials() != 2 {
		t.Errorf("dial count = %d", d.dials())
	}

	m.Fanout(testEvent())
	waitFor(t, time.Second, func() bool { return len(newConn.snapshot()) == 1 }, "publish to new conn")
	if got := len(oldConn.snapshot()); got != 0 {
		t.Errorf("old conn received %d publishes after swap", got)
	}
	m.Delete(specID)
}

func TestManagerApplyFailedRedrive(t *testing.T) {
	raw := json.RawMessage(`{"broker_url":"tcp://x","topic_template":"t","reconnect":{"min_backoff_ms":100,"max_backoff_ms":100,"max_attempts":1}}`)
	d := newDialer()
	d.fail.Store(true)

	m := NewExportManager(logger.NewMockClient())
	states := make(chan string, 8)
	m.SetStateCallback(func(sid string, rev int64, state, reason string) {
		states <- state
	})
	m.SetDialer(d.dial)

	specID := "spec-fail"
	state, _, err := m.Apply(specID, 1, "h1", raw)
	if err != nil {
		t.Fatalf("apply: %s", err)
	}
	if state != ExportStateDegraded {
		t.Errorf("first state = %s, want DEGRADED", state)
	}

	waitFor(t, 2*time.Second, func() bool { return m.NeedsRestart(specID) }, "FAILED state")

	// Same hash + older revision now: FAILED means the manager must rebuild.
	state, _, err = m.Apply(specID, 1, "h1", raw)
	if err != nil {
		t.Fatalf("redrive: %s", err)
	}
	if state != ExportStateDegraded {
		t.Errorf("redrive first state = %s", state)
	}
	m.Delete(specID)
}

func TestManagerDisabledSpec(t *testing.T) {
	raw := json.RawMessage(`{"enabled":false,"broker_url":"mqtt://x","topic_template":"t"}`)
	m := NewExportManager(logger.NewMockClient())
	d := newDialer()
	m.SetDialer(d.dial)
	state, _, err := m.Apply("spec-off", 1, "h", raw)
	if err != nil {
		t.Fatalf("apply disabled: %s", err)
	}
	if state != ExportStateApplied {
		t.Errorf("disabled state = %s, want APPLIED", state)
	}
	if d.dials() != 0 {
		t.Errorf("disabled spec must not dial, got %d", d.dials())
	}
	m.Fanout(testEvent())
	if d.dials() != 0 {
		t.Errorf("disabled spec must not dial after fanout, got %d", d.dials())
	}
}

func TestManagerInvalidSpecDeletes(t *testing.T) {
	m := NewExportManager(logger.NewMockClient())
	d := newDialer()
	m.SetDialer(d.dial)
	// Start a healthy instance first.
	specID, rev, hash, raw := validSpec(t)
	if _, _, err := m.Apply(specID, rev, hash, raw); err != nil {
		t.Fatalf("apply: %s", err)
	}
	// Pushing an invalid config must remove the stale runtime, not keep exporting.
	bad := json.RawMessage(`{"broker_url":123}`) //nolint:govet // intentional malformed type
	if _, _, err := m.Apply(specID, rev+1, "h9", bad); err == nil {
		t.Fatal("invalid config must error")
	}
	if _, exists := m.State(specID); exists {
		t.Error("stale runtime survived an invalid config")
	}
	if !d.latest().closed {
		t.Error("stale conn not closed after invalid config")
	}
}

func TestManagerFilterDrops(t *testing.T) {
	raw := json.RawMessage(`{"broker_url":"mqtt://x","topic_template":"t","filter":{"device_names":["other"]}}`)
	m := NewExportManager(logger.NewMockClient())
	d := newDialer()
	m.SetDialer(d.dial)
	if _, _, err := m.Apply("spec-f", 1, "h", raw); err != nil {
		t.Fatalf("apply: %s", err)
	}
	m.Fanout(testEvent())
	time.Sleep(100 * time.Millisecond)
	if got := len(d.latest().snapshot()); got != 0 {
		t.Errorf("filtered event published %d times", got)
	}
	m.Delete("spec-f")
}
