// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

// publishTimeout bounds one MQTT publish. Paho returns token errors when the
// connection is gone, so this only guards a wedged broker.
const publishTimeout = 5 * time.Second

// mqttConn abstracts the paho client so the runtime loop is testable without
// a broker.
type mqttConn interface {
	Publish(topic string, qos byte, retained bool, payload []byte) error
	IsConnected() bool
	Close()
	// Lost is closed once the connection is gone.
	Lost() <-chan struct{}
}

// dialFunc connects and returns a ready mqttConn, or an error to retry.
type dialFunc func(ctx context.Context, cfg ExportConfig, specID string) (mqttConn, error)

// queuedEvent is one event plus the readings that passed the filter.
type queuedEvent struct {
	event    *dtos.Event
	readings []dtos.BaseReading
}

// exportInstance is the runtime for one node-level export spec: it connects to
// the external broker, keeps a bounded queue of events, and reconnects with
// exponential backoff.
type exportInstance struct {
	manager  *ExportManager
	specID   string
	revision int64
	hash     string
	cfg      ExportConfig

	queue  chan queuedEvent
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	first  chan firstResult
	dial   dialFunc

	mu          sync.Mutex
	state       string
	reason      string
	overflowed  bool
	warnedTopic bool

	dropped atomic.Uint64
}

// firstResult is the outcome of the first dial attempt, consumed synchronously
// by ExportManager.Apply so the edge reports APPLIED/DEGRADED in the right
// order (never DEGRADED then APPLIED).
type firstResult struct {
	state  string
	reason string
}

func newExportInstance(m *ExportManager, specID string, revision int64, hash string, cfg ExportConfig) *exportInstance {
	ctx, cancel := context.WithCancel(context.Background())
	return &exportInstance{
		manager:  m,
		specID:   specID,
		revision: revision,
		hash:     hash,
		cfg:      cfg,
		queue:    make(chan queuedEvent, defaultQueueDepth),
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		first:    make(chan firstResult, 1),
		dial:     m.dial,
	}
}

// run is the reconnect loop: dial, serve while connected, then back off and
// retry. It reports APPLIED on connect, DEGRADED on connect failures/loss and
// FAILED once max_attempts (when non-zero) is exhausted.
func (i *exportInstance) run() {
	defer close(i.done)
	first := true
	attempt := 0
	for {
		if i.ctx.Err() != nil {
			return
		}
		conn, err := i.dial(i.ctx, i.cfg, i.specID)
		if err != nil {
			if i.ctx.Err() != nil {
				return
			}
			attempt++
			reason := "connect failed: " + err.Error()
			if first {
				i.signalFirst(ExportStateDegraded, reason)
				first = false
			}
			i.report(ExportStateDegraded, reason)
			if max := i.cfg.Reconnect.MaxAttempts; max > 0 && attempt >= max {
				i.report(ExportStateFailed, fmt.Sprintf("giving up after %d reconnect attempts: %s", attempt, err))
				i.park()
				return
			}
			if !sleepCtx(i.ctx, i.backoff(attempt)) {
				return
			}
			continue
		}
		attempt = 0
		i.report(ExportStateApplied, "")
		if first {
			i.signalFirst(ExportStateApplied, "")
			first = false
		}
		i.serve(conn)
		conn.Close()
		if i.ctx.Err() != nil {
			return
		}
		i.report(ExportStateDegraded, "connection lost")
	}
}

// serve publishes queued events until the context is cancelled or the
// connection is lost. Publish errors trigger a reconnect after repeated
// failures; a lost connection returns immediately.
func (i *exportInstance) serve(conn mqttConn) {
	consecutive := 0
	for {
		select {
		case <-i.ctx.Done():
			i.drain(conn)
			return
		case <-conn.Lost():
			return
		case q := <-i.queue:
			if err := i.publish(conn, q); err != nil {
				consecutive++
				i.manager.lc.Warnf("export publish failed: spec=%s device=%s: %s", i.specID, q.event.DeviceName, err)
				if consecutive >= 3 || !conn.IsConnected() {
					return
				}
				continue
			}
			consecutive = 0
			i.recoverFromOverflow()
		}
	}
}

// drain makes a bounded best-effort attempt to flush queued events during a
// graceful stop (delete/hot update) before the connection closes.
func (i *exportInstance) drain(conn mqttConn) {
	deadline := time.Now().Add(defaultDrainWindow)
	for time.Now().Before(deadline) && conn.IsConnected() {
		select {
		case q := <-i.queue:
			_ = i.publish(conn, q)
		default:
			return
		}
	}
}

// publish renders the topic and payload and sends one event.
func (i *exportInstance) publish(conn mqttConn, q queuedEvent) error {
	topic, unknown := RenderTopic(i.cfg.TopicTemplate, q.event)
	if unknown {
		i.mu.Lock()
		warn := !i.warnedTopic
		i.warnedTopic = true
		i.mu.Unlock()
		if warn {
			i.manager.lc.Warnf("export topic_template has unknown placeholders, using default: spec=%s template=%q",
				i.specID, i.cfg.TopicTemplate)
		}
	}
	payload, err := BuildPayload(i.cfg, q.event, q.readings)
	if err != nil {
		return fmt.Errorf("build payload: %w", err)
	}
	return conn.Publish(topic, i.cfg.QoS, i.cfg.Retained, payload)
}

// enqueue filters and queues one event without blocking. A full queue drops
// the event and reports DEGRADED so the platform can see the overload.
func (i *exportInstance) enqueue(ev *dtos.Event) {
	if ev == nil {
		return
	}
	readings := matchingReadings(i.cfg, ev)
	if readings == nil {
		return
	}
	select {
	case i.queue <- queuedEvent{event: ev, readings: readings}:
	default:
		i.dropped.Add(1)
		i.mu.Lock()
		i.overflowed = true
		i.mu.Unlock()
		i.report(ExportStateDegraded, "export buffer full; dropping events")
	}
}

// recoverFromOverflow reports APPLIED again after a successful publish once an
// overflow was seen.
func (i *exportInstance) recoverFromOverflow() {
	i.mu.Lock()
	was := i.overflowed
	i.overflowed = false
	i.mu.Unlock()
	if was {
		i.report(ExportStateApplied, "")
	}
}

// stop cancels the runtime and waits for it to exit cleanly.
func (i *exportInstance) stop() {
	i.cancel()
	<-i.done
}

// report publishes a state transition through the manager, deduplicating
// repeated reports of the same state to avoid status spam during backoff.
func (i *exportInstance) report(state, reason string) {
	i.mu.Lock()
	if i.state == state {
		i.mu.Unlock()
		return
	}
	i.state, i.reason = state, reason
	i.mu.Unlock()
	if i.manager.onState != nil {
		i.manager.onState(i.specID, i.revision, state, reason)
	}
}

// snapshot returns the current state and reason.
func (i *exportInstance) snapshot() (string, string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.state, i.reason
}

// signalFirst records the first dial outcome for Apply to consume. The channel
// is buffered so this never blocks even if Apply already gave up waiting.
func (i *exportInstance) signalFirst(state, reason string) {
	select {
	case i.first <- firstResult{state: state, reason: reason}:
	default:
	}
}

// waitFirst waits for the first dial outcome up to timeout. ok=false means the
// attempt is still in flight (pipeline started, treat as APPLIED).
func (i *exportInstance) waitFirst(timeout time.Duration) (state, reason string, ok bool) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case r := <-i.first:
		return r.state, r.reason, true
	case <-t.C:
		return "", "", false
	}
}

// park blocks until the instance is stopped; used by the terminal FAILED state.
func (i *exportInstance) park() {
	<-i.ctx.Done()
}

// backoff returns the reconnect delay for the given consecutive-failure count.
func (i *exportInstance) backoff(attempt int) time.Duration {
	min := time.Duration(i.cfg.Reconnect.MinBackoffMS) * time.Millisecond
	max := time.Duration(i.cfg.Reconnect.MaxBackoffMS) * time.Millisecond
	return backoffDelay(attempt, min, max)
}

// ExportManager owns every node-level export runtime, keyed by spec_id.
type ExportManager struct {
	lc      logger.LoggingClient
	dial    dialFunc
	mu      sync.RWMutex
	insts   map[string]*exportInstance
	onState func(specID string, revision int64, state, reason string)
}

// NewExportManager creates an empty manager. dial defaults to the paho client
// and can be overridden in tests via SetDialer.
func NewExportManager(lc logger.LoggingClient) *ExportManager {
	return &ExportManager{
		lc:    lc,
		dial:  dialPaho,
		insts: make(map[string]*exportInstance),
	}
}

// SetStateCallback registers the runtime status reporter (DEGRADED/APPLIED/
// FAILED transitions after the initial Apply result).
func (m *ExportManager) SetStateCallback(fn func(specID string, revision int64, state, reason string)) {
	m.mu.Lock()
	m.onState = fn
	m.mu.Unlock()
}

// SetDialer overrides the MQTT dialer (tests only).
func (m *ExportManager) SetDialer(d dialFunc) {
	m.dial = d
}

// Apply hot-swaps the export runtime for a spec: the new instance starts
// before the old one stops. It returns the outcome of the first connect
// attempt so the caller can report APPLIED or DEGRADED. The caller reports
// CREATING beforehand.
func (m *ExportManager) Apply(specID string, revision int64, hash string, raw json.RawMessage) (state, reason string, err error) {
	cfg, err := ParseExportConfig(raw)
	if err != nil {
		// Invalid desired config must not keep a stale pipeline exporting.
		m.Delete(specID)
		return "", "", err
	}
	if !cfg.EnabledOrDefault() {
		m.Delete(specID)
		return ExportStateApplied, "", nil
	}

	m.mu.Lock()
	old := m.insts[specID]
	if old != nil && old.hash == hash && old.revision >= revision {
		state, reason = old.snapshot()
		if state != ExportStateFailed {
			// Idempotent redrive (hello AC8): keep the healthy runtime running.
			m.mu.Unlock()
			return state, reason, nil
		}
		// Terminal FAILED (max_attempts exhausted): a same-config redrive must
		// rebuild, so fall through to a fresh instance below.
	}
	inst := newExportInstance(m, specID, revision, hash, cfg)
	m.insts[specID] = inst
	m.mu.Unlock()

	go inst.run()
	if old != nil {
		old.stop()
	}

	if state, reason, ok := inst.waitFirst(defaultConnectWait + 2*time.Second); ok {
		return state, reason, nil
	}
	return ExportStateApplied, "", nil
}

// Delete stops and removes the runtime for a spec, if any.
func (m *ExportManager) Delete(specID string) {
	m.mu.Lock()
	inst := m.insts[specID]
	delete(m.insts, specID)
	m.mu.Unlock()
	if inst != nil {
		inst.stop()
	}
}

// Fanout non-blockingly delivers an event to every active export runtime.
func (m *ExportManager) Fanout(ev *dtos.Event) {
	m.mu.RLock()
	insts := make([]*exportInstance, 0, len(m.insts))
	for _, inst := range m.insts {
		insts = append(insts, inst)
	}
	m.mu.RUnlock()
	for _, inst := range insts {
		inst.enqueue(ev)
	}
}

// StopAll stops every runtime; used on shutdown.
func (m *ExportManager) StopAll() {
	m.mu.Lock()
	insts := m.insts
	m.insts = make(map[string]*exportInstance)
	m.mu.Unlock()
	for _, inst := range insts {
		inst.stop()
	}
}

// NeedsRestart reports whether a spec has no runtime, or its runtime is in the
// terminal FAILED state, and therefore must be (re)applied even when the
// desired config hash and revision are unchanged.
func (m *ExportManager) NeedsRestart(specID string) bool {
	m.mu.RLock()
	inst := m.insts[specID]
	m.mu.RUnlock()
	if inst == nil {
		return true
	}
	state, _ := inst.snapshot()
	return state == ExportStateFailed
}

// State returns the current state of a spec runtime, if present.
func (m *ExportManager) State(specID string) (string, bool) {
	m.mu.RLock()
	inst := m.insts[specID]
	m.mu.RUnlock()
	if inst == nil {
		return "", false
	}
	state, _ := inst.snapshot()
	return state, true
}

// sleepCtx sleeps for d unless ctx is cancelled first; false means cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// pahoConn is the production mqttConn backed by paho.
type pahoConn struct {
	client paho.Client
	lost   chan struct{}
}

func (c *pahoConn) Publish(topic string, qos byte, retained bool, payload []byte) error {
	tok := c.client.Publish(topic, qos, retained, payload)
	if !tok.WaitTimeout(publishTimeout) {
		return errors.New("publish timed out")
	}
	return tok.Error()
}

func (c *pahoConn) IsConnected() bool { return c.client.IsConnected() }

func (c *pahoConn) Close() { c.client.Disconnect(250) }

func (c *pahoConn) Lost() <-chan struct{} { return c.lost }

// dialPaho connects one paho client. AutoReconnect/ConnectRetry are disabled:
// the runtime owns reconnection so max_attempts and the backoff curve are
// exactly the connector config.
func dialPaho(_ context.Context, cfg ExportConfig, specID string) (mqttConn, error) {
	tlsCfg, err := buildTLS(cfg)
	if err != nil {
		return nil, err
	}
	lost := make(chan struct{})
	var once sync.Once
	opts := paho.NewClientOptions()
	opts.AddBroker(cfg.BrokerURL)
	opts.SetClientID(externalClientID(cfg, specID))
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)
	opts.SetConnectTimeout(defaultConnectWait)
	opts.SetKeepAlive(time.Duration(cfg.KeepAliveS) * time.Second)
	opts.SetPingTimeout(5 * time.Second)
	opts.SetOrderMatters(false)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
	}
	if cfg.Password != "" {
		opts.SetPassword(cfg.Password)
	}
	if tlsCfg != nil {
		opts.SetTLSConfig(tlsCfg)
	}
	opts.SetConnectionLostHandler(func(_ paho.Client, _ error) {
		once.Do(func() { close(lost) })
	})

	client := paho.NewClient(opts)
	tok := client.Connect()
	if !tok.WaitTimeout(defaultConnectWait) {
		return nil, errors.New("connect timed out")
	}
	if err := tok.Error(); err != nil {
		return nil, err
	}
	return &pahoConn{client: client, lost: lost}, nil
}

// externalClientID picks a broker-unique client id: configured value, else a
// deterministic id derived from the spec so restarts keep the session stable.
func externalClientID(cfg ExportConfig, specID string) string {
	if cfg.ClientID != "" {
		return cfg.ClientID
	}
	var b strings.Builder
	b.WriteString("nereid-edge-export-")
	for _, r := range specID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
		if b.Len() >= 60 {
			break
		}
	}
	return b.String()
}

// buildTLS builds a TLS config for mqtts/ssl brokers or when certs are
// supplied. It returns nil when the broker is plaintext and no certs were set.
func buildTLS(cfg ExportConfig) (*tls.Config, error) {
	secure := strings.HasPrefix(cfg.BrokerURL, "ssl://") || strings.HasPrefix(cfg.BrokerURL, "mqtts://")
	if !secure && cfg.CACert == "" && cfg.ClientCert == "" && !cfg.InsecureSkipVerify {
		return nil, nil
	}
	out := &tls.Config{ //nolint:gosec // InsecureSkipVerify is an explicit operator opt-in.
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}
	if cfg.CACert != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM([]byte(cfg.CACert)) {
			return nil, errors.New("ca_cert is not valid PEM")
		}
		out.RootCAs = pool
	}
	if cfg.ClientCert != "" || cfg.ClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(cfg.ClientCert), []byte(cfg.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("client_cert/client_key: %w", err)
		}
		out.Certificates = []tls.Certificate{cert}
	}
	return out, nil
}
