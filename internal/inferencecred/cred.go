// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package inferencecred fans the onvif-basic camera credential of a
// stream-mode edge-inference binding into NereidInference (ADR-0039 stream
// acquisition). NI's onvif-camera federation resolves the RTSP URL itself but
// authenticates ffmpeg with a named secret from its OWN EdgeX secret store;
// the cloud delivers onvif-basic secrets only to device-onvif-camera, so the
// edge bridges the gap without any URL or password ever appearing in a
// connector config, a protocol property or a log line:
//
//   - the camera -> secret-name mapping is read from device-onvif-camera's
//     core-keeper AppCustom region (CredentialsMap / DefaultSecretName),
//     joining on the camera MAC exactly like the onvif binder does;
//   - the secret document is read back from the onvif device service's
//     Writable/InsecureSecrets keeper region and POSTed under the same name
//     to device-inference's common /api/v3/secret endpoint;
//   - only the username/password keys cross services (NI reads exactly
//     those); the onvif mode stays with the onvif device service;
//   - removal deletes an unreferenced replica through the same keeper
//     prefix-delete semantics the onvif binder uses, and only a name that
//     exists as an onvif-basic secret on the source side is ever pruned, so
//     unrelated NI secrets (OpenAI keys, hand-provisioned RTSP secrets) are
//     untouched. Secrets still referenced by any NI Job device - including
//     untagged hand-built jobs - are always protected.
package inferencecred

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

const (
	defaultKeeperURL           = "http://edgex-core-keeper:59890"
	defaultOnvifKVSBase        = "edgex/v4/device-onvif-camera"
	defaultInferenceKVSBase    = "edgex/v4/device-inference"
	defaultInferenceServiceURL = "http://edgex-device-inference:59992"
	defaultInferenceDSName     = "device-inference"

	kvsAPIPrefix     = "/api/v3/kvs/key/"
	secretAPIPath    = "/api/v3/secret"
	apiVersion       = "v3"
	defaultTimeout   = 10 * time.Second
	maxRespBytes     = 1 << 14
	inferenceService = "Inference"
	onvifProtocol    = "Onvif"
	macAddressKey    = "MACAddress"
	secretNameKey    = "SecretName"

	appCustomCredentialsMap    = "AppCustom/CredentialsMap"
	appCustomDefaultSecretName = "AppCustom/DefaultSecretName"
	writableInsecureSecrets    = "Writable/InsecureSecrets"

	usernameKey = "username"
	passwordKey = "password"
)

// DeviceReader resolves an EdgeX device by name from core-metadata. The app
// service adapter fulfills it; tests substitute a fake.
type DeviceReader interface {
	DeviceByName(ctx context.Context, name string) (dtos.Device, error)
}

// ServiceDeviceLister lists every EdgeX device registered against one device
// service. The protection scan uses it so a secret referenced by a hand-built
// (untagged) NI job is never pruned.
type ServiceDeviceLister interface {
	ListServiceDevices(ctx context.Context, serviceName string) ([]dtos.Device, error)
}

// Binding is one live stream acquisition: the EdgeX camera name and the
// onvif-basic secret name its job references.
type Binding struct {
	CameraEdgeXName string
	SecretName      string
}

// Config carries the Manager dependencies. Empty URLs/stems select the
// docker-network defaults.
type Config struct {
	Metadata            DeviceReader
	Devices             ServiceDeviceLister
	KeeperURL           string
	OnvifKVSBase        string
	InferenceKVSBase    string
	InferenceServiceURL string
	InferenceDSName     string
	Logger              logger.LoggingClient
	Timeout             time.Duration
	// PruneGrace overrides the delay a replica must stay unreferenced
	// before it is deleted. Zero selects the 60s default.
	PruneGrace time.Duration
}

// Manager resolves camera secret names and converges the NI secret replica
// set to the live stream bindings.
type Manager struct {
	metadata DeviceReader
	devices  ServiceDeviceLister

	keeperURL        string
	onvifBase        string
	inferenceBase    string
	inferenceService string
	dsName           string

	lc   logger.LoggingClient
	http *http.Client

	// pruneGrace is the time a replica must stay unreferenced before it is
	// deleted, so delete-then-create update windows and transient sweeps
	// never remove a secret a new binding references seconds later.
	pruneGrace time.Duration

	mu                sync.Mutex
	unreferencedSince map[string]time.Time
	// delivered holds names confirmed present in NI this process run; it
	// only quiets idempotent replay logs and is reset after a delivery
	// failure so the next success is visible again.
	delivered map[string]struct{}
}

// New builds a Manager. Metadata is required for name resolution; the device
// lister may be nil (prune protection then covers only NE-created bindings).
func New(cfg Config) *Manager {
	if cfg.KeeperURL == "" {
		cfg.KeeperURL = defaultKeeperURL
	}
	if cfg.OnvifKVSBase == "" {
		cfg.OnvifKVSBase = defaultOnvifKVSBase
	}
	if cfg.InferenceKVSBase == "" {
		cfg.InferenceKVSBase = defaultInferenceKVSBase
	}
	if cfg.InferenceServiceURL == "" {
		cfg.InferenceServiceURL = defaultInferenceServiceURL
	}
	if cfg.InferenceDSName == "" {
		cfg.InferenceDSName = defaultInferenceDSName
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	pruneGrace := cfg.PruneGrace
	if pruneGrace <= 0 {
		pruneGrace = defaultPruneGrace
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewMockClient()
	}
	return &Manager{
		metadata:          cfg.Metadata,
		devices:           cfg.Devices,
		keeperURL:         strings.TrimSuffix(cfg.KeeperURL, "/"),
		onvifBase:         strings.Trim(cfg.OnvifKVSBase, "/"),
		inferenceBase:     strings.Trim(cfg.InferenceKVSBase, "/"),
		inferenceService:  strings.TrimSuffix(cfg.InferenceServiceURL, "/"),
		dsName:            cfg.InferenceDSName,
		lc:                cfg.Logger,
		http:              &http.Client{Timeout: cfg.Timeout},
		pruneGrace:        pruneGrace,
		unreferencedSince: map[string]time.Time{},
		delivered:         map[string]struct{}{},
	}
}

// defaultPruneGrace delays a prune until a replica has stayed unreferenced
// for a full minute, covering delete-old/create-new update churn.
const defaultPruneGrace = 60 * time.Second

// ResolveSecretName returns the onvif-basic secret name currently mapped to
// an EdgeX camera's MAC in device-onvif-camera's AppCustom region, falling
// back to the service-wide DefaultSecretName. An empty name with no error
// means the camera has no mapped credential and no default; transport or
// metadata failures return an error so the caller can retry.
func (m *Manager) ResolveSecretName(ctx context.Context, cameraEdgeXName string) (string, error) {
	if m.metadata == nil || cameraEdgeXName == "" {
		return "", errors.New("inferencecred: missing metadata reader or empty camera name")
	}
	dev, err := m.metadata.DeviceByName(ctx, cameraEdgeXName)
	if err != nil {
		return "", fmt.Errorf("resolve camera %q: %w", cameraEdgeXName, err)
	}
	mac, ok := protocolString(dev.Protocols, onvifProtocol, macAddressKey)
	if !ok {
		return "", fmt.Errorf("camera %q has no Onvif MACAddress", cameraEdgeXName)
	}
	normalized, ok := normalizeMAC(mac)
	if !ok {
		return "", fmt.Errorf("camera %q has an invalid MAC %q", cameraEdgeXName, mac)
	}

	entries, err := m.kvsList(ctx, m.onvifBase+"/"+appCustomCredentialsMap)
	if err != nil {
		return "", err
	}
	// Deterministic precedence when several names list the same MAC.
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	prefix := m.onvifBase + "/" + appCustomCredentialsMap + "/"
	for _, e := range entries {
		if !strings.HasPrefix(e.key, prefix) || strings.Contains(strings.TrimPrefix(e.key, prefix), "/") {
			continue
		}
		for _, part := range strings.Split(e.value, ",") {
			if candidate, ok := normalizeMAC(part); ok && candidate == normalized {
				return strings.TrimPrefix(e.key, prefix), nil
			}
		}
	}

	defaults, err := m.kvsList(ctx, m.onvifBase+"/"+appCustomDefaultSecretName)
	if err != nil {
		return "", err
	}
	for _, e := range defaults {
		if e.key == m.onvifBase+"/"+appCustomDefaultSecretName && e.value != "" {
			return e.value, nil
		}
	}
	return "", nil
}

// Ensure replicates every named onvif-basic secret into the NI secret
// store, creating missing documents and idempotently rewriting existing
// ones. It is used on the apply hot path (so the secret exists before NI
// observes the job device) and by restart replay; per-name failures are
// reported but never abort the other names.
func (m *Manager) Ensure(ctx context.Context, names []string) error {
	var firstErr error
	seen := map[string]struct{}{}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		if err := m.ensure(ctx, name); err != nil {
			m.lc.Errorf("inference stream credential %q not delivered to NI: %s", name, err.Error())
			m.markDeliveryFailed(name)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Reconcile converges the NI secret replica set to the live stream bindings:
// every referenced name is replicated from the onvif device service region,
// and replicas no longer referenced by either a live binding or ANY NI Job
// device are deleted - but only after they have stayed unreferenced for the
// prune grace window (delete-old/add-new update bursts never hit a zero-ref
// intermediate state). A replica is pruned only when the same name exists on
// the onvif source side, so unrelated NI secrets are never touched.
// Best-effort: an individual ensure/prune failure is logged and does not
// abort the sweep; the next trigger retries it.
func (m *Manager) Reconcile(ctx context.Context, live []Binding) error {
	wanted := make(map[string]struct{}, len(live))
	for _, b := range live {
		if b.SecretName != "" {
			wanted[b.SecretName] = struct{}{}
		}
	}
	if err := m.Ensure(ctx, mapKeys(wanted)); err != nil {
		// Delivery failure must never drive a prune; the grace state is
		// simply not advanced below.
		m.lc.Warnf("inference credential ensure incomplete: %s", err.Error())
	}

	protected := make(map[string]struct{}, len(wanted))
	for name := range wanted {
		protected[name] = struct{}{}
	}
	if m.devices != nil {
		jobs, err := m.devices.ListServiceDevices(ctx, m.dsName)
		if err != nil {
			// Without the job list the prune decision is unsafe; skip the
			// whole sweep (the next trigger retries).
			return fmt.Errorf("list NI jobs for secret protection: %w", err)
		}
		for _, job := range jobs {
			if name, ok := protocolString(job.Protocols, inferenceService, secretNameKey); ok && name != "" {
				protected[name] = struct{}{}
			}
		}
	}

	niNames, err := m.secretNames(ctx, m.inferenceBase)
	if err != nil {
		return fmt.Errorf("list NI secret replicas: %w", err)
	}
	onvifNames, err := m.secretNames(ctx, m.onvifBase)
	if err != nil {
		return fmt.Errorf("list onvif source secrets: %w", err)
	}

	// Restart replay must cover EVERY referenced secret, including names the
	// in-memory binding snapshot temporarily misses (cloud disconnect, edge
	// restart before hello settles) and untagged hand-built jobs. Only names
	// that exist on the onvif source side are replicable; genuinely foreign
	// NI secrets are left alone.
	replay := make([]string, 0)
	for name := range protected {
		if _, wantedNow := wanted[name]; wantedNow {
			continue
		}
		if _, onSource := onvifNames[name]; onSource {
			replay = append(replay, name)
		}
	}
	if len(replay) > 0 {
		sort.Strings(replay)
		if err := m.Ensure(ctx, replay); err != nil {
			m.lc.Warnf("inference credential replay incomplete: %s", err.Error())
		}
	}

	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	// A name referenced again leaves the grace window immediately, so an
	// update burst cannot delete the secret a freshly created job needs.
	for name := range protected {
		delete(m.unreferencedSince, name)
	}
	for name := range niNames {
		if _, keep := protected[name]; keep {
			delete(m.unreferencedSince, name)
			continue
		}
		if _, managed := onvifNames[name]; !managed {
			// Not an onvif-basic replica: never touch foreign NI secrets.
			continue
		}
		since, seen := m.unreferencedSince[name]
		if !seen {
			since = now
			m.unreferencedSince[name] = since
			m.lc.Infof("inference stream credential %q unreferenced; pruning in %s unless re-referenced",
				name, m.pruneGrace)
			continue
		}
		if now.Sub(since) < m.pruneGrace {
			continue
		}
		delete(m.unreferencedSince, name)
		m.mu.Unlock()
		err := m.deleteReplica(ctx, name)
		m.mu.Lock()
		if err != nil {
			m.lc.Errorf("prune NI stream credential %q failed: %s", name, err.Error())
			// Retry on the next pass after a fresh grace interval.
			m.unreferencedSince[name] = now
			continue
		}
		delete(m.delivered, name)
		m.lc.Infof("pruned unreferenced inference stream credential %q", name)
	}
	// Forget grace markers for replicas that have already vanished.
	for name := range m.unreferencedSince {
		if _, exists := niNames[name]; !exists {
			delete(m.unreferencedSince, name)
		}
	}
	return nil
}

func mapKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ensure replicates one onvif-basic secret document into the NI secret store
// under the same name, carrying only username/password.
func (m *Manager) ensure(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("empty secret name")
	}
	entries, err := m.kvsList(ctx, m.onvifBase+"/"+writableInsecureSecrets+"/"+name)
	if err != nil {
		return err
	}
	values := map[string]string{}
	secretPrefix := m.onvifBase + "/" + writableInsecureSecrets + "/" + name + "/SecretData/"
	for _, e := range entries {
		if strings.HasPrefix(e.key, secretPrefix) {
			values[strings.TrimPrefix(e.key, secretPrefix)] = e.value
		}
	}
	username, password := values[usernameKey], values[passwordKey]
	if username == "" || password == "" {
		return fmt.Errorf("onvif secret %q has no username/password on source side", name)
	}

	body, err := json.Marshal(map[string]any{
		"apiVersion": apiVersion,
		"secretName": name,
		"secretData": []map[string]string{
			{"key": usernameKey, "value": username},
			{"key": passwordKey, "value": password},
		},
	})
	if err != nil {
		return fmt.Errorf("encode secret request: %w", err)
	}
	status, resp, err := m.do(ctx, http.MethodPost, m.inferenceService+secretAPIPath, body)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("NI rejected secret store (%d): %s", status, safeResp(resp))
	}
	m.noteDelivered(name)
	return nil
}

func (m *Manager) noteDelivered(name string) {
	m.mu.Lock()
	_, repeat := m.delivered[name]
	m.delivered[name] = struct{}{}
	m.mu.Unlock()
	if repeat {
		// Periodic/hello replay re-posts the same document; keep the log
		// quiet while still making the first delivery visible.
		m.lc.Debugf("inference stream credential %q re-delivered to NI", name)
		return
	}
	m.lc.Infof("inference stream credential %q delivered to NI", name)
}

func (m *Manager) markDeliveryFailed(name string) {
	m.mu.Lock()
	delete(m.delivered, name)
	m.mu.Unlock()
}

// deleteReplica removes one secret from the NI keeper region with prefix
// semantics, mirroring the onvif binder's idempotent removal.
func (m *Manager) deleteReplica(ctx context.Context, name string) error {
	path := m.inferenceBase + "/" + writableInsecureSecrets + "/" + name + "?prefixMatch=true"
	status, resp, err := m.do(ctx, http.MethodDelete, m.keeperURL+kvsAPIPrefix+path, nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("keeper rejected secret delete %q (%d): %s", name, status, safeResp(resp))
	}
	return nil
}

// secretNames lists the named secrets stored under a service's
// Writable/InsecureSecrets keeper region.
func (m *Manager) secretNames(ctx context.Context, base string) (map[string]struct{}, error) {
	entries, err := m.kvsList(ctx, base+"/"+writableInsecureSecrets)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	prefix := base + "/" + writableInsecureSecrets + "/"
	for _, e := range entries {
		if !strings.HasPrefix(e.key, prefix) {
			continue
		}
		rest := strings.TrimPrefix(e.key, prefix)
		// First path segment is the secret name
		// (Writable/InsecureSecrets/<name>/{SecretName,SecretData/...}).
		if idx := strings.Index(rest, "/"); idx > 0 {
			out[rest[:idx]] = struct{}{}
		}
	}
	return out, nil
}

type kvEntry struct {
	key   string
	value string
}

// kvsList reads a core-keeper key subtree (flatten) and base64-decodes each
// value. A 404 means an empty subtree.
func (m *Manager) kvsList(ctx context.Context, keyPath string) ([]kvEntry, error) {
	u := m.keeperURL + kvsAPIPrefix + keyPath + "?flatten=true"
	status, resp, err := m.do(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("keeper GET %q failed (%d): %s", keyPath, status, safeResp(resp))
	}
	var payload struct {
		Response []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(resp), &payload); err != nil {
		return nil, fmt.Errorf("decode keeper response for %q: %w", keyPath, err)
	}
	out := make([]kvEntry, 0, len(payload.Response))
	for _, row := range payload.Response {
		decoded, err := base64.StdEncoding.DecodeString(row.Value)
		if err != nil {
			// A non-encoded keeper value is tolerated verbatim.
			decoded = []byte(row.Value)
		}
		out = append(out, kvEntry{key: row.Key, value: string(decoded)})
	}
	return out, nil
}

func (m *Manager) do(ctx context.Context, method, rawURL string, body []byte) (int, string, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return 0, "", fmt.Errorf("build %s request: %w", method, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.http.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("%s: %w", rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	if err != nil {
		return 0, "", fmt.Errorf("read %s response: %w", method, err)
	}
	return resp.StatusCode, string(payload), nil
}

func normalizeMAC(s string) (string, bool) {
	mac, err := net.ParseMAC(strings.TrimSpace(s))
	if err != nil {
		return "", false
	}
	return strings.ToLower(mac.String()), true
}

func protocolString(props map[string]dtos.ProtocolProperties, protocol, key string) (string, bool) {
	nested, ok := props[protocol]
	if !ok {
		return "", false
	}
	v, ok := nested[key]
	if !ok || v == nil {
		return "", false
	}
	return fmt.Sprint(v), true
}

func safeResp(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxRespBytes {
		s = s[:maxRespBytes]
	}
	return s
}
