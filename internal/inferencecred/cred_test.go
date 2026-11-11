// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package inferencecred

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

const (
	testCameraName = "dev-cam-1"
	testMAC        = "c0:6d:ed:eb:cb:59"
)

type fakeReader struct{ dev dtos.Device }

func (f fakeReader) DeviceByName(_ context.Context, name string) (dtos.Device, error) {
	if name != testCameraName {
		return dtos.Device{}, errNotFound
	}
	return f.dev, nil
}

type fakeLister struct{ jobs []dtos.Device }

func (f fakeLister) ListServiceDevices(_ context.Context, _ string) ([]dtos.Device, error) {
	return f.jobs, nil
}

type fakeError string

func (e fakeError) Error() string { return string(e) }

const errNotFound fakeError = "not found"

// testHarness stands up one server implementing the core-keeper KVS API plus
// NI's common /api/v3/secret endpoint, keeping every mutation in memory.
type testHarness struct {
	t      *testing.T
	server *httptest.Server

	mu      sync.Mutex
	kvs     map[string]string // full key -> plaintext value
	secrets map[string]map[string]string
	posts   []postedSecret
	deletes []string
}

type postedSecret struct {
	name string
	data map[string]string
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	h := &testHarness{
		t:       t,
		kvs:     map[string]string{},
		secrets: map[string]map[string]string{},
	}
	h.server = httptest.NewServer(http.HandlerFunc(h.handle))
	return h
}

func (h *testHarness) handle(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, secretAPIPath):
		var body struct {
			SecretName string `json:"secretName"`
			SecretData []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"secretData"`
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data := map[string]string{}
		for _, d := range body.SecretData {
			data[d.Key] = d.Value
		}
		h.secrets[body.SecretName] = data
		h.posts = append(h.posts, postedSecret{name: body.SecretName, data: data})
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": apiVersion, "statusCode": 201})
	case strings.Contains(r.URL.Path, kvsAPIPrefix):
		keyPath := strings.SplitN(r.URL.Path, kvsAPIPrefix, 2)[1]
		switch r.Method {
		case http.MethodGet:
			type row struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			var rows []row
			for k, v := range h.kvs {
				if strings.HasPrefix(k, keyPath) {
					rows = append(rows, row{Key: k, Value: base64.StdEncoding.EncodeToString([]byte(v))})
				}
			}
			if len(rows) == 0 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": apiVersion, "response": rows})
		case http.MethodDelete:
			h.deletes = append(h.deletes, keyPath)
			if r.URL.Query().Get("prefixMatch") == "true" {
				for k := range h.kvs {
					if strings.HasPrefix(k, keyPath) {
						delete(h.kvs, k)
					}
				}
				w.WriteHeader(http.StatusOK)
				return
			}
			if _, ok := h.kvs[keyPath]; !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			delete(h.kvs, keyPath)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (h *testHarness) putKVS(key, value string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.kvs[key] = value
}

func (h *testHarness) manager(cfg Config) *Manager {
	cfg.KeeperURL = h.server.URL
	cfg.InferenceServiceURL = h.server.URL
	return New(cfg)
}

func cameraDevice() dtos.Device {
	return dtos.Device{Protocols: map[string]dtos.ProtocolProperties{
		onvifProtocol: {macAddressKey: testMAC},
	}}
}

// putSourceSecret stores one onvif-basic document (username/password only
// is needed for replication) and putNIReplica marks the same name as
// present in the NI keeper region.
func putSourceSecret(h *testHarness, name, user, pw string) {
	base := defaultOnvifKVSBase
	h.putKVS(base+"/"+writableInsecureSecrets+"/"+name+"/SecretData/username", user)
	h.putKVS(base+"/"+writableInsecureSecrets+"/"+name+"/SecretData/password", pw)
}

func putNIReplica(h *testHarness, name string) {
	h.putKVS(defaultInferenceKVSBase+"/"+writableInsecureSecrets+"/"+name+"/SecretName", name)
}

func niSecretNames(h *testHarness) map[string]bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string]bool{}
	for k := range h.kvs {
		prefix := defaultInferenceKVSBase + "/" + writableInsecureSecrets + "/"
		if strings.HasPrefix(k, prefix) {
			out[strings.SplitN(strings.TrimPrefix(k, prefix), "/", 2)[0]] = true
		}
	}
	return out
}

func TestResolveSecretNameFromCredentialsMap(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()
	base := defaultOnvifKVSBase
	// Two mappings; the target MAC lives in the second name.
	h.putKVS(base+"/"+appCustomCredentialsMap+"/other-cam", "aa:bb:cc:dd:ee:ff")
	h.putKVS(base+"/"+appCustomCredentialsMap+"/camera-auth", "ec:60:73:dc:ba:1a,"+testMAC)

	m := h.manager(Config{Metadata: fakeReader{dev: cameraDevice()}})
	name, err := m.ResolveSecretName(context.Background(), testCameraName)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if name != "camera-auth" {
		t.Fatalf("name=%q, want camera-auth", name)
	}
}

func TestResolveSecretNameDefaultFallback(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()
	base := defaultOnvifKVSBase
	h.putKVS(base+"/"+appCustomCredentialsMap+"/other-cam", "aa:bb:cc:dd:ee:ff")
	h.putKVS(base+"/"+appCustomDefaultSecretName, "credentials001")

	m := h.manager(Config{Metadata: fakeReader{dev: cameraDevice()}})
	name, err := m.ResolveSecretName(context.Background(), testCameraName)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if name != "credentials001" {
		t.Fatalf("name=%q, want default credentials001", name)
	}
}

func TestResolveSecretNameNoMapping(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()
	m := h.manager(Config{Metadata: fakeReader{dev: cameraDevice()}})
	name, err := m.ResolveSecretName(context.Background(), testCameraName)
	if err != nil {
		t.Fatalf("empty regions must not error: %v", err)
	}
	if name != "" {
		t.Fatalf("name=%q, want empty", name)
	}
}

// TestReconcilePostsUsernamePasswordOnly verifies the same onvif-basic
// document reaches NI with username/password keys and never the onvif mode.
func TestReconcilePostsUsernamePasswordOnly(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()
	base := defaultOnvifKVSBase
	h.putKVS(base+"/"+writableInsecureSecrets+"/camera-auth/SecretName", "camera-auth")
	h.putKVS(base+"/"+writableInsecureSecrets+"/camera-auth/SecretData/username", "admin")
	h.putKVS(base+"/"+writableInsecureSecrets+"/camera-auth/SecretData/password", "a12345678")
	h.putKVS(base+"/"+writableInsecureSecrets+"/camera-auth/SecretData/mode", "usernametoken")

	m := h.manager(Config{Metadata: fakeReader{dev: cameraDevice()}})
	if err := m.Reconcile(context.Background(), []Binding{{CameraEdgeXName: testCameraName, SecretName: "camera-auth"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(h.posts) != 1 {
		t.Fatalf("posts=%d, want 1", len(h.posts))
	}
	post := h.posts[0]
	if post.name != "camera-auth" {
		t.Fatalf("posted name=%q", post.name)
	}
	if post.data["username"] != "admin" || post.data["password"] != "a12345678" {
		t.Fatalf("posted credentials=%v", post.data)
	}
	if _, hasMode := post.data["mode"]; hasMode {
		t.Fatalf("NI secret must not carry the onvif-only mode key: %v", post.data)
	}
}

// TestReconcilePrunesUnreferencedReplica verifies an onvif-named replica in
// NI is deleted only after it stays unreferenced for the prune grace
// window, while unrelated NI secrets and job-referenced secrets are left
// alone forever.
func TestReconcilePrunesUnreferencedReplica(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()

	// Source side: stale and live onvif secrets.
	putSourceSecret(h, "stale-cam", "admin", "pw")
	putSourceSecret(h, "camera-auth", "admin", "pw")
	// NI side: stale replica, live replica, and a foreign (non-onvif) secret.
	putNIReplica(h, "stale-cam")
	putNIReplica(h, "camera-auth")
	putNIReplica(h, "openai-key")

	// A hand-built untagged NI job references handbuilt-cam: it must be
	// protected even though no NE binding uses it, and replayed on restart.
	jobs := fakeLister{jobs: []dtos.Device{{
		Protocols: map[string]dtos.ProtocolProperties{
			inferenceService: {secretNameKey: "handbuilt-cam"},
		},
	}}}
	putSourceSecret(h, "handbuilt-cam", "u", "p")
	putNIReplica(h, "handbuilt-cam")

	m := h.manager(Config{
		Metadata:   fakeReader{dev: cameraDevice()},
		Devices:    jobs,
		PruneGrace: time.Millisecond,
	})
	live := []Binding{{CameraEdgeXName: testCameraName, SecretName: "camera-auth"}}
	if err := m.Reconcile(context.Background(), live); err != nil {
		t.Fatalf("reconcile pass 1: %v", err)
	}
	// First sighting only arms the grace timer: nothing is pruned yet.
	if remaining := niSecretNames(h); remaining["stale-cam"] != true {
		t.Fatalf("stale-cam must survive the first unreferenced pass: %v", remaining)
	}

	// Second pass after the grace window prunes the stale replica.
	time.Sleep(5 * time.Millisecond)
	if err := m.Reconcile(context.Background(), live); err != nil {
		t.Fatalf("reconcile pass 2: %v", err)
	}
	remaining := niSecretNames(h)
	for _, want := range []string{"camera-auth", "handbuilt-cam", "openai-key"} {
		if !remaining[want] {
			t.Errorf("secret %q must remain; remaining=%v", want, remaining)
		}
	}
	if remaining["stale-cam"] {
		t.Errorf("stale-cam replica must be pruned after grace; remaining=%v", remaining)
	}

	// The hand-built job's secret must have been replayed (restart cover
	// for references the live binding set does not include).
	posted := map[string]bool{}
	h.mu.Lock()
	for _, p := range h.posts {
		posted[p.name] = true
	}
	h.mu.Unlock()
	if !posted["handbuilt-cam"] {
		t.Errorf("job-only referenced secret must be replayed; posted=%v", posted)
	}
}

// TestReconcilePruneGraceSurvivesDeleteCreateWindow pins defect 2: when an
// old binding is deleted and a new one referencing the same secret appears
// within the grace window (reconcile passes: wanted -> unreferenced ->
// wanted), the replica must never be deleted and no fresh grace is needed.
func TestReconcilePruneGraceSurvivesDeleteCreateWindow(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()
	putSourceSecret(h, "camera-auth", "admin", "a12345678")
	putNIReplica(h, "camera-auth")

	m := h.manager(Config{
		Metadata:   fakeReader{dev: cameraDevice()},
		PruneGrace: time.Minute,
	})
	live := []Binding{{CameraEdgeXName: testCameraName, SecretName: "camera-auth"}}

	if err := m.Reconcile(context.Background(), live); err != nil {
		t.Fatalf("reconcile live: %v", err)
	}
	// Old binding gone, intermediate zero-reference state: arms only.
	if err := m.Reconcile(context.Background(), nil); err != nil {
		t.Fatalf("reconcile unbound: %v", err)
	}
	if len(h.deletes) != 0 {
		t.Fatalf("zero-reference intermediate must never prune: %v", h.deletes)
	}
	// New binding referencing the same name lands: marker cleared, secret
	// re-posted immediately even though grace already elapsed in wall time.
	time.Sleep(2 * time.Millisecond)
	if err := m.Reconcile(context.Background(), live); err != nil {
		t.Fatalf("reconcile rebound: %v", err)
	}
	if len(h.deletes) != 0 {
		t.Fatalf("re-referenced secret must never be pruned: %v", h.deletes)
	}
	if !niSecretNames(h)["camera-auth"] {
		t.Fatal("camera-auth replica must still exist")
	}

	// A genuinely unmanaged secret (foreign, not on the onvif side) never
	// even arms a marker and is never deleted.
	putNIReplica(h, "foreign-key")
	if err := m.Reconcile(context.Background(), nil); err != nil {
		t.Fatalf("reconcile foreign: %v", err)
	}
	if !niSecretNames(h)["foreign-key"] {
		t.Fatal("foreign NI secret must never be touched")
	}
}

// TestEnsureDedupesAndCarriesUsernamePassword pins the apply-hot-path API:
// repeated/blank names post at most once per call and only user/password
// keys cross services.
func TestEnsureDedupesAndCarriesUsernamePassword(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()
	putSourceSecret(h, "camera-auth", "admin", "a12345678")
	h.putKVS(defaultOnvifKVSBase+"/"+writableInsecureSecrets+"/camera-auth/SecretData/mode", "usernametoken")

	m := h.manager(Config{Metadata: fakeReader{dev: cameraDevice()}})
	if err := m.Ensure(context.Background(), []string{" camera-auth ", "camera-auth", "", "  "}); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.posts) != 1 {
		t.Fatalf("posts=%d, want 1", len(h.posts))
	}
	post := h.posts[0]
	if post.name != "camera-auth" || post.data["username"] != "admin" ||
		post.data["password"] != "a12345678" {
		t.Fatalf("unexpected post: %+v", post)
	}
	if _, hasMode := post.data["mode"]; hasMode {
		t.Fatalf("onvif-only mode key must not cross: %v", post.data)
	}
	if _, ok := h.secrets["camera-auth"]; !ok {
		t.Fatal("secret must land in the NI store")
	}
}

// TestEnsurePartialFailureContinues verifies one undeliverable name neither
// aborts the other names nor hides the error from the caller; a later
// successful delivery is logged as a first delivery again.
func TestEnsurePartialFailureContinues(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()
	putSourceSecret(h, "good-secret", "admin", "pw")
	// "missing-secret" exists on neither side: its ensure fails.

	m := h.manager(Config{Metadata: fakeReader{dev: cameraDevice()}})
	err := m.Ensure(context.Background(), []string{"missing-secret", "good-secret"})
	if err == nil {
		t.Fatal("partial failure must return an error")
	}
	h.mu.Lock()
	goodPosted := len(h.posts) == 1 && h.posts[0].name == "good-secret"
	h.mu.Unlock()
	if !goodPosted {
		t.Fatalf("good-secret must still be delivered: %+v", h.posts)
	}

	// Once the source document arrives, a retry succeeds.
	putSourceSecret(h, "missing-secret", "admin", "pw2")
	if err := m.Ensure(context.Background(), []string{"missing-secret"}); err != nil {
		t.Fatalf("retry after source arrives: %v", err)
	}
	h.mu.Lock()
	posted := map[string]bool{}
	for _, p := range h.posts {
		posted[p.name] = true
	}
	h.mu.Unlock()
	if !posted["missing-secret"] {
		t.Fatalf("missing-secret must post on retry: %v", posted)
	}
}

// TestReconcileMissingSourceDocument keeps the name protected (posted again
// later) instead of pruning a replica whose document delivery is pending.
func TestReconcileMissingSourceDocument(t *testing.T) {
	h := newHarness(t)
	defer h.server.Close()
	infBase := defaultInferenceKVSBase
	h.putKVS(infBase+"/"+writableInsecureSecrets+"/camera-auth/SecretName", "camera-auth")

	m := h.manager(Config{Metadata: fakeReader{dev: cameraDevice()}})
	if err := m.Reconcile(context.Background(), []Binding{{CameraEdgeXName: testCameraName, SecretName: "camera-auth"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(h.posts) != 0 {
		t.Fatalf("missing source document must not be posted, got %v", h.posts)
	}
	if len(h.deletes) != 0 {
		t.Fatalf("wanted secret must never be pruned, deletes=%v", h.deletes)
	}
}
