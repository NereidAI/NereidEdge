// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package onvifbind

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	"github.com/nereidai/nereidedge/internal/secrets"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// fakeDevices is an in-memory core-metadata reader.
type fakeDevices struct {
	byName map[string]dtos.Device
	err    error
}

func (f *fakeDevices) DeviceByName(_ context.Context, name string) (dtos.Device, error) {
	if f.err != nil {
		return dtos.Device{}, f.err
	}
	dev, ok := f.byName[name]
	if !ok {
		return dtos.Device{}, errors.New("not found")
	}
	return dev, nil
}

func onvifDevice(name, mac string) dtos.Device {
	return dtos.Device{
		Name: name,
		Protocols: map[string]dtos.ProtocolProperties{
			"Other": {"x": "y"},
			"Onvif": {"MACAddress": mac, "Address": "10.0.0.1"},
		},
	}
}

func camEntry(name string, revision int64, selectors []device.SecretSelector, doc map[string]any) device.SecretEntry {
	return device.SecretEntry{
		Name:      name,
		Type:      onvifSecretType,
		Revision:  revision,
		Selectors: selectors,
		Document:  doc,
	}
}

// record keeps the interesting requests a fake EdgeX endpoint received.
type record struct {
	mu   sync.Mutex
	urls []string
	body []map[string]any
}

func (r *record) add(rawURL string, rawBody []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.urls = append(r.urls, rawURL)
	if len(rawBody) == 0 {
		r.body = append(r.body, nil)
		return
	}
	var decoded map[string]any
	_ = json.Unmarshal(rawBody, &decoded)
	r.body = append(r.body, decoded)
}

func (r *record) last() (string, map[string]any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.urls) == 0 {
		return "", nil, false
	}
	return r.urls[len(r.urls)-1], r.body[len(r.body)-1], true
}

func (r *record) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.urls)
}

// harness wires a binder against in-process device-service and keeper stubs.
type harness struct {
	binder *Onvif
	dev    *record
	keeper *record
	ds     *httptest.Server
	kp     *httptest.Server
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	devRec := &record{}
	ds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		devRec.add(req.Method+" "+req.URL.RequestURI(), body)
		_, _ = w.Write([]byte(`{"apiVersion":"v3","statusCode":201}`))
	}))
	t.Cleanup(ds.Close)

	kRec := &record{}
	kp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		kRec.add(req.Method+" "+req.URL.RequestURI(), body)
		if req.Method == http.MethodDelete && !strings.Contains(req.URL.RawQuery, "prefixMatch=true") {
			// Simulate an already-removed plain key mapping.
			_, _ = w.Write([]byte(`{"apiVersion":"v3","statusCode":404}`))
			return
		}
		_, _ = w.Write([]byte(`{"apiVersion":"v3","statusCode":200}`))
	}))
	t.Cleanup(kp.Close)

	b := New(Config{
		DeviceServiceURL: ds.URL,
		KeeperURL:        kp.URL,
		KVSBase:          "edgex/v4/device-onvif-camera",
		Devices:          &fakeDevices{byName: map[string]dtos.Device{"cam1": onvifDevice("cam1", "AA:BB:CC:DD:EE:FF")}},
		LookupName:       func(ref string) (string, bool) { return "cam1", ref == "uuid-1" },
		Logger:           logger.NewMockClient(),
	})
	return &harness{binder: b, dev: devRec, keeper: kRec, ds: ds, kp: kp}
}

func TestApplyIgnoresNonOnvifType(t *testing.T) {
	h := newHarness(t)
	entry := device.SecretEntry{Name: "other", Type: "mqtt-basic", Revision: 1,
		Document: map[string]any{"password": "x"}}
	if err := h.binder.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if h.dev.count() != 0 || h.keeper.count() != 0 {
		t.Fatal("non-onvif secret hit the device service or keeper")
	}
}

func TestApplyDeviceSelectorBindsMAC(t *testing.T) {
	h := newHarness(t)
	entry := camEntry("cred", 1, []device.SecretSelector{{Kind: "device", Value: "cam1"}},
		map[string]any{"username": "admin", "password": "hunter2"})
	if err := h.binder.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if u, body, ok := h.dev.last(); !ok || u != "POST /api/v3/secret" {
		t.Fatalf("secret store call=%v(body %v)", ok, u)
	} else if data := body["secretData"].([]any); len(data) < 2 {
		t.Fatalf("secretData=%v", body["secretData"])
	}

	u, body, ok := h.keeper.last()
	if !ok || u != "PUT /api/v3/kvs/key/edgex/v4/device-onvif-camera/AppCustom/CredentialsMap/cred?flatten=false" {
		t.Fatalf("keeper map call=%v", u)
	}
	if got := body["value"]; got != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("mapped MAC=%v, want lowercase colon form", got)
	}
}

func TestApplyMACSelectorVerbatim(t *testing.T) {
	h := newHarness(t)
	entry := camEntry("cred", 1, []device.SecretSelector{{Kind: "mac", Value: "AA-BB-CC-DD-EE-11"}},
		map[string]any{"username": "admin", "password": "p"})
	if err := h.binder.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_, body, ok := h.keeper.last()
	if !ok {
		t.Fatal("no keeper write")
	}
	if got := body["value"]; got != "aa:bb:cc:dd:ee:11" {
		t.Fatalf("mapped MAC=%v", got)
	}
}

func TestApplyCommaSeparatedMACSelector(t *testing.T) {
	h := newHarness(t)
	entry := camEntry("cred", 1,
		[]device.SecretSelector{{Kind: "mac", Value: "AA-BB-CC-DD-EE-11, 11:22:33:44:55:66"}},
		map[string]any{"username": "admin", "password": "p"})
	if err := h.binder.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_, body, ok := h.keeper.last()
	if !ok {
		t.Fatal("no keeper write")
	}
	if got := body["value"]; got != "aa:bb:cc:dd:ee:11,11:22:33:44:55:66" {
		t.Fatalf("mapped MACs=%v", got)
	}
}

func TestSecretTypeAdverts(t *testing.T) {
	adverts := SecretTypeAdverts()
	if len(adverts) != 1 {
		t.Fatalf("want exactly 1 advert, got %d", len(adverts))
	}
	a := adverts[0]
	if a.Type != "onvif-basic" || a.TypeVersion != "1.0" {
		t.Fatalf("advert = %+v", a)
	}
	if !json.Valid(a.FieldsSchema) {
		t.Fatalf("fields_schema is not valid JSON: %s", a.FieldsSchema)
	}
	var schema map[string]any
	if err := json.Unmarshal(a.FieldsSchema, &schema); err != nil {
		t.Fatal(err)
	}
	required, ok := schema["required"].([]any)
	if !ok {
		t.Fatal("schema.required not a list")
	}
	if len(required) != 2 || required[0] != "username" || required[1] != "password" {
		t.Fatalf("schema.required = %v", required)
	}
	if a.DisplayName["en"] == "" || a.DisplayName["zh"] == "" {
		t.Fatalf("display_name = %v", a.DisplayName)
	}
}

func TestApplyResolvesNLUUIDViaLookup(t *testing.T) {
	h := newHarness(t)
	entry := camEntry("cred", 1, []device.SecretSelector{{Kind: "device", Value: "uuid-1"}},
		map[string]any{"username": "admin", "password": "p"})
	if err := h.binder.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_, body, _ := h.keeper.last()
	if got := body["value"]; got != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("mapped MAC=%v", got)
	}
}

func TestApplyMultiSelectorJoinsMACs(t *testing.T) {
	h := newHarness(t)
	devices := &fakeDevices{byName: map[string]dtos.Device{
		"cam1": onvifDevice("cam1", "aa:bb:cc:dd:ee:ff"),
		"cam2": onvifDevice("cam2", "11:22:33:44:55:66"),
	}}
	b := New(Config{DeviceServiceURL: h.ds.URL, KeeperURL: h.kp.URL,
		Devices: devices, Logger: logger.NewMockClient()})
	entry := camEntry("cred", 1, []device.SecretSelector{
		{Kind: "device", Value: "cam1"}, {Kind: "device", Value: "cam2"},
	}, map[string]any{"username": "admin", "password": "p"})
	if err := b.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_, body, _ := h.keeper.last()
	if got := body["value"]; got != "aa:bb:cc:dd:ee:ff,11:22:33:44:55:66" {
		t.Fatalf("mapped MACs=%v", got)
	}
}

func TestApplyNoSelectorsSetsDefault(t *testing.T) {
	h := newHarness(t)
	entry := camEntry("cred", 1, nil, map[string]any{"username": "admin", "password": "p"})
	if err := h.binder.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	u, body, _ := h.keeper.last()
	if !strings.Contains(u, "AppCustom/DefaultSecretName") {
		t.Fatalf("default write went to %q", u)
	}
	if got := body["value"]; got != "cred" {
		t.Fatalf("default secret name=%v", got)
	}
}

func TestApplyUnresolvableSelectorIsErrNoTarget(t *testing.T) {
	h := newHarness(t)
	entry := camEntry("cred", 1, []device.SecretSelector{{Kind: "device", Value: "ghost"}},
		map[string]any{"username": "admin", "password": "p"})
	err := h.binder.Apply(context.Background(), entry)
	if !errors.Is(err, secrets.ErrNoTarget) {
		t.Fatalf("err=%v, want ErrNoTarget", err)
	}
	if h.dev.count() != 0 || h.keeper.count() != 0 {
		t.Fatal("failed resolution still bound or stored")
	}
}

func TestApplyMissingCredentialsRejected(t *testing.T) {
	h := newHarness(t)
	entry := camEntry("cred", 1, nil, map[string]any{"username": "admin"})
	if err := h.binder.Apply(context.Background(), entry); err == nil {
		t.Fatal("document without password was accepted")
	}
}

func TestApplyDefaultsMode(t *testing.T) {
	h := newHarness(t)
	entry := camEntry("cred", 1, nil, map[string]any{"username": "admin", "password": "p"})
	if err := h.binder.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_, body, ok := h.dev.last()
	if !ok {
		t.Fatal("no secret store call")
	}
	data := body["secretData"].([]any)
	var gotMode string
	for _, kv := range data {
		pair := kv.(map[string]any)
		if pair["key"] == "mode" {
			gotMode = pair["value"].(string)
		}
	}
	if gotMode != "usernametoken" {
		t.Fatalf("mode=%q, want default usernametoken", gotMode)
	}
}

func TestRemoveDeletesMappingAndSecret(t *testing.T) {
	h := newHarness(t)
	if err := h.binder.Remove(context.Background(), "cred"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if h.keeper.count() != 2 {
		t.Fatalf("keeper deletes=%d, want 2", h.keeper.count())
	}
	urls := h.keeper.urls
	var mapping, secret bool
	for _, u := range urls {
		if strings.HasPrefix(u, "DELETE ") &&
			strings.Contains(u, "AppCustom/CredentialsMap/cred") {
			mapping = true
		}
		if strings.HasPrefix(u, "DELETE ") &&
			strings.Contains(u, "Writable/InsecureSecrets/cred?prefixMatch=true") {
			secret = true
		}
	}
	if !mapping || !secret {
		t.Fatalf("deletes missing mapping=%v secret=%v: %v", mapping, secret, urls)
	}
}

func TestRemoveMissingMappingIsIdempotent(t *testing.T) {
	h := newHarness(t)
	// The keeper stub replies 404 on the first non-prefix DELETE, which is
	// exactly the already-gone case; removal must still report success.
	if err := h.binder.Remove(context.Background(), "cred"); err != nil {
		t.Fatalf("Remove of missing mapping: %v", err)
	}
}
