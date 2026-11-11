// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package inferencebridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"

	"github.com/nereidai/nereidedge/internal/corecmd"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// modelsEvent builds the core-command EventResponse envelope carrying one
// ModelsJSON String reading, exactly as NereidInference answers.
func modelsEvent(t *testing.T, rawJSON string) responses.EventResponse {
	t.Helper()
	reading, err := dtos.NewSimpleReading("nereid-inference", "nereid-inference-manager",
		"ModelsJSON", common.ValueTypeString, rawJSON)
	if err != nil {
		t.Fatal(err)
	}
	return responses.NewEventResponse("req", "", http.StatusOK,
		dtos.Event{ProfileName: "nereid-inference-manager", Readings: []dtos.BaseReading{reading}})
}

func newTestBridge(t *testing.T, negotiated func() bool, handler http.HandlerFunc) (*Bridge, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	var changes atomic.Int32
	b := New(Config{
		Core:          httpCore{corecmd.New(srv.URL, 0)},
		Interval:      time.Hour,
		Negotiated:    negotiated,
		OnChange:      func() { changes.Add(1) },
		ManagerDevice: "nereid-inference",
		Resource:      "ModelsJSON",
	})
	return b, &hits, &changes
}

// httpCore adapts an httptest base URL to the CoreReader interface using the
// real corecmd.Client, exercising URL/query construction end to end.
type httpCore struct {
	*corecmd.Client
}

func serveModels(t *testing.T, w http.ResponseWriter, modelsJSON string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(modelsEvent(t, modelsJSON)); err != nil {
		t.Fatal(err)
	}
}

// TestBridgeSkipsReadsBeforeNegotiation is the byte-identical old-firmware
// guarantee: without an accepted ack the manager device is never polled and no
// inventory segment is ever provided.
func TestBridgeSkipsReadsBeforeNegotiation(t *testing.T) {
	b, hits, _ := newTestBridge(t, func() bool { return false }, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("manager device must not be read before negotiation")
	})

	b.Refresh(context.Background())
	b.Refresh(context.Background())

	if got := hits.Load(); got != 0 {
		t.Fatalf("core-command hits=%d, want 0 before negotiation", got)
	}
	if _, ok := b.Models(); ok {
		t.Fatal("Models() available before negotiation; segment would appear on the wire")
	}
	status := b.Status()
	if status.State != runtimeOffline {
		t.Fatalf("state=%q, want OFFLINE while un-negotiated", status.State)
	}
}

func TestBridgeInventoryAndRuntimeAggregate(t *testing.T) {
	inventory := `[
		{"id":"person","version":"1.0.0","task":"detection","backend":"kserve","state":"READY","active_bindings":2},
		{"id":"car","version":"2.1.0","task":"detection","backend":"openai","state":"FAILED","reason":"model-not-ready","active_bindings":0}
	]`
	b, _, changes := newTestBridge(t, func() bool { return true },
		func(w http.ResponseWriter, r *http.Request) { serveModels(t, w, inventory) })

	b.Refresh(context.Background())

	models, ok := b.Models()
	if !ok {
		t.Fatal("Models() unavailable after successful read")
	}
	if len(models) != 2 {
		t.Fatalf("models=%d, want 2", len(models))
	}
	if models[0].ID != "person" || models[0].State != device.ModelStateReady {
		t.Fatalf("first model = %+v", models[0])
	}
	if changes.Load() == 0 {
		t.Fatal("OnChange did not fire for fresh inventory")
	}
	status := b.Status()
	if status.State != runtimeOnline {
		t.Fatalf("state=%q, want ONLINE with >=1 READY", status.State)
	}
	if status.ModelsReady != 1 || status.ModelsTotal != 2 || status.ActiveBindings != 2 {
		t.Fatalf("aggregate=%+v", status)
	}
	if status.Protocol != "kserve" {
		t.Fatalf("protocol=%q, want kserve priority", status.Protocol)
	}
	if m, ready := b.LookupModel("person", "1.0.0"); !ready || m.ID != "person" {
		t.Fatal("READY model lookup failed")
	}
	if _, ready := b.LookupModel("car", "2.1.0"); ready {
		t.Fatal("FAILED model must not report ready")
	}
}

// TestBridgeReadFailureKeepsInventoryButOmitsSegment: a transient outage must
// never be authoritative (no empty-array prune); the cached payload survives
// but Models() flips to unavailable until the next good read.
func TestBridgeReadFailureKeepsInventoryButOmitsSegment(t *testing.T) {
	inventory := `[{"id":"person","version":"1.0.0","task":"detection","backend":"openai","state":"READY","active_bindings":1}]`
	failing := true
	b, _, changes := newTestBridge(t, func() bool { return true }, func(w http.ResponseWriter, r *http.Request) {
		if failing {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		serveModels(t, w, inventory)
	})

	// Seed good data.
	failing = false
	b.Refresh(context.Background())
	if _, ok := b.Models(); !ok {
		t.Fatal("seed inventory unavailable")
	}
	baseline := changes.Load()

	// One failing read must keep the cached models but stop providing them.
	failing = true
	b.Refresh(context.Background())
	if models, ok := b.Models(); ok {
		t.Fatalf("Models() available after failed read (len=%d); NL could be pruned", len(models))
	}
	if status := b.Status(); status.State != runtimeOffline {
		t.Fatalf("state after failure=%q, want OFFLINE", status.State)
	}
	if changes.Load() <= baseline {
		t.Fatal("reachability change did not notify shadow/manifest")
	}

	// Recovery re-publishes the same cached-looking payload.
	failing = false
	b.Refresh(context.Background())
	models, ok := b.Models()
	if !ok || len(models) != 1 || models[0].ID != "person" {
		t.Fatalf("recovered inventory=%v ok=%v", models, ok)
	}
}

func TestBridgeExplicitEmptyInventory(t *testing.T) {
	b, _, _ := newTestBridge(t, func() bool { return true },
		func(w http.ResponseWriter, r *http.Request) { serveModels(t, w, `[]`) })

	b.Refresh(context.Background())
	models, ok := b.Models()
	if !ok {
		t.Fatal("explicit [] must be authoritative (available=true)")
	}
	if len(models) != 0 {
		t.Fatalf("models=%v, want empty slice", models)
	}
	if models == nil {
		t.Fatal("zero inventory returned a nil slice: manifest pointer would encode as null, " +
			"NL treats neither key nor [] and keeps stale rows")
	}
	// The wire frame must carry the literal "models":[] — not "models":null.
	frame := struct {
		Models []device.ManifestModel `json:"models"`
	}{Models: models}
	raw, _ := json.Marshal(frame)
	if string(raw) != `{"models":[]}` {
		t.Fatalf("frame=%s, want {\"models\":[]}", raw)
	}
	if status := b.Status(); status.ModelsTotal != 0 || status.State != runtimeUnknown {
		t.Fatalf("status=%+v, want zero inventory UNKNOWN", status)
	}
}

// TestApplyBindingCountsOverridesDSNumbers verifies the NE-owned live spec
// count replaces the DS aggregate (which includes hand-built/orphan jobs)
// and floating specs are attributed only when the id has a single version.
func TestApplyBindingCounts(t *testing.T) {
	models := []device.ManifestModel{
		{ID: "person", Version: "1.0.0", State: device.ModelStateReady, ActiveBindings: 4},
		{ID: "person", Version: "2.0.0", State: device.ModelStateReady, ActiveBindings: 0},
		{ID: "car", Version: "9", State: device.ModelStateReady, ActiveBindings: 7},
	}
	counts := map[string]int{
		"person\x001.0.0": 1, // one pinned live binding
		"car\x00":         1, // floating: one version installed -> attributed
		"ghost\x001":      1, // not in inventory: dropped
	}
	got := ApplyBindingCounts(models, counts)
	want := map[string]int{
		"person/1.0.0": 1,
		"person/2.0.0": 0,
		"car/9":        1,
	}
	for _, m := range got {
		if n := want[m.ID+"/"+m.Version]; m.ActiveBindings != n {
			t.Errorf("%s/%s active_bindings=%d, want %d", m.ID, m.Version, m.ActiveBindings, n)
		}
	}

	// Floating across multiple versions cannot be assigned: leave both at 0
	// rather than guessing.
	multi := []device.ManifestModel{
		{ID: "person", Version: "1.0.0"},
		{ID: "person", Version: "2.0.0"},
	}
	ambiguous := ApplyBindingCounts(multi, map[string]int{"person\x00": 1})
	for _, m := range ambiguous {
		if m.ActiveBindings != 0 {
			t.Errorf("floating binding assigned to %s/%s (%d); must be dropped on multi-version inventory",
				m.ID, m.Version, m.ActiveBindings)
		}
	}

	// The input slice is not mutated and zero inventory stays a no-op.
	if models[0].ActiveBindings != 4 {
		t.Fatal("ApplyBindingCounts mutated the input inventory")
	}
	if out := ApplyBindingCounts(nil, counts); out != nil {
		t.Fatalf("nil inventory produced %v, want nil", out)
	}
}

func TestBridgeNoReadyModelsIsDegraded(t *testing.T) {
	inventory := `[{"id":"car","version":"1","task":"detection","backend":"openai","state":"VALIDATING","active_bindings":0}]`
	b, _, _ := newTestBridge(t, func() bool { return true },
		func(w http.ResponseWriter, r *http.Request) { serveModels(t, w, inventory) })

	b.Refresh(context.Background())
	if status := b.Status(); status.State != runtimeDegraded || status.Protocol != "openai" {
		t.Fatalf("status=%+v, want DEGRADED/openai", status)
	}
}

func TestBridgeRejectsMalformedInventory(t *testing.T) {
	b, _, _ := newTestBridge(t, func() bool { return true },
		func(w http.ResponseWriter, r *http.Request) { serveModels(t, w, `[{"id":"x"}]`) })

	b.Refresh(context.Background())
	if _, ok := b.Models(); ok {
		t.Fatal("entry missing version must not become inventory")
	}
}
