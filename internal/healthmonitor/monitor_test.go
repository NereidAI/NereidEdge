// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package healthmonitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"

	"github.com/nereidai/nereidedge/internal/corecmd"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// fakeCore emulates the core-command API with controllable probe failure.
type fakeCore struct {
	mu            sync.Mutex
	fail          bool
	failCommands  map[string]bool
	empty         bool
	emptyCommands map[string]bool
	probes        []string
	requests      int
	commands      []dtos.CoreCommand
	edgexName     string
}

func newFakeCore(edgexName string, commands ...dtos.CoreCommand) *fakeCore {
	return &fakeCore{edgexName: edgexName, commands: commands, failCommands: make(map[string]bool), emptyCommands: make(map[string]bool)}
}

func (f *fakeCore) setFail(v bool) {
	f.mu.Lock()
	f.fail = v
	f.mu.Unlock()
}

func (f *fakeCore) setCommandFail(command string, v bool) {
	f.mu.Lock()
	f.failCommands[command] = v
	f.mu.Unlock()
}

func (f *fakeCore) setEmpty(v bool) {
	f.mu.Lock()
	f.empty = v
	f.mu.Unlock()
}

func (f *fakeCore) probesHit() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.probes))
	copy(out, f.probes)
	return out
}

func (f *fakeCore) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func (f *fakeCore) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		fail := f.fail
		f.requests++
		f.mu.Unlock()

		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/device/name/"), "/")
		switch len(parts) {
		case 1:
			writeJSON(w, http.StatusOK, responses.DeviceCoreCommandResponse{
				BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
				DeviceCoreCommand: dtos.DeviceCoreCommand{
					DeviceName:   f.edgexName,
					CoreCommands: f.commands,
				},
			})
		case 2:
			f.mu.Lock()
			f.probes = append(f.probes, parts[1])
			cmdFail := f.failCommands[parts[1]]
			empty := f.empty || f.emptyCommands[parts[1]]
			f.mu.Unlock()
			if fail || cmdFail {
				writeJSON(w, http.StatusInternalServerError, responses.EventResponse{
					BaseResponse: dtoCommon.NewBaseResponse("", "command unsupported", http.StatusInternalServerError),
				})
				return
			}
			if r.URL.Query().Get("ds-pushevent") != "false" || r.URL.Query().Get("ds-returnevent") != "true" {
				http.Error(w, "probe must suppress the bus push and return the event", http.StatusBadRequest)
				return
			}
			if empty {
				writeJSON(w, http.StatusOK, responses.EventResponse{
					BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
				})
				return
			}
			writeJSON(w, http.StatusOK, responses.EventResponse{
				BaseResponse: dtoCommon.NewBaseResponse("", "", http.StatusOK),
				Event: dtos.Event{
					DeviceName: f.edgexName,
					Readings:   []dtos.BaseReading{{ResourceName: parts[1]}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// recorder collects presence announcements driven by the monitor.
type recorder struct {
	connectCh    chan string
	disconnectCh chan string
	notifyCh     chan struct{}
}

func newRecorder() *recorder {
	return &recorder{
		connectCh:    make(chan string, 32),
		disconnectCh: make(chan string, 32),
		notifyCh:     make(chan struct{}, 32),
	}
}

func (r *recorder) connect(_ context.Context, uuid string) error {
	select {
	case r.connectCh <- uuid:
	default:
	}
	return nil
}

func (r *recorder) disconnect(_ context.Context, uuid string) error {
	select {
	case r.disconnectCh <- uuid:
	default:
	}
	return nil
}

func (r *recorder) notify() {
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

func newMonitor(t *testing.T, srv *httptest.Server, lookup ProfileLookup) (*Monitor, *recorder) {
	t.Helper()
	rec := newRecorder()
	m := New(Config{
		Core:          corecmd.New(srv.URL, 0),
		LookupProfile: lookup,
		Connect:       rec.connect,
		Disconnect:    rec.disconnect,
		Notify:        rec.notify,
		GlobalEnabled: true,
		ProbeTimeout:  2 * time.Second,
	})
	return m, rec
}

func waitChan(t *testing.T, ch <-chan string, what string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return ""
	}
}

func assertEmpty(t *testing.T, ch <-chan string, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %s", what, v)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestLocalDeviceDetachAlwaysWithdrawsPresence pins defect 3: a local
// spec device (tags.local=true, e.g. the virtual inference output) is
// announced online immediately; deleting its spec must always withdraw it —
// the old code only set online=true on the probe path, so OnDetach skipped
// disconnect and the UUID lingered in manifest devices[].
func TestLocalDeviceDetachAlwaysWithdrawsPresence(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("local devices must never be probed: %s", r.URL.Path)
	}))
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("inference-spec-1", "uuid-virtual-1", "detection/v1", "", true, nil)
	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-virtual-1" {
		t.Fatalf("connect uuid = %q, want uuid-virtual-1", got)
	}

	m.OnDetach("inference-spec-1")
	if got := waitChan(t, rec.disconnectCh, "disconnect"); got != "uuid-virtual-1" {
		t.Fatalf("detach disconnect uuid = %q, want uuid-virtual-1", got)
	}

	// A second detach is a no-op (no duplicate disconnect).
	m.OnDetach("inference-spec-1")
	assertEmpty(t, rec.disconnectCh, "duplicate disconnect")
}

func TestOnAttachReachableAnnouncesOnline(t *testing.T) {
	fake := newFakeCore("camera-109", dtos.CoreCommand{Name: "DeviceInformation", Get: true})
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)

	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}
	assertEmpty(t, rec.disconnectCh, "disconnect")
	if hits := fake.probesHit(); len(hits) != 1 || hits[0] != "DeviceInformation" {
		t.Errorf("probes = %v", hits)
	}
}

func TestOnAttachUnreachableStaysOfflineThenSweepRecovers(t *testing.T) {
	fake := newFakeCore("camera-109", dtos.CoreCommand{Name: "DeviceInformation", Get: true})
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	fake.setFail(true)
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)
	assertEmpty(t, rec.connectCh, "connect")

	fake.setFail(false)
	m.sweep(context.Background())
	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}
}

func TestThresholdWithdrawsThenRecovers(t *testing.T) {
	fake := newFakeCore("camera-109", dtos.CoreCommand{Name: "DeviceInformation", Get: true})
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)
	if got := waitChan(t, rec.connectCh, "initial connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}

	fake.setFail(true)
	m.sweep(context.Background())
	m.sweep(context.Background())
	assertEmpty(t, rec.disconnectCh, "early disconnect")
	m.sweep(context.Background())
	if got := waitChan(t, rec.disconnectCh, "disconnect"); got != "uuid-1" {
		t.Errorf("disconnect uuid = %q", got)
	}

	fake.setFail(false)
	m.sweep(context.Background())
	if got := waitChan(t, rec.connectCh, "reconnect"); got != "uuid-1" {
		t.Errorf("reconnect uuid = %q", got)
	}
}

func TestLocalDeviceConnectsWithoutProbe(t *testing.T) {
	fake := newFakeCore("gpio-7", dtos.CoreCommand{Name: "Level", Get: true})
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("gpio-7", "uuid-gpio", "gpio-profile", "", true, nil)

	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-gpio" {
		t.Errorf("connect uuid = %q", got)
	}
	if got := fake.requestCount(); got != 0 {
		t.Errorf("local device issued %d HTTP requests, want 0", got)
	}
}

func TestOnDetachWithdrawsOnlineDevice(t *testing.T) {
	fake := newFakeCore("camera-109", dtos.CoreCommand{Name: "DeviceInformation", Get: true})
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)
	waitChan(t, rec.connectCh, "connect")

	m.OnDetach("camera-109")
	if got := waitChan(t, rec.disconnectCh, "disconnect"); got != "uuid-1" {
		t.Errorf("disconnect uuid = %q", got)
	}
}

func TestFirstCandidateUnsupportedFallsBack(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "DeviceInformation", Get: true},
		dtos.CoreCommand{Name: "Profiles", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	fake.setCommandFail("DeviceInformation", true)
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)

	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}
	hits := fake.probesHit()
	if len(hits) < 2 || hits[0] != "DeviceInformation" || hits[1] != "Profiles" {
		t.Errorf("probes = %v, want DeviceInformation then Profiles fallback", hits)
	}

	fake.probes = nil
	m.sweep(context.Background())
	steady := fake.probesHit()
	if len(steady) != 1 || steady[0] != "Profiles" {
		t.Errorf("steady-state probes = %v, want cached winner \"Profiles\" only", steady)
	}
}

func TestCachedWinnerFailsSwitchesWithoutOffline(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "DeviceInformation", Get: true},
		dtos.CoreCommand{Name: "Profiles", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)
	waitChan(t, rec.connectCh, "connect")

	// The cached winner (DeviceInformation, the first candidate) stops
	// working; the alternative (Profiles) must take over without crossing
	// the failure threshold and announcing offline.
	fake.setCommandFail("DeviceInformation", true)
	m.sweep(context.Background())
	assertEmpty(t, rec.disconnectCh, "disconnect")
	hits := fake.probesHit()
	if len(hits) < 2 || hits[len(hits)-1] != "Profiles" {
		t.Errorf("probes = %v, want fallback to Profiles", hits)
	}

	fake.probes = nil
	m.sweep(context.Background())
	steady := fake.probesHit()
	if len(steady) != 1 || steady[0] != "Profiles" {
		t.Errorf("steady-state probes = %v, want switched winner \"Profiles\" only", steady)
	}
}

func TestSpecProbeResourcePreferred(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "DeviceInformation", Get: true},
		dtos.CoreCommand{Name: "Profiles", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	// DeviceInformation is unsupported on this device; the spec designates
	// Profiles as the baseline, which must be tried first.
	fake.setCommandFail("DeviceInformation", true)
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "Profiles", false, nil)

	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}
	hits := fake.probesHit()
	if len(hits) != 1 || hits[0] != "Profiles" {
		t.Errorf("probes = %v, want designated \"Profiles\" only", hits)
	}
}

func TestUnknownSpecProbeFallsBack(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "DeviceInformation", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "NotACommand", false, nil)

	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}
	hits := fake.probesHit()
	if len(hits) != 1 || hits[0] != "DeviceInformation" {
		t.Errorf("probes = %v, want fallback \"DeviceInformation\"", hits)
	}
}

func TestMarkedResourcePreferred(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "DeviceInformation", Get: true},
		dtos.CoreCommand{Name: "Health", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	lookup := func(_ context.Context, _ string) (dtos.DeviceProfile, error) {
		return dtos.DeviceProfile{
			DeviceResources: []dtos.DeviceResource{{
				Name:       "Health",
				Properties: dtos.ResourceProperties{ValueType: "String", ReadWrite: "R"},
				Attributes: map[string]any{"healthcheck": true},
			}},
		}, nil
	}
	m, rec := newMonitor(t, srv, lookup)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)
	waitChan(t, rec.connectCh, "connect")

	hits := fake.probesHit()
	if len(hits) == 0 || hits[0] != "Health" {
		t.Errorf("probes = %v, want first command \"Health\"", hits)
	}
}

func TestMarkedAttribute(t *testing.T) {
	cases := map[string]bool{
		"true":  true,
		"TRUE":  true,
		"yes":   true,
		"1":     true,
		"false": false,
		"no":    false,
	}
	for raw, want := range cases {
		if got := marked(map[string]any{"healthcheck": raw}); got != want {
			t.Errorf("marked(%q) = %v, want %v", raw, got, want)
		}
	}
	if !marked(map[string]any{"healthcheck": true}) {
		t.Error("marked(bool true) = false")
	}
	if marked(map[string]any{"healthcheck": 42}) {
		t.Error("marked(int) = true")
	}
	if marked(map[string]any{}) {
		t.Error("marked(empty) = true")
	}
}

// makeHC builds a per-instance probe policy for tests.
func makeHC(resource string, intervalMS, timeoutMS, threshold int) *device.ConnectorHealthCheck {
	return &device.ConnectorHealthCheck{
		Resource:      resource,
		IntervalMS:    intervalMS,
		TimeoutMS:     timeoutMS,
		FailThreshold: threshold,
	}
}

func TestInstanceHCReachableAnnouncesOnline(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "DeviceInformation", Get: true},
		dtos.CoreCommand{Name: "Status", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, makeHC("Status", 30000, 2000, 3))

	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}
	assertEmpty(t, rec.disconnectCh, "disconnect")
	hits := fake.probesHit()
	if len(hits) != 1 || hits[0] != "Status" {
		t.Errorf("probes = %v, want only designated \"Status\"", hits)
	}
}

func TestInstanceHCThresholdDisconnectAndRecover(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "DeviceInformation", Get: true},
		dtos.CoreCommand{Name: "Status", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	hc := makeHC("Status", 30000, 2000, 2)
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, hc)
	waitChan(t, rec.connectCh, "connect")

	m.mu.Lock()
	gen := m.devices["camera-109"].generation
	m.mu.Unlock()

	fake.setFail(true)
	m.instanceProbe("camera-109", gen, makePolicy(hc))
	assertEmpty(t, rec.disconnectCh, "early disconnect")
	m.instanceProbe("camera-109", gen, makePolicy(hc))
	if got := waitChan(t, rec.disconnectCh, "disconnect"); got != "uuid-1" {
		t.Errorf("disconnect uuid = %q", got)
	}

	fake.setFail(false)
	m.instanceProbe("camera-109", gen, makePolicy(hc))
	if got := waitChan(t, rec.connectCh, "reconnect"); got != "uuid-1" {
		t.Errorf("reconnect uuid = %q", got)
	}
}

func TestInstanceHCPolicyReplacementStopsOldLoop(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "Status", Get: true},
		dtos.CoreCommand{Name: "Ping", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, _ := newMonitor(t, srv, nil)

	hc1 := makeHC("Status", 30000, 2000, 3)
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, hc1)
	m.mu.Lock()
	oldStop := m.devices["camera-109"].stop
	oldGen := m.devices["camera-109"].generation
	m.mu.Unlock()

	hc2 := makeHC("Ping", 15000, 1000, 2)
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, hc2)
	m.mu.Lock()
	newStop := m.devices["camera-109"].stop
	newGen := m.devices["camera-109"].generation
	m.mu.Unlock()

	if oldStop == newStop {
		t.Fatal("loop stop channel not replaced")
	}
	if newGen == oldGen {
		t.Fatal("generation not bumped on policy replacement")
	}
	select {
	case <-oldStop:
	default:
		t.Fatal("old instance loop not stopped")
	}

	// Let the new loop's immediate probe complete while the device is still
	// reachable, so it cannot be mistaken for the stale probe below.
	want := fake.requestCount() + 1
	deadline := time.Now().Add(time.Second)
	for fake.requestCount() < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fake.requestCount() < want {
		t.Fatal("new loop immediate probe did not run")
	}

	// A stale probe from the superseded generation must not drive state.
	fake.setFail(true)
	m.instanceProbe("camera-109", oldGen, makePolicy(hc1))
	m.mu.Lock()
	failures := m.devices["camera-109"].failures
	m.mu.Unlock()
	if failures != 0 {
		t.Errorf("stale generation probe counted failures = %d, want 0", failures)
	}
}

func TestInstanceHCRemovedFallsBackToLegacy(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "DeviceInformation", Get: true},
		dtos.CoreCommand{Name: "Status", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, makeHC("Status", 30000, 2000, 3))
	waitChan(t, rec.connectCh, "connect")

	// Drop health_check: the instance loop stops and the legacy global regime
	// re-probes via candidate enumeration.
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)

	m.mu.Lock()
	st := m.devices["camera-109"]
	stopped := st.stop == nil
	hcCleared := st.hc == nil
	m.mu.Unlock()
	if !stopped {
		t.Error("instance loop still running after health_check removal")
	}
	if !hcCleared {
		t.Error("health_check policy not cleared")
	}
	assertEmpty(t, rec.disconnectCh, "disconnect during policy switch")
}

func TestInstanceHCDetachStopsLoop(t *testing.T) {
	fake := newFakeCore("camera-109",
		dtos.CoreCommand{Name: "Status", Get: true},
	)
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, makeHC("Status", 30000, 2000, 3))
	waitChan(t, rec.connectCh, "connect")

	m.OnDetach("camera-109")
	if got := waitChan(t, rec.disconnectCh, "disconnect"); got != "uuid-1" {
		t.Errorf("disconnect uuid = %q", got)
	}
	m.mu.Lock()
	_, present := m.devices["camera-109"]
	m.mu.Unlock()
	if present {
		t.Error("device state not removed on detach")
	}
}

// TestInstanceHCInconclusiveDoesNotFlap verifies a 200 without readings
// neither announces online nor counts toward the failure threshold; a later
// real failure still trips the threshold, and a real reading recovers.
func TestInstanceHCInconclusiveDoesNotFlap(t *testing.T) {
	fake := newFakeCore("camera-109", dtos.CoreCommand{Name: "Status", Get: true})
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	hc := makeHC("Status", 30000, 2000, 2)
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, hc)
	if got := waitChan(t, rec.connectCh, "initial connect"); got != "uuid-1" {
		t.Fatalf("initial connect uuid = %q", got)
	}
	m.mu.Lock()
	gen := m.devices["camera-109"].generation
	m.mu.Unlock()

	// Inconclusive probes (200, no readings) must not count failures nor
	// withdraw presence, even well past the failure threshold.
	fake.setEmpty(true)
	for i := 0; i < 5; i++ {
		m.instanceProbe("camera-109", gen, makePolicy(hc))
	}
	assertEmpty(t, rec.disconnectCh, "disconnect on inconclusive probes")
	m.mu.Lock()
	failures := m.devices["camera-109"].failures
	online := m.devices["camera-109"].online
	m.mu.Unlock()
	if failures != 0 || !online {
		t.Fatalf("after inconclusive probes failures=%d online=%v, want 0/true", failures, online)
	}

	// Real failures still count from zero and trip the threshold.
	fake.setEmpty(false)
	fake.setFail(true)
	m.instanceProbe("camera-109", gen, makePolicy(hc))
	assertEmpty(t, rec.disconnectCh, "early disconnect")
	m.instanceProbe("camera-109", gen, makePolicy(hc))
	if got := waitChan(t, rec.disconnectCh, "disconnect"); got != "uuid-1" {
		t.Fatalf("disconnect uuid = %q", got)
	}

	// Inconclusive while offline must not announce a premature recovery.
	fake.setFail(false)
	fake.setEmpty(true)
	m.instanceProbe("camera-109", gen, makePolicy(hc))
	assertEmpty(t, rec.connectCh, "connect on inconclusive while offline")

	// A real reading recovers.
	fake.setEmpty(false)
	m.instanceProbe("camera-109", gen, makePolicy(hc))
	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}
}

// TestLegacyInconclusiveStaysOffline verifies the legacy candidate path
// treats a 200 without readings as not reachable but also not as a counted
// failure; a subsequent sweep with readings announces online.
func TestLegacyInconclusiveStaysOffline(t *testing.T) {
	fake := newFakeCore("camera-109", dtos.CoreCommand{Name: "DeviceInformation", Get: true})
	srv := fake.server()
	defer srv.Close()
	m, rec := newMonitor(t, srv, nil)

	fake.setEmpty(true)
	m.OnAttach("camera-109", "uuid-1", "onvif-profile", "", false, nil)
	assertEmpty(t, rec.connectCh, "connect on inconclusive initial probe")
	for i := 0; i < 4; i++ {
		m.sweep(context.Background())
	}
	assertEmpty(t, rec.connectCh, "connect on repeated inconclusive sweeps")
	m.mu.Lock()
	failures := m.devices["camera-109"].failures
	m.mu.Unlock()
	if failures != 0 {
		t.Fatalf("inconclusive sweeps accumulated failures=%d, want 0", failures)
	}

	fake.setEmpty(false)
	m.sweep(context.Background())
	if got := waitChan(t, rec.connectCh, "connect"); got != "uuid-1" {
		t.Errorf("connect uuid = %q", got)
	}
}

func TestSameHealthCheck(t *testing.T) {
	base := makeHC("Status", 30000, 5000, 3)
	cases := []struct {
		name string
		mut  func(hc *device.ConnectorHealthCheck)
		want bool
	}{
		{"identical", func(_ *device.ConnectorHealthCheck) {}, true},
		{"resource changed", func(h *device.ConnectorHealthCheck) { h.Resource = "Hostname" }, false},
		{"interval_ms changed", func(h *device.ConnectorHealthCheck) { h.IntervalMS = 10000 }, false},
		{"timeout_ms changed", func(h *device.ConnectorHealthCheck) { h.TimeoutMS = 2000 }, false},
		{"threshold changed", func(h *device.ConnectorHealthCheck) { h.FailThreshold = 5 }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			other := *base
			tc.mut(&other)
			if got := sameHealthCheck(base, &other); got != tc.want {
				t.Errorf("sameHealthCheck = %v, want %v", got, tc.want)
			}
		})
	}
	oneNil := sameHealthCheck(base, nil)
	bothNil := sameHealthCheck(nil, nil)
	if oneNil || !bothNil {
		t.Errorf("nil handling: one-nil=%v both-nil=%v, want false/true", oneNil, bothNil)
	}
}

func TestInstanceHCZeroValuesUseDefaults(t *testing.T) {
	p := makePolicy(makeHC("Status", 0, 0, 0))
	if p.resource != "Status" {
		t.Errorf("resource = %q", p.resource)
	}
	if p.interval != defaultInterval {
		t.Errorf("interval = %v, want %v", p.interval, defaultInterval)
	}
	if p.timeout != defaultInstanceProbeTime {
		t.Errorf("timeout = %v, want %v", p.timeout, defaultInstanceProbeTime)
	}
	if p.threshold != defaultThreshold {
		t.Errorf("threshold = %d, want %d", p.threshold, defaultThreshold)
	}
}
