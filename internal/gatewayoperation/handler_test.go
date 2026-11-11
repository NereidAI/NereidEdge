// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gatewayoperation

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// fakeGateway records published results and tests Handle in isolation.
type fakeGateway struct {
	results []device.OperationResult
}

func (f *fakeGateway) SubscribeOperations(context.Context, byte, func(device.OperationInvoke)) error {
	return nil
}

func (f *fakeGateway) RespondOperation(_ context.Context, frame device.OperationResult, _ byte) error {
	f.results = append(f.results, frame)
	return nil
}

func TestHandleUnknownOperation(t *testing.T) {
	fake := &fakeGateway{}
	h := New(Config{Gateway: fake})

	h.Handle(device.OperationInvoke{ID: "abc", Operation: "no.such"})

	if len(fake.results) != 1 {
		t.Fatalf("want 1 result, got %d", len(fake.results))
	}
	if got := fake.results[0]; got.OK || got.Error != "unknown operation" || got.ID != "abc" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestHandleDuplicateIgnored(t *testing.T) {
	fake := &fakeGateway{}
	h := New(Config{Gateway: fake})

	inv := device.OperationInvoke{ID: "dup", Operation: "no.such"}
	h.Handle(inv)
	h.Handle(inv)

	if len(fake.results) != 1 {
		t.Fatalf("want duplicate suppressed, got %d results", len(fake.results))
	}
}

func TestParseRTT(t *testing.T) {
	out := `64 bytes from 127.0.0.1: icmp_seq=1 ttl=64 time=0.031 ms
64 bytes from 127.0.0.1: icmp_seq=2 ttl=64 time=1.25 ms`
	got := parseRTT(out)
	if len(got) != 2 || got[0] != 0.031 || got[1] != 1.25 {
		t.Fatalf("unexpected rtts: %v", got)
	}
}

func TestParseLoss(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want float64
	}{
		{"iputils", "2 packets transmitted, 2 received, 0% packet loss, time 1007ms", 0},
		{"partial", "4 packets transmitted, 3 packets received, 25% packet loss", 25},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseLoss(tc.out)
			if !ok || got != tc.want {
				t.Fatalf("want %v/%v, got %v/%v", tc.want, true, got, ok)
			}
		})
	}
}

func TestOpTimeoutClamps(t *testing.T) {
	advert := device.OperationAdvert{TimeoutMs: 10000}
	cases := []struct {
		name     string
		invokeMs int
		want     time.Duration
	}{
		{"invoke smaller", 3000, 3 * time.Second},
		{"invoke larger", 30000, 10 * time.Second},
		{"invoke unset", 0, 10 * time.Second},
		{"clamp low", 50, 100 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := opTimeout(advert, tc.invokeMs); got != tc.want {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
		})
	}

	if got := opTimeout(device.OperationAdvert{TimeoutMs: 90000}, 0); got != 60*time.Second {
		t.Fatalf("clamp high: want 1m0s, got %v", got)
	}
}

func TestCatalogWellFormed(t *testing.T) {
	adverts := Adverts(Options{})
	if len(adverts) != 18 {
		t.Fatalf("want 18 adverts without a service allowlist, got %d", len(adverts))
	}
	seen := make(map[string]bool)
	for _, a := range adverts {
		if a.Operation == "" || a.TypeVersion == "" {
			t.Fatalf("advert missing identity: %+v", a)
		}
		if seen[a.Operation] {
			t.Fatalf("duplicate operation: %s", a.Operation)
		}
		seen[a.Operation] = true
		if a.TimeoutMs < 100 || a.TimeoutMs > 60000 {
			t.Fatalf("%s timeout out of range: %d", a.Operation, a.TimeoutMs)
		}
		for name, raw := range map[string]json.RawMessage{"param": a.ParamSchema, "result": a.ResultSchema} {
			if len(raw) == 0 {
				t.Fatalf("%s %s schema empty", a.Operation, name)
			}
			if strings.Contains(string(raw), "$ref") {
				t.Fatalf("%s %s schema uses external $ref", a.Operation, name)
			}
			var doc map[string]any
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatalf("operation %s: %s schema invalid: %v", a.Operation, name, err)
			}
			if _, ok := doc["$schema"]; !ok {
				t.Fatalf("operation %s %s schema missing $schema", a.Operation, name)
			}
		}
	}
	if !seen[OperationSysReboot] {
		t.Fatal("sys.reboot advert missing")
	}
}

func TestServiceRestartConditional(t *testing.T) {
	if _, ok := New(Config{}).catalog[OperationServiceRestart]; ok {
		t.Fatal("service.restart must not be advertised without a service allowlist")
	}
	withServices := New(Config{Options: Options{Services: map[string]string{"app": "app.service"}}})
	if _, ok := withServices.catalog[OperationServiceRestart]; !ok {
		t.Fatal("service.restart must be advertised with a service allowlist")
	}
}

func TestOneWayOperations(t *testing.T) {
	h := New(Config{})
	for _, op := range []string{OperationSysReboot, OperationSysAgentRestart, OperationNetworkReconnect} {
		advert, ok := h.catalog[op]
		if !ok {
			t.Fatalf("%s not registered", op)
		}
		if advert.Interaction != device.OperationInteractionOneWay {
			t.Fatalf("%s must be one-way, got %q", op, advert.Interaction)
		}
	}
}

func TestOptionalAndRequiredInt(t *testing.T) {
	params := map[string]any{"count": json.Number("7"), "port": 8080}
	if v, err := optionalInt(params, "count", 1, 10, 4); err != nil || v != 7 {
		t.Fatalf("want 7/nil, got %v/%v", v, err)
	}
	if v, err := optionalInt(params, "missing", 1, 10, 4); err != nil || v != 4 {
		t.Fatalf("want default 4, got %v/%v", v, err)
	}
	if v, err := optionalInt(params, "port", 1, 10, 4); err != nil || v != 10 {
		t.Fatalf("want clamped 10, got %v/%v", v, err)
	}
	if _, err := requiredInt(params, "missing", 1, 10); err == nil {
		t.Fatal("want error for missing required param")
	}
	if v, err := requiredInt(params, "port", 1, 65535); err != nil || v != 8080 {
		t.Fatalf("want 8080/nil, got %v/%v", v, err)
	}
}

func TestLogRedactionRemovesSecrets(t *testing.T) {
	line := `token=supersecret password: hunter2 ref secret://kv/plain access_key=AKIA123 private -----BEGIN RSA PRIVATE KEY----- MIIabc -----END RSA PRIVATE KEY----- end`
	got := redactLog(line)
	for _, leaked := range []string{"supersecret", "hunter2", "secret://kv/plain", "AKIA123", "MIIabc"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("redactLog leaked %q in %q", leaked, got)
		}
	}
}

func TestTailLines(t *testing.T) {
	got := tailLines("a\nb\nc\nd\n", 2)
	if len(got) != 2 || got[0] != "c" || got[1] != "d" {
		t.Fatalf("want [c d], got %q", got)
	}
	if got := tailLines("", 5); got != nil {
		t.Fatalf("want nil for empty input, got %q", got)
	}
	if got := tailLines("only\n", 5); len(got) != 1 || got[0] != "only" {
		t.Fatalf("want [only], got %q", got)
	}
}

func TestRunLogsTailReturnsLineArray(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.log")
	if err := os.WriteFile(path, []byte("one\ntwo\nthree\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := New(Config{Options: Options{LogFile: path}})
	raw, err := h.runLogsTail(device.OperationInvoke{Params: map[string]any{"lines": float64(2)}}, device.OperationAdvert{})
	if err != nil {
		t.Fatalf("runLogsTail: %v", err)
	}
	var out sysLogsTailResult
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Entries) != 2 || out.Entries[0] != "two" || out.Entries[1] != "three" {
		t.Fatalf("want [two three], got %q", out.Entries)
	}
}

func TestRunLogLevelGet(t *testing.T) {
	h := New(Config{Gateway: &fakeGateway{}, Logger: logger.NewMockClient()})

	out, err := h.runLogLevelGet(device.OperationInvoke{}, device.OperationAdvert{})
	if err != nil {
		t.Fatalf("runLogLevelGet: %v", err)
	}
	var got sysLogLevelResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	// The mock logger reports INFO; the operation lower-cases it.
	if got.Level != "info" {
		t.Fatalf("level = %q, want info", got.Level)
	}
}

func TestLogLevelAdvertIsReadOnly(t *testing.T) {
	var found bool
	for _, a := range Adverts(Options{}) {
		if a.Operation == OperationSysLogLevel {
			found = true
			if a.Risk != device.OperationRiskRead {
				t.Fatalf("risk = %v, want read", a.Risk)
			}
		}
		if a.Operation == "sys.log_level_set" {
			t.Fatal("sys.log_level_set must no longer be advertised")
		}
	}
	if !found {
		t.Fatal("sys.log_level not advertised")
	}
}

func TestUnescapeMount(t *testing.T) {
	cases := map[string]string{
		"/data":              "/data",
		`/mnt/with\040space`: "/mnt/with space",
		`/mnt/tab\011here`:   "/mnt/tab\there",
	}
	for in, want := range cases {
		if got := unescapeMount(in); got != want {
			t.Fatalf("unescapeMount(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSysOperationsExecute(t *testing.T) {
	h := New(Config{})
	cases := []struct {
		op     string
		params map[string]any
	}{
		{OperationSysInfo, nil},
		{OperationSysResources, map[string]any{"sample_ms": 100}},
		{OperationSysNetwork, nil},
		{OperationSysProcessesTop, map[string]any{"limit": 3, "sort": "mem"}},
		{OperationSysNTPStatus, nil},
		{OperationDiagUplinkStatus, nil},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			advert, ok := h.catalog[tc.op]
			if !ok {
				t.Fatalf("operation %s not in catalog", tc.op)
			}
			out, err := h.execute(device.OperationInvoke{Operation: tc.op, Params: tc.params}, advert)
			if err != nil {
				t.Fatalf("execute returned error: %v", err)
			}
			if !json.Valid(out) {
				t.Fatalf("execute returned invalid JSON: %s", out)
			}
		})
	}
}

func TestIPv4FromProcHex(t *testing.T) {
	if got := ipv4FromProcHex("0100A8C0"); got != "192.168.0.1" {
		t.Fatalf("want 192.168.0.1, got %q", got)
	}
	if got := ipv4FromProcHex("bogus"); got != "" {
		t.Fatalf("want empty, got %q", got)
	}
	if got := ipv4FromProcHex("00000000"); got != "0.0.0.0" {
		t.Fatalf("want 0.0.0.0, got %q", got)
	}
}
