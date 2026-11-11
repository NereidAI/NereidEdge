// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package attributes

import (
	"encoding/json"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

func raw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	return b
}

// nullRaw is the JSON null literal, which a push uses to delete a key.
var nullRaw = json.RawMessage("null")

func TestCacheApplyUpsertAndDelete(t *testing.T) {
	c := newAttrCache()
	c.applySelf(device.AttributeScopeShared, map[string]json.RawMessage{
		"a": raw(t, 1),
		"b": raw(t, "two"),
	})
	c.applySelf(device.AttributeScopeShared, map[string]json.RawMessage{
		"b": nullRaw, // JSON null deletes the key
		"c": raw(t, 3.5),
	})

	got := c.self[device.AttributeScopeShared]
	if _, ok := got["b"]; ok {
		t.Fatalf("b should have been deleted, got %v", got)
	}
	if got["a"] != float64(1) {
		t.Fatalf("a = %v, want 1", got["a"])
	}
	if got["c"] != 3.5 {
		t.Fatalf("c = %v, want 3.5", got["c"])
	}
}

func TestCacheReplaceDropsAbsentKeys(t *testing.T) {
	c := newAttrCache()
	c.applySelf(device.AttributeScopeServer, map[string]json.RawMessage{
		"a": raw(t, 1),
		"b": raw(t, 2),
	})
	// A get/ack is authoritative for the whole scope: absent keys are gone.
	c.replaceSelf(device.AttributeScopeServer, map[string]json.RawMessage{
		"a": raw(t, 9),
	})

	got := c.self[device.AttributeScopeServer]
	if len(got) != 1 || got["a"] != float64(9) {
		t.Fatalf("replace result = %v, want {a:9}", got)
	}
}

func TestSubEffectiveMergesScopesSharedWins(t *testing.T) {
	c := newAttrCache()
	c.applySub("dev-1", device.AttributeScopeServer, map[string]json.RawMessage{
		"a": raw(t, 1),
		"b": raw(t, "srv"),
	})
	c.applySub("dev-1", device.AttributeScopeShared, map[string]json.RawMessage{
		"a": raw(t, 2),
	})

	got := c.subEffective("dev-1")
	if got["a"] != float64(2) {
		t.Fatalf("a = %v, want shared value 2", got["a"])
	}
	if got["b"] != "srv" {
		t.Fatalf("b = %v, want srv", got["b"])
	}
}

func TestSubReplaceAndDeleteConverge(t *testing.T) {
	c := newAttrCache()
	c.applySub("dev-1", device.AttributeScopeShared, map[string]json.RawMessage{
		"gone": raw(t, true),
		"kept": raw(t, 1),
	})
	c.replaceSub("dev-1", device.AttributeScopeShared, map[string]json.RawMessage{
		"kept": raw(t, 2),
	})
	c.applySub("dev-1", device.AttributeScopeShared, map[string]json.RawMessage{
		"kept": nullRaw,
	})

	got := c.subEffective("dev-1")
	if len(got) != 0 {
		t.Fatalf("expected empty effective map, got %v", got)
	}
}

func TestValidScope(t *testing.T) {
	f := &Filler{lc: logger.NewMockClient()}
	for _, scope := range []string{device.AttributeScopeServer, device.AttributeScopeShared} {
		if !f.validScope(scope, "dev") {
			t.Fatalf("scope %q should be accepted", scope)
		}
	}
	for _, scope := range []string{device.AttributeScopeClient, "BOGUS"} {
		if f.validScope(scope, "dev") {
			t.Fatalf("scope %q should be rejected", scope)
		}
	}
}
