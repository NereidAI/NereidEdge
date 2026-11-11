// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import "testing"

// TestCanonicalProfileHashStable verifies that equivalent profiles hash to the
// same value regardless of slice-capacity or optional-field representation.
func TestCanonicalProfileHashStable(t *testing.T) {
	a := EdgeProfile{
		Name: "P",
		DeviceResources: []EdgeDeviceResource{
			{Name: "B", ValueType: "Bool"},
			{Name: "S", ValueType: "String", Units: "C"},
		},
	}
	b := EdgeProfile{
		Name: "P",
		DeviceResources: []EdgeDeviceResource{
			{Name: "B", ValueType: "Bool", Units: ""},
			{Name: "S", ValueType: "String", Units: "C"},
		},
	}
	ha, err := CanonicalProfileHash(a)
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	hb, err := CanonicalProfileHash(b)
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if ha != hb {
		t.Fatalf("equivalent profiles differ: %s vs %s", ha, hb)
	}
	if len(ha) != 64 {
		t.Fatalf("expected 64-hex sha256, got %q", ha)
	}

	c := b
	c.DeviceResources[0].ValueType = "Int32"
	hc, _ := CanonicalProfileHash(c)
	if hc == ha {
		t.Fatalf("changed valueType should change hash")
	}
}
