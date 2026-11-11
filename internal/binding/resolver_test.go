// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package binding

import "testing"

func TestResolverPriority(t *testing.T) {
	r := NewResolver(map[string]string{"dev-static": "uuid-static"})

	if uuid, ok := r.Resolve("dev-static"); !ok || uuid != "uuid-static" {
		t.Fatalf("static mapping wrong: %s %v", uuid, ok)
	}

	r.SetSpec("spec-1", "dev-spec", "uuid-spec")
	if uuid, ok := r.Resolve("dev-spec"); !ok || uuid != "uuid-spec" {
		t.Fatalf("spec mapping wrong: %s %v", uuid, ok)
	}

	r.SetProvisioned("dev-disc", "uuid-disc")
	if uuid, ok := r.Resolve("dev-disc"); !ok || uuid != "uuid-disc" {
		t.Fatalf("provisioned mapping wrong: %s %v", uuid, ok)
	}

	// Static takes precedence over spec for the same name.
	r.SetSpec("spec-2", "dev-static", "uuid-spec-override")
	if uuid, _ := r.Resolve("dev-static"); uuid != "uuid-static" {
		t.Fatalf("static should win, got %s", uuid)
	}

	if _, ok := r.Resolve("unknown"); ok {
		t.Fatal("unknown should not resolve")
	}

	all := r.UUIDs()
	// Effective UUIDs: static dev-static wins over the shadowed spec entry.
	if len(all) != 3 {
		t.Fatalf("expected 3 effective UUIDs, got %d: %v", len(all), all)
	}

	r.ReleaseSpec("spec-1", "dev-spec")
	if _, ok := r.Resolve("dev-spec"); ok {
		t.Fatal("spec release failed")
	}
}

// TestResolverSpecRebindOwnership covers the case where two specs
// successively bind the same EdgeX device name: the old owner must not be
// able to tear down the new owner's binding on delete.
func TestResolverSpecRebindOwnership(t *testing.T) {
	r := NewResolver(nil)

	r.SetSpec("spec-A", "dev", "uuid-A")
	r.SetSpec("spec-B", "dev", "uuid-B")

	if uuid, _ := r.Resolve("dev"); uuid != "uuid-B" {
		t.Fatalf("last writer should win, got %s", uuid)
	}

	// Old owner delete must not release the current binding.
	r.ReleaseSpec("spec-A", "dev")
	if uuid, ok := r.Resolve("dev"); !ok || uuid != "uuid-B" {
		t.Fatalf("new owner binding must survive old owner release, got %s %v", uuid, ok)
	}

	// Current owner delete releases it.
	r.ReleaseSpec("spec-B", "dev")
	if _, ok := r.Resolve("dev"); ok {
		t.Fatal("current owner release failed")
	}
}

// TestResolverNameReverse covers UUID -> EdgeX name lookup across the three
// sources and confirms source priority.
func TestResolverNameReverse(t *testing.T) {
	r := NewResolver(map[string]string{"dev-static": "uuid-static"})
	r.SetSpec("spec-1", "dev-spec", "uuid-spec")
	r.SetProvisioned("dev-disc", "uuid-disc")

	if name, ok := r.Name("uuid-static"); !ok || name != "dev-static" {
		t.Fatalf("static reverse wrong: %s %v", name, ok)
	}
	if name, ok := r.Name("uuid-spec"); !ok || name != "dev-spec" {
		t.Fatalf("spec reverse wrong: %s %v", name, ok)
	}
	if name, ok := r.Name("uuid-disc"); !ok || name != "dev-disc" {
		t.Fatalf("provisioned reverse wrong: %s %v", name, ok)
	}
	if name, ok := r.Name("uuid-missing"); ok {
		t.Fatalf("missing uuid should not resolve, got %s", name)
	}
	if name, ok := r.Name(""); ok {
		t.Fatalf("empty uuid should not resolve, got %s", name)
	}
}

// TestResolverNameReversePriority ensures the reverse lookup prefers the
// static name when a spec later rebinds the same UUID.
func TestResolverNameReversePriority(t *testing.T) {
	r := NewResolver(map[string]string{"dev-static": "uuid-shared"})
	r.SetSpec("spec-1", "dev-static", "uuid-spec")

	// The spec cannot override the static binding for the same name.
	if uuid, _ := r.Resolve("dev-static"); uuid != "uuid-shared" {
		t.Fatalf("static should win forward lookup, got %s", uuid)
	}
	for uuid, name := range map[string]string{
		"uuid-shared": "dev-static",
	} {
		if got, ok := r.Name(uuid); !ok || got != name {
			t.Fatalf("reverse for %s should be %s, got %s %v", uuid, name, got, ok)
		}
	}
	if name, ok := r.Name("uuid-spec"); ok {
		t.Fatalf("shadowed spec uuid should not resolve, got %s", name)
	}
}
