// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package binding resolves EdgeX device names to NereidLink sub-device UUIDs.
//
// Resolution sources, in priority order:
//  1. static DeviceMapper configuration (escape hatch)
//  2. connector specs pushed from NereidLink (NL-first lifecycle)
//  3. discovery auto-provision results (edge-first lifecycle, future)
package binding

import (
	"sync"
)

// Resolver maintains the EdgeX device name -> NL sub-device UUID mapping.
type Resolver struct {
	mu sync.RWMutex

	// static entries from configuration (lowest priority among dynamic sources
	// is handled by callers; here static always wins to keep operator override).
	static map[string]string

	// specs keyed by EdgeX device name, populated from connector specs.
	specs map[string]string

	// specOwners records which spec id currently owns each EdgeX name so a
	// spec can release its own binding even when another spec later rebound
	// the same name.
	specOwners map[string]string

	// provisioned keyed by EdgeX device name from discovery results.
	provisioned map[string]string
}

// NewResolver creates a resolver seeded with static mappings.
func NewResolver(static map[string]string) *Resolver {
	r := &Resolver{
		static:      make(map[string]string),
		specs:       make(map[string]string),
		specOwners:  make(map[string]string),
		provisioned: make(map[string]string),
	}
	for k, v := range static {
		r.static[k] = v
	}
	return r
}

// Resolve returns the NL UUID for an EdgeX device name and whether it is known.
func (r *Resolver) Resolve(edgexName string) (string, bool) {
	if r == nil {
		return edgexName, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if uuid, ok := r.static[edgexName]; ok {
		return uuid, true
	}
	if uuid, ok := r.specs[edgexName]; ok {
		return uuid, true
	}
	if uuid, ok := r.provisioned[edgexName]; ok {
		return uuid, true
	}
	return edgexName, false
}

// SetSpec records/removes a binding learned from a connector spec. Passing an
// empty uuid removes the entry. specID identifies the owning spec so that
// releasing a name cannot remove another spec's binding when device names
// were rebound between revisions.
func (r *Resolver) SetSpec(specID, edgexName, uuid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if uuid == "" {
		return
	}
	if owner, taken := r.specOwners[edgexName]; taken && owner != specID {
		// Two specs target the same EdgeX device name; last writer wins but
		// ownership moves so the old owner can no longer release the binding.
		delete(r.specOwners, edgexName)
	}
	r.specs[edgexName] = uuid
	r.specOwners[edgexName] = specID
}

// ReleaseSpec releases the EdgeX name owned by specID. It is safe to call
// when the spec never owned the name.
func (r *Resolver) ReleaseSpec(specID, edgexName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner, ok := r.specOwners[edgexName]; !ok || owner != specID {
		return
	}
	delete(r.specs, edgexName)
	delete(r.specOwners, edgexName)
}

// SetProvisioned records a binding learned from discovery auto-provisioning.
func (r *Resolver) SetProvisioned(edgexName, uuid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if uuid == "" {
		delete(r.provisioned, edgexName)
		return
	}
	r.provisioned[edgexName] = uuid
}

// Name returns the EdgeX device name mapped to the given NL sub-device UUID.
// Static entries win over spec entries which win over provisioned entries,
// matching the forward Resolve priority. Shadowed bindings (a lower-priority
// source rebound a name already owned by a higher-priority source) never
// resolve. When the UUID is unknown false is returned.
func (r *Resolver) Name(uuid string) (string, bool) {
	if r == nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]struct{}, len(r.static)+len(r.specs)+len(r.provisioned))
	// Static entries are the operator override and win the reverse lookup too.
	for name, bound := range r.static {
		if bound == uuid {
			return name, true
		}
		seen[name] = struct{}{}
	}
	// Spec entries, unless a static binding already owns the same name.
	for name, bound := range r.specs {
		if _, shadowed := seen[name]; shadowed {
			continue
		}
		if bound == uuid {
			return name, true
		}
		seen[name] = struct{}{}
	}
	// Provisioned entries, unless shadowed by a static or spec binding.
	for name, bound := range r.provisioned {
		if _, shadowed := seen[name]; shadowed {
			continue
		}
		if bound == uuid {
			return name, true
		}
	}
	return "", false
}

// MappedNames returns all EdgeX names currently resolvable from any source.
func (r *Resolver) MappedNames() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]struct{}, len(r.static)+len(r.specs)+len(r.provisioned))
	out := make([]string, 0, len(seen))
	add := func(k string) {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			out = append(out, k)
		}
	}
	for k := range r.static {
		add(k)
	}
	for k := range r.specs {
		add(k)
	}
	for k := range r.provisioned {
		add(k)
	}
	return out
}

// UUIDs returns the distinct set of NL sub-device UUIDs currently effective.
// When several sources map the same EdgeX name, the static entry wins and the
// shadowed UUID is excluded.
func (r *Resolver) UUIDs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make(map[string]struct{}, len(r.static)+len(r.specs)+len(r.provisioned))
	for k := range r.static {
		names[k] = struct{}{}
	}
	for k := range r.specs {
		names[k] = struct{}{}
	}
	for k := range r.provisioned {
		names[k] = struct{}{}
	}
	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for name := range names {
		var uuid string
		if u, ok := r.static[name]; ok {
			uuid = u
		} else if u, ok := r.specs[name]; ok {
			uuid = u
		} else {
			uuid = r.provisioned[name]
		}
		if _, dup := seen[uuid]; dup {
			continue
		}
		seen[uuid] = struct{}{}
		out = append(out, uuid)
	}
	return out
}
