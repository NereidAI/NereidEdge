// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"sync"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// Publisher de-duplicates EdgeX profile reports by profile name and canonical hash.
type Publisher struct {
	mu        sync.Mutex
	published map[string]string
}

// NewPublisher creates a profile publisher de-duplication cache.
func NewPublisher() *Publisher {
	return &Publisher{published: make(map[string]string)}
}

// MarkPublished records the canonical hash of a successfully published profile.
func (p *Publisher) MarkPublished(name, hash string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.published[name] = hash
	p.mu.Unlock()
}

// Reset clears the de-duplication cache. It is invoked after a gateway
// session loss so the post-hello full profile inventory is replayed.
func (p *Publisher) Reset() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.published = make(map[string]string)
	p.mu.Unlock()
}

// ShouldPublish reports whether a profile has never been published or changed.
func (p *Publisher) ShouldPublish(profile device.EdgeProfile) bool {
	if p == nil {
		return true
	}
	hash, err := device.CanonicalProfileHash(profile)
	if err != nil {
		return true
	}
	p.mu.Lock()
	previous, ok := p.published[profile.Name]
	p.mu.Unlock()
	return !ok || previous != hash
}

// Hash returns the canonical profile hash used by NereidLink.
func (p *Publisher) Hash(profile device.EdgeProfile) (string, error) {
	return device.CanonicalProfileHash(profile)
}
