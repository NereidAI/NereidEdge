// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"context"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// PublishFunc publishes one batch of edge profiles to NereidLink.
type PublishFunc func(ctx context.Context, profiles []device.EdgeProfile) error

// Syncer scans core-metadata for EdgeX device profiles and reports
// new or changed profiles to NereidLink. De-duplication is keyed by profile
// name and the SDK canonical content hash: a gateway restart re-publishes
// the full set once (matching the protocol's hello-replay expectation), and
// unchanged periodic scans are no-ops on the wire.
type Syncer struct {
	fetcher *Fetcher
	pub     *Publisher
	publish PublishFunc
}

// NewSyncer creates a profile syncer with the given de-duplication cache and
// publish callback.
func NewSyncer(fetcher *Fetcher, pub *Publisher, publish PublishFunc) *Syncer {
	return &Syncer{fetcher: fetcher, pub: pub, publish: publish}
}

// ToEdgeProfile converts the EdgeX profile subset NereidLink needs into the
// uplink edge profile frame.
func ToEdgeProfile(prof dtos.DeviceProfile) device.EdgeProfile {
	resources := make([]device.EdgeDeviceResource, 0, len(prof.DeviceResources))
	for _, r := range prof.DeviceResources {
		resources = append(resources, device.EdgeDeviceResource{
			Name:        r.Name,
			ValueType:   r.Properties.ValueType,
			Units:       r.Properties.Units,
			Description: r.Description,
		})
	}
	return device.EdgeProfile{Name: prof.Name, DeviceResources: resources}
}

// SyncOne fetches one profile by name and publishes it when unseen or
// changed. Fetch/publish failures are returned to the caller and never
// mutate the de-duplication cache, so a later retry still reports the
// profile.
func (s *Syncer) SyncOne(ctx context.Context, name string) error {
	if s == nil || s.fetcher == nil || s.pub == nil || s.publish == nil || name == "" {
		return nil
	}
	prof, err := s.fetcher.GetByName(ctx, name)
	if err != nil {
		return err
	}
	return s.report(ctx, ToEdgeProfile(prof))
}

// SyncDTO publishes a profile already in hand, for callers that received the
// full DTO from an EdgeX system event and do not need a core-metadata fetch.
func (s *Syncer) SyncDTO(ctx context.Context, prof dtos.DeviceProfile) error {
	if s == nil || s.pub == nil || s.publish == nil || prof.Name == "" {
		return nil
	}
	return s.report(ctx, ToEdgeProfile(prof))
}

// SyncAll enumerates every profile in core-metadata and publishes all
// profiles that are new or whose content hash changed.
func (s *Syncer) SyncAll(ctx context.Context) (int, error) {
	if s == nil || s.fetcher == nil || s.pub == nil || s.publish == nil {
		return 0, nil
	}
	profiles, err := s.fetcher.All(ctx, 100)
	if err != nil {
		return 0, err
	}
	batch := make([]device.EdgeProfile, 0, len(profiles))
	for _, prof := range profiles {
		edgeProfile := ToEdgeProfile(prof)
		if !s.pub.ShouldPublish(edgeProfile) {
			continue
		}
		batch = append(batch, edgeProfile)
	}
	if len(batch) == 0 {
		return 0, nil
	}
	if err := s.publish(ctx, batch); err != nil {
		return 0, err
	}
	for _, edgeProfile := range batch {
		hash, err := device.CanonicalProfileHash(edgeProfile)
		if err != nil {
			continue
		}
		s.pub.MarkPublished(edgeProfile.Name, hash)
	}
	return len(batch), nil
}

// report publishes a single profile and records it in the cache on success.
func (s *Syncer) report(ctx context.Context, edgeProfile device.EdgeProfile) error {
	if !s.pub.ShouldPublish(edgeProfile) {
		return nil
	}
	if err := s.publish(ctx, []device.EdgeProfile{edgeProfile}); err != nil {
		return err
	}
	hash, err := device.CanonicalProfileHash(edgeProfile)
	if err != nil {
		return err
	}
	s.pub.MarkPublished(edgeProfile.Name, hash)
	return nil
}
