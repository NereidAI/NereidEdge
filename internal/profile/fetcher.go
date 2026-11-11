// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package profile owns both directions of the device-profile protocol:
// uplink enumerates local EdgeX core-metadata profiles and reports new or
// changed ones to NereidLink (Phase 2 descriptor synchronization), and the
// downlink (ADR-0038) lands library profiles NereidLink pushes on
// v1/gateway/profiles/spec and reports their apply state.
package profile

import (
	"context"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// Fetcher fetches EdgeX device profiles from core-metadata.
type Fetcher struct {
	svc interfaces.ApplicationService
}

// NewFetcher creates a new ProfileFetcher.
func NewFetcher(svc interfaces.ApplicationService) *Fetcher {
	return &Fetcher{svc: svc}
}

// GetByName retrieves a device profile by name from core-metadata.
func (f *Fetcher) GetByName(ctx context.Context, name string) (dtos.DeviceProfile, error) {
	if f == nil || f.svc == nil {
		return dtos.DeviceProfile{}, errors.NewCommonEdgeX(errors.KindContractInvalid, "service not initialized", nil)
	}
	prof, err := f.svc.DeviceProfileClient().DeviceProfileByName(ctx, name)
	if err != nil {
		return dtos.DeviceProfile{}, err
	}
	return prof.Profile, nil
}

// All retrieves every device profile currently registered in core-metadata,
// paging through the AllDeviceProfiles response until TotalCount is reached.
// pageSize controls the per-request page size; non-positive values default
// to 100.
func (f *Fetcher) All(ctx context.Context, pageSize int) ([]dtos.DeviceProfile, error) {
	if f == nil || f.svc == nil {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "service not initialized", nil)
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	var out []dtos.DeviceProfile
	for offset := 0; ; offset += pageSize {
		resp, err := f.svc.DeviceProfileClient().AllDeviceProfiles(ctx, nil, offset, pageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, resp.Profiles...)
		if int64(len(out)) >= resp.TotalCount || len(resp.Profiles) == 0 {
			break
		}
	}
	return out, nil
}
