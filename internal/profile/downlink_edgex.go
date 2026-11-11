// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/requests"
	edgexErrors "github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// Sentinel errors classify apply failures for transport-safe status reasons.
var (
	// ErrInvalidProfileDocument means the rendered document cannot be parsed
	// or validated as the declared format. It maps to a FAILED reason that
	// carries no document content.
	ErrInvalidProfileDocument = errors.New("invalid profile document")
	// ErrProfileInUse means core-metadata refuses deletion because devices
	// still reference the profile. The platform only sends removing after
	// connector references are gone, so this normally flags an edge-local
	// reference and must not be papered over.
	ErrProfileInUse = errors.New("profile in use")
)

// EdgeXApplier lands edgex-device-profile/3 documents in local EdgeX
// core-metadata. It is the reason NereidEdge may honestly declare that
// format in hello: documents are really ingested, upserted idempotently by
// name, not merely logged.
type EdgeXApplier struct {
	client interfaces.DeviceProfileClient
}

// NewEdgeXApplier creates the applier over a core-metadata profile client.
func NewEdgeXApplier(client interfaces.DeviceProfileClient) *EdgeXApplier {
	return &EdgeXApplier{client: client}
}

// Format identifies the render format this applier consumes.
func (a *EdgeXApplier) Format() string { return device.ProfileFormatEdgeX }

// Apply upserts one EdgeX DeviceProfile in core-metadata. It returns
// ErrInvalidProfileDocument for parse/validation failures so the handler
// reason stays generic; transport failures propagate as ordinary errors.
func (a *EdgeXApplier) Apply(ctx context.Context, entry device.ProfileEntry) error {
	if len(entry.Document) == 0 {
		return ErrInvalidProfileDocument
	}
	raw, err := json.Marshal(entry.Document)
	if err != nil {
		return ErrInvalidProfileDocument
	}
	var prof dtos.DeviceProfile
	if err := json.Unmarshal(raw, &prof); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidProfileDocument, err)
	}
	// The cloud renders document.name from the same library entry, so a
	// mismatch indicates a corrupt or spoofed frame and must not land under
	// a different core-metadata name.
	if prof.Name == "" || prof.Name != entry.Name {
		return ErrInvalidProfileDocument
	}
	normalizeReadWrite(&prof)
	if err := prof.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidProfileDocument, err)
	}

	req := requests.DeviceProfileRequest{
		BaseRequest: dtoCommon.NewBaseRequest(),
		Profile:     prof,
	}
	if existing, err := a.client.DeviceProfileByName(ctx, entry.Name); err == nil {
		// Update requires the existing id; the request DTO has no way to
		// address the row by name alone.
		req.Profile.Id = existing.Profile.Id
		if _, err := a.client.Update(ctx, []requests.DeviceProfileRequest{req}); err != nil {
			if edgexErrors.Kind(err) == edgexErrors.KindEntityDoesNotExist {
				return a.addWithDuplicateFallback(ctx, req)
			}
			return fmt.Errorf("update device profile: %w", err)
		}
		return nil
	} else if edgexErrors.Kind(err) != edgexErrors.KindEntityDoesNotExist {
		return fmt.Errorf("lookup device profile: %w", err)
	}
	return a.addWithDuplicateFallback(ctx, req)
}

// normalizeReadWrite fills the EdgeX-required properties.readWrite when the
// cloud-rendered document omits it. The library projection can legitimately
// lack it: the v1/gateway/profiles uplink shape has no readWrite field and
// the neutral shape makes read_write optional. The EdgeX renderer then emits
// no readWrite, but EdgeX v4 rejects a resource without it. Pre-v2 EdgeX
// defined an absent readWrite as "RW" (fully usable), so this boundary
// normalization restores that exact semantic; an explicitly declared "R"/"W"
// is never changed. After the RW fill, a renderer-hardcoded "R" deviceCommand
// passes permission alignment for every resource; a genuinely write-only
// resource still fails validation honestly.
func normalizeReadWrite(prof *dtos.DeviceProfile) {
	for i := range prof.DeviceResources {
		if prof.DeviceResources[i].Properties.ReadWrite == "" {
			prof.DeviceResources[i].Properties.ReadWrite = "RW"
		}
	}
}

// addWithDuplicateFallback adds the profile and, when another path created
// the same name concurrently, re-enters the update path.
func (a *EdgeXApplier) addWithDuplicateFallback(ctx context.Context,
	req requests.DeviceProfileRequest) error {
	if _, err := a.client.Add(ctx, []requests.DeviceProfileRequest{req}); err != nil {
		if edgexErrors.Kind(err) == edgexErrors.KindDuplicateName {
			existing, lookupErr := a.client.DeviceProfileByName(ctx, req.Profile.Name)
			if lookupErr != nil {
				return fmt.Errorf("re-lookup duplicate profile: %w", lookupErr)
			}
			req.Profile.Id = existing.Profile.Id
			if _, err := a.client.Update(ctx, []requests.DeviceProfileRequest{req}); err != nil {
				return fmt.Errorf("update duplicate profile: %w", err)
			}
			return nil
		}
		return fmt.Errorf("add device profile: %w", err)
	}
	return nil
}

// Remove deletes one profile from core-metadata by name. A missing profile
// is success (idempotent redrive); an in-use profile returns
// ErrProfileInUse. It must only be called for a name this downlink owns:
// the handler fences calls with its delivery ledger.
func (a *EdgeXApplier) Remove(ctx context.Context, name string) error {
	_, err := a.client.DeleteByName(ctx, name)
	if err == nil {
		return nil
	}
	switch edgexErrors.Kind(err) {
	case edgexErrors.KindEntityDoesNotExist:
		return nil
	case edgexErrors.KindStatusConflict, edgexErrors.KindNotAllowed:
		return ErrProfileInUse
	default:
		return fmt.Errorf("delete device profile: %w", err)
	}
}
