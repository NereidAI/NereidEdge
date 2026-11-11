// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"time"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// ConfigureManagerResources enables opaque manager-table delivery through the
// same core-command client used by inference health reads. allowedDevice is a
// write boundary; empty selects nereid-inference. storeDir persists only the
// ownership/delete ledger, never the opaque definitions themselves.
func (a *Applier) ConfigureManagerResources(
	commands ManagerCommandClient, allowedDevice, storeDir string, retry time.Duration,
) error {
	a.managerResources.configure(commands, allowedDevice, retry)
	return a.managerResources.openStore(storeDir)
}

// ManagerResourceHandler applies one identity in an opaque manager JSON array.
type ManagerResourceHandler struct {
	applier *Applier
}

func NewManagerResourceHandler(applier *Applier) *ManagerResourceHandler {
	return &ManagerResourceHandler{applier: applier}
}

func (h *ManagerResourceHandler) Type() string { return TypeManagerResource }

func (h *ManagerResourceHandler) Apply(ctx context.Context, push device.ConnectorPush) error {
	a := h.applier
	if a == nil {
		return nil
	}
	if a.managerResources.replayRejection(push) {
		return nil
	}
	if a.managerResources.superseded(push.SpecID, push.Revision) {
		return nil
	}
	if push.DeviceID != "" {
		a.managerResources.reject(push, device.ConnectorStateFailed, "config-invalid")
		return nil
	}
	a.managerResources.mu.Lock()
	allowedDevice := a.managerResources.allowedDevice
	a.managerResources.mu.Unlock()
	cfg, err := parseManagerResourceConfig(push.Config, allowedDevice)
	if err != nil {
		a.managerResources.reject(push, device.ConnectorStateFailed, "config-invalid")
		return nil
	}
	hash, err := device.CanonicalConfigHash(push.Config, push.Mappings)
	if err != nil {
		a.managerResources.reject(push, device.ConnectorStateFailed, "config-invalid")
		return nil
	}
	if a.managerResources.staleApply(push.SpecID, push.Revision, hash) {
		return nil
	}
	a.mu.Lock()
	previous, known := a.applied[push.SpecID]
	a.mu.Unlock()
	if known && previous.hash == hash && previous.revision >= push.Revision &&
		(a.observe || !a.managerResources.needsRestart(push.SpecID)) {
		return nil
	}

	a.reportCreating(ctx, push)
	if a.observe {
		a.finishNodeApply(push, hash)
		a.reportApplied(ctx, push)
		return nil
	}
	result := a.managerResources.apply(ctx, push, cfg, hash)
	if result.stale {
		return nil
	}
	if result.state == device.ConnectorStateFailed {
		a.managerResources.publishResult(push.SpecID, push, result)
		return nil
	}
	if !result.desiredAccepted {
		// The new desired revision was not durably accepted. Do not advance the
		// manifest revision/hash: an identical redrive must retry the ledger write.
		a.managerResources.publishResult(push.SpecID, push, result)
		return nil
	}
	// DEGRADED desired state is deliberately retained: the retry loop and
	// subsequent hello redrives can converge it without a new NL revision.
	a.finishNodeApply(push, hash)
	a.managerResources.publishResult(push.SpecID, push, result)
	return nil
}

func (h *ManagerResourceHandler) Delete(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	h.applier.applyDelete(ctx, push)
	return nil
}

var _ Handler = (*ManagerResourceHandler)(nil)
