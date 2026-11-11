// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package secrets implements ADR-0034 Edge Secret delivery. The platform owns
// each secret and pushes the full desired set on v1/gateway/secrets/sync after
// every (re)hello; the gateway keeps only an encrypted copy and answers with
// per-secret states on v1/gateway/secrets/status. The frame is idempotent and
// complete, so the handler upserts by name, de-duplicates by revision, erases
// on removing entries and never deletes copies the frame does not mention.
package secrets

import (
	"context"
	"errors"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// defaultReportTimeout bounds publishing one status frame. It is independent
// of any sync deadline because the uplink is fire-and-forget.
const defaultReportTimeout = 5 * time.Second

// defaultApplyTimeout bounds one Binder apply/remove (metadata lookups plus
// EdgeX HTTP calls). It is independent of the report timeout so a slow binding
// does not eat into the time budget for publishing the status frame.
const defaultApplyTimeout = 10 * time.Second

// Failure reasons reported in the status `reason` field. They are deliberately
// generic so no secret material or host path can leak into the platform.
const (
	reasonStoreFailed    = "store failed"
	reasonEraseFailed    = "erase failed"
	reasonBindFailed     = "bind failed"
	reasonTargetNotFound = "target not found"
)

// Gateway is the subset of the NereidLink gateway client surface the handler
// needs. *nlgateway.Client satisfies it; tests substitute a recording fake.
type Gateway interface {
	SubscribeSecrets(ctx context.Context, qos byte, onSync func(device.SecretSync)) error
	ReportSecretStatus(ctx context.Context, frame device.SecretStatusFrame, qos byte) error
}

// Config carries the dependencies for the secret Handler.
type Config struct {
	// Gateway is the connected NereidLink gateway client.
	Gateway Gateway
	// Store persists the encrypted secret copies.
	Store *Store
	// Binder pushes onvif-basic secrets into an EdgeX device service. It is
	// optional; without it every secret exists only as the encrypted copy.
	Binder Binder
	// Logger is the application logging client.
	Logger logger.LoggingClient
	// Timeout bounds publishing one status frame.
	Timeout time.Duration
	// ApplyTimeout bounds one Binder apply/remove. Zero selects
	// defaultApplyTimeout so a slow binding does not eat into the report
	// timeout.
	ApplyTimeout time.Duration
}

// Handler applies NL secret sync frames and reports their states.
type Handler struct {
	gw           Gateway
	store        *Store
	binder       Binder
	timeout      time.Duration
	applyTimeout time.Duration
	qos          byte
	lc           logger.LoggingClient
}

// New creates a secret delivery handler.
func New(cfg Config) *Handler {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultReportTimeout
	}
	if cfg.ApplyTimeout <= 0 {
		cfg.ApplyTimeout = defaultApplyTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewMockClient()
	}
	return &Handler{
		gw:           cfg.Gateway,
		store:        cfg.Store,
		binder:       cfg.Binder,
		timeout:      cfg.Timeout,
		applyTimeout: cfg.ApplyTimeout,
		lc:           cfg.Logger,
	}
}

// Start subscribes to NL secret downlinks. Each frame is handled on its own
// goroutine so sealing and file I/O never block the MQTT dispatch.
func (h *Handler) Start(ctx context.Context, qos byte) error {
	if h.gw == nil {
		h.lc.Warn("secret handler has no gateway; skipping subscription")
		return nil
	}
	h.qos = qos
	return h.gw.SubscribeSecrets(ctx, qos, func(sync device.SecretSync) {
		go h.Handle(sync)
	})
}

// Handle applies one complete desired set and reports every processed entry in
// a single status frame. An entry is never logged with its document.
func (h *Handler) Handle(sync device.SecretSync) {
	reports := make([]device.SecretStatus, 0, len(sync.Entries))
	for _, entry := range sync.Entries {
		if entry.Name == "" {
			h.lc.Warn("secret sync entry without a name; skipping")
			continue
		}
		reports = append(reports, h.apply(entry))
	}
	if len(reports) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	frame := device.SecretStatusFrame{Reports: reports}
	if err := h.gw.ReportSecretStatus(ctx, frame, h.qos); err != nil {
		h.lc.Errorf("report secret status failed: %s", err.Error())
	}
}

// apply stores or erases one entry and returns its reported state. Existing
// copies at the same or a newer revision are not rewritten, which keeps a
// re-delivered set idempotent. The EdgeX binding is always re-affirmed on a
// delivery so external drift heals and a previously failed apply is retried;
// Binder.Apply is cheap and idempotent by contract.
func (h *Handler) apply(entry device.SecretEntry) device.SecretStatus {
	ctx, cancel := context.WithTimeout(context.Background(), h.applyTimeout)
	defer cancel()

	if entry.Removing {
		if _, err := h.store.Remove(entry.Name); err != nil {
			h.lc.Errorf("erase secret %s failed: %s", entry.Name, err.Error())
			return secretStatus(entry, device.SecretStateFailed, reasonEraseFailed)
		}
		if h.binder != nil {
			if err := h.binder.Remove(ctx, entry.Name); err != nil {
				h.lc.Errorf("erase secret %s binding failed: %s", entry.Name, err.Error())
				return secretStatus(entry, device.SecretStateFailed, reasonEraseFailed)
			}
		}
		h.lc.Infof("secret %s revision %d removed", entry.Name, entry.Revision)
		return secretStatus(entry, device.SecretStateRemoved, "")
	}

	if revision, ok := h.store.Revision(entry.Name); ok && revision >= entry.Revision {
		// Same or newer revision already local: the encrypted copy is current,
		// but the EdgeX binding is still re-affirmed below so a platform
		// binding stuck in DISPATCHED converges.
	} else if err := h.store.Upsert(entry); err != nil {
		h.lc.Errorf("store secret %s revision %d failed: %s", entry.Name, entry.Revision, err.Error())
		return secretStatus(entry, device.SecretStateFailed, reasonStoreFailed)
	}

	if h.binder != nil {
		if err := h.binder.Apply(ctx, entry); err != nil {
			h.lc.Errorf("bind secret %s revision %d failed: %s", entry.Name, entry.Revision, err.Error())
			return secretStatus(entry, device.SecretStateFailed, binderReason(err))
		}
	}
	h.lc.Infof("secret %s revision %d applied", entry.Name, entry.Revision)
	return secretStatus(entry, device.SecretStateApplied, "")
}

// binderReason maps a Binder error to a generic, secret-safe status reason.
func binderReason(err error) string {
	if errors.Is(err, ErrNoTarget) {
		return reasonTargetNotFound
	}
	return reasonBindFailed
}

// secretStatus builds a status report without repeating the entry fields.
func secretStatus(entry device.SecretEntry, state, reason string) device.SecretStatus {
	return device.SecretStatus{
		Name:     entry.Name,
		Revision: entry.Revision,
		Status:   state,
		Reason:   reason,
	}
}
