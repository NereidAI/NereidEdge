// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// downlink.go implements the ADR-0038 device-profile delivery state machine:
// NL owns the desired set and pushes the full profile spec on
// v1/gateway/profiles/spec after every (re)hello; the gateway lands each
// rendered document, de-duplicates by (name, revision) against a durable
// ledger and reports APPLIED/FAILED/REJECTED/REMOVED on
// v1/gateway/profiles/status.
//
// Safety invariants:
//   - only an explicitly negotiated "profiles" feature activates handling;
//   - a profile absent from a full-set frame is NEVER deleted, only entries
//     with removing=true are;
//   - an external landing (EdgeX core-metadata) is torn down only for a name
//     the ledger proves this downlink owns, so locally authored profiles are
//     fenced off;
//   - FAILED/REJECTED reasons are generic constants: no document content,
//     path or secret material ever reaches the platform.

package profile

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// negotiationWait bounds how long a spec frame that beats the ack
// processing waits for negotiation to complete. NL redrives the full set
// immediately after hello, so ack delivery and the first frame can be
// scheduled concurrently across paho goroutines; this grace window removes
// that scheduling race without accepting permanently unnegotiated frames.
const negotiationWait = 3 * time.Second

// defaultReportTimeout bounds publishing one status frame. The uplink is
// fire-and-forget.
const defaultDownlinkReportTimeout = 5 * time.Second

// defaultApplyTimeout bounds one external applier call (core-metadata HTTP).
const defaultApplyTimeout = 10 * time.Second

// Failure reasons reported in the status `reason` field. They stay generic by
// contract; the profile name already travels in the report itself.
const (
	reasonUnsupportedFormat = "unsupported profile format"
	reasonInvalidDocument   = "invalid profile document"
	reasonStoreFailed       = "store failed"
	reasonApplyFailed       = "apply failed"
	reasonRemoveFailed      = "remove failed"
	reasonProfileInUse      = "profile in use"
)

// Gateway is the subset of the SDK gateway surface the handler needs.
// *nlgateway.Client satisfies it; tests substitute a recording fake.
type Gateway interface {
	SubscribeProfiles(ctx context.Context, qos byte, onSpec func(device.ProfileSpecFrame)) error
	ReportProfileStatus(ctx context.Context, frame device.ProfileStatusFrame, qos byte) error
}

// ProfileApplier lands a rendered document for one format beyond the local
// ledger (EdgeX core-metadata today). Implementations must be idempotent:
// the full desired set is re-sent after every hello.
type ProfileApplier interface {
	// Format returns the format id this applier consumes.
	Format() string
	// Apply lands one entry, returning ErrInvalidProfileDocument for
	// content the format cannot parse.
	Apply(ctx context.Context, entry device.ProfileEntry) error
	// Remove undoes a previous Apply by name; an already-missing target is
	// success.
	Remove(ctx context.Context, name string) error
}

// DownlinkConfig carries the dependencies for the delivery handler.
type DownlinkConfig struct {
	// Gateway is the connected NereidLink gateway client.
	Gateway Gateway
	// Store persists the applied-revision ledger and neutral documents.
	Store *DownlinkStore
	// Appliers lands non-neutral formats; nil selects neutral-only.
	Appliers []ProfileApplier
	// Logger is the application logging client.
	Logger logger.LoggingClient
	// Timeout bounds publishing one status frame.
	Timeout time.Duration
	// ApplyTimeout bounds one applier call. Zero selects the default.
	ApplyTimeout time.Duration
}

// DownlinkHandler applies profile spec frames and reports their states.
type DownlinkHandler struct {
	gw           Gateway
	store        *DownlinkStore
	appliers     map[string]ProfileApplier
	timeout      time.Duration
	applyTimeout time.Duration
	qos          byte
	lc           logger.LoggingClient

	// negotiated flips on only when the hello ack intersection contains the
	// "profiles" feature. Subscriptions exist before hello (the redrive runs
	// immediately after ack), but a frame that arrives before the edge
	// processes the ack waits on negotiatedCh rather than being dropped.
	negotiated   atomic.Bool
	negMu        sync.Mutex
	negotiatedCh chan struct{}
}

// NewDownlinkHandler constructs the delivery handler.
func NewDownlinkHandler(cfg DownlinkConfig) *DownlinkHandler {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultDownlinkReportTimeout
	}
	if cfg.ApplyTimeout <= 0 {
		cfg.ApplyTimeout = defaultApplyTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewMockClient()
	}
	appliers := make(map[string]ProfileApplier, len(cfg.Appliers))
	for _, a := range cfg.Appliers {
		appliers[a.Format()] = a
	}
	return &DownlinkHandler{
		gw:           cfg.Gateway,
		store:        cfg.Store,
		appliers:     appliers,
		timeout:      cfg.Timeout,
		applyTimeout: cfg.ApplyTimeout,
		lc:           cfg.Logger,
		negotiatedCh: make(chan struct{}),
	}
}

// Start subscribes to profile spec pushes. It is safe to call again after a
// transport reconnect because the SDK uses a clean MQTT session.
func (h *DownlinkHandler) Start(ctx context.Context, qos byte) error {
	if h.gw == nil {
		h.lc.Warn("profile downlink handler has no gateway; skipping subscription")
		return nil
	}
	h.qos = qos
	return h.gw.SubscribeProfiles(ctx, qos, func(spec device.ProfileSpecFrame) {
		go h.Handle(spec)
	})
}

// SetNegotiation records whether the latest hello ack enabled the profiles
// feature and which render formats the platform accepted. Enabling releases
// frames waiting in the ack/spec scheduling window; disabling installs a
// fresh closed-on-enable channel so later acks can re-arm the gate.
func (h *DownlinkHandler) SetNegotiation(enabled bool, formats []string) {
	h.negMu.Lock()
	defer h.negMu.Unlock()
	if enabled && !h.negotiated.Load() {
		close(h.negotiatedCh)
	}
	if !enabled && h.negotiated.Load() {
		h.negotiatedCh = make(chan struct{})
	}
	h.negotiated.Store(enabled)
	if enabled {
		h.lc.Infof("profile downlink negotiated: formats=%v", formats)
	} else {
		h.lc.Warn("profile downlink not negotiated; spec frames will be ignored")
	}
}

// waitNegotiated reports whether the profiles feature became negotiated. A
// frame that races the ack processing waits up to negotiationWait; anything
// still unnegotiated afterwards is dropped.
func (h *DownlinkHandler) waitNegotiated() bool {
	if h.negotiated.Load() {
		return true
	}
	h.negMu.Lock()
	ch := h.negotiatedCh
	h.negMu.Unlock()
	select {
	case <-ch:
		return h.negotiated.Load()
	case <-time.After(negotiationWait):
		return false
	}
}

// Handle applies one full desired set and reports every processed entry in a
// single status frame. Documents are never logged.
func (h *DownlinkHandler) Handle(spec device.ProfileSpecFrame) {
	if !h.waitNegotiated() {
		h.lc.Warn("profile spec received without a negotiated feature; dropping")
		return
	}
	reports := make([]device.ProfileStatus, 0, len(spec.Entries))
	for _, entry := range spec.Entries {
		if entry.Name == "" {
			h.lc.Warn("profile spec entry without a name; skipping")
			continue
		}
		if entry.Removing {
			reports = append(reports, h.remove(entry))
			continue
		}
		reports = append(reports, h.apply(entry))
	}
	if len(reports) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	frame := device.ProfileStatusFrame{Reports: reports}
	if err := h.gw.ReportProfileStatus(ctx, frame, h.qos); err != nil {
		h.lc.Errorf("report profile status failed: %s", err.Error())
	}
}

// apply lands one normal entry and returns its reported state.
func (h *DownlinkHandler) apply(entry device.ProfileEntry) device.ProfileStatus {
	if entry.Format != device.ProfileFormatNeutral && h.appliers[entry.Format] == nil {
		// Do not guess the parser from document fields: the format is
		// undeclared locally, so the document cannot be honestly landed.
		h.lc.Errorf("profile %s revision %d uses unsupported format %q",
			entry.Name, entry.Revision, entry.Format)
		return profileStatus(entry, device.ProfileStateFailed, reasonUnsupportedFormat)
	}

	// Idempotency: a successfully applied equal-or-newer revision is simply
	// re-confirmed so post-hello redrives never rewrite core-metadata or the
	// ledger. FAILED records fall through so the same revision is retried.
	if rec, ok := h.store.Get(entry.Name); ok &&
		rec.Status == device.ProfileStateApplied && rec.Revision >= entry.Revision {
		return profileStatus(entry, device.ProfileStateApplied, "")
	}

	if entry.Format == device.ProfileFormatNeutral {
		return h.applyNeutral(entry)
	}
	return h.applyExternal(entry)
}

// applyNeutral lands the neutral document inside the durable ledger itself.
func (h *DownlinkHandler) applyNeutral(entry device.ProfileEntry) device.ProfileStatus {
	if err := validateNeutralDocument(entry); err != nil {
		h.lc.Errorf("profile %s revision %d invalid: %s", entry.Name, entry.Revision, err.Error())
		h.recordFailure(entry, "")
		return profileStatus(entry, device.ProfileStateFailed, reasonInvalidDocument)
	}
	rec := downlinkRecord{
		ProfileID: entry.ProfileID,
		Revision:  entry.Revision,
		Format:    device.ProfileFormatNeutral,
		Status:    device.ProfileStateApplied,
		AppliedAt: time.Now().UTC(),
		Document:  entry.Document,
	}
	if err := h.store.Put(entry.Name, rec); err != nil {
		h.lc.Errorf("persist profile %s revision %d failed: %s", entry.Name, entry.Revision, err.Error())
		return profileStatus(entry, device.ProfileStateFailed, reasonStoreFailed)
	}
	h.lc.Infof("profile %s revision %d applied (neutral)", entry.Name, entry.Revision)
	return profileStatus(entry, device.ProfileStateApplied, "")
}

// applyExternal pushes a rendered document to its format-specific landing,
// recording only the coordinates in the ledger. A failure is recorded so a
// restart still retries the revision instead of treating it as applied.
func (h *DownlinkHandler) applyExternal(entry device.ProfileEntry) device.ProfileStatus {
	ctx, cancel := context.WithTimeout(context.Background(), h.applyTimeout)
	defer cancel()

	err := h.appliers[entry.Format].Apply(ctx, entry)
	if err != nil {
		reason := reasonApplyFailed
		switch {
		case errors.Is(err, ErrInvalidProfileDocument):
			reason = reasonInvalidDocument
		case errors.Is(err, ErrProfileInUse):
			reason = reasonProfileInUse
		}
		h.lc.Errorf("apply profile %s revision %d format %s failed: %s",
			entry.Name, entry.Revision, entry.Format, err.Error())
		h.recordFailure(entry, reason)
		return profileStatus(entry, device.ProfileStateFailed, reason)
	}

	rec := downlinkRecord{
		ProfileID: entry.ProfileID,
		Revision:  entry.Revision,
		Format:    entry.Format,
		Status:    device.ProfileStateApplied,
		AppliedAt: time.Now().UTC(),
	}
	if err := h.store.Put(entry.Name, rec); err != nil {
		// The external landing succeeded but the ledger write failed: do
		// not report APPLIED with a stale ledger, or a redrive would apply
		// twice. The external apply is idempotent, so FAILED retries it.
		h.lc.Errorf("persist profile %s revision %d failed: %s", entry.Name, entry.Revision, err.Error())
		return profileStatus(entry, device.ProfileStateFailed, reasonStoreFailed)
	}
	h.lc.Infof("profile %s revision %d applied (%s)", entry.Name, entry.Revision, entry.Format)
	return profileStatus(entry, device.ProfileStateApplied, "")
}

// remove tears down a removing entry. The external landing is removed only
// when the ledger proves this downlink owns the name; an unknown name
// converges to REMOVED without touching local state. Removal ordering
// (external first, ledger second) keeps the entry retryable on failure.
func (h *DownlinkHandler) remove(entry device.ProfileEntry) device.ProfileStatus {
	rec, owned := h.store.Get(entry.Name)
	format := entry.Format
	if format == "" && owned {
		format = rec.Format
	}
	applier := h.appliers[format]

	if owned && applier != nil {
		ctx, cancel := context.WithTimeout(context.Background(), h.applyTimeout)
		err := applier.Remove(ctx, entry.Name)
		cancel()
		if err != nil {
			reason := reasonRemoveFailed
			if errors.Is(err, ErrProfileInUse) {
				reason = reasonProfileInUse
			}
			h.lc.Errorf("remove profile %s failed: %s", entry.Name, err.Error())
			h.recordFailure(entry, reason)
			return profileStatus(entry, device.ProfileStateFailed, reason)
		}
	}

	if _, err := h.store.Remove(entry.Name); err != nil {
		h.lc.Errorf("erase profile %s ledger entry failed: %s", entry.Name, err.Error())
		return profileStatus(entry, device.ProfileStateFailed, reasonStoreFailed)
	}
	h.lc.Infof("profile %s revision %d removed", entry.Name, entry.Revision)
	return profileStatus(entry, device.ProfileStateRemoved, "")
}

// recordFailure best-effort persists the failing coordinates so the revision
// is retried after a restart instead of being mistaken for applied.
func (h *DownlinkHandler) recordFailure(entry device.ProfileEntry, reason string) {
	format := entry.Format
	if format == "" {
		format = device.ProfileFormatNeutral
	}
	rec := downlinkRecord{
		ProfileID: entry.ProfileID,
		Revision:  entry.Revision,
		Format:    format,
		Status:    device.ProfileStateFailed,
		Reason:    reason,
	}
	if err := h.store.Put(entry.Name, rec); err != nil {
		h.lc.Errorf("persist failed profile %s revision %d: %s", entry.Name, entry.Revision, err.Error())
	}
}

// validateNeutralDocument enforces the minimal compliance set of the neutral
// profile shape (protocol §7): name must match the entry and points must be a
// list of {name (string), type (string)}. Unknown type vocabulary is not
// rejected here; landing only needs the structure to be sound.
func validateNeutralDocument(entry device.ProfileEntry) error {
	doc := entry.Document
	if len(doc) == 0 {
		return ErrInvalidProfileDocument
	}
	name, _ := doc["name"].(string)
	if name != entry.Name {
		return ErrInvalidProfileDocument
	}
	rawPoints, ok := doc["points"]
	if !ok {
		return ErrInvalidProfileDocument
	}
	points, ok := rawPoints.([]any)
	if !ok {
		return ErrInvalidProfileDocument
	}
	for _, raw := range points {
		point, ok := raw.(map[string]any)
		if !ok {
			return ErrInvalidProfileDocument
		}
		if _, ok := point["name"].(string); !ok {
			return ErrInvalidProfileDocument
		}
		if _, ok := point["type"].(string); !ok {
			return ErrInvalidProfileDocument
		}
	}
	return nil
}

// profileStatus builds a status report for an entry.
func profileStatus(entry device.ProfileEntry, state, reason string) device.ProfileStatus {
	return device.ProfileStatus{
		Name:     entry.Name,
		Revision: entry.Revision,
		Status:   state,
		Reason:   reason,
	}
}
