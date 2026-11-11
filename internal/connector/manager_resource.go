// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"

	"github.com/nereidai/nereidedge/internal/corecmd"
	device "github.com/nereidai/nereidlink/sdk/device/go"
)

const (
	TypeManagerResource       = "edge-manager-resource"
	defaultManagerDevice      = "nereid-inference"
	managerResourceRetry      = 10 * time.Second
	managerResourceReadQuery  = jobReadQuery
	managerResourceWriteQuery = "ds-pushevent=false&ds-returnevent=false"
	managerStoreDefaultDir    = "/app/data"
	managerStoreFileName      = "manager-resources.json"
	managerStoreVersion       = 1
	pipelinesResource         = "PipelinesJSON"
	externalServablesResource = "ExternalServablesJSON"
)

var (
	managerSafeNamePattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)
	managerResourceNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	managerIDFieldPattern      = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
)

var managerResourceSchema = json.RawMessage(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"object",
  "required":["manager_device","resource"],
  "additionalProperties":false,
  "oneOf":[
    {"required":["pipeline_id"],"not":{"required":["entry_id"]}},
    {"required":["entry_id","id_field"],"not":{"required":["pipeline_id"]}}
  ],
  "properties":{
    "manager_device":{"type":"string","minLength":1,"maxLength":128},
    "resource":{"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9_.-]+$"},
    "pipeline_id":{"type":"string","minLength":1,"maxLength":128,"pattern":"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$"},
    "entry_id":{"type":"string","minLength":1,"maxLength":128},
    "id_field":{"type":"string","minLength":1,"maxLength":64,"pattern":"^[A-Za-z][A-Za-z0-9_]*$"},
    "definition":{"type":"object","description":"Opaque manager-table entry; NE handles only its identity and pipeline version"}
  }
}`)

// ManagerCommandClient is the core-command surface needed for serialized
// manager-table delivery. Read is shared with the inference health poller.
type ManagerCommandClient interface {
	CommandReader
	SetWithQuery(ctx context.Context, edgexName, commandName, rawQuery string, settings any) (dtoCommon.BaseResponse, int, error)
}

// managerEntryKey is the only manager content NE interprets. PipelinesJSON is
// versioned; all other tables use their declared identity field alone.
type managerEntryKey struct {
	ManagerDevice string `json:"manager_device"`
	Resource      string `json:"resource"`
	IDField       string `json:"id_field"`
	EntryID       string `json:"entry_id"`
	Version       string `json:"version,omitempty"`
}

func (k managerEntryKey) ownershipKey() string {
	return strings.Join([]string{k.ManagerDevice, k.Resource, k.IDField, k.EntryID, k.Version}, "\x00")
}

func (k managerEntryKey) resourceKey() string {
	return k.ManagerDevice + "\x00" + k.Resource
}

func (k managerEntryKey) config() managerResourceConfig {
	return managerResourceConfig{
		ManagerDevice: k.ManagerDevice, Resource: k.Resource, IDField: k.IDField,
		EntryID: k.EntryID, EntryVersion: k.Version,
	}
}

// managerResourceConfig supports NL's pipeline_registration shape directly
// and a resource-neutral entry_id+id_field form for other manager tables.
type managerResourceConfig struct {
	ManagerDevice string          `json:"manager_device"`
	Resource      string          `json:"resource"`
	PipelineID    string          `json:"pipeline_id"`
	EntryID       string          `json:"entry_id"`
	IDField       string          `json:"id_field"`
	Definition    json.RawMessage `json:"definition"`
	EntryVersion  string          `json:"-"`
	managed       bool
}

func (c managerResourceConfig) key() managerEntryKey {
	return managerEntryKey{
		ManagerDevice: c.ManagerDevice, Resource: c.Resource, IDField: c.IDField,
		EntryID: c.EntryID, Version: c.EntryVersion,
	}
}

func parseManagerResourceConfig(raw json.RawMessage, allowedDevice string) (managerResourceConfig, error) {
	var cfg managerResourceConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("config-invalid: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return cfg, fmt.Errorf("config-invalid: multiple JSON values")
	}
	cfg.ManagerDevice = strings.TrimSpace(cfg.ManagerDevice)
	cfg.Resource = strings.TrimSpace(cfg.Resource)
	cfg.PipelineID = strings.TrimSpace(cfg.PipelineID)
	cfg.EntryID = strings.TrimSpace(cfg.EntryID)
	cfg.IDField = strings.TrimSpace(cfg.IDField)
	if allowedDevice == "" {
		allowedDevice = defaultManagerDevice
	}
	if cfg.ManagerDevice != allowedDevice || len(cfg.Resource) > 128 ||
		!managerResourceNamePattern.MatchString(cfg.Resource) {
		return cfg, fmt.Errorf("config-invalid: manager_device or resource is not allowed")
	}
	if (cfg.PipelineID == "") == (cfg.EntryID == "") {
		return cfg, fmt.Errorf("config-invalid: exactly one of pipeline_id or entry_id is required")
	}
	if cfg.PipelineID != "" {
		if !managerSafeNamePattern.MatchString(cfg.PipelineID) {
			return cfg, fmt.Errorf("config-invalid: unsafe pipeline_id")
		}
		if cfg.IDField != "" && cfg.IDField != "pipelineId" {
			return cfg, fmt.Errorf("config-invalid: pipeline_id requires id_field pipelineId")
		}
		cfg.EntryID = cfg.PipelineID
		cfg.IDField = "pipelineId"
	}
	if cfg.EntryID == "" || len(cfg.EntryID) > 128 || len(cfg.IDField) > 64 ||
		!managerIDFieldPattern.MatchString(cfg.IDField) {
		return cfg, fmt.Errorf("config-invalid: manager entry identity is required")
	}
	if len(cfg.Definition) > 0 {
		// NI's current ExternalServablesJSON GET returns a merged
		// provisioned/runtime status view, while PUT replaces only the runtime
		// table. Treating that view as a symmetric document would mutate
		// provisioned entries. Require a future runtime-only resource instead.
		if cfg.Resource == externalServablesResource {
			return cfg, fmt.Errorf("config-invalid: merged external-servable status is read-only")
		}
		value, err := decodeOpaqueJSON(cfg.Definition)
		if err != nil {
			return cfg, fmt.Errorf("config-invalid: definition is not valid JSON: %w", err)
		}
		definition, ok := value.(map[string]any)
		if !ok {
			return cfg, fmt.Errorf("config-invalid: definition must be an object")
		}
		if identity, exists := definition[cfg.IDField]; exists {
			if text, ok := identity.(string); !ok || strings.TrimSpace(text) != cfg.EntryID {
				return cfg, fmt.Errorf("config-invalid: definition identity does not match desired entry")
			}
		}
		cfg.managed = true
		if cfg.Resource == pipelinesResource && cfg.IDField == "pipelineId" {
			version, ok := definition["version"].(string)
			cfg.EntryVersion = strings.TrimSpace(version)
			if !ok || !managerSafeNamePattern.MatchString(cfg.EntryVersion) {
				return cfg, fmt.Errorf("config-invalid: a pipeline definition requires a safe string version")
			}
		}
	}
	return cfg, nil
}

type managerResourceResult struct {
	state              string
	reason             string
	observedHash       string
	observedKey        string
	observedEntry      string
	desiredAccepted    bool
	stale              bool
	rejection          bool
	transientRejection bool
}

// managerResourceDesired is replaced, never mutated in its operation fields,
// for every apply/delete. Status and observed fields remain protected by mu.
type managerResourceDesired struct {
	push           device.ConnectorPush
	cfg            managerResourceConfig
	desiredHash    string
	remove         bool
	owned          []managerEntryKey
	awaitingReplay bool
	state          string
	reason         string
	observedHash   string
	observed       map[string]string
	reportedKey    string
}

type managerPendingReport struct {
	push       device.ConnectorPush
	result     managerResourceResult
	generation uint64
}

type managerRejection struct {
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	durable bool
}

// managerResourceManager serializes a same-spec operation before a
// manager/resource transaction. The spec lock is the generation fence: a stale
// retry cannot PUT after a newer apply/delete has replaced its desired record.
type managerResourceManager struct {
	lc              logger.LoggingClient
	mu              sync.Mutex
	commands        ManagerCommandClient
	allowedDevice   string
	retry           time.Duration
	desired         map[string]*managerResourceDesired
	owners          map[string]string
	revisions       map[string]int64
	durableDeletes  map[string]int64
	rejections      map[string]managerRejection
	resourceLocks   map[string]*sync.Mutex
	specLocks       map[string]*sync.Mutex
	storePath       string
	storeDirty      bool
	pendingReports  map[string]*managerPendingReport
	reportSequence  uint64
	reportLatest    map[string]uint64
	reportRevisions map[string]int64
	report          func(string, device.ConnectorPush, managerResourceResult) error
}

func newManagerResourceManager(lc logger.LoggingClient) *managerResourceManager {
	return &managerResourceManager{
		lc: lc, retry: managerResourceRetry,
		desired: make(map[string]*managerResourceDesired), owners: make(map[string]string),
		revisions: make(map[string]int64), durableDeletes: make(map[string]int64),
		rejections:    make(map[string]managerRejection),
		resourceLocks: make(map[string]*sync.Mutex), specLocks: make(map[string]*sync.Mutex),
		pendingReports: make(map[string]*managerPendingReport), reportLatest: make(map[string]uint64),
		reportRevisions: make(map[string]int64),
	}
}

func (m *managerResourceManager) configure(commands ManagerCommandClient, allowedDevice string, retry time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = commands
	m.allowedDevice = allowedDevice
	if retry > 0 {
		m.retry = retry
	}
}

func (m *managerResourceManager) configured() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commands != nil
}

func (m *managerResourceManager) has(specID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, rejected := m.rejections[specID]
	// A rejected node-level spec has no desired record, but its explicit delete
	// still belongs to this owner even when the invalid frame carried a device id.
	return m.desired[specID] != nil || rejected
}

func (m *managerResourceManager) needsRestart(specID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	desired := m.desired[specID]
	return desired == nil || desired.awaitingReplay || desired.state == device.ConnectorStateDegraded
}

func (m *managerResourceManager) specLock(specID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock := m.specLocks[specID]
	if lock == nil {
		lock = &sync.Mutex{}
		m.specLocks[specID] = lock
	}
	return lock
}

func (m *managerResourceManager) resourceLock(key string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock := m.resourceLocks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		m.resourceLocks[key] = lock
	}
	return lock
}

func (m *managerResourceManager) apply(
	ctx context.Context, push device.ConnectorPush, cfg managerResourceConfig, desiredHash string,
) managerResourceResult {
	lock := m.specLock(push.SpecID)
	lock.Lock()
	defer lock.Unlock()

	key := cfg.key()
	m.mu.Lock()
	if m.staleApplyLocked(push.SpecID, push.Revision, desiredHash) {
		m.mu.Unlock()
		return managerResourceResult{stale: true}
	}
	if cfg.managed {
		if owner := m.owners[key.ownershipKey()]; owner != "" && owner != push.SpecID {
			result := m.noteRejectedLocked(push.SpecID, push.Revision, device.ConnectorStateFailed, "config-invalid")
			m.mu.Unlock()
			return result
		}
	}
	previous := m.desired[push.SpecID]
	if previous != nil && !managerIdentityTransitionAllowed(previous.cfg, cfg) {
		result := m.noteRejectedLocked(push.SpecID, push.Revision, device.ConnectorStateFailed, "config-invalid")
		m.mu.Unlock()
		return result
	}
	owned := append([]managerEntryKey(nil), previousOwned(previous)...)
	if !cfg.managed {
		// A reference-only revision never deletes through NI, including when an
		// earlier revision of the same spec owned managed definitions.
		owned = nil
	}
	newKey := cfg.managed && !containsManagerKey(owned, key)
	if newKey {
		owned = append(owned, key)
	}
	desired := &managerResourceDesired{
		push: push, cfg: cfg, desiredHash: desiredHash, owned: owned,
		state: device.ConnectorStateCreating, observed: make(map[string]string),
	}
	oldOwner := m.owners[key.ownershipKey()]
	previousHigh, hadPreviousHigh := m.revisions[push.SpecID]
	previousRejection, hadPreviousRejection := m.rejections[push.SpecID]
	released := make([]managerEntryKey, 0)
	if !cfg.managed && previous != nil {
		for _, ownedKey := range previous.owned {
			wireKey := ownedKey.ownershipKey()
			if m.owners[wireKey] == push.SpecID {
				delete(m.owners, wireKey)
				released = append(released, ownedKey)
			}
		}
	}
	m.desired[push.SpecID] = desired
	m.revisions[push.SpecID] = push.Revision
	delete(m.rejections, push.SpecID)
	if cfg.managed {
		m.owners[key.ownershipKey()] = push.SpecID
	}
	if err := m.persistLocked(); err != nil {
		m.desired[push.SpecID] = previous
		if previous == nil {
			delete(m.desired, push.SpecID)
		}
		if hadPreviousHigh {
			m.revisions[push.SpecID] = previousHigh
		} else {
			delete(m.revisions, push.SpecID)
		}
		if hadPreviousRejection {
			m.rejections[push.SpecID] = previousRejection
		} else {
			delete(m.rejections, push.SpecID)
		}
		for _, ownedKey := range released {
			m.owners[ownedKey.ownershipKey()] = push.SpecID
		}
		if newKey {
			if oldOwner == "" {
				delete(m.owners, key.ownershipKey())
			} else {
				m.owners[key.ownershipKey()] = oldOwner
			}
		}
		// The rollback restores the durable lifecycle, but this newer revision is
		// still the latest observed intent. Fence older reports before its own
		// retryable DEGRADED publication starts.
		m.advanceReportFenceLocked(push.SpecID, push.Revision)
		m.mu.Unlock()
		return managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
	}
	m.advanceReportFenceLocked(push.SpecID, push.Revision)
	m.mu.Unlock()

	result := m.reconcileOne(ctx, desired)
	result, _ = m.recordResult(desired, result)
	result.desiredAccepted = true
	return result
}

func (m *managerResourceManager) superseded(specID string, revision int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.supersededLocked(specID, revision)
}

func (m *managerResourceManager) supersededLocked(specID string, revision int64) bool {
	high := m.revisions[specID]
	if revision < high || revision < m.reportRevisions[specID] {
		return true
	}
	previous := m.desired[specID]
	if previous == nil {
		// An explicit same-revision delete retires a retained rejection. The
		// revision fence remains; only the obsolete terminal lifecycle is removed.
		if _, rejected := m.rejections[specID]; rejected && revision == high {
			return false
		}
		return high > 0 && revision <= high
	}
	return previous.remove && revision <= high
}

func (m *managerResourceManager) staleApply(specID string, revision int64, desiredHash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.staleApplyLocked(specID, revision, desiredHash)
}

func (m *managerResourceManager) staleApplyLocked(specID string, revision int64, desiredHash string) bool {
	if m.supersededLocked(specID, revision) {
		return true
	}
	if _, rejected := m.rejections[specID]; rejected && revision <= m.revisions[specID] {
		return true
	}
	previous := m.desired[specID]
	return previous != nil && revision == m.revisions[specID] && previous.desiredHash != desiredHash
}

// reject fences a newer invalid desired revision without accepting its opaque
// content. Terminal rejection is reported only after its revision and reason
// reach the ledger together; until then DEGRADED keeps NL redriving it.
func (m *managerResourceManager) reject(
	push device.ConnectorPush, state, reason string,
) {
	lock := m.specLock(push.SpecID)
	lock.Lock()
	m.mu.Lock()
	if _, exists := m.rejections[push.SpecID]; exists && push.Revision == m.revisions[push.SpecID] {
		result := m.rejectionResultLocked(push.SpecID)
		m.mu.Unlock()
		lock.Unlock()
		m.publishResult(push.SpecID, push, result)
		return
	}
	if m.supersededLocked(push.SpecID, push.Revision) ||
		(m.desired[push.SpecID] != nil && push.Revision == m.revisions[push.SpecID]) {
		m.mu.Unlock()
		lock.Unlock()
		return
	}
	result := m.noteRejectedLocked(push.SpecID, push.Revision, state, reason)
	m.mu.Unlock()
	lock.Unlock()
	m.publishResult(push.SpecID, push, result)
}

// replayRejection regenerates a durable terminal report after restart without
// parsing or accepting the redriven opaque content. It also retries a rejection
// whose first ledger write failed before terminal FAILED could be reported.
func (m *managerResourceManager) replayRejection(push device.ConnectorPush) bool {
	lock := m.specLock(push.SpecID)
	lock.Lock()
	m.mu.Lock()
	if _, exists := m.rejections[push.SpecID]; !exists || push.Revision != m.revisions[push.SpecID] {
		m.mu.Unlock()
		lock.Unlock()
		return false
	}
	result := m.rejectionResultLocked(push.SpecID)
	m.mu.Unlock()
	lock.Unlock()
	m.publishResult(push.SpecID, push, result)
	return true
}

// noteRejectedLocked also covers validation failures discovered after the
// handler has entered apply (owner conflicts and immutable identity changes).
func (m *managerResourceManager) noteRejectedLocked(
	specID string, revision int64, state, reason string,
) managerResourceResult {
	m.revisions[specID] = revision
	m.rejections[specID] = managerRejection{State: state, Reason: reason}
	m.advanceReportFenceLocked(specID, revision)
	return m.rejectionResultLocked(specID)
}

func (m *managerResourceManager) rejectionResultLocked(specID string) managerResourceResult {
	if !m.rejections[specID].durable {
		if err := m.persistLocked(); err != nil {
			return managerResourceResult{
				state: device.ConnectorStateDegraded, reason: "runtime-unreachable",
				rejection: true, transientRejection: true,
			}
		}
	}
	rejection := m.rejections[specID]
	return managerResourceResult{state: rejection.State, reason: rejection.Reason, rejection: true}
}

func previousOwned(previous *managerResourceDesired) []managerEntryKey {
	if previous == nil {
		return nil
	}
	return previous.owned
}

func managerIdentityTransitionAllowed(previous, next managerResourceConfig) bool {
	oldKey, newKey := previous.key(), next.key()
	if oldKey.ownershipKey() == newKey.ownershipKey() {
		return true
	}
	// NL keeps one pipeline-registration spec while its immutable version
	// advances. Preserve versions previously landed by that spec for rollback.
	return oldKey.Resource == pipelinesResource && newKey.Resource == pipelinesResource &&
		oldKey.ManagerDevice == newKey.ManagerDevice && oldKey.IDField == newKey.IDField &&
		oldKey.EntryID == newKey.EntryID
}

func containsManagerKey(keys []managerEntryKey, want managerEntryKey) bool {
	for _, key := range keys {
		if key.ownershipKey() == want.ownershipKey() {
			return true
		}
	}
	return false
}

// delete retains a failed tombstone for background retry and process restart.
// Only an explicit NL delete can reach this path; redrive absence is not a
// deletion signal because connector delivery is best-effort.
func (m *managerResourceManager) delete(
	ctx context.Context, specID string, revision int64,
) managerResourceResult {
	lock := m.specLock(specID)
	lock.Lock()
	defer lock.Unlock()

	m.mu.Lock()
	if m.supersededLocked(specID, revision) {
		m.mu.Unlock()
		return managerResourceResult{stale: true}
	}
	previous := m.desired[specID]
	if previous == nil {
		// A type-less delete can race ahead of its delayed spec. Retain only the
		// revision fence, never invent an NI definition or issue a blind PUT.
		m.revisions[specID] = revision
		delete(m.rejections, specID)
		m.advanceReportFenceLocked(specID, revision)
		if err := m.persistLocked(); err != nil {
			m.mu.Unlock()
			return managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
		}
		m.mu.Unlock()
		return managerResourceResult{state: device.ConnectorStateApplied}
	}
	desired := &managerResourceDesired{
		push: previous.push, cfg: previous.cfg, desiredHash: previous.desiredHash,
		remove: true, owned: append([]managerEntryKey(nil), previous.owned...),
		state: device.ConnectorStateCreating, observed: make(map[string]string),
	}
	desired.push.Revision = revision
	m.desired[specID] = desired
	m.revisions[specID] = revision
	delete(m.rejections, specID)
	m.advanceReportFenceLocked(specID, revision)
	if err := m.persistLocked(); err != nil {
		// Keep the in-memory tombstone and high-water mark even when their
		// durable update failed. After restart the older record waits for an
		// explicit redrive/delete; absence alone never executes it.
		desired.state = device.ConnectorStateDegraded
		desired.reason = "runtime-unreachable"
		m.mu.Unlock()
		return managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
	}
	m.mu.Unlock()

	result := m.reconcileOne(ctx, desired)
	if result.state == device.ConnectorStateFailed {
		result = managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
	}
	if result.state == device.ConnectorStateApplied {
		return m.finishDelete(specID, desired)
	}
	result, _ = m.recordResult(desired, result)
	return result
}

func (m *managerResourceManager) finishDelete(
	specID string, desired *managerResourceDesired,
) managerResourceResult {
	m.mu.Lock()
	if m.desired[specID] != desired {
		m.mu.Unlock()
		return managerResourceResult{}
	}
	delete(m.desired, specID)
	changedOwners := make([]string, 0, len(desired.owned))
	for _, key := range desired.owned {
		wireKey := key.ownershipKey()
		if m.owners[wireKey] == specID {
			delete(m.owners, wireKey)
			changedOwners = append(changedOwners, wireKey)
		}
	}
	if err := m.persistLocked(); err != nil {
		m.desired[specID] = desired
		for _, wireKey := range changedOwners {
			m.owners[wireKey] = specID
		}
		desired.state = device.ConnectorStateDegraded
		desired.reason = "runtime-unreachable"
		m.mu.Unlock()
		return managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
	}
	m.advanceReportFenceLocked(specID, desired.push.Revision)
	m.mu.Unlock()
	return managerResourceResult{state: device.ConnectorStateApplied, observedHash: desired.observedHash}
}

// prepareReplay prevents persisted definitions from executing until NL
// redrives their opaque content. Missing frames never become deletes: NL's
// redrive is best-effort, so only an explicit versioned delete is destructive.
func (m *managerResourceManager) prepareReplay() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, desired := range m.desired {
		if !desired.remove {
			desired.awaitingReplay = true
		}
	}
}

func (m *managerResourceManager) run(ctx context.Context) {
	m.mu.Lock()
	interval := m.retry
	commands := m.commands
	m.mu.Unlock()
	if commands == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

func (m *managerResourceManager) reconcile(ctx context.Context) {
	m.retryStore()
	m.mu.Lock()
	snapshot := make([]*managerResourceDesired, 0, len(m.desired))
	for _, desired := range m.desired {
		if desired.state != device.ConnectorStateFailed {
			snapshot = append(snapshot, desired)
		}
	}
	m.mu.Unlock()
	for _, desired := range snapshot {
		lock := m.specLock(desired.push.SpecID)
		lock.Lock()
		m.mu.Lock()
		current := m.desired[desired.push.SpecID] == desired
		m.mu.Unlock()
		if !current {
			lock.Unlock()
			continue
		}
		result := m.reconcileOne(ctx, desired)
		if result.state == "" {
			lock.Unlock()
			continue
		}
		if desired.remove {
			if result.state == device.ConnectorStateFailed {
				result = managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
			}
			if result.state == device.ConnectorStateApplied {
				result = m.finishDelete(desired.push.SpecID, desired)
			} else {
				result, _ = m.recordResult(desired, result)
			}
			if result.state != "" && result.state != device.ConnectorStateApplied {
				m.publishResult(desired.push.SpecID, desired.push, result)
			}
			lock.Unlock()
			continue
		}
		result, changed := m.recordResult(desired, result)
		if changed {
			m.publishResult(desired.push.SpecID, desired.push, result)
		}
		lock.Unlock()
	}
	m.retryPendingReports()
}

func (m *managerResourceManager) retryStore() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.storeDirty {
		_ = m.persistLocked()
	}
}

// publishResult keeps a failed publication dirty so the ordinary manager retry
// cadence republishes it even when the runtime state and observed hash did not
// change. A sequence is assigned before publication: an older network completion
// can neither clear nor replace a newer pending report.
func (m *managerResourceManager) publishResult(
	specID string, push device.ConnectorPush, result managerResourceResult,
) {
	m.mu.Lock()
	if m.report == nil || result.state == "" || m.staleReportLocked(specID, push.Revision, result) {
		m.mu.Unlock()
		return
	}
	m.reportSequence++
	generation := m.reportSequence
	m.reportLatest[specID] = generation
	m.reportRevisions[specID] = push.Revision
	report := m.report
	m.mu.Unlock()

	err := report(specID, push, result)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reportLatest[specID] != generation || m.staleReportLocked(specID, push.Revision, result) {
		return
	}
	if err == nil {
		if pending := m.pendingReports[specID]; pending != nil && pending.generation <= generation {
			delete(m.pendingReports, specID)
		}
		return
	}
	m.pendingReports[specID] = &managerPendingReport{
		push: push, result: result, generation: generation,
	}
	if desired := m.desired[specID]; desired != nil && desired.push.Revision == push.Revision {
		desired.reportedKey = ""
	}
}

func (m *managerResourceManager) retryPendingReports() {
	m.mu.Lock()
	if m.report == nil {
		m.mu.Unlock()
		return
	}
	snapshot := make([]*managerPendingReport, 0, len(m.pendingReports))
	for _, pending := range m.pendingReports {
		snapshot = append(snapshot, pending)
	}
	m.mu.Unlock()
	for _, pending := range snapshot {
		m.mu.Lock()
		specID := pending.push.SpecID
		if m.pendingReports[specID] != pending {
			m.mu.Unlock()
			continue
		}
		if m.staleReportLocked(specID, pending.push.Revision, pending.result) {
			delete(m.pendingReports, specID)
			m.mu.Unlock()
			continue
		}
		m.reportSequence++
		generation := m.reportSequence
		pending.generation = generation
		m.reportLatest[specID] = generation
		m.reportRevisions[specID] = pending.push.Revision
		report := m.report
		push, result := pending.push, pending.result
		m.mu.Unlock()

		err := report(specID, push, result)
		m.mu.Lock()
		if m.pendingReports[specID] != pending || m.reportLatest[specID] != generation {
			m.mu.Unlock()
			continue
		}
		if err == nil {
			delete(m.pendingReports, specID)
			if desired := m.desired[specID]; desired != nil &&
				desired.push.Revision == push.Revision && desired.state == result.state {
				desired.reportedKey = managerResultKey(desired.push, result)
			}
		}
		m.mu.Unlock()
	}
}

// staleReportLocked checks lifecycle identity as well as revision. In
// particular, a same-revision delete can retire a rejection while its recovered
// report is still waiting to run, and a revision-only delete fence supersedes
// every older desired/apply report once its ledger write is durable.
func (m *managerResourceManager) staleReportLocked(
	specID string, revision int64, result managerResourceResult,
) bool {
	high := m.revisions[specID]
	if revision < high || revision < m.reportRevisions[specID] {
		return true
	}
	rejection, rejected := m.rejections[specID]
	if result.rejection {
		if !rejected || revision != high {
			return true
		}
		if result.transientRejection {
			// A retryable pre-durability result belongs to this rejection only
			// until any successful full-ledger write makes its terminal state durable.
			return rejection.durable
		}
		return rejection.State != result.state || rejection.Reason != result.reason
	}
	if desired := m.desired[specID]; desired != nil && desired.push.Revision == revision {
		current := managerResourceResult{
			state: desired.state, reason: desired.reason, observedHash: desired.observedHash,
		}
		// A result captured before a same-revision reconcile must not start a
		// new publication after that reconcile has advanced the desired status.
		reportPush := device.ConnectorPush{SpecID: specID, Revision: revision}
		return managerResultKey(desired.push, current) != managerResultKey(reportPush, result)
	}
	if m.desired[specID] == nil && !rejected && high > 0 && revision <= high {
		// storeDirty is global and a committed rejection at this revision is not
		// a committed delete. Only a revision-only delete in the last successful
		// full ledger makes this transient delete result obsolete.
		return m.durableDeletes[specID] >= revision
	}
	return false
}

// advanceReportFenceLocked invalidates in-flight and pending reports as soon as
// a newer lifecycle decision is accepted, including deletes that have no final
// connector-status frame of their own.
func (m *managerResourceManager) advanceReportFenceLocked(specID string, revision int64) {
	if m.reportRevisions[specID] <= revision {
		m.reportSequence++
		m.reportLatest[specID] = m.reportSequence
		m.reportRevisions[specID] = revision
	}
	if pending := m.pendingReports[specID]; pending != nil && pending.push.Revision <= revision {
		delete(m.pendingReports, specID)
	}
}

func (m *managerResourceManager) recordResult(
	desired *managerResourceDesired, result managerResourceResult,
) (managerResourceResult, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.desired[desired.push.SpecID] != desired {
		return managerResourceResult{}, false
	}
	previous := desired.reportedKey
	desired.state, desired.reason, desired.observedHash = result.state, result.reason, result.observedHash
	if result.observedKey != "" {
		desired.observed[result.observedKey] = result.observedEntry
	}
	desired.reportedKey = managerResultKey(desired.push, result)
	if err := m.persistLocked(); err != nil {
		result = managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
		desired.state, desired.reason = result.state, result.reason
		desired.reportedKey = managerResultKey(desired.push, result)
	}
	return result, previous != desired.reportedKey
}

func managerResultKey(push device.ConnectorPush, result managerResourceResult) string {
	frame := connectorStatusFrame{ConnectorStatus: device.ConnectorStatus{
		SpecID: push.SpecID, State: result.state, Revision: push.Revision, Reason: result.reason,
	}}
	if result.state == device.ConnectorStateApplied {
		frame.ObservedRevision = push.Revision
		frame.ObservedHash = result.observedHash
	}
	return frame.key()
}

// managerOperation is an immutable snapshot; no desired.remove or config field
// is read concurrently with a newer apply/delete replacing the record.
type managerOperation struct {
	push           device.ConnectorPush
	cfg            managerResourceConfig
	remove         bool
	owned          []managerEntryKey
	awaitingReplay bool
	observed       map[string]string
}

func (m *managerResourceManager) operation(desired *managerResourceDesired) (managerOperation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.desired[desired.push.SpecID] != desired ||
		(!desired.remove && desired.push.Revision != m.revisions[desired.push.SpecID]) {
		return managerOperation{}, false
	}
	observed := make(map[string]string, len(desired.observed))
	for key, value := range desired.observed {
		observed[key] = value
	}
	return managerOperation{
		push: desired.push, cfg: desired.cfg, remove: desired.remove,
		owned:          append([]managerEntryKey(nil), desired.owned...),
		awaitingReplay: desired.awaitingReplay, observed: observed,
	}, true
}

func (m *managerResourceManager) reconcileOne(
	ctx context.Context, desired *managerResourceDesired,
) managerResourceResult {
	op, current := m.operation(desired)
	if !current || (op.awaitingReplay && !op.remove) {
		return managerResourceResult{}
	}
	m.mu.Lock()
	commands := m.commands
	m.mu.Unlock()
	if commands == nil {
		return managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
	}
	if op.remove {
		return m.reconcileDelete(ctx, commands, op)
	}
	return m.reconcileApply(ctx, commands, op)
}

func (m *managerResourceManager) reconcileApply(
	ctx context.Context, commands ManagerCommandClient, op managerOperation,
) managerResourceResult {
	key := op.cfg.key()
	lock := m.resourceLock(key.resourceKey())
	lock.Lock()
	defer lock.Unlock()

	document, result := m.readDocument(ctx, commands, key.config())
	if result.state != "" {
		return result
	}
	matches := matchingManagerEntries(document.entries, key)
	if !op.cfg.managed {
		if len(matches) == 0 {
			return managerResourceResult{state: device.ConnectorStateDegraded, reason: "manager-entry-missing"}
		}
		return managerResourceResult{state: device.ConnectorStateApplied, observedHash: document.hash}
	}
	wireKey := key.ownershipKey()
	if baseline := op.observed[wireKey]; baseline != "" && len(matches) == 1 {
		if canonical, err := json.Marshal(document.entries[matches[0]]); err == nil && string(canonical) == baseline {
			return managerResourceResult{
				state: device.ConnectorStateApplied, observedHash: document.hash,
				observedKey: wireKey, observedEntry: baseline,
			}
		}
	}
	value, err := decodeOpaqueJSON(op.cfg.Definition)
	if err != nil {
		return managerResourceResult{state: device.ConnectorStateFailed, reason: "config-invalid"}
	}
	definition := value.(map[string]any)
	if _, exists := definition[key.IDField]; !exists {
		definition[key.IDField] = key.EntryID
	}
	next := make([]map[string]any, 0, len(document.entries)+1)
	inserted := false
	for i, entry := range document.entries {
		if managerEntryMatches(entry, key) {
			if !inserted {
				next = append(next, definition)
				inserted = true
			}
			continue
		}
		next = append(next, document.entries[i])
	}
	if !inserted {
		next = append(next, definition)
	}
	canonical, err := json.Marshal(next)
	if err != nil {
		return managerResourceResult{state: device.ConnectorStateFailed, reason: "config-invalid"}
	}
	if len(matches) == 1 && bytes.Equal(canonical, document.canonical) {
		actual, _ := json.Marshal(next[matches[0]])
		return managerResourceResult{
			state: device.ConnectorStateApplied, observedHash: document.hash,
			observedKey: wireKey, observedEntry: string(actual),
		}
	}
	readback, result := m.writeDocument(ctx, commands, key.config(), next)
	if result.state != "" {
		return result
	}
	actual, result := verifyManagerApplyReadback(next, readback, key)
	if result.state != "" {
		return result
	}
	return managerResourceResult{
		state: device.ConnectorStateApplied, observedHash: readback.hash,
		observedKey: wireKey, observedEntry: actual,
	}
}

func (m *managerResourceManager) reconcileDelete(
	ctx context.Context, commands ManagerCommandClient, op managerOperation,
) managerResourceResult {
	observedHash := ""
	for _, key := range op.owned {
		lock := m.resourceLock(key.resourceKey())
		lock.Lock()
		document, result := m.readDocument(ctx, commands, key.config())
		if result.state != "" {
			lock.Unlock()
			return result
		}
		next := make([]map[string]any, 0, len(document.entries))
		for _, entry := range document.entries {
			if !managerEntryMatches(entry, key) {
				next = append(next, entry)
			}
		}
		observedHash = document.hash
		if len(next) == len(document.entries) {
			lock.Unlock()
			continue
		}
		readback, result := m.writeDocument(ctx, commands, key.config(), next)
		if result.state != "" {
			lock.Unlock()
			return result
		}
		if result := verifyManagerDeleteReadback(next, readback, key); result.state != "" {
			lock.Unlock()
			return result
		}
		observedHash = readback.hash
		lock.Unlock()
	}
	return managerResourceResult{state: device.ConnectorStateApplied, observedHash: observedHash}
}

type managerDocument struct {
	entries   []map[string]any
	canonical []byte
	hash      string
}

func (m *managerResourceManager) readDocument(
	ctx context.Context, commands ManagerCommandClient, cfg managerResourceConfig,
) (managerDocument, managerResourceResult) {
	response, status, err := commands.Read(ctx, cfg.ManagerDevice, cfg.Resource, managerResourceReadQuery)
	if err != nil || status != 200 {
		return managerDocument{}, managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
	}
	value, exists := corecmd.ReadingsToMap(response.Event.Readings)[cfg.Resource]
	if !exists {
		return managerDocument{}, managerResourceResult{state: device.ConnectorStateDegraded, reason: "readback-invalid"}
	}
	var raw []byte
	switch typed := value.(type) {
	case string:
		raw = []byte(typed)
	default:
		raw, err = json.Marshal(value)
		if err != nil {
			return managerDocument{}, managerResourceResult{state: device.ConnectorStateDegraded, reason: "readback-invalid"}
		}
	}
	document, err := parseManagerDocument(raw)
	if err != nil {
		return managerDocument{}, managerResourceResult{state: device.ConnectorStateDegraded, reason: "readback-invalid"}
	}
	return document, managerResourceResult{}
}

func (m *managerResourceManager) writeDocument(
	ctx context.Context, commands ManagerCommandClient, cfg managerResourceConfig, entries []map[string]any,
) (managerDocument, managerResourceResult) {
	// Defense in depth for ledgers written by older builds: never copy NI's
	// merged provisioned/runtime status view back into its runtime-only PUT.
	if cfg.Resource == externalServablesResource {
		return managerDocument{}, managerResourceResult{state: device.ConnectorStateFailed, reason: "config-invalid"}
	}
	canonical, err := json.Marshal(entries)
	if err != nil {
		return managerDocument{}, managerResourceResult{state: device.ConnectorStateFailed, reason: "config-invalid"}
	}
	response, status, err := commands.SetWithQuery(ctx, cfg.ManagerDevice, cfg.Resource,
		managerResourceWriteQuery, map[string]string{cfg.Resource: string(canonical)})
	if err == nil && response.StatusCode >= 400 {
		status = response.StatusCode
		err = fmt.Errorf("manager write returned status %d", status)
	}
	if err != nil {
		if status >= 400 && status < 500 && status != 408 && status != 409 && status != 429 {
			return managerDocument{}, managerResourceResult{state: device.ConnectorStateFailed, reason: "config-invalid"}
		}
		return managerDocument{}, managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"}
	}
	return m.readDocument(ctx, commands, cfg)
}

// verifyManagerApplyReadback accepts NI's normalization and table sorting. The
// owned id+version must be unique; every non-owned entry must survive exactly,
// independent of array order. Opaque desired fields are deliberately not
// compared byte-for-byte with NI's normalized representation.
func verifyManagerApplyReadback(
	expected []map[string]any, actual managerDocument, key managerEntryKey,
) (string, managerResourceResult) {
	matches := matchingManagerEntries(actual.entries, key)
	if len(matches) != 1 || len(actual.entries) != len(expected) {
		return "", managerResourceResult{state: device.ConnectorStateDegraded, reason: "readback-mismatch"}
	}
	if !managerNonTargetEntriesEqual(expected, actual.entries, key, true) {
		return "", managerResourceResult{state: device.ConnectorStateDegraded, reason: "readback-mismatch"}
	}
	canonical, err := json.Marshal(actual.entries[matches[0]])
	if err != nil {
		return "", managerResourceResult{state: device.ConnectorStateDegraded, reason: "readback-invalid"}
	}
	return string(canonical), managerResourceResult{}
}

func verifyManagerDeleteReadback(
	expected []map[string]any, actual managerDocument, key managerEntryKey,
) managerResourceResult {
	if len(matchingManagerEntries(actual.entries, key)) != 0 || len(actual.entries) != len(expected) ||
		!managerNonTargetEntriesEqual(expected, actual.entries, key, false) {
		return managerResourceResult{state: device.ConnectorStateDegraded, reason: "readback-mismatch"}
	}
	return managerResourceResult{}
}

func managerNonTargetEntriesEqual(
	expected, actual []map[string]any, key managerEntryKey, skipExpectedTarget bool,
) bool {
	counts := make(map[string]int, len(expected))
	for _, entry := range expected {
		if skipExpectedTarget && managerEntryMatches(entry, key) {
			continue
		}
		canonical, err := json.Marshal(entry)
		if err != nil {
			return false
		}
		counts[string(canonical)]++
	}
	for _, entry := range actual {
		if managerEntryMatches(entry, key) {
			continue
		}
		canonical, err := json.Marshal(entry)
		if err != nil || counts[string(canonical)] == 0 {
			return false
		}
		counts[string(canonical)]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func matchingManagerEntries(entries []map[string]any, key managerEntryKey) []int {
	matches := make([]int, 0, 1)
	for i, entry := range entries {
		if managerEntryMatches(entry, key) {
			matches = append(matches, i)
		}
	}
	return matches
}

func managerEntryMatches(entry map[string]any, key managerEntryKey) bool {
	identity, _ := entry[key.IDField].(string)
	if identity != key.EntryID {
		return false
	}
	if key.Version == "" {
		return true
	}
	version, _ := entry["version"].(string)
	return version == key.Version
}

func parseManagerDocument(raw []byte) (managerDocument, error) {
	value, err := decodeOpaqueJSON(raw)
	if err != nil {
		return managerDocument{}, err
	}
	array, ok := value.([]any)
	if !ok {
		return managerDocument{}, fmt.Errorf("manager document must be an array")
	}
	entries := make([]map[string]any, 0, len(array))
	for _, item := range array {
		entry, ok := item.(map[string]any)
		if !ok {
			return managerDocument{}, fmt.Errorf("manager document entries must be objects")
		}
		entries = append(entries, entry)
	}
	canonical, err := json.Marshal(entries)
	if err != nil {
		return managerDocument{}, err
	}
	sum := sha256.Sum256(canonical)
	return managerDocument{entries: entries, canonical: canonical, hash: hex.EncodeToString(sum[:])}, nil
}

func decodeOpaqueJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

// managerDiskRecord deliberately excludes Definition and observed entry
// content. Only identities and lifecycle state belong to NE's durable ledger;
// NI remains the owner of opaque manager documents.
type managerDiskRecord struct {
	Push         device.ConnectorPush `json:"push"`
	Current      managerEntryKey      `json:"current"`
	Managed      bool                 `json:"managed"`
	DesiredHash  string               `json:"desired_hash,omitempty"`
	Remove       bool                 `json:"remove,omitempty"`
	Owned        []managerEntryKey    `json:"owned,omitempty"`
	State        string               `json:"state"`
	Reason       string               `json:"reason,omitempty"`
	ObservedHash string               `json:"observed_hash,omitempty"`
}

type managerDiskFile struct {
	Version    int                          `json:"version"`
	Entries    map[string]managerDiskRecord `json:"entries"`
	Revisions  map[string]int64             `json:"revisions,omitempty"`
	Rejections map[string]managerRejection  `json:"rejections,omitempty"`
}

// openStore loads the ownership/delete ledger. Opaque desired definitions are
// await-replay after reopening; only tombstones can execute before NL redrives
// their full desired configurations.
func (m *managerResourceManager) openStore(dir string) error {
	if dir == "" {
		dir = managerStoreDefaultDir
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create manager resource store: %w", err)
	}
	probe, err := os.CreateTemp(dir, ".manager-resources-probe-")
	if err != nil {
		return fmt.Errorf("probe manager resource store writability: %w", err)
	}
	probePath := probe.Name()
	if err := probe.Close(); err != nil {
		_ = os.Remove(probePath)
		return fmt.Errorf("close manager resource store probe: %w", err)
	}
	if err := os.Remove(probePath); err != nil {
		return fmt.Errorf("remove manager resource store probe: %w", err)
	}
	path := filepath.Join(dir, managerStoreFileName)
	raw, err := os.ReadFile(path) //nolint:gosec // service-owned ledger path
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read manager resource store: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if os.IsNotExist(err) {
		m.storePath = path
		return nil
	}
	var stored managerDiskFile
	if err := json.Unmarshal(raw, &stored); err != nil {
		return fmt.Errorf("decode manager resource store: %w", err)
	}
	if stored.Version != 0 && stored.Version != managerStoreVersion {
		return fmt.Errorf("unsupported manager resource store version %d", stored.Version)
	}
	for specID, revision := range stored.Revisions {
		m.revisions[specID] = revision
	}
	for specID, record := range stored.Entries {
		cfg := record.Current.config()
		cfg.managed = record.Managed
		desired := &managerResourceDesired{
			push: record.Push, cfg: cfg, desiredHash: record.DesiredHash,
			remove: record.Remove, owned: append([]managerEntryKey(nil), record.Owned...),
			awaitingReplay: !record.Remove, state: record.State, reason: record.Reason,
			observedHash: record.ObservedHash, observed: make(map[string]string),
		}
		if desired.push.SpecID == "" {
			desired.push.SpecID = specID
		}
		m.desired[specID] = desired
		if desired.push.Revision > m.revisions[specID] {
			m.revisions[specID] = desired.push.Revision
		}
		for _, key := range desired.owned {
			m.owners[key.ownershipKey()] = specID
		}
	}
	for specID, rejection := range stored.Rejections {
		revision := m.revisions[specID]
		if revision <= 0 {
			return fmt.Errorf("manager resource rejection %q has no revision fence", specID)
		}
		rejection.durable = true
		m.rejections[specID] = rejection
		// Reports are at-least-once. Replaying a durable terminal rejection after
		// restart is safer than losing it when the previous publish was lost.
		m.pendingReports[specID] = newManagerRejectionReport(specID, revision, rejection)
	}
	m.durableDeletes = m.revisionOnlyDeletesLocked()
	m.storePath = path
	return nil
}

func (m *managerResourceManager) persistLocked() error {
	err := m.writeStoreLocked()
	m.storeDirty = err != nil
	if err == nil {
		m.durableDeletes = m.revisionOnlyDeletesLocked()
		m.markRejectionsDurableLocked()
	}
	return err
}

// revisionOnlyDeletesLocked distinguishes a committed revision-only delete
// from another lifecycle at the same revision, especially a durable rejection
// whose same-revision delete has not reached the ledger yet.
func (m *managerResourceManager) revisionOnlyDeletesLocked() map[string]int64 {
	deletes := make(map[string]int64)
	for specID, revision := range m.revisions {
		if m.desired[specID] != nil {
			continue
		}
		if _, rejected := m.rejections[specID]; rejected {
			continue
		}
		deletes[specID] = revision
	}
	return deletes
}

// markRejectionsDurableLocked runs after every successful full-ledger write,
// not only after retryStore. An unrelated spec's write therefore cannot clear
// storeDirty while leaving another rejection without its terminal report.
func (m *managerResourceManager) markRejectionsDurableLocked() {
	for specID, rejection := range m.rejections {
		if rejection.durable {
			continue
		}
		rejection.durable = true
		m.rejections[specID] = rejection
		revision := m.revisions[specID]
		if m.reportRevisions[specID] > revision {
			// A newer apply was observed but its ledger write failed and rolled
			// back to this rejection. Preserve that newer retryable report.
			continue
		}
		m.advanceReportFenceLocked(specID, revision)
		pending := newManagerRejectionReport(specID, revision, rejection)
		pending.generation = m.reportSequence
		m.pendingReports[specID] = pending
	}
}

func newManagerRejectionReport(
	specID string, revision int64, rejection managerRejection,
) *managerPendingReport {
	return &managerPendingReport{
		push: device.ConnectorPush{
			Kind: device.ConnectorKindSpec, SpecID: specID,
			ConnectorType: TypeManagerResource, Revision: revision,
		},
		result: managerResourceResult{
			state: rejection.State, reason: rejection.Reason, rejection: true,
		},
	}
}

func (m *managerResourceManager) writeStoreLocked() error {
	if m.storePath == "" {
		return nil
	}
	stored := managerDiskFile{
		Version: managerStoreVersion, Entries: make(map[string]managerDiskRecord, len(m.desired)),
		Revisions: m.revisions, Rejections: m.rejections,
	}
	for specID, desired := range m.desired {
		push := desired.push
		push.Config, push.Mappings, push.HealthCheck = nil, nil, nil
		stored.Entries[specID] = managerDiskRecord{
			Push: push, Current: desired.cfg.key(), Managed: desired.cfg.managed,
			DesiredHash: desired.desiredHash, Remove: desired.remove,
			Owned: append([]managerEntryKey(nil), desired.owned...), State: desired.state,
			Reason: desired.reason, ObservedHash: desired.observedHash,
		}
	}
	raw, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manager resource store: %w", err)
	}
	tmp := m.storePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write manager resource store: %w", err)
	}
	if err := os.Rename(tmp, m.storePath); err != nil {
		return fmt.Errorf("commit manager resource store: %w", err)
	}
	return nil
}

// reportManagerResult publishes retry/readback transitions on the existing
// connector-status channel with optional observed revision/hash fields.
func (a *Applier) reportManagerResult(_ string, push device.ConnectorPush, result managerResourceResult) error {
	frame := connectorStatusFrame{ConnectorStatus: device.ConnectorStatus{
		SpecID: push.SpecID, State: result.state, Revision: push.Revision, Reason: result.reason,
	}}
	if result.state == device.ConnectorStateApplied {
		frame.ObservedRevision = push.Revision
		frame.ObservedHash = result.observedHash
	}
	return a.reportStatusFrame(frame)
}
