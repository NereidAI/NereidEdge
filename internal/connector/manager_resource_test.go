// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

type statusReporterFunc func(context.Context, device.ConnectorStatus, byte) error

func (f statusReporterFunc) ReportConnectorStatus(
	ctx context.Context, status device.ConnectorStatus, qos byte,
) error {
	return f(ctx, status, qos)
}

type fakeManagerCommands struct {
	mu             sync.Mutex
	document       string
	writes         int
	reads          int
	failWrites     int
	lastWriteQuery string
	lastWriteBody  string
	mismatchOnce   bool
	readbackOnce   string
	writeDelay     time.Duration
	transformWrite func(string) string
	blockWrites    bool
	writeEntered   chan struct{}
	writeRelease   chan struct{}
}

func (f *fakeManagerCommands) Read(_ context.Context, _, resource, _ string) (responses.EventResponse, int, error) {
	f.mu.Lock()
	f.reads++
	document := f.document
	if f.readbackOnce != "" {
		document = f.readbackOnce
		f.readbackOnce = ""
	}
	f.mu.Unlock()
	reading, err := dtos.NewSimpleReading("nereid-inference-manager", "nereid-inference", resource, common.ValueTypeString, document)
	if err != nil {
		return responses.EventResponse{}, 0, err
	}
	return responses.NewEventResponse("req", "", 200, dtos.Event{
		DeviceName: "nereid-inference", ProfileName: "nereid-inference-manager",
		Readings: []dtos.BaseReading{reading},
	}), 200, nil
}

func (f *fakeManagerCommands) SetWithQuery(
	_ context.Context, _, resource, query string, settings any,
) (dtoCommon.BaseResponse, int, error) {
	f.mu.Lock()
	f.writes++
	f.lastWriteQuery = query
	if f.failWrites > 0 {
		f.failWrites--
		f.mu.Unlock()
		return dtoCommon.BaseResponse{}, 503, errors.New("temporary manager outage")
	}
	values, ok := settings.(map[string]string)
	if !ok {
		f.mu.Unlock()
		return dtoCommon.BaseResponse{}, 400, errors.New("settings must be a string map")
	}
	body := values[resource]
	f.lastWriteBody = body
	if f.blockWrites {
		f.blockWrites = false
		entered, release := f.writeEntered, f.writeRelease
		f.mu.Unlock()
		if entered != nil {
			entered <- struct{}{}
		}
		if release != nil {
			<-release
		}
		f.mu.Lock()
	}
	if f.mismatchOnce {
		f.readbackOnce = f.document
		f.mismatchOnce = false
	}
	f.document = body
	if f.transformWrite != nil {
		f.document = f.transformWrite(body)
	}
	delay := f.writeDelay
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	return dtoCommon.BaseResponse{}, 200, nil
}

func managerPush(specID string, revision int64, definition json.RawMessage) device.ConnectorPush {
	config := map[string]any{
		"manager_device": "nereid-inference",
		"resource":       "PipelinesJSON",
		"pipeline_id":    specID,
	}
	if definition != nil {
		config["definition"] = definition
	}
	raw, _ := json.Marshal(config)
	return device.ConnectorPush{
		Kind: device.ConnectorKindSpec, SpecID: specID, ConnectorType: TypeManagerResource,
		Revision: revision, Config: raw,
	}
}

func configureFakeManager(a *Applier, commands *fakeManagerCommands) {
	a.managerResources.configure(commands, "nereid-inference", 5*time.Millisecond)
}

func decodeExtendedStatus(t *testing.T, raw []byte) connectorStatusFrame {
	t.Helper()
	var frame connectorStatusFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode extended status: %v; raw=%s", err, raw)
	}
	return frame
}

func waitRawStatus(t *testing.T, ch <-chan []byte, state string) connectorStatusFrame {
	t.Helper()
	select {
	case raw := <-ch:
		frame := decodeExtendedStatus(t, raw)
		if frame.State != state {
			t.Fatalf("state=%s, want %s; raw=%s", frame.State, state, raw)
		}
		return frame
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for raw %s status", state)
		return connectorStatusFrame{}
	}
}

func TestManagerResourceAtomicMergeAndObservedReadback(t *testing.T) {
	commands := &fakeManagerCommands{document: `[
		{"pipelineId":"other","version":"1","graph":{"opaque":[1,2,3]}},
		{"pipelineId":"spec-merge","version":"2","obsolete":true}
	]`}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 4)
	a.reportRawStatus = func(_ context.Context, raw []byte) error {
		rawStatuses <- append([]byte(nil), raw...)
		return nil
	}
	definition := json.RawMessage(`{"version":"2","graph":{"nodes":[{"type":"opaque"}]}}`)

	if err := NewManagerResourceHandler(a).Apply(context.Background(), managerPush("spec-merge", 7, definition)); err != nil {
		t.Fatal(err)
	}
	frame := waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if frame.ObservedRevision != 7 || len(frame.ObservedHash) != 64 {
		t.Fatalf("observed status=%+v, want revision 7 and SHA-256", frame)
	}

	commands.mu.Lock()
	finalDocument := commands.document
	writeQuery := commands.lastWriteQuery
	writeBody := commands.lastWriteBody
	commands.mu.Unlock()
	if writeQuery != "ds-pushevent=false&ds-returnevent=false" {
		t.Fatalf("PUT query=%q", writeQuery)
	}
	if writeBody == "" {
		t.Fatal("manager PUT was not issued")
	}
	parsed, err := parseManagerDocument([]byte(finalDocument))
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.entries) != 2 {
		t.Fatalf("entries=%d, want existing + replaced entry", len(parsed.entries))
	}
	if parsed.entries[0]["pipelineId"] != "other" {
		t.Fatalf("unrelated entry was lost or reordered: %+v", parsed.entries)
	}
	merged := parsed.entries[1]
	if merged["pipelineId"] != "spec-merge" || merged["version"] != "2" {
		t.Fatalf("merged entry=%+v", merged)
	}
	if _, exists := merged["obsolete"]; exists {
		t.Fatalf("replacement retained obsolete fields: %+v", merged)
	}
	if parsed.hash != frame.ObservedHash {
		t.Fatalf("observed hash=%q, readback hash=%q", frame.ObservedHash, parsed.hash)
	}
}

func TestManagerResourceTransientFailureRetainsDesiredAndRetries(t *testing.T) {
	commands := &fakeManagerCommands{document: `[]`, failWrites: 1}
	a, rec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 4)
	a.reportRawStatus = func(_ context.Context, raw []byte) error {
		rawStatuses <- raw
		return nil
	}
	push := managerPush("spec-retry", 2, json.RawMessage(`{"version":"1"}`))

	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, device.ConnectorStateDegraded, "runtime-unreachable")
	a.mu.Lock()
	_, retained := a.applied[push.SpecID]
	a.mu.Unlock()
	if !retained {
		t.Fatal("transient failure did not retain desired connector state")
	}

	a.managerResources.reconcile(context.Background())
	frame := waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if frame.ObservedRevision != 2 || frame.ObservedHash == "" {
		t.Fatalf("retry status=%+v", frame)
	}
	commands.mu.Lock()
	writes := commands.writes
	document := commands.document
	commands.mu.Unlock()
	if writes != 2 {
		t.Fatalf("writes=%d, want one failed attempt plus one retry", writes)
	}
	parsed, err := parseManagerDocument([]byte(document))
	if err != nil || len(parsed.entries) != 1 {
		t.Fatalf("retried document=%s err=%v", document, err)
	}
}

func TestManagerResourceReadbackMismatchIsDegradedThenConverges(t *testing.T) {
	commands := &fakeManagerCommands{document: `[]`, mismatchOnce: true}
	a, rec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 2)
	a.reportRawStatus = func(_ context.Context, raw []byte) error { rawStatuses <- raw; return nil }
	push := managerPush("spec-readback", 1, json.RawMessage(`{"version":"1"}`))

	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, device.ConnectorStateDegraded, "readback-mismatch")
	a.managerResources.reconcile(context.Background())
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
}

func TestManagerResourceNormalizedReadbackDoesNotRewrite(t *testing.T) {
	commands := &fakeManagerCommands{document: `[]`}
	commands.transformWrite = func(body string) string {
		document, err := parseManagerDocument([]byte(body))
		if err != nil {
			return body
		}
		for _, entry := range document.entries {
			entry["limits"] = map[string]any{"maxObjects": 8, "maxEventBytes": 65536}
			entry["description"] = "NI normalized"
		}
		sort.Slice(document.entries, func(i, j int) bool {
			left, _ := document.entries[i]["pipelineId"].(string)
			right, _ := document.entries[j]["pipelineId"].(string)
			return left > right
		})
		raw, _ := json.Marshal(document.entries)
		return string(raw)
	}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 4)
	a.reportRawStatus = func(_ context.Context, raw []byte) error { rawStatuses <- raw; return nil }
	push := managerPush("spec-normalized", 4, json.RawMessage(`{"version":"1","nodes":[{"id":"source"}]}`))
	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	frame := waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)

	commands.mu.Lock()
	document := commands.document
	writes := commands.writes
	commands.mu.Unlock()
	if writes != 1 || !strings.Contains(document, `"maxObjects":8`) {
		t.Fatalf("normalized readback writes=%d document=%s", writes, document)
	}
	parsed, err := parseManagerDocument([]byte(document))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.hash != frame.ObservedHash {
		t.Fatalf("status hash is not the normalized GET hash: %+v", frame)
	}
	// A periodic reconcile must use the normalized owned-entry baseline rather
	// than repeatedly comparing it with the raw desired JSON.
	a.managerResources.reconcile(context.Background())
	commands.mu.Lock()
	writes = commands.writes
	commands.mu.Unlock()
	if writes != 1 {
		t.Fatalf("normalization caused a repeated PUT: writes=%d", writes)
	}
}

func TestManagerResourceReferenceOnlyDoesNotInventOrDeleteDefinition(t *testing.T) {
	commands := &fakeManagerCommands{document: `[{"pipelineId":"spec-existing","version":"3"}]`}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 1)
	a.reportRawStatus = func(_ context.Context, raw []byte) error { rawStatuses <- raw; return nil }

	push := managerPush("spec-existing", 1, nil)
	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if result := a.managerResources.delete(context.Background(), push.SpecID, 1); result.state != device.ConnectorStateApplied {
		t.Fatalf("reference-only delete=%+v", result)
	}
	commands.mu.Lock()
	writes := commands.writes
	document := commands.document
	commands.mu.Unlock()
	if writes != 0 || document != `[{"pipelineId":"spec-existing","version":"3"}]` {
		t.Fatalf("reference-only lifecycle wrote %d times, document=%s", writes, document)
	}
}

func TestManagerResourceSerializesConcurrentWritersWithoutLostEntries(t *testing.T) {
	commands := &fakeManagerCommands{document: `[{"pipelineId":"existing","version":"1"}]`, writeDelay: time.Millisecond}
	manager := newManagerResourceManager(nil)
	manager.configure(commands, "nereid-inference", time.Second)
	pushes := []device.ConnectorPush{
		managerPush("one", 1, json.RawMessage(`{"version":"1"}`)),
		managerPush("two", 1, json.RawMessage(`{"version":"1"}`)),
	}
	var wg sync.WaitGroup
	results := make(chan managerResourceResult, len(pushes))
	for _, push := range pushes {
		push := push
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg, err := parseManagerResourceConfig(push.Config, "nereid-inference")
			if err != nil {
				t.Error(err)
				return
			}
			hash, _ := device.CanonicalConfigHash(push.Config, push.Mappings)
			results <- manager.apply(context.Background(), push, cfg, hash)
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.state != device.ConnectorStateApplied {
			t.Fatalf("concurrent apply=%+v", result)
		}
	}
	parsed, err := parseManagerDocument([]byte(commands.document))
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.entries) != 3 {
		t.Fatalf("concurrent merge lost entries: %s", commands.document)
	}
}

func TestManagerResourceVersionUpdateRollbackAndDelete(t *testing.T) {
	commands := &fakeManagerCommands{document: `[
		{"pipelineId":"pipeline","version":"1","graph":{"keep":"v1"}},
		{"pipelineId":"pipeline","version":"2","graph":{"keep":"v2"}},
		{"pipelineId":"other","version":"9","graph":{"keep":"other"}}
	]`}
	manager := newManagerResourceManager(nil)
	manager.configure(commands, "nereid-inference", time.Second)
	apply := func(revision int64, definition string) {
		t.Helper()
		push := managerPush("pipeline", revision, json.RawMessage(definition))
		cfg, err := parseManagerResourceConfig(push.Config, "nereid-inference")
		if err != nil {
			t.Fatal(err)
		}
		if result := manager.apply(context.Background(), push, cfg, "hash"); result.state != device.ConnectorStateApplied {
			t.Fatalf("apply rev=%d result=%+v", revision, result)
		}
	}
	apply(1, `{"version":"3","graph":{"new":"v3"}}`)
	// Rolling the same NL registration spec back to v2 must not replace v1 or
	// remove v3. NI's immutable v2 content remains byte-equivalent.
	apply(2, `{"version":"2","graph":{"keep":"v2"}}`)
	commands.mu.Lock()
	writes := commands.writes
	document := commands.document
	commands.mu.Unlock()
	parsed, err := parseManagerDocument([]byte(document))
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.entries) != 4 || writes != 1 {
		t.Fatalf("version rollout entries=%d writes=%d document=%s", len(parsed.entries), writes, document)
	}
	manager.reconcile(context.Background())
	commands.mu.Lock()
	writes = commands.writes
	commands.mu.Unlock()
	if writes != 1 {
		t.Fatalf("version retry created duplicates/rewrites: writes=%d document=%s", writes, commands.document)
	}
	if result := manager.delete(context.Background(), "pipeline", 3); result.state != device.ConnectorStateApplied {
		t.Fatalf("delete=%+v", result)
	}
	parsed, err = parseManagerDocument([]byte(commands.document))
	if err != nil {
		t.Fatal(err)
	}
	// Only versions owned by this spec (v2/v3) are removed. Pre-existing v1 and
	// the unrelated pipeline survive.
	if len(parsed.entries) != 2 || parsed.entries[0]["version"] != "1" || parsed.entries[1]["pipelineId"] != "other" {
		t.Fatalf("owned-version delete preserved wrong entries: %s", commands.document)
	}
}

func TestManagerResourceDeleteFailureKeepsTombstoneForRetry(t *testing.T) {
	commands := &fakeManagerCommands{document: `[]`}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 4)
	a.reportRawStatus = func(_ context.Context, raw []byte) error { rawStatuses <- raw; return nil }
	push := managerPush("spec-delete", 1, json.RawMessage(`{"version":"1"}`))
	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	// A reconnect can clear the applied ledger before NL's delete arrives;
	// the manager-owned desired entry must still route this type-less delete.
	a.Reset()
	commands.failWrites = 1
	a.applyDelete(context.Background(), device.ConnectorPush{SpecID: push.SpecID, Revision: 2})

	a.managerResources.mu.Lock()
	tombstone := a.managerResources.desired[push.SpecID]
	a.managerResources.mu.Unlock()
	if tombstone == nil || !tombstone.remove {
		t.Fatal("failed delete did not retain its retry tombstone")
	}
	a.managerResources.reconcile(context.Background())
	a.managerResources.mu.Lock()
	_, pending := a.managerResources.desired[push.SpecID]
	a.managerResources.mu.Unlock()
	if pending || commands.document != `[]` {
		t.Fatalf("delete retry did not converge: pending=%v document=%s", pending, commands.document)
	}
}

func TestManagerResourceRetryCannotResurrectDeletedEntry(t *testing.T) {
	commands := &fakeManagerCommands{
		document: `[]`, failWrites: 1, blockWrites: true,
		writeEntered: make(chan struct{}), writeRelease: make(chan struct{}),
	}
	manager := newManagerResourceManager(nil)
	manager.configure(commands, "nereid-inference", time.Second)
	push := managerPush("fenced", 1, json.RawMessage(`{"version":"1","generation":"old"}`))
	cfg, err := parseManagerResourceConfig(push.Config, "nereid-inference")
	if err != nil {
		t.Fatal(err)
	}
	if result := manager.apply(context.Background(), push, cfg, "old"); result.state != device.ConnectorStateDegraded {
		t.Fatalf("initial apply=%+v", result)
	}
	retryDone := make(chan struct{})
	go func() {
		manager.reconcile(context.Background())
		close(retryDone)
	}()
	<-commands.writeEntered
	deleteDone := make(chan managerResourceResult, 1)
	go func() { deleteDone <- manager.delete(context.Background(), push.SpecID, 2) }()
	close(commands.writeRelease)
	<-retryDone
	if result := <-deleteDone; result.state != device.ConnectorStateApplied {
		t.Fatalf("delete=%+v", result)
	}
	if commands.document != `[]` {
		t.Fatalf("stale retry resurrected deleted entry: %s", commands.document)
	}
	// A later retry snapshot has no live desired record and must stay empty.
	manager.reconcile(context.Background())
	if commands.document != `[]` {
		t.Fatalf("post-delete retry resurrected entry: %s", commands.document)
	}
}

func TestManagerResourceSameSpecUpdateWinsOverRetry(t *testing.T) {
	commands := &fakeManagerCommands{
		document: `[]`, failWrites: 1, blockWrites: true,
		writeEntered: make(chan struct{}), writeRelease: make(chan struct{}),
	}
	manager := newManagerResourceManager(nil)
	manager.configure(commands, "nereid-inference", time.Second)
	oldPush := managerPush("same-spec", 1, json.RawMessage(`{"version":"1","generation":"old"}`))
	oldCfg, _ := parseManagerResourceConfig(oldPush.Config, "nereid-inference")
	if result := manager.apply(context.Background(), oldPush, oldCfg, "old"); result.state != device.ConnectorStateDegraded {
		t.Fatalf("initial apply=%+v", result)
	}
	retryDone := make(chan struct{})
	go func() { manager.reconcile(context.Background()); close(retryDone) }()
	<-commands.writeEntered
	newPush := managerPush("same-spec", 2, json.RawMessage(`{"version":"1","generation":"new"}`))
	newCfg, _ := parseManagerResourceConfig(newPush.Config, "nereid-inference")
	applyDone := make(chan managerResourceResult, 1)
	go func() { applyDone <- manager.apply(context.Background(), newPush, newCfg, "new") }()
	close(commands.writeRelease)
	<-retryDone
	if result := <-applyDone; result.state != device.ConnectorStateApplied {
		t.Fatalf("new apply=%+v", result)
	}
	parsed, err := parseManagerDocument([]byte(commands.document))
	if err != nil || len(parsed.entries) != 1 {
		t.Fatalf("document=%s err=%v", commands.document, err)
	}
	if parsed.entries[0]["generation"] != "new" {
		t.Fatalf("stale retry won same-spec race: %s", commands.document)
	}
}

func TestManagerResourceStoreRejectsCorruptLedger(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/manager-resources.json", []byte(`{"version":1,"entries":`), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := newManagerResourceManager(nil)
	if err := manager.openStore(dir); err == nil {
		t.Fatal("corrupt manager ledger must fail startup store opening")
	}
}

func TestManagerResourceStoreRetriesTombstoneAfterProcessReconstruction(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	manager := newManagerResourceManager(nil)
	manager.configure(commands, "nereid-inference", time.Second)
	if err := manager.openStore(dir); err != nil {
		t.Fatal(err)
	}
	push := managerPush("persistent", 8, json.RawMessage(`{"version":"1","graph":{"opaque":"do-not-persist"}}`))
	cfg, _ := parseManagerResourceConfig(push.Config, "nereid-inference")
	if result := manager.apply(context.Background(), push, cfg, "hash"); result.state != device.ConnectorStateApplied {
		t.Fatalf("apply=%+v", result)
	}
	raw, err := os.ReadFile(dir + "/manager-resources.json")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "do-not-persist") || strings.Contains(string(raw), `"definition"`) {
		t.Fatalf("opaque definition leaked into NE ledger: %s", raw)
	}
	commands.failWrites = 1
	if result := manager.delete(context.Background(), push.SpecID, 9); result.state != device.ConnectorStateDegraded {
		t.Fatalf("first delete=%+v", result)
	}

	reopened := newManagerResourceManager(nil)
	reopened.configure(commands, "nereid-inference", time.Second)
	if err := reopened.openStore(dir); err != nil {
		t.Fatal(err)
	}
	reopened.reconcile(context.Background())
	if commands.document != `[]` {
		t.Fatalf("reopened tombstone did not retry: %s", commands.document)
	}
	reopened.mu.Lock()
	_, pending := reopened.desired[push.SpecID]
	reopened.mu.Unlock()
	if pending {
		t.Fatal("reopened tombstone remains pending after successful delete")
	}
}

func TestManagerResourceStoreRequiresExplicitDeleteAfterIncompleteRedrive(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	manager := newManagerResourceManager(nil)
	manager.configure(commands, "nereid-inference", time.Second)
	if err := manager.openStore(dir); err != nil {
		t.Fatal(err)
	}
	push := managerPush("offline-delete", 1, json.RawMessage(`{"version":"1"}`))
	cfg, _ := parseManagerResourceConfig(push.Config, "nereid-inference")
	if result := manager.apply(context.Background(), push, cfg, "hash"); result.state != device.ConnectorStateApplied {
		t.Fatalf("apply=%+v", result)
	}

	// Reconstruct without delivering the old spec. A dropped, invalid, partial,
	// or failed redrive is indistinguishable from absence, so it must preserve
	// the live NI definition indefinitely rather than guessing deletion.
	reopened := newManagerResourceManager(nil)
	reopened.configure(commands, "nereid-inference", time.Second)
	if err := reopened.openStore(dir); err != nil {
		t.Fatal(err)
	}
	reopened.prepareReplay()
	for range 3 {
		reopened.reconcile(context.Background())
	}
	reopened.mu.Lock()
	desired := reopened.desired[push.SpecID]
	reopened.mu.Unlock()
	if desired == nil || desired.remove || !desired.awaitingReplay || commands.document == `[]` {
		t.Fatalf("incomplete redrive deleted live state: desired=%+v document=%s", desired, commands.document)
	}

	// NL's durable delete retry/outbox supplies the explicit deletion fence.
	if result := reopened.delete(context.Background(), push.SpecID, 2); result.state != device.ConnectorStateApplied {
		t.Fatalf("explicit offline delete=%+v", result)
	}
	if commands.document != `[]` {
		t.Fatalf("explicit delete did not converge: %s", commands.document)
	}
}

func TestManagerResourceRedriveClearsAwaitingReplay(t *testing.T) {
	commands := &fakeManagerCommands{document: `[]`}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	push := managerPush("fast-redrive", 2, json.RawMessage(`{"version":"1"}`))
	handler := NewManagerResourceHandler(a)
	if err := handler.Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	a.managerResources.prepareReplay()
	if err := handler.Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	a.managerResources.mu.Lock()
	desired := a.managerResources.desired[push.SpecID]
	awaiting := desired.awaitingReplay
	a.managerResources.mu.Unlock()
	if awaiting {
		t.Fatal("an identical redrive did not reactivate the persisted definition")
	}
}

func TestManagerResourceRevisionHighWaterFencesReorderedFrames(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	manager := newManagerResourceManager(nil)
	manager.configure(commands, "nereid-inference", time.Second)
	if err := manager.openStore(dir); err != nil {
		t.Fatal(err)
	}
	apply := func(revision int64, generation string) managerResourceResult {
		t.Helper()
		push := managerPush("ordered", revision, json.RawMessage(`{"version":"1","generation":"`+generation+`"}`))
		cfg, err := parseManagerResourceConfig(push.Config, "nereid-inference")
		if err != nil {
			t.Fatal(err)
		}
		return manager.apply(context.Background(), push, cfg, generation)
	}
	if result := apply(2, "new"); result.state != device.ConnectorStateApplied {
		t.Fatalf("new apply=%+v", result)
	}
	if result := apply(1, "old"); !result.stale {
		t.Fatalf("delayed older apply was not fenced: %+v", result)
	}
	if !strings.Contains(commands.document, `"generation":"new"`) {
		t.Fatalf("older apply replaced newer content: %s", commands.document)
	}
	if result := manager.delete(context.Background(), "ordered", 3); result.state != device.ConnectorStateApplied {
		t.Fatalf("delete=%+v", result)
	}
	if result := apply(2, "duplicate"); !result.stale || commands.document != `[]` {
		t.Fatalf("post-delete duplicate resurrected entry: result=%+v document=%s", result, commands.document)
	}

	// The high-water tombstone survives even though the applied desired record
	// and opaque definition have been removed from the ledger.
	reopened := newManagerResourceManager(nil)
	reopened.configure(commands, "nereid-inference", time.Second)
	if err := reopened.openStore(dir); err != nil {
		t.Fatal(err)
	}
	push := managerPush("ordered", 3, json.RawMessage(`{"version":"1","generation":"duplicate"}`))
	cfg, _ := parseManagerResourceConfig(push.Config, "nereid-inference")
	if result := reopened.apply(context.Background(), push, cfg, "duplicate"); !result.stale {
		t.Fatalf("restart forgot delete high-water mark: %+v", result)
	}
	commands.mu.Lock()
	writes := commands.writes
	commands.mu.Unlock()
	if writes != 2 || commands.document != `[]` {
		t.Fatalf("stale frames caused writes=%d document=%s", writes, commands.document)
	}
}

func TestManagerResourceUnknownDeleteFencesDelayedApplyAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	if err := a.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	a.applyDelete(context.Background(), device.ConnectorPush{SpecID: "delete-first", Revision: 5})
	a.managerResources.mu.Lock()
	high := a.managerResources.revisions["delete-first"]
	a.managerResources.mu.Unlock()
	if high != 5 {
		t.Fatalf("unknown delete high-water=%d, want 5", high)
	}
	push := managerPush("delete-first", 5, json.RawMessage(`{"version":"1"}`))
	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	if commands.writes != 0 || commands.document != `[]` {
		t.Fatalf("delayed pre-delete spec resurrected NI entry: %s", commands.document)
	}

	reopened := newManagerResourceManager(nil)
	reopened.configure(commands, "nereid-inference", time.Second)
	if err := reopened.openStore(dir); err != nil {
		t.Fatal(err)
	}
	cfg, _ := parseManagerResourceConfig(push.Config, "nereid-inference")
	if result := reopened.apply(context.Background(), push, cfg, "hash"); !result.stale {
		t.Fatalf("restart forgot unknown-delete fence: %+v", result)
	}
}

func TestManagerResourceRejectedRevisionFencesOlderValidApply(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	a, rec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	if err := a.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	invalid := device.ConnectorPush{
		Kind: device.ConnectorKindSpec, SpecID: "rejected-newer", ConnectorType: TypeManagerResource,
		Revision: 10, Config: json.RawMessage(`{}`),
	}
	if err := NewManagerResourceHandler(a).Apply(context.Background(), invalid); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, device.ConnectorStateFailed, "config-invalid")
	older := managerPush("rejected-newer", 9, json.RawMessage(`{"version":"1"}`))
	if err := NewManagerResourceHandler(a).Apply(context.Background(), older); err != nil {
		t.Fatal(err)
	}
	if commands.writes != 0 || commands.document != `[]` {
		t.Fatalf("older valid apply bypassed rejected revision fence: %s", commands.document)
	}

	reopened := newManagerResourceManager(nil)
	reopened.configure(commands, "nereid-inference", time.Second)
	if err := reopened.openStore(dir); err != nil {
		t.Fatal(err)
	}
	cfg, _ := parseManagerResourceConfig(older.Config, "nereid-inference")
	if result := reopened.apply(context.Background(), older, cfg, "hash"); !result.stale {
		t.Fatalf("restart forgot rejected revision fence: %+v", result)
	}
}

func TestManagerResourceRejectionDurabilityAndRestartReplay(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	a, rec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	if err := a.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	invalid := device.ConnectorPush{
		Kind: device.ConnectorKindSpec, SpecID: "durable-rejection", ConnectorType: TypeManagerResource,
		Revision: 10, Config: json.RawMessage(`{}`),
	}
	if err := NewManagerResourceHandler(a).Apply(context.Background(), invalid); err != nil {
		t.Fatal(err)
	}
	// The in-memory fence is immediate, but an undurable rejection is retryable
	// and must never be reported as terminal FAILED.
	rec.waitFor(t, device.ConnectorStateDegraded, "runtime-unreachable")
	a.managerResources.mu.Lock()
	rejection := a.managerResources.rejections[invalid.SpecID]
	a.managerResources.mu.Unlock()
	if rejection.durable {
		t.Fatal("rejection became durable despite the failed ledger write")
	}
	if err := os.Remove(dir + "/manager-resources.json.tmp"); err != nil {
		t.Fatal(err)
	}
	a.managerResources.reconcile(context.Background())
	rec.waitFor(t, device.ConnectorStateFailed, "config-invalid")

	// Reconstruction schedules an at-least-once terminal report even if no
	// redrive arrives. The same-revision redrive also regenerates that report.
	reopened, reopenedRec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(reopened, commands)
	if err := reopened.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	reopened.managerResources.reconcile(context.Background())
	reopenedRec.waitFor(t, device.ConnectorStateFailed, "config-invalid")
	if err := NewManagerResourceHandler(reopened).Apply(context.Background(), invalid); err != nil {
		t.Fatal(err)
	}
	reopenedRec.waitFor(t, device.ConnectorStateFailed, "config-invalid")

	older := managerPush(invalid.SpecID, 9, json.RawMessage(`{"version":"1"}`))
	if err := NewManagerResourceHandler(reopened).Apply(context.Background(), older); err != nil {
		t.Fatal(err)
	}
	if commands.writes != 0 || commands.document != `[]` {
		t.Fatalf("older apply crossed durable rejection fence: %s", commands.document)
	}
}

func TestManagerResourceOutOfOrderFailureKeepsNewerPendingReport(t *testing.T) {
	manager := newManagerResourceManager(nil)
	oldPush := device.ConnectorPush{SpecID: "ordered-report", Revision: 1}
	manager.revisions[oldPush.SpecID] = oldPush.Revision
	manager.desired[oldPush.SpecID] = &managerResourceDesired{
		push: oldPush, state: device.ConnectorStateDegraded, reason: "old", observed: make(map[string]string),
	}
	oldEntered := make(chan struct{})
	oldRelease := make(chan struct{})
	oldDone := make(chan struct{})
	manager.report = func(_ string, push device.ConnectorPush, result managerResourceResult) error {
		if result.state == device.ConnectorStateDegraded {
			close(oldEntered)
			<-oldRelease
			return errors.New("older publication failed")
		}
		return errors.New("newer publication failed")
	}
	go func() {
		manager.publishResult(oldPush.SpecID, oldPush, managerResourceResult{
			state: device.ConnectorStateDegraded, reason: "old",
		})
		close(oldDone)
	}()
	<-oldEntered
	current, _ := manager.recordResult(manager.desired[oldPush.SpecID], managerResourceResult{
		state: device.ConnectorStateApplied, observedHash: "new",
	})
	manager.publishResult(oldPush.SpecID, oldPush, current)
	close(oldRelease)
	<-oldDone

	manager.mu.Lock()
	pending := manager.pendingReports[oldPush.SpecID]
	latest := manager.reportLatest[oldPush.SpecID]
	manager.mu.Unlock()
	if pending == nil || pending.result.state != device.ConnectorStateApplied || pending.generation != latest {
		t.Fatalf("older completion replaced newer pending report: pending=%+v latest=%d", pending, latest)
	}

	published := make(chan managerResourceResult, 1)
	manager.report = func(_ string, _ device.ConnectorPush, result managerResourceResult) error {
		published <- result
		return nil
	}
	manager.retryPendingReports()
	if result := <-published; result.state != device.ConnectorStateApplied || result.observedHash != "new" {
		t.Fatalf("retried wrong report: %+v", result)
	}
	manager.mu.Lock()
	pending = manager.pendingReports[oldPush.SpecID]
	manager.mu.Unlock()
	if pending != nil {
		t.Fatal("successful newer retry remained pending")
	}
}

func TestManagerResourceDelayedSameRevisionResultCannotDisplaceCurrentReport(t *testing.T) {
	manager := newManagerResourceManager(nil)
	push := managerPush("delayed-result", 3, nil)
	desired := &managerResourceDesired{
		push: push, state: device.ConnectorStateCreating, observed: make(map[string]string),
	}
	manager.desired[push.SpecID] = desired
	manager.revisions[push.SpecID] = push.Revision
	manager.reportRevisions[push.SpecID] = push.Revision
	delayed, _ := manager.recordResult(desired, managerResourceResult{
		state: device.ConnectorStateDegraded, reason: "runtime-unreachable",
	})
	current, _ := manager.recordResult(desired, managerResourceResult{
		state: device.ConnectorStateApplied, observedHash: "current-hash",
	})
	manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error {
		return errors.New("current publication failed")
	}
	manager.publishResult(push.SpecID, push, current)

	attempts := 0
	manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error {
		attempts++
		return nil
	}
	manager.publishResult(push.SpecID, push, delayed)
	manager.mu.Lock()
	pending := manager.pendingReports[push.SpecID]
	manager.mu.Unlock()
	if attempts != 0 || pending == nil || pending.result.state != device.ConnectorStateApplied ||
		pending.result.observedHash != "current-hash" {
		t.Fatalf("delayed result displaced current report: attempts=%d pending=%+v", attempts, pending)
	}
}

func TestManagerResourceFailedStatusPublicationRetries(t *testing.T) {
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, &fakeManagerCommands{document: `[]`})
	statuses := make(chan device.ConnectorStatus, 2)
	attempts := 0
	a.reporter = statusReporterFunc(func(_ context.Context, status device.ConnectorStatus, _ byte) error {
		attempts++
		if attempts == 1 {
			return errors.New("failed status publish failed")
		}
		statuses <- status
		return nil
	})
	invalid := device.ConnectorPush{
		Kind: device.ConnectorKindSpec, SpecID: "failed-status", ConnectorType: TypeManagerResource,
		Revision: 1, Config: json.RawMessage(`{}`),
	}
	if err := NewManagerResourceHandler(a).Apply(context.Background(), invalid); err != nil {
		t.Fatal(err)
	}
	a.managerResources.reconcile(context.Background())
	select {
	case status := <-statuses:
		if status.State != device.ConnectorStateFailed {
			t.Fatalf("retried state=%s", status.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal FAILED status was not retried")
	}
	if attempts != 2 {
		t.Fatalf("FAILED publish attempts=%d, want failure plus retry", attempts)
	}
}

func TestManagerResourcePreAcceptanceDegradedStatusRetries(t *testing.T) {
	dir := t.TempDir()
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, &fakeManagerCommands{document: `[]`})
	if err := a.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	statuses := make(chan device.ConnectorStatus, 2)
	attempts := 0
	a.reporter = statusReporterFunc(func(_ context.Context, status device.ConnectorStatus, _ byte) error {
		if status.State != device.ConnectorStateDegraded {
			return nil
		}
		attempts++
		if attempts == 1 {
			return errors.New("degraded status publish failed")
		}
		statuses <- status
		return nil
	})
	push := managerPush("degraded-status", 1, json.RawMessage(`{"version":"1"}`))
	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	a.managerResources.reconcile(context.Background())
	select {
	case status := <-statuses:
		if status.State != device.ConnectorStateDegraded {
			t.Fatalf("retried state=%s", status.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pre-acceptance DEGRADED status was not retried")
	}
	if attempts != 2 {
		t.Fatalf("DEGRADED publish attempts=%d, want failure plus retry", attempts)
	}
}

func TestManagerResourceSameRevisionDeleteRetiresUnknownRejection(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	a, rec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	if err := a.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	invalid := device.ConnectorPush{
		Kind: device.ConnectorKindSpec, SpecID: "retire-rejection", ConnectorType: TypeManagerResource,
		Revision: 4, Config: json.RawMessage(`{}`),
	}
	if err := NewManagerResourceHandler(a).Apply(context.Background(), invalid); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, device.ConnectorStateFailed, "config-invalid")
	a.applyDelete(context.Background(), device.ConnectorPush{SpecID: invalid.SpecID, Revision: invalid.Revision})
	a.managerResources.mu.Lock()
	high := a.managerResources.revisions[invalid.SpecID]
	_, rejected := a.managerResources.rejections[invalid.SpecID]
	pending := a.managerResources.pendingReports[invalid.SpecID]
	a.managerResources.mu.Unlock()
	if high != invalid.Revision || rejected || pending != nil {
		t.Fatalf("delete did not retire rejection: high=%d rejected=%v pending=%+v", high, rejected, pending)
	}

	// A recovered report captured before the delete carries rejection identity
	// and cannot start a new publication after that identity has been retired.
	attempts := 0
	a.managerResources.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error {
		attempts++
		return nil
	}
	a.managerResources.publishResult(invalid.SpecID, invalid, managerResourceResult{
		state: device.ConnectorStateFailed, reason: "config-invalid", rejection: true,
	})
	if attempts != 0 {
		t.Fatal("retired rejection started a new publication")
	}

	reopened, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(reopened, commands)
	if err := reopened.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	reopened.managerResources.mu.Lock()
	_, rejected = reopened.managerResources.rejections[invalid.SpecID]
	pending = reopened.managerResources.pendingReports[invalid.SpecID]
	reopened.managerResources.mu.Unlock()
	if rejected || pending != nil {
		t.Fatal("obsolete rejection was restored after restart")
	}
	delayed := managerPush(invalid.SpecID, invalid.Revision, json.RawMessage(`{"version":"1"}`))
	if err := NewManagerResourceHandler(reopened).Apply(context.Background(), delayed); err != nil {
		t.Fatal(err)
	}
	if commands.writes != 0 || commands.document != `[]` {
		t.Fatalf("delete fence did not survive rejection retirement: %s", commands.document)
	}
}

func TestManagerResourceSameRevisionDeleteRetiresDeviceBoundRejection(t *testing.T) {
	dir := t.TempDir()
	a, rec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, &fakeManagerCommands{document: `[]`})
	if err := a.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	invalid := managerPush("device-rejection", 6, json.RawMessage(`{"version":"1"}`))
	invalid.DeviceID = "unexpected-device"
	if err := NewManagerResourceHandler(a).Apply(context.Background(), invalid); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, device.ConnectorStateFailed, "config-invalid")
	a.applyDelete(context.Background(), device.ConnectorPush{
		SpecID: invalid.SpecID, DeviceID: invalid.DeviceID, Revision: invalid.Revision,
	})
	a.managerResources.mu.Lock()
	high := a.managerResources.revisions[invalid.SpecID]
	_, rejected := a.managerResources.rejections[invalid.SpecID]
	pending := a.managerResources.pendingReports[invalid.SpecID]
	a.managerResources.mu.Unlock()
	if high != invalid.Revision || rejected || pending != nil {
		t.Fatalf("device-bound delete retained rejection: high=%d rejected=%v pending=%+v", high, rejected, pending)
	}

	reopened, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(reopened, &fakeManagerCommands{document: `[]`})
	if err := reopened.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	reopened.managerResources.mu.Lock()
	_, rejected = reopened.managerResources.rejections[invalid.SpecID]
	pending = reopened.managerResources.pendingReports[invalid.SpecID]
	reopened.managerResources.mu.Unlock()
	if rejected || pending != nil {
		t.Fatal("device-bound rejection was restored after its explicit delete")
	}
}

func TestManagerResourceUnrelatedWritePromotesAllRecoveredRejections(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	manager := newManagerResourceManager(nil)
	manager.configure(commands, "nereid-inference", time.Second)
	if err := manager.openStore(dir); err != nil {
		t.Fatal(err)
	}
	type reported struct {
		specID string
		state  string
	}
	var reports []reported
	manager.report = func(specID string, _ device.ConnectorPush, result managerResourceResult) error {
		reports = append(reports, reported{specID: specID, state: result.state})
		return nil
	}
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	for _, specID := range []string{"rejection-a", "rejection-b"} {
		manager.reject(device.ConnectorPush{SpecID: specID, Revision: 3}, device.ConnectorStateFailed, "config-invalid")
	}
	if err := os.Remove(dir + "/manager-resources.json.tmp"); err != nil {
		t.Fatal(err)
	}
	valid := managerPush("unrelated-write", 1, json.RawMessage(`{"version":"1"}`))
	cfg, _ := parseManagerResourceConfig(valid.Config, "nereid-inference")
	if result := manager.apply(context.Background(), valid, cfg, "hash"); result.state != device.ConnectorStateApplied {
		t.Fatalf("unrelated apply=%+v", result)
	}
	manager.retryPendingReports()
	terminal := make(map[string]bool)
	for _, report := range reports {
		if report.state == device.ConnectorStateFailed {
			terminal[report.specID] = true
		}
	}
	if !terminal["rejection-a"] || !terminal["rejection-b"] {
		t.Fatalf("unrelated durable write did not promote every rejection: reports=%+v", reports)
	}
	manager.mu.Lock()
	aDurable := manager.rejections["rejection-a"].durable
	bDurable := manager.rejections["rejection-b"].durable
	pending := len(manager.pendingReports)
	manager.mu.Unlock()
	if !aDurable || !bDurable || pending != 0 {
		t.Fatalf("recovered rejection state: a=%v b=%v pending=%d", aDurable, bDurable, pending)
	}
}

func TestManagerResourceRecoveryKeepsNewerPreAcceptanceReport(t *testing.T) {
	dir := t.TempDir()
	manager := newManagerResourceManager(nil)
	manager.configure(&fakeManagerCommands{document: `[]`}, "nereid-inference", time.Second)
	if err := manager.openStore(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error { return nil }
	rejected := managerPush("recovery-order", 10, json.RawMessage(`{}`))
	manager.reject(rejected, device.ConnectorStateFailed, "config-invalid")

	newer := managerPush(rejected.SpecID, 11, json.RawMessage(`{"version":"1"}`))
	cfg, err := parseManagerResourceConfig(newer.Config, "nereid-inference")
	if err != nil {
		t.Fatal(err)
	}
	result := manager.apply(context.Background(), newer, cfg, "newer-hash")
	if result.state != device.ConnectorStateDegraded || result.desiredAccepted {
		t.Fatalf("newer apply unexpectedly accepted: %+v", result)
	}
	manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error {
		return errors.New("newer pre-acceptance publication failed")
	}
	manager.publishResult(newer.SpecID, newer, result)
	if err := os.Remove(dir + "/manager-resources.json.tmp"); err != nil {
		t.Fatal(err)
	}
	manager.retryStore()

	manager.mu.Lock()
	pending := manager.pendingReports[newer.SpecID]
	manager.mu.Unlock()
	if pending == nil || pending.push.Revision != newer.Revision || pending.result.state != device.ConnectorStateDegraded {
		t.Fatalf("recovery replaced newer pending report: %+v", pending)
	}
	published := make(chan device.ConnectorPush, 1)
	manager.report = func(_ string, push device.ConnectorPush, _ managerResourceResult) error {
		published <- push
		return nil
	}
	manager.retryPendingReports()
	if push := <-published; push.Revision != newer.Revision {
		t.Fatalf("retried revision=%d, want newer revision %d", push.Revision, newer.Revision)
	}
}

func TestManagerResourceFailedNewerApplyFencesOlderDelete(t *testing.T) {
	dir := t.TempDir()
	manager := newManagerResourceManager(nil)
	if err := manager.openStore(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	rejected := managerPush("observed-order", 10, json.RawMessage(`{}`))
	manager.reject(rejected, device.ConnectorStateFailed, "config-invalid")
	newer := managerPush(rejected.SpecID, 11, json.RawMessage(`{"version":"1"}`))
	cfg, err := parseManagerResourceConfig(newer.Config, "nereid-inference")
	if err != nil {
		t.Fatal(err)
	}
	if result := manager.apply(context.Background(), newer, cfg, "newer-hash"); result.desiredAccepted {
		t.Fatalf("newer apply unexpectedly accepted: %+v", result)
	}
	if result := manager.delete(context.Background(), rejected.SpecID, rejected.Revision); !result.stale {
		t.Fatalf("older delete crossed newer observed revision: %+v", result)
	}
	manager.mu.Lock()
	_, retained := manager.rejections[rejected.SpecID]
	manager.mu.Unlock()
	if !retained {
		t.Fatal("older delete retired the rejection after a newer apply")
	}
}

func TestManagerResourceRecoveryFencesPreDurabilityDegraded(t *testing.T) {
	for _, tc := range []struct {
		name       string
		publishErr error
	}{
		{name: "success", publishErr: nil},
		{name: "failure", publishErr: errors.New("stale transient publication failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			manager := newManagerResourceManager(nil)
			if err := manager.openStore(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
				t.Fatal(err)
			}
			push := managerPush("transient-order", 8, json.RawMessage(`{}`))
			manager.mu.Lock()
			transient := manager.noteRejectedLocked(
				push.SpecID, push.Revision, device.ConnectorStateFailed, "config-invalid",
			)
			manager.mu.Unlock()
			if transient.state != device.ConnectorStateDegraded {
				t.Fatalf("pre-durability result=%+v", transient)
			}
			if err := os.Remove(dir + "/manager-resources.json.tmp"); err != nil {
				t.Fatal(err)
			}
			manager.retryStore()

			attempts := 0
			manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error {
				attempts++
				return tc.publishErr
			}
			manager.publishResult(push.SpecID, push, transient)
			manager.mu.Lock()
			pending := manager.pendingReports[push.SpecID]
			manager.mu.Unlock()
			if attempts != 0 || pending == nil || !pending.result.rejection ||
				pending.result.state != device.ConnectorStateFailed {
				t.Fatalf("stale transient displaced terminal report: attempts=%d pending=%+v", attempts, pending)
			}
		})
	}
}

func TestManagerResourceSuccessfulDeleteInvalidatesPendingDegraded(t *testing.T) {
	manager := newManagerResourceManager(nil)
	manager.configure(&fakeManagerCommands{document: `[]`}, "nereid-inference", time.Second)
	push := managerPush("delete-report", 2, nil)
	desired := &managerResourceDesired{
		push: push, remove: true, state: device.ConnectorStateDegraded,
		reason: "runtime-unreachable", observed: make(map[string]string),
	}
	manager.desired[push.SpecID] = desired
	manager.revisions[push.SpecID] = push.Revision
	manager.pendingReports[push.SpecID] = &managerPendingReport{
		push: push, result: managerResourceResult{state: device.ConnectorStateDegraded, reason: "runtime-unreachable"},
	}
	attempts := 0
	manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error {
		attempts++
		return nil
	}
	manager.reconcile(context.Background())
	manager.mu.Lock()
	_, exists := manager.desired[push.SpecID]
	pending := manager.pendingReports[push.SpecID]
	manager.mu.Unlock()
	if exists || pending != nil || attempts != 0 {
		t.Fatalf("completed delete republished stale DEGRADED: exists=%v pending=%+v attempts=%d", exists, pending, attempts)
	}
}

func TestManagerResourceRecoveredUnknownDeleteDropsPendingDegraded(t *testing.T) {
	dir := t.TempDir()
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, &fakeManagerCommands{document: `[]`})
	if err := a.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	a.reporter = statusReporterFunc(func(_ context.Context, _ device.ConnectorStatus, _ byte) error {
		attempts++
		return errors.New("delete status publish failed")
	})
	a.applyDelete(context.Background(), device.ConnectorPush{SpecID: "unknown-delete-report", Revision: 5})
	if attempts != 1 {
		t.Fatalf("initial delete publish attempts=%d, want 1", attempts)
	}
	if err := os.Remove(dir + "/manager-resources.json.tmp"); err != nil {
		t.Fatal(err)
	}
	a.managerResources.reconcile(context.Background())
	a.managerResources.mu.Lock()
	pending := a.managerResources.pendingReports["unknown-delete-report"]
	high := a.managerResources.revisions["unknown-delete-report"]
	a.managerResources.mu.Unlock()
	if pending != nil || high != 5 || attempts != 1 {
		t.Fatalf("durable unknown delete retained stale report: pending=%+v high=%d attempts=%d", pending, high, attempts)
	}
}

func TestManagerResourceFailedSameRevisionRejectionDeletePublishesDegraded(t *testing.T) {
	dir := t.TempDir()
	manager := newManagerResourceManager(nil)
	if err := manager.openStore(dir); err != nil {
		t.Fatal(err)
	}
	manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error { return nil }
	push := managerPush("rejection-delete-durability", 7, json.RawMessage(`{}`))
	manager.reject(push, device.ConnectorStateFailed, "config-invalid")
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	result := manager.delete(context.Background(), push.SpecID, push.Revision)
	if result.state != device.ConnectorStateDegraded {
		t.Fatalf("failed rejection delete=%+v", result)
	}
	attempts := 0
	manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error {
		attempts++
		return errors.New("delete DEGRADED publication failed")
	}
	manager.publishResult(push.SpecID, push, result)
	manager.mu.Lock()
	pending := manager.pendingReports[push.SpecID]
	manager.mu.Unlock()
	if attempts != 1 || pending == nil || pending.result.state != device.ConnectorStateDegraded {
		t.Fatalf("undurable rejection delete was not queued: attempts=%d pending=%+v", attempts, pending)
	}

	if err := os.Remove(dir + "/manager-resources.json.tmp"); err != nil {
		t.Fatal(err)
	}
	manager.retryStore()
	manager.retryPendingReports()
	manager.mu.Lock()
	pending = manager.pendingReports[push.SpecID]
	manager.mu.Unlock()
	if attempts != 1 || pending != nil {
		t.Fatalf("durable rejection delete retained stale DEGRADED: attempts=%d pending=%+v", attempts, pending)
	}
}

func TestManagerResourceUnrelatedStoreFailureDoesNotReadmitDeleteDegraded(t *testing.T) {
	dir := t.TempDir()
	manager := newManagerResourceManager(nil)
	if err := manager.openStore(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	push := device.ConnectorPush{SpecID: "durable-delete", Revision: 5}
	delayed := manager.delete(context.Background(), push.SpecID, push.Revision)
	if delayed.state != device.ConnectorStateDegraded {
		t.Fatalf("initial unknown delete=%+v", delayed)
	}
	if err := os.Remove(dir + "/manager-resources.json.tmp"); err != nil {
		t.Fatal(err)
	}
	manager.retryStore()

	// A later unrelated failure sets the global dirty flag, but cannot make the
	// already committed delete revision undurable again.
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	manager.reject(device.ConnectorPush{SpecID: "unrelated", Revision: 1},
		device.ConnectorStateFailed, "config-invalid")
	attempts := 0
	manager.report = func(_ string, _ device.ConnectorPush, _ managerResourceResult) error {
		attempts++
		return nil
	}
	manager.publishResult(push.SpecID, push, delayed)
	if attempts != 0 {
		t.Fatal("unrelated store failure re-admitted an obsolete delete DEGRADED")
	}
}

func TestManagerResourceStaleDeletePreservesAppliedLedger(t *testing.T) {
	commands := &fakeManagerCommands{document: `[]`}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 2)
	a.reportRawStatus = func(_ context.Context, raw []byte) error { rawStatuses <- raw; return nil }
	push := managerPush("delete-order", 2, json.RawMessage(`{"version":"1"}`))
	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	a.applyDelete(context.Background(), device.ConnectorPush{SpecID: push.SpecID, Revision: 1})
	a.mu.Lock()
	applied, exists := a.applied[push.SpecID]
	a.mu.Unlock()
	if !exists || applied.revision != 2 || commands.document == `[]` {
		t.Fatalf("stale delete cleared newer state: applied=%+v exists=%v document=%s", applied, exists, commands.document)
	}
}

func TestManagerResourceManagedToReferenceOnlyReleasesOwnership(t *testing.T) {
	commands := &fakeManagerCommands{document: `[]`}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 4)
	a.reportRawStatus = func(_ context.Context, raw []byte) error { rawStatuses <- raw; return nil }
	handler := NewManagerResourceHandler(a)
	managed := managerPush("reference-transition", 1, json.RawMessage(`{"version":"1"}`))
	if err := handler.Apply(context.Background(), managed); err != nil {
		t.Fatal(err)
	}
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	reference := managerPush("reference-transition", 2, nil)
	if err := handler.Apply(context.Background(), reference); err != nil {
		t.Fatal(err)
	}
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	a.managerResources.mu.Lock()
	desired := a.managerResources.desired[reference.SpecID]
	owned := len(desired.owned)
	owners := len(a.managerResources.owners)
	a.managerResources.mu.Unlock()
	if owned != 0 || owners != 0 {
		t.Fatalf("reference-only revision retained deletion ownership: owned=%d owners=%d", owned, owners)
	}
	if result := a.managerResources.delete(context.Background(), reference.SpecID, 3); result.state != device.ConnectorStateApplied {
		t.Fatalf("reference-only delete=%+v", result)
	}
	commands.mu.Lock()
	writes := commands.writes
	document := commands.document
	commands.mu.Unlock()
	if writes != 1 || strings.Contains(document, "reference-transition") == false {
		t.Fatalf("reference transition/delete mutated NI: writes=%d document=%s", writes, document)
	}
}

func TestManagerResourceLedgerFailureDoesNotAdvanceAppliedRevision(t *testing.T) {
	dir := t.TempDir()
	commands := &fakeManagerCommands{document: `[]`}
	a, rec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	if err := a.managerResources.openStore(dir); err != nil {
		t.Fatal(err)
	}
	rawStatuses := make(chan []byte, 4)
	a.reportRawStatus = func(_ context.Context, raw []byte) error { rawStatuses <- raw; return nil }
	handler := NewManagerResourceHandler(a)
	first := managerPush("durable-revision", 1, json.RawMessage(`{"version":"1","generation":"one"}`))
	if err := handler.Apply(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if err := os.Mkdir(dir+"/manager-resources.json.tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	second := managerPush("durable-revision", 2, json.RawMessage(`{"version":"1","generation":"two"}`))
	if err := handler.Apply(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, device.ConnectorStateDegraded, "runtime-unreachable")
	a.mu.Lock()
	applied := a.applied[first.SpecID]
	a.mu.Unlock()
	a.managerResources.mu.Lock()
	high := a.managerResources.revisions[first.SpecID]
	a.managerResources.mu.Unlock()
	if applied.revision != 1 || high != 1 {
		t.Fatalf("failed ledger write advanced revision: applied=%+v high=%d", applied, high)
	}
	if err := os.Remove(dir + "/manager-resources.json.tmp"); err != nil {
		t.Fatal(err)
	}
	if err := handler.Apply(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	frame := waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if frame.Revision != 2 {
		t.Fatalf("retried status revision=%d, want 2", frame.Revision)
	}
	a.mu.Lock()
	applied = a.applied[first.SpecID]
	a.mu.Unlock()
	if applied.revision != 2 {
		t.Fatalf("identical redrive was incorrectly skipped after ledger recovery: %+v", applied)
	}
}

func TestManagerResourceStatusPublicationFailureRetries(t *testing.T) {
	commands := &fakeManagerCommands{document: `[]`}
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	configureFakeManager(a, commands)
	rawStatuses := make(chan []byte, 2)
	attempts := 0
	a.reportRawStatus = func(_ context.Context, raw []byte) error {
		attempts++
		if attempts == 1 {
			return errors.New("status publish failed")
		}
		rawStatuses <- raw
		return nil
	}
	push := managerPush("status-retry", 1, json.RawMessage(`{"version":"1"}`))
	if err := NewManagerResourceHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	a.managerResources.mu.Lock()
	reportedKey := a.managerResources.desired[push.SpecID].reportedKey
	a.managerResources.mu.Unlock()
	if reportedKey != "" {
		t.Fatal("failed status publication was deduplicated as delivered")
	}
	a.managerResources.reconcile(context.Background())
	waitRawStatus(t, rawStatuses, device.ConnectorStateApplied)
	if attempts != 2 {
		t.Fatalf("status publish attempts=%d, want failure plus retry", attempts)
	}
}

func TestManagerResourceStoreRejectsUnwritableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write through directory permission bits")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }()
	manager := newManagerResourceManager(nil)
	if err := manager.openStore(dir); err == nil {
		t.Fatal("unwritable manager store directory must fail startup")
	}
}

func TestManagerResourceExternalMergedStatusFailsClosed(t *testing.T) {
	raw := json.RawMessage(`{
		"manager_device":"nereid-inference",
		"resource":"ExternalServablesJSON",
		"entry_id":"external-one",
		"id_field":"servableId",
		"definition":{"endpoint":"localhost:9000"}
	}`)
	if _, err := parseManagerResourceConfig(raw, "nereid-inference"); err == nil {
		t.Fatal("managed write through NI's merged status resource must be rejected")
	}
	commands := &fakeManagerCommands{document: `[
		{"servableId":"provisioned","channel":"provisioned","state":"READY"}
	]`}
	manager := newManagerResourceManager(nil)
	cfg := managerResourceConfig{
		ManagerDevice: "nereid-inference", Resource: externalServablesResource,
		EntryID: "external-one", IDField: "servableId", managed: true,
	}
	if _, result := manager.writeDocument(context.Background(), commands, cfg, nil); result.state != device.ConnectorStateFailed {
		t.Fatalf("legacy-ledger write guard=%+v", result)
	}
	if commands.writes != 0 || !strings.Contains(commands.document, `"channel":"provisioned"`) {
		t.Fatalf("external status view was mutated: writes=%d document=%s", commands.writes, commands.document)
	}
}
