// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeInferenceCreds stands in for the inference credential manager: it
// resolves a fixed secret name and records every delivered binding set.
type fakeInferenceCreds struct {
	mu             sync.Mutex
	name           string
	err            error
	ensureErr      error
	bindings       []InferenceSecretBinding
	reconcileCalls int
	ensured        [][]string
	// onEnsure, when set, runs outside the fake lock during Ensure so tests
	// can assert on job visibility ordering.
	onEnsure func(names []string)
}

func (f *fakeInferenceCreds) ResolveSecretName(_ context.Context, _ string) (string, error) {
	return f.name, f.err
}

func (f *fakeInferenceCreds) Ensure(_ context.Context, names []string) error {
	f.mu.Lock()
	f.ensured = append(f.ensured, append([]string(nil), names...))
	hook := f.onEnsure
	f.mu.Unlock()
	if hook != nil {
		hook(append([]string(nil), names...))
	}
	return f.ensureErr
}

func (f *fakeInferenceCreds) Reconcile(_ context.Context, live []InferenceSecretBinding) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcileCalls++
	f.bindings = append([]InferenceSecretBinding(nil), live...)
	return nil
}

func (f *fakeInferenceCreds) snapshot() []InferenceSecretBinding {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]InferenceSecretBinding(nil), f.bindings...)
}

// lastEnsured returns the names passed to the most recent Ensure call.
func (f *fakeInferenceCreds) lastEnsured() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.ensured) == 0 {
		return nil
	}
	return append([]string(nil), f.ensured[len(f.ensured)-1]...)
}

// ensureCount reports how often Ensure was invoked.
func (f *fakeInferenceCreds) ensureCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ensured)
}

func streamInferenceConfig(trigger string) map[string]any {
	cfg := validInferenceConfig(trigger)
	cfg["source"] = map[string]any{
		"device":   infCameraUUID,
		"resource": "snapshot_jpeg",
		"mode":     "stream",
	}
	return cfg
}

// TestStreamBindingCarriesSecretName pins gap-1: stream translation must put
// the camera's onvif-basic secret name on the NI job's Inference properties.
func TestStreamBindingCarriesSecretName(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	a.SetInferenceCredentials(&fakeInferenceCreds{name: "camera-auth"})

	translated, err := a.inferenceDeviceConfig(inferencePush("spec-stream-secret", streamInferenceConfig("periodic")))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	props := translated.Protocols[inferenceProtocol]
	if got := props["SecretName"]; got != "camera-auth" {
		t.Fatalf("SecretName=%v, want camera-auth (props=%v)", got, props)
	}
	if props["SourceKind"] != "onvif-camera" {
		t.Fatalf("SourceKind=%v, want onvif-camera", props["SourceKind"])
	}
}

// TestSnapshotBindingOmitsSecretName pins byte compatibility: snapshot
// translation never emits SecretName even while the credential bridge exists.
func TestSnapshotBindingOmitsSecretName(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	a.SetInferenceCredentials(&fakeInferenceCreds{name: "camera-auth"})

	translated, err := a.inferenceDeviceConfig(inferencePush("spec-snap-secret", validInferenceConfig("periodic")))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	props := translated.Protocols[inferenceProtocol]
	if _, present := props["SecretName"]; present {
		t.Fatalf("snapshot job must not carry SecretName: %v", props)
	}
}

// TestStreamSecretResolutionFailureOmitsName verifies a keeper/metadata
// hiccup neither fails the binding nor fabricates a secret name; the job then
// reports source-no-frame honestly until delivery catches up.
func TestStreamSecretResolutionFailureOmitsName(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	a.SetInferenceCredentials(&fakeInferenceCreds{err: errors.New("keeper down")})

	translated, err := a.inferenceDeviceConfig(inferencePush("spec-stream-nosecret", streamInferenceConfig("periodic")))
	if err != nil {
		t.Fatalf("translation must not fail on soft secret resolution: %v", err)
	}
	if _, present := translated.Protocols[inferenceProtocol]["SecretName"]; present {
		t.Fatal("SecretName must be omitted when resolution fails")
	}
}

// TestCredentialReconcileDeliversAndPrunes covers the apply -> reconcile and
// delete -> prune lifecycle through the real applier methods.
func TestCredentialReconcileDeliversAndPrunes(t *testing.T) {
	a, dev, _ := newInferenceApplier(t)
	creds := &fakeInferenceCreds{name: "camera-auth"}
	a.SetInferenceCredentials(creds)
	a.settledAt = time.Now().Add(-time.Minute)

	h := NewInferenceHandler(a)
	push := inferencePushRev("spec-cred-deliver", 1, streamInferenceConfig("periodic"))
	if err := h.Apply(context.Background(), push); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := creds.lastEnsured(); len(got) != 1 || got[0] != "camera-auth" {
		t.Fatalf("stream apply must pre-deliver its secret, ensured=%v", got)
	}
	jobName := inferenceDeviceName("spec-cred-deliver")
	dev.mu.Lock()
	if got := dev.devices[jobName].Protocols[inferenceProtocol]["SecretName"]; got != "camera-auth" {
		t.Fatalf("applied job SecretName=%v, want camera-auth", got)
	}
	dev.mu.Unlock()

	a.reconcileInferenceCredentials(context.Background())
	bindings := creds.snapshot()
	if len(bindings) != 1 || bindings[0].CameraEdgeXName != infCameraName || bindings[0].SecretName != "camera-auth" {
		t.Fatalf("bindings=%v, want one camera-auth binding for %s", bindings, infCameraName)
	}

	// Unbind: the replica set must converge to empty (pruning is delegated
	// to the credential manager, which reference-counts).
	if err := h.Delete(context.Background(), push); err != nil {
		t.Fatalf("delete: %v", err)
	}
	a.reconcileInferenceCredentials(context.Background())
	if bindings := creds.snapshot(); len(bindings) != 0 {
		t.Fatalf("after delete bindings=%v, want empty", bindings)
	}
}

// directURLInferenceConfig builds a mode=url rtsp binding, optionally
// referencing an edge-provisioned secret name.
func directURLInferenceConfig(secretName string) map[string]any {
	cfg := validInferenceConfig("continuous")
	source := map[string]any{
		"mode":      "url",
		"transport": "rtsp",
		"url":       "rtsp://192.168.123.108/stream2",
	}
	if secretName != "" {
		source["secret_name"] = secretName
	}
	cfg["source"] = source
	return cfg
}

// TestApplyEnsuresSecretBeforeJobVisible pins defect 1 for federated stream
// bindings: the referenced secret MUST be posted to NI before the job device
// is upserted, so NI never observes an unresolvable SecretName.
func TestApplyEnsuresSecretBeforeJobVisible(t *testing.T) {
	a, dev, _ := newInferenceApplier(t)
	creds := &fakeInferenceCreds{name: "camera-auth"}
	a.SetInferenceCredentials(creds)
	a.settledAt = time.Now().Add(-time.Minute)

	jobName := inferenceDeviceName("spec-preapply")
	creds.onEnsure = func(names []string) {
		dev.mu.Lock()
		_, exists := dev.devices[jobName]
		dev.mu.Unlock()
		if exists {
			t.Error("Ensure must run before the job device is upserted")
		}
		if len(names) != 1 || names[0] != "camera-auth" {
			t.Errorf("Ensure names=%v, want [camera-auth]", names)
		}
	}

	h := NewInferenceHandler(a)
	if err := h.Apply(context.Background(), inferencePush("spec-preapply", streamInferenceConfig("periodic"))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := creds.lastEnsured(); len(got) != 1 || got[0] != "camera-auth" {
		t.Fatalf("ensured=%v, want [camera-auth]", got)
	}
	dev.mu.Lock()
	defer dev.mu.Unlock()
	if _, ok := dev.devices[jobName]; !ok {
		t.Fatal("job must be upserted after Ensure")
	}
}

// TestDirectURLBindingEnsuresAndReconciles pins defect 1 for direct-rtsp
// bindings (SourceKind=rtsp): apply delivers source.secret_name before the
// job is visible, and the periodic reconcile protects and replays it.
func TestDirectURLBindingEnsuresAndReconciles(t *testing.T) {
	a, dev, _ := newInferenceApplier(t)
	creds := &fakeInferenceCreds{name: "camera-auth"}
	a.SetInferenceCredentials(creds)
	a.settledAt = time.Now().Add(-time.Minute)

	h := NewInferenceHandler(a)
	push := inferencePushRev("spec-url-secret", 1, directURLInferenceConfig("camera-auth"))
	if err := h.Apply(context.Background(), push); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := creds.lastEnsured(); len(got) != 1 || got[0] != "camera-auth" {
		t.Fatalf("direct-rtsp apply must Ensure its secret before upsert, got %v", got)
	}
	jobName := inferenceDeviceName("spec-url-secret")
	dev.mu.Lock()
	if got := dev.devices[jobName].Protocols[inferenceProtocol]["SecretName"]; got != "camera-auth" {
		t.Fatalf("job SecretName=%v, want camera-auth", got)
	}
	dev.mu.Unlock()

	a.reconcileInferenceCredentials(context.Background())
	bindings := creds.snapshot()
	if len(bindings) != 1 || bindings[0].SecretName != "camera-auth" {
		t.Fatalf("direct-rtsp binding must be in the protected/replay set, bindings=%v", bindings)
	}
}

// TestDirectURLBindingStripsRemovedSecretName verifies that removing
// secret_name from a url binding strips the job property and releases the
// replica reference, even when the change only reaches the stored desired
// document (the retroactive reconcile path).
func TestDirectURLBindingStripsRemovedSecretName(t *testing.T) {
	a, dev, _ := newInferenceApplier(t)
	creds := &fakeInferenceCreds{name: "camera-auth"}
	a.SetInferenceCredentials(creds)
	a.settledAt = time.Now().Add(-time.Minute)

	h := NewInferenceHandler(a)
	specID := "spec-url-strip"
	jobName := inferenceDeviceName(specID)
	if err := h.Apply(context.Background(), inferencePushRev(specID, 1, directURLInferenceConfig("camera-auth"))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	dev.mu.Lock()
	if got := dev.devices[jobName].Protocols[inferenceProtocol]["SecretName"]; got != "camera-auth" {
		t.Fatalf("precondition SecretName=%v", got)
	}
	dev.mu.Unlock()

	// Revised desired document without secret_name; pretend the upsert never
	// reached EdgeX so reconciliation has to converge it.
	raw, err := json.Marshal(directURLInferenceConfig(""))
	if err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	sp := a.applied[specID]
	sp.config = raw
	a.applied[specID] = sp
	a.mu.Unlock()

	a.reconcileInferenceCredentials(context.Background())

	dev.mu.Lock()
	_, present := dev.devices[jobName].Protocols[inferenceProtocol]["SecretName"]
	dev.mu.Unlock()
	if present {
		t.Fatal("SecretName must be stripped from the job after secret_name is removed")
	}
	for _, b := range creds.snapshot() {
		if b.SecretName == "camera-auth" {
			t.Fatalf("stripped secret must leave the referenced set: %v", creds.snapshot())
		}
	}
}

// TestSnapshotApplyDoesNotEnsureSecret pins byte/behavior compatibility:
// snapshot jobs carry no SecretName and trigger no credential delivery.
func TestSnapshotApplyDoesNotEnsureSecret(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	creds := &fakeInferenceCreds{name: "camera-auth"}
	a.SetInferenceCredentials(creds)

	h := NewInferenceHandler(a)
	if err := h.Apply(context.Background(), inferencePush("spec-snap-noensure", validInferenceConfig("periodic"))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if n := creds.ensureCount(); n != 0 {
		t.Fatalf("snapshot job must not trigger secret delivery, got %d Ensure calls", n)
	}
}

// TestReplayTickerReconcilesReferencedSecrets pins defect 3: the reconciler
// re-runs on a steady cadence even with no apply events, re-posting every
// referenced secret so an NI restart self-heals.
func TestReplayTickerReconcilesReferencedSecrets(t *testing.T) {
	oldInterval, oldSettle := inferenceCredReplayInterval, inferenceCredSettleDelay
	inferenceCredReplayInterval = 5 * time.Millisecond
	inferenceCredSettleDelay = time.Millisecond
	t.Cleanup(func() {
		inferenceCredReplayInterval, inferenceCredSettleDelay = oldInterval, oldSettle
	})

	a, _, _ := newInferenceApplier(t)
	creds := &fakeInferenceCreds{name: "camera-auth"}
	a.SetInferenceCredentials(creds)
	a.settledAt = time.Now().Add(-time.Minute)

	h := NewInferenceHandler(a)
	if err := h.Apply(context.Background(), inferencePush("spec-replay", directURLInferenceConfig("camera-auth"))); err != nil {
		t.Fatalf("apply: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.runInferenceCredentialReconciler(ctx)
	}()
	// Stop the reconciler before Cleanup restores the package-level timers.
	defer func() {
		cancel()
		<-done
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		creds.mu.Lock()
		n := creds.reconcileCalls
		creds.mu.Unlock()
		if n >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("periodic replay must reconcile the referenced secret set repeatedly")
}

// TestRetroactiveSecretDeliveryUpdatesJob verifies a credential delivered
// AFTER the stream binding was applied re-adds SecretName through an
// UpdateDevice without any spec revision change.
func TestRetroactiveSecretDeliveryUpdatesJob(t *testing.T) {
	a, dev, _ := newInferenceApplier(t)
	missing := &fakeInferenceCreds{name: ""}
	a.SetInferenceCredentials(missing)
	a.settledAt = time.Now().Add(-time.Minute)

	h := NewInferenceHandler(a)
	push := inferencePushRev("spec-retro", 1, streamInferenceConfig("periodic"))
	if err := h.Apply(context.Background(), push); err != nil {
		t.Fatalf("apply: %v", err)
	}
	jobName := inferenceDeviceName("spec-retro")
	dev.mu.Lock()
	if _, present := dev.devices[jobName].Protocols[inferenceProtocol]["SecretName"]; present {
		t.Fatal("job must start without SecretName")
	}
	dev.mu.Unlock()

	// Secret delivery lands: the onvif binder would notify the applier.
	missing.name = "camera-auth"
	a.reconcileInferenceCredentials(context.Background())

	dev.mu.Lock()
	defer dev.mu.Unlock()
	if dev.updCalls < 1 {
		t.Fatal("retroactive secret must UpdateDevice the job")
	}
	if got := dev.devices[jobName].Protocols[inferenceProtocol]["SecretName"]; got != "camera-auth" {
		t.Fatalf("refreshed job SecretName=%v, want camera-auth", got)
	}
}
