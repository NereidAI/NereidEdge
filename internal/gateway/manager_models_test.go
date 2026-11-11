// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func ackWithInference() *device.HelloAck {
	return &device.HelloAck{
		Inference: &device.InferenceAdvert{
			Contract: device.InferenceContractModel1,
			Backends: []string{"kserve"},
		},
	}
}

func TestBuildManifestOmitsModelsWithoutProvider(t *testing.T) {
	m := NewManager(nil)
	m.SetAck(ackWithInference())

	mf := m.BuildManifest(context.Background())
	if mf.Models != nil {
		t.Fatalf("models segment must be nil without a provider, got %+v", *mf.Models)
	}
}

func TestBuildManifestOmitsModelsBeforeNegotiation(t *testing.T) {
	m := NewManager(nil)
	m.SetModelProvider(func(_ context.Context) []device.ManifestModel {
		t.Fatal("provider must not be called before the feature is negotiated")
		return nil
	})
	// No SetAck: feature not negotiated.
	mf := m.BuildManifest(context.Background())
	if mf.Models != nil {
		t.Fatalf("models segment must be nil before negotiation, got %+v", *mf.Models)
	}
}

func TestBuildManifestIncludesModelsAfterNegotiation(t *testing.T) {
	m := NewManager(nil)
	m.SetAck(ackWithInference())
	want := []device.ManifestModel{{
		ID:       "yiqisoft-yolov8n",
		Version:  "1.0.0",
		Revision: "76937",
		Task:     "detection",
		Backend:  "kserve",
		State:    device.ModelStateReady,
		Labels:   []string{"person"},
	}}
	m.SetModelProvider(func(_ context.Context) []device.ManifestModel {
		return want
	})

	mf := m.BuildManifest(context.Background())
	if mf.Models == nil {
		t.Fatal("models segment missing after negotiation")
	}
	if len(*mf.Models) != 1 || (*mf.Models)[0].ID != want[0].ID {
		t.Fatalf("models=%+v, want %+v", *mf.Models, want)
	}
}

func TestBuildManifestModelsEmptyIsExplicitZeroInventory(t *testing.T) {
	m := NewManager(nil)
	m.SetAck(ackWithInference())
	m.SetModelProvider(func(_ context.Context) []device.ManifestModel {
		return []device.ManifestModel{}
	})

	mf := m.BuildManifest(context.Background())
	if mf.Models == nil {
		t.Fatal("explicit empty inventory must stay a non-nil (empty) slice")
	}
	if len(*mf.Models) != 0 {
		t.Fatalf("models=%+v, want empty slice", *mf.Models)
	}
	raw, _ := json.Marshal(mf)
	if !bytes.Contains(raw, []byte(`"models":[]`)) {
		t.Fatalf("manifest frame %s does not carry explicit \"models\":[]", raw)
	}
}

// TestBuildManifestReadFailureOmitsSegment: when the provider cannot supply
// fresh data it returns nil and the models key must be absent entirely — never
// [] (which would authoritatively prune NL's inventory).
func TestBuildManifestReadFailureOmitsSegment(t *testing.T) {
	m := NewManager(nil)
	m.SetAck(ackWithInference())
	m.SetModelProvider(func(_ context.Context) []device.ManifestModel { return nil })

	raw, _ := json.Marshal(m.BuildManifest(context.Background()))
	if bytes.Contains(raw, []byte(`"models"`)) {
		t.Fatalf("failed-read frame %s must omit the models key", raw)
	}
}

func TestInferenceNegotiatedReflectsAck(t *testing.T) {
	m := NewManager(nil)
	if m.InferenceNegotiated() {
		t.Fatal("must not report negotiated without an ack")
	}
	m.SetAck(&device.HelloAck{})
	if m.InferenceNegotiated() {
		t.Fatal("ack without the inference block must not count as negotiated")
	}
	m.SetAck(ackWithInference())
	if !m.InferenceNegotiated() {
		t.Fatal("must report negotiated once the ack carries the inference block")
	}
}
