// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"testing"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

type stubHandler struct {
	typ     string
	applied int
	deleted int
}

func (h *stubHandler) Type() string { return h.typ }

func (h *stubHandler) Apply(_ context.Context, _ device.ConnectorPush) error {
	h.applied++
	return nil
}

func (h *stubHandler) Delete(_ context.Context, _ device.ConnectorPush) error {
	h.deleted++
	return nil
}

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	h := &stubHandler{typ: "edge-export-mqtt"}
	r.Register(h)

	got, ok := r.Get("edge-export-mqtt")
	if !ok || got != Handler(h) {
		t.Fatalf("Get returned %v, %v; want registered handler", got, ok)
	}
	if _, ok := r.Get("unknown"); ok {
		t.Fatal("Get(unknown) reported ok")
	}
	if n := len(r.GetAll()); n != 1 {
		t.Fatalf("GetAll len = %d, want 1", n)
	}
}

func TestRegistryOverwriteByType(t *testing.T) {
	r := NewRegistry()
	first := &stubHandler{typ: "t"}
	second := &stubHandler{typ: "t"}
	r.Register(first)
	r.Register(second)

	got, _ := r.Get("t")
	if got != Handler(second) {
		t.Fatal("later Register for same type should replace the handler")
	}
}

func TestRegistryNilSafety(t *testing.T) {
	var r *Registry
	r.Register(&stubHandler{typ: "t"})
	if h, ok := r.Get("t"); ok {
		t.Fatalf("nil registry Get returned %v", h)
	}
	if r.GetAll() != nil {
		t.Fatal("nil registry GetAll should be nil")
	}
}
