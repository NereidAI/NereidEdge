// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package shadow

import (
	"context"
	"errors"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

type fakeClient struct {
	subscribed int
	reports    []device.ReportedPatch
	failNext   bool
}

func (f *fakeClient) SubscribeShadowDesired(_ context.Context, _ byte,
	_ func(device.DesiredPush)) error {
	f.subscribed++
	return nil
}

func (f *fakeClient) ReportShadow(_ context.Context, patch device.ReportedPatch, _ byte) error {
	if f.failNext {
		f.failNext = false
		return errors.New("publish failed")
	}
	f.reports = append(f.reports, patch)
	return nil
}

func newHandler(f *fakeClient, state map[string]any) *Handler {
	if state == nil {
		state = map[string]any{"agent_version": "test"}
	}
	return New(f, logger.NewMockClient(), func() map[string]any {
		out := make(map[string]any, len(state))
		for k, v := range state {
			out[k] = v
		}
		return out
	})
}

func TestReportPublishesRealState(t *testing.T) {
	f := &fakeClient{}
	h := newHandler(f, map[string]any{"agent_version": "v1"})

	h.Report(true)

	if len(f.reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(f.reports))
	}
	if got := f.reports[0].State["agent_version"]; got != "v1" {
		t.Fatalf("agent_version = %v, want v1", got)
	}
	if f.reports[0].Version != nil {
		t.Fatalf("version = %v, want nil before any desired push", *f.reports[0].Version)
	}
}

func TestReportDeduplicatesUnchangedState(t *testing.T) {
	f := &fakeClient{}
	h := newHandler(f, map[string]any{"a": 1})

	h.Report(true)
	h.Report(false)

	if len(f.reports) != 1 {
		t.Fatalf("reports = %d, want 1 (second is a no-op)", len(f.reports))
	}
}

func TestReportForceAlwaysPublishes(t *testing.T) {
	f := &fakeClient{}
	h := newHandler(f, map[string]any{"a": 1})

	h.Report(true)
	h.Report(true)

	if len(f.reports) != 2 {
		t.Fatalf("reports = %d, want 2 (force bypasses de-dupe)", len(f.reports))
	}
}

func TestReportIncludesAppliedVersion(t *testing.T) {
	f := &fakeClient{}
	h := newHandler(f, map[string]any{"a": 1})

	h.onDesired(device.DesiredPush{Version: 3, Desired: map[string]any{"x": 1}})

	if len(f.reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(f.reports))
	}
	if f.reports[0].Version == nil || *f.reports[0].Version != 3 {
		t.Fatalf("version = %v, want 3", f.reports[0].Version)
	}
}

func TestReportIgnoresStaleDesiredVersion(t *testing.T) {
	f := &fakeClient{}
	h := newHandler(f, nil)

	h.onDesired(device.DesiredPush{Version: 5, Desired: map[string]any{"a": 1}})
	before := len(f.reports)
	h.onDesired(device.DesiredPush{Version: 3, Desired: map[string]any{"a": 2}})

	if len(f.reports) != before {
		t.Fatalf("stale version produced a report: %d -> %d", before, len(f.reports))
	}
}

func TestSetIdentityAddsProtoAndFeatures(t *testing.T) {
	f := &fakeClient{}
	h := newHandler(f, nil)
	h.SetIdentity("1.4", []string{"telemetry", "shadow"})

	h.Report(true)

	got, ok := f.reports[0].State["features"].([]string)
	if !ok || len(got) != 2 {
		t.Fatalf("features = %v, want two entries", f.reports[0].State["features"])
	}
	if f.reports[0].State["proto"] != "1.4" {
		t.Fatalf("proto = %v, want 1.4", f.reports[0].State["proto"])
	}
}

func TestReportRetriesAfterFailure(t *testing.T) {
	f := &fakeClient{failNext: true}
	h := newHandler(f, map[string]any{"a": 1})

	h.Report(false) // fails, must not be remembered as sent
	h.Report(false) // same state must retry

	if len(f.reports) != 1 {
		t.Fatalf("reports = %d, want 1 (retry after failure)", len(f.reports))
	}
}
