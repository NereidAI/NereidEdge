// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"testing"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func TestNorthboundFilterDeleteSurvivesAppliedLedgerReset(t *testing.T) {
	a, rec := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	push := device.ConnectorPush{
		Kind: device.ConnectorKindSpec, SpecID: "filter-spec", ConnectorType: TypeNorthboundFilter,
		Revision: 1,
		Config:   []byte(`{"rules":[{"action":"deny","profile_names":["raw-profile"]}]}`),
	}
	if err := NewNorthboundFilterHandler(a).Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, device.ConnectorStateApplied, "")
	if a.uplinkFilters.NeedsRestart(push.SpecID) {
		t.Fatal("filter was not installed")
	}

	a.Reset()
	a.applyDelete(context.Background(), device.ConnectorPush{SpecID: push.SpecID})
	if !a.uplinkFilters.NeedsRestart(push.SpecID) {
		t.Fatal("type-less delete after Reset left the filter active")
	}
}
