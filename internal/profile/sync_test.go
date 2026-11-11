// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"context"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func TestToEdgeProfile(t *testing.T) {
	prof := dtos.DeviceProfile{
		DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Name: "Random-Boolean-Device"},
		DeviceResources: []dtos.DeviceResource{
			{
				Name:        "Bool",
				Description: "random bool",
				Properties:  dtos.ResourceProperties{ValueType: "Bool"},
			},
		},
	}
	got := ToEdgeProfile(prof)
	if got.Name != "Random-Boolean-Device" {
		t.Fatalf("name = %q", got.Name)
	}
	if len(got.DeviceResources) != 1 {
		t.Fatalf("resources = %d", len(got.DeviceResources))
	}
	r := got.DeviceResources[0]
	if r.Name != "Bool" || r.ValueType != "Bool" || r.Description != "random bool" {
		t.Fatalf("resource = %+v", r)
	}
}

func TestSyncerReportDedup(t *testing.T) {
	pub := NewPublisher()
	published := make([]device.EdgeProfile, 0)
	syncer := NewSyncer(nil, pub, func(_ context.Context, profiles []device.EdgeProfile) error {
		published = append(published, profiles...)
		return nil
	})
	ctx := context.Background()
	p1 := device.EdgeProfile{
		Name:            "P",
		DeviceResources: []device.EdgeDeviceResource{{Name: "A", ValueType: "Bool"}},
	}
	if err := syncer.report(ctx, p1); err != nil {
		t.Fatalf("report 1: %v", err)
	}
	if err := syncer.report(ctx, p1); err != nil {
		t.Fatalf("report 2: %v", err)
	}
	if len(published) != 1 {
		t.Fatalf("unchanged re-report published %d frames", len(published))
	}

	p2 := device.EdgeProfile{
		Name:            "P",
		DeviceResources: []device.EdgeDeviceResource{{Name: "A", ValueType: "Int32"}},
	}
	if err := syncer.report(ctx, p2); err != nil {
		t.Fatalf("report 3: %v", err)
	}
	if len(published) != 2 {
		t.Fatalf("changed profile should publish, got %d frames", len(published))
	}
}
