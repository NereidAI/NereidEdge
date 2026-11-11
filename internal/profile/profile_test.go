// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"testing"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func TestShouldPublishFirstTime(t *testing.T) {
	pub := NewPublisher()
	p := device.EdgeProfile{
		Name: "Random-Boolean-Device",
		DeviceResources: []device.EdgeDeviceResource{
			{Name: "Bool", ValueType: "Bool"},
		},
	}
	if !pub.ShouldPublish(p) {
		t.Fatal("first publish should be true")
	}
	hash, err := pub.Hash(p)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	pub.MarkPublished(p.Name, hash)
	if pub.ShouldPublish(p) {
		t.Fatal("second publish should be false for same content")
	}
}

func TestShouldPublishOnChange(t *testing.T) {
	pub := NewPublisher()
	p := device.EdgeProfile{
		Name:            "P",
		DeviceResources: []device.EdgeDeviceResource{{Name: "A", ValueType: "Bool"}},
	}
	hash, _ := pub.Hash(p)
	pub.MarkPublished(p.Name, hash)
	p2 := device.EdgeProfile{
		Name:            "P",
		DeviceResources: []device.EdgeDeviceResource{{Name: "A", ValueType: "Int32"}},
	}
	if !pub.ShouldPublish(p2) {
		t.Fatal("changed content should publish")
	}
}
