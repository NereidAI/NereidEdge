// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func TestWatchConfigDefaults(t *testing.T) {
	got := (WatchConfig{}).withDefaults()
	if got.Type != "mqtt" || got.Protocol != "tcp" || got.Host != "edgex-mqtt-broker" ||
		got.Port != 1883 || got.BaseTopic != "edgex" {
		t.Fatalf("unexpected defaults: %+v", got)
	}
	explicit := WatchConfig{Type: "nats-core", Protocol: "tcp", Host: "bus", Port: 4222, BaseTopic: "custom"}.withDefaults()
	if explicit.Type != "nats-core" || explicit.Host != "bus" || explicit.Port != 4222 || explicit.BaseTopic != "custom" {
		t.Fatalf("explicit values should win: %+v", explicit)
	}
}

func TestNewWatcherTopic(t *testing.T) {
	w, err := NewWatcher(WatchConfig{}, mustSyncer(), nil)
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if want := "edgex/system-events/core-metadata/deviceprofile/#"; w.topic != want {
		t.Fatalf("topic = %q, want %q", w.topic, want)
	}
}

func TestNewWatcherRequiresSyncer(t *testing.T) {
	if _, err := NewWatcher(WatchConfig{}, nil, nil); err == nil {
		t.Fatal("expected error for nil syncer")
	}
}

func TestProfileFromSystemEvent(t *testing.T) {
	profile := dtos.DeviceProfile{
		DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Name: "Random-Boolean-Device"},
		DeviceResources: []dtos.DeviceResource{
			{Name: "Bool", Properties: dtos.ResourceProperties{ValueType: "Bool"}},
		},
	}

	systemEventJSON := func(eventType, action string) []byte {
		evt := dtos.NewSystemEvent(eventType, action, common.CoreMetaDataServiceKey, common.CoreMetaDataServiceKey, nil, profile)
		data, err := json.Marshal(evt)
		if err != nil {
			t.Fatalf("marshal system event: %v", err)
		}
		return data
	}

	cases := []struct {
		name      string
		payload   any
		wantOK    bool
		wantName  string
		wantError bool
	}{
		{name: "add raw bytes", payload: systemEventJSON(common.DeviceProfileSystemEventType, common.SystemEventActionAdd), wantOK: true, wantName: profile.Name},
		{name: "update raw bytes", payload: systemEventJSON(common.DeviceProfileSystemEventType, common.SystemEventActionUpdate), wantOK: true, wantName: profile.Name},
		{name: "delete ignored", payload: systemEventJSON(common.DeviceProfileSystemEventType, common.SystemEventActionDelete), wantOK: false},
		{name: "other type ignored", payload: systemEventJSON(common.DeviceSystemEventType, common.SystemEventActionAdd), wantOK: false},
		{name: "undecodable payload", payload: []byte("not-json"), wantError: true},
		{name: "string payload", payload: string(systemEventJSON(common.DeviceProfileSystemEventType, common.SystemEventActionAdd)), wantOK: true, wantName: profile.Name},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prof, ok, err := profileFromSystemEvent(tc.payload)
			if tc.wantError {
				if err == nil {
					t.Fatal("expected decode error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && prof.Name != tc.wantName {
				t.Fatalf("profile name = %q, want %q", prof.Name, tc.wantName)
			}
		})
	}
}

func TestSyncDTODedup(t *testing.T) {
	pub := NewPublisher()
	var published []device.EdgeProfile
	syncer := NewSyncer(nil, pub, func(_ context.Context, profiles []device.EdgeProfile) error {
		published = append(published, profiles...)
		return nil
	})
	ctx := context.Background()

	prof := dtos.DeviceProfile{
		DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Name: "P"},
		DeviceResources:        []dtos.DeviceResource{{Name: "A", Properties: dtos.ResourceProperties{ValueType: "Bool"}}},
	}
	if err := syncer.SyncDTO(ctx, prof); err != nil {
		t.Fatalf("SyncDTO 1: %v", err)
	}
	if err := syncer.SyncDTO(ctx, prof); err != nil {
		t.Fatalf("SyncDTO 2: %v", err)
	}
	if len(published) != 1 {
		t.Fatalf("unchanged SyncDTO published %d frames, want 1", len(published))
	}

	changed := prof
	changed.DeviceResources = []dtos.DeviceResource{{Name: "A", Properties: dtos.ResourceProperties{ValueType: "Int32"}}}
	if err := syncer.SyncDTO(ctx, changed); err != nil {
		t.Fatalf("SyncDTO 3: %v", err)
	}
	if len(published) != 2 {
		t.Fatalf("changed SyncDTO published %d frames, want 2", len(published))
	}
}

// mustSyncer builds a syncer that discards publishes, for constructor tests.
func mustSyncer() *Syncer {
	return NewSyncer(nil, NewPublisher(), func(context.Context, []device.EdgeProfile) error { return nil })
}
