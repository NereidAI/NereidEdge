// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"context"
	"errors"
	"sync"
	"testing"

	coreclients "github.com/edgexfoundry/go-mod-core-contracts/v4/clients/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/requests"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
	edgexErrors "github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// fakeProfileClient emulates the core-metadata subset for profile upsert
// and delete.
type fakeProfileClient struct {
	coreclients.DeviceProfileClient

	mu       sync.Mutex
	profiles map[string]dtos.DeviceProfile
	adds     int
	updates  int
	deletes  []string

	addErr     error
	deleteErr  error
	notFoundOn bool
}

func newFakeProfileClient() *fakeProfileClient {
	return &fakeProfileClient{profiles: map[string]dtos.DeviceProfile{}}
}

func (c *fakeProfileClient) DeviceProfileByName(_ context.Context, name string) (responses.DeviceProfileResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if prof, ok := c.profiles[name]; ok {
		return responses.DeviceProfileResponse{Profile: prof}, nil
	}
	return responses.DeviceProfileResponse{}, edgexErrors.NewCommonEdgeX(edgexErrors.KindEntityDoesNotExist, "missing", nil)
}

func (c *fakeProfileClient) Add(_ context.Context, reqs []requests.DeviceProfileRequest) ([]dtoCommon.BaseWithIdResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.addErr != nil {
		if ex, ok := c.addErr.(edgexErrors.EdgeX); ok {
			return nil, ex
		}
		return nil, edgexErrors.NewCommonEdgeXWrapper(c.addErr)
	}
	for _, req := range reqs {
		if _, exists := c.profiles[req.Profile.Name]; exists {
			return nil, edgexErrors.NewCommonEdgeX(edgexErrors.KindDuplicateName, "dup", nil)
		}
		id := "id-" + req.Profile.Name
		prof := req.Profile
		prof.Id = id
		c.profiles[prof.Name] = prof
		c.adds++
	}
	return []dtoCommon.BaseWithIdResponse{{Id: "id-" + reqs[0].Profile.Name}}, nil
}

func (c *fakeProfileClient) Update(_ context.Context, reqs []requests.DeviceProfileRequest) ([]dtoCommon.BaseResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, req := range reqs {
		if c.notFoundOn {
			return nil, edgexErrors.NewCommonEdgeX(edgexErrors.KindEntityDoesNotExist, "missing", nil)
		}
		c.profiles[req.Profile.Name] = req.Profile
		c.updates++
	}
	return []dtoCommon.BaseResponse{{}}, nil
}

func (c *fakeProfileClient) DeleteByName(_ context.Context, name string) (dtoCommon.BaseResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deleteErr != nil {
		if ex, ok := c.deleteErr.(edgexErrors.EdgeX); ok {
			return dtoCommon.BaseResponse{}, ex
		}
		return dtoCommon.BaseResponse{}, edgexErrors.NewCommonEdgeXWrapper(c.deleteErr)
	}
	if _, ok := c.profiles[name]; !ok {
		return dtoCommon.BaseResponse{}, edgexErrors.NewCommonEdgeX(edgexErrors.KindEntityDoesNotExist, "missing", nil)
	}
	delete(c.profiles, name)
	c.deletes = append(c.deletes, name)
	return dtoCommon.BaseResponse{}, nil
}

func TestEdgeXApplierFormat(t *testing.T) {
	a := NewEdgeXApplier(newFakeProfileClient())
	if a.Format() != device.ProfileFormatEdgeX {
		t.Fatalf("format=%q", a.Format())
	}
}

func TestEdgeXApplyAddsNewProfile(t *testing.T) {
	client := newFakeProfileClient()
	a := NewEdgeXApplier(client)
	if err := a.Apply(context.Background(), edgexEntry("A1", 1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if client.adds != 1 || client.updates != 0 {
		t.Fatalf("adds=%d updates=%d", client.adds, client.updates)
	}
	prof := client.profiles["A1"]
	if prof.Id != "id-A1" || len(prof.DeviceResources) != 1 ||
		prof.DeviceResources[0].Properties.ValueType != "Float32" {
		t.Fatalf("stored profile: %+v", prof)
	}
}

func TestEdgeXApplyUpdatesExistingWithID(t *testing.T) {
	client := newFakeProfileClient()
	client.profiles["A1"] = dtos.DeviceProfile{
		DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Id: "stored-id", Name: "A1"},
		DeviceResources: []dtos.DeviceResource{
			{Name: "old", Properties: dtos.ResourceProperties{ValueType: "String", ReadWrite: "R"}},
		},
	}
	a := NewEdgeXApplier(client)
	if err := a.Apply(context.Background(), edgexEntry("A1", 2)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if client.updates != 1 || client.adds != 0 {
		t.Fatalf("adds=%d updates=%d", client.adds, client.updates)
	}
	if prof := client.profiles["A1"]; prof.Id != "stored-id" {
		t.Fatalf("update lost id: %+v", prof)
	}
}

func TestEdgeXApplyAddDuplicateFallsBackToUpdate(t *testing.T) {
	client := newFakeProfileClient()
	client.addErr = edgexErrors.NewCommonEdgeX(edgexErrors.KindDuplicateName, "dup", nil)
	client.profiles["A1"] = dtos.DeviceProfile{DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Id: "raced-id", Name: "A1"}}
	a := NewEdgeXApplier(client)
	if err := a.Apply(context.Background(), edgexEntry("A1", 1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if client.updates != 1 {
		t.Fatalf("expected duplicate fallback update, updates=%d", client.updates)
	}
}

func TestEdgeXApplyFillsMissingReadWrite(t *testing.T) {
	client := newFakeProfileClient()
	a := NewEdgeXApplier(client)
	entry := device.ProfileEntry{
		Name:   "S7-Device",
		Format: device.ProfileFormatEdgeX,
		Document: map[string]any{
			"name": "S7-Device",
			"deviceResources": []any{
				map[string]any{"name": "bool",
					"properties": map[string]any{"valueType": "Bool"}},
				map[string]any{"name": "msg",
					"properties": map[string]any{"valueType": "String", "readWrite": "R"}},
			},
		},
	}
	if err := a.Apply(context.Background(), entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	resources := client.profiles["S7-Device"].DeviceResources
	if resources[0].Properties.ReadWrite != "RW" {
		t.Fatalf("missing readWrite must default to RW, got %q", resources[0].Properties.ReadWrite)
	}
	if resources[1].Properties.ReadWrite != "R" {
		t.Fatalf("explicit readWrite must be preserved, got %q", resources[1].Properties.ReadWrite)
	}
}

func TestEdgeXApplyWriteOnlyResourceUnderReadCommandStillFails(t *testing.T) {
	client := newFakeProfileClient()
	a := NewEdgeXApplier(client)
	entry := device.ProfileEntry{
		Name:   "W1",
		Format: device.ProfileFormatEdgeX,
		Document: map[string]any{
			"name": "W1",
			"deviceResources": []any{
				map[string]any{"name": "trigger",
					"properties": map[string]any{"valueType": "String", "readWrite": "W"}},
			},
			"deviceCommands": []any{
				map[string]any{"name": "fire", "readWrite": "R",
					"resourceOperations": []any{
						map[string]any{"deviceResource": "trigger"},
					}},
			},
		},
	}
	if err := a.Apply(context.Background(), entry); !errors.Is(err, ErrInvalidProfileDocument) {
		t.Fatalf("err=%v, want ErrInvalidProfileDocument", err)
	}
}

func TestEdgeXApplyRejectsInvalidDocuments(t *testing.T) {
	a := NewEdgeXApplier(newFakeProfileClient())
	cases := []struct {
		name  string
		entry device.ProfileEntry
	}{
		{"empty document", device.ProfileEntry{Name: "A1", Format: device.ProfileFormatEdgeX}},
		{"name mismatch", device.ProfileEntry{
			Name: "A1", Format: device.ProfileFormatEdgeX,
			Document: map[string]any{"name": "Other", "deviceResources": []any{}},
		}},
		{"missing valueType", device.ProfileEntry{
			Name: "A1", Format: device.ProfileFormatEdgeX,
			Document: map[string]any{"name": "A1", "deviceResources": []any{
				map[string]any{"name": "x"},
			}},
		}},
		{"wrong field types", device.ProfileEntry{
			Name: "A1", Format: device.ProfileFormatEdgeX,
			// A string where an array is expected fails DTO parsing.
			Document: map[string]any{"name": "A1", "deviceResources": "nope"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := a.Apply(context.Background(), tc.entry); !errors.Is(err, ErrInvalidProfileDocument) {
				t.Fatalf("err=%v, want ErrInvalidProfileDocument", err)
			}
		})
	}
}

func TestEdgeXRemove(t *testing.T) {
	client := newFakeProfileClient()
	client.profiles["A1"] = dtos.DeviceProfile{DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Id: "id-A1", Name: "A1"}}
	a := NewEdgeXApplier(client)

	if err := a.Remove(context.Background(), "ghost"); err != nil {
		t.Fatalf("missing profile must remove idempotently: %v", err)
	}
	if err := a.Remove(context.Background(), "A1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if len(client.deletes) != 1 || client.deletes[0] != "A1" {
		t.Fatalf("deletes=%v", client.deletes)
	}

	client.profiles["A1"] = dtos.DeviceProfile{DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Id: "id-A1", Name: "A1"}}
	client.deleteErr = edgexErrors.NewCommonEdgeX(edgexErrors.KindStatusConflict, "in use", nil)
	if err := a.Remove(context.Background(), "A1"); !errors.Is(err, ErrProfileInUse) {
		t.Fatalf("err=%v, want ErrProfileInUse", err)
	}

	client.deleteErr = edgexErrors.NewCommonEdgeX(edgexErrors.KindServiceUnavailable, "down", nil)
	if err := a.Remove(context.Background(), "A1"); errors.Is(err, ErrProfileInUse) || err == nil {
		t.Fatalf("generic failure must propagate, got %v", err)
	}
}
