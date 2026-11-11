// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	clientsinterfaces "github.com/edgexfoundry/go-mod-core-contracts/v4/clients/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/requests"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
	edgexErrors "github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	device "github.com/nereidai/nereidlink/sdk/device/go"

	"github.com/nereidai/nereidedge/internal/binding"
)

// silentLogger discards all log output in queue tests.
type silentLogger struct{}

func (silentLogger) SetLogLevel(string) error      { return nil }
func (silentLogger) LogLevel() string              { return "TRACE" }
func (silentLogger) Debug(string, ...interface{})  {}
func (silentLogger) Error(string, ...interface{})  {}
func (silentLogger) Info(string, ...interface{})   {}
func (silentLogger) Trace(string, ...interface{})  {}
func (silentLogger) Warn(string, ...interface{})   {}
func (silentLogger) Debugf(string, ...interface{}) {}
func (silentLogger) Errorf(string, ...interface{}) {}
func (silentLogger) Infof(string, ...interface{})  {}
func (silentLogger) Tracef(string, ...interface{}) {}
func (silentLogger) Warnf(string, ...interface{})  {}

// fakeAppService exposes only the two clients the applier uses; every other
// ApplicationService method panics if unexpectedly invoked.
type fakeAppService struct {
	interfaces.ApplicationService
	dev *fakeDeviceClient
	log logger.LoggingClient
}

func (f *fakeAppService) DeviceClient() clientsinterfaces.DeviceClient { return f.dev }
func (f *fakeAppService) LoggingClient() logger.LoggingClient          { return f.log }

// fakeDeviceClient emulates the subset of the core-metadata device client
// the applier calls. byName blocks until the caller deadline when hung.
type fakeDeviceClient struct {
	clientsinterfaces.DeviceClient
	mu       sync.Mutex
	hung     bool
	notFound bool
	created  map[string]bool
	devices  map[string]dtos.Device
	added    []dtos.Device
	addCalls int
	delCalls []string
	updCalls int
	// allDevices fuels AllDevices paged listings for the orphan sweep.
	allDevices []dtos.Device
	// addReject makes Add return a 207-style per-item validation failure
	// without storing the device.
	addReject string
}

func (c *fakeDeviceClient) DeviceByName(ctx context.Context, name string) (responses.DeviceResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	hung, exists := c.hung, c.created[name]
	dev := c.devices[name]
	c.mu.Unlock()
	if hung {
		<-ctx.Done()
		return responses.DeviceResponse{}, edgexErrors.NewCommonEdgeXWrapper(ctx.Err())
	}
	if c.notFound && !exists {
		return responses.DeviceResponse{}, edgexErrors.NewCommonEdgeX(edgexErrors.KindEntityDoesNotExist, "device does not exist", nil)
	}
	return responses.DeviceResponse{Device: dev}, nil
}

func (c *fakeDeviceClient) setHung(v bool) {
	c.mu.Lock()
	c.hung = v
	c.mu.Unlock()
}

func (c *fakeDeviceClient) Add(_ context.Context, reqs []requests.AddDeviceRequest) ([]dtoCommon.BaseWithIdResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	c.addCalls++
	if c.addReject != "" {
		c.mu.Unlock()
		// EdgeX multi-status 207: transport succeeds but the per-item
		// device-service validation failed (e.g. NI FFmpegExtra allowlist).
		return []dtoCommon.BaseWithIdResponse{{
			BaseResponse: dtoCommon.BaseResponse{StatusCode: 500, Message: c.addReject},
		}}, nil
	}
	if c.created == nil {
		c.created = make(map[string]bool)
	}
	for _, r := range reqs {
		c.created[r.Device.Name] = true
		c.added = append(c.added, r.Device)
		if c.devices == nil {
			c.devices = make(map[string]dtos.Device)
		}
		c.devices[r.Device.Name] = r.Device
	}
	c.mu.Unlock()
	return nil, nil
}

func (c *fakeDeviceClient) DeleteDeviceByName(_ context.Context, name string) (dtoCommon.BaseResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	c.delCalls = append(c.delCalls, name)
	c.mu.Unlock()
	return dtoCommon.BaseResponse{}, nil
}

func (c *fakeDeviceClient) Update(_ context.Context, reqs []requests.UpdateDeviceRequest) ([]dtoCommon.BaseResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	c.updCalls++
	for _, r := range reqs {
		d := r.Device
		if d.Name == nil || d.Protocols == nil {
			continue
		}
		dev := c.devices[*d.Name]
		dev.Name = *d.Name
		if d.ServiceName != nil {
			dev.ServiceName = *d.ServiceName
		}
		if d.ProfileName != nil {
			dev.ProfileName = *d.ProfileName
		}
		dev.Protocols = d.Protocols
		if c.created == nil {
			c.created = make(map[string]bool)
		}
		if c.devices == nil {
			c.devices = make(map[string]dtos.Device)
		}
		c.created[*d.Name] = true
		c.devices[*d.Name] = dev
	}
	c.mu.Unlock()
	return nil, nil
}

func (c *fakeDeviceClient) deleteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.delCalls)
}

// AllDevices returns the configured fixture page at offset 0 and an empty
// trailing page, exercising the sweep pagination stop condition.
func (c *fakeDeviceClient) AllDevices(_ context.Context, _ []string, offset, limit int) (responses.MultiDevicesResponse, edgexErrors.EdgeX) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= len(c.allDevices) {
		return responses.MultiDevicesResponse{Devices: []dtos.Device{}}, nil
	}
	end := offset + limit
	if end > len(c.allDevices) {
		end = len(c.allDevices)
	}
	return responses.MultiDevicesResponse{Devices: append([]dtos.Device(nil), c.allDevices[offset:end]...)}, nil
}

// statusRecord captures reported connector lifecycle states.
type statusRecord struct {
	ch chan device.ConnectorStatus
}

func newStatusRecord() *statusRecord {
	return &statusRecord{ch: make(chan device.ConnectorStatus, 32)}
}

func (r *statusRecord) ReportConnectorStatus(_ context.Context, status device.ConnectorStatus, _ byte) error {
	r.ch <- status
	return nil
}

func (r *statusRecord) waitFor(t *testing.T, state, wantReason string) device.ConnectorStatus {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case st := <-r.ch:
			if st.State != state {
				continue
			}
			if wantReason != "" && !strings.Contains(st.Reason, wantReason) {
				t.Fatalf("state %s reason = %q, want substring %q", state, st.Reason, wantReason)
			}
			return st
		case <-deadline:
			t.Fatalf("timed out waiting for state %s", state)
			return device.ConnectorStatus{}
		}
	}
}

// newQueueApplier builds an applier with fakes and a shortened metadata
// timeout, never calling Start (no real MQTT subscription).
func newQueueApplier(t *testing.T, dev *fakeDeviceClient, attach AttachFunc, detach DetachFunc) (*Applier, *statusRecord) {
	t.Helper()
	svc := &fakeAppService{dev: dev, log: silentLogger{}}
	a := NewApplier(svc, nil, binding.NewResolver(nil), false, attach, detach)
	a.metadataTimeout = 100 * time.Millisecond
	rec := newStatusRecord()
	a.reporter = rec
	return a, rec
}

func validSpecPush(specID string) device.ConnectorPush {
	return validSpecPushNamed(specID, "camera-109")
}

func validSpecPushNamed(specID, deviceName string) device.ConnectorPush {
	cfg, _ := json.Marshal(SpecConfig{
		DSName:      "device-onvif-camera",
		ProfileName: "onvif-camera",
		DeviceName:  deviceName,
	})
	return device.ConnectorPush{
		Kind:          device.ConnectorKindSpec,
		SpecID:        specID,
		DeviceID:      "07704cf9-f0fc-4876-8955-64e6141540c7",
		ConnectorType: TypeEdgeXConnector,
		Revision:      1,
		Config:        cfg,
	}
}

// TestEnqueueNeverBlocksOnHungMetadata is the production incident regression:
// the paho downlink callback must return immediately even while metadata is
// unreachable; the apply worker then bounds the call and reports FAILED,
// continues draining, and APPLIED once metadata recovers.
func TestEnqueueNeverBlocksOnHungMetadata(t *testing.T) {
	dev := &fakeDeviceClient{hung: true}
	a, rec := newQueueApplier(t, dev, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		a.enqueuePush(validSpecPush("spec-hung"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("enqueuePush blocked waiting for core-metadata")
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("enqueuePush took %v, want sub-millisecond", elapsed)
	}

	go a.runPushWorker(ctx)
	st := rec.waitFor(t, device.ConnectorStateFailed, "core-metadata")
	if st.Revision != 1 {
		t.Errorf("reported revision = %d, want 1", st.Revision)
	}

	// Recovery: unblock metadata; the next redrive of the same spec applies.
	dev.setHung(false)
	a.enqueuePush(validSpecPush("spec-hung"))
	rec.waitFor(t, device.ConnectorStateApplied, "")
}

// blockingReporter never acknowledges until its context expires, emulating a
// stalled paho publish; the worker must bound the wait and keep processing.
type blockingReporter struct {
	calls int
	mu    sync.Mutex
}

func (r *blockingReporter) ReportConnectorStatus(ctx context.Context, _ device.ConnectorStatus, _ byte) error {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (r *blockingReporter) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// TestStatusReportIsBounded ensures a stalled status publish cannot hang the
// apply worker: CREATING and APPLIED for two specs each block until the
// bounded deadline, yet the worker drains both pushes.
func TestStatusReportIsBounded(t *testing.T) {
	dev := &fakeDeviceClient{notFound: true}
	svc := &fakeAppService{dev: dev, log: silentLogger{}}
	a := NewApplier(svc, nil, binding.NewResolver(nil), false, nil, nil)
	a.metadataTimeout = 100 * time.Millisecond
	a.statusTimeout = 50 * time.Millisecond
	blocked := &blockingReporter{}
	a.reporter = blocked

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.runPushWorker(ctx)

	start := time.Now()
	a.enqueuePush(validSpecPushNamed("spec-blocked-report-1", "camera-101"))
	a.enqueuePush(validSpecPushNamed("spec-blocked-report-2", "camera-102"))

	// Each spec emits CREATING then APPLIED: 4 bounded reports in total.
	deadline := time.After(2 * time.Second)
	for blocked.callCount() < 4 {
		select {
		case <-deadline:
			t.Fatalf("worker stalled on blocked reporter, calls=%d", blocked.callCount())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("four bounded reports took %v, worker appears blocked", elapsed)
	}
}

// TestWorkerSurvivesBadSpec ensures one malformed push never stalls the
// serial worker: the following valid push is still applied.
func TestWorkerSurvivesBadSpec(t *testing.T) {
	dev := &fakeDeviceClient{notFound: true}
	a, rec := newQueueApplier(t, dev, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.runPushWorker(ctx)

	bad := validSpecPush("spec-bad")
	bad.Config = json.RawMessage("{not-json")
	a.enqueuePush(bad)
	rec.waitFor(t, device.ConnectorStateFailed, "invalid edgex-device config")

	a.enqueuePush(validSpecPush("spec-good"))
	rec.waitFor(t, device.ConnectorStateApplied, "")

	if dev.addCalls != 1 {
		t.Errorf("metadata Add calls = %d, want 1 for the good spec", dev.addCalls)
	}
}

// TestEnqueueDropsWhenQueueFull verifies the bounded queue never blocks the
// downlink callback when the worker is stalled.
func TestEnqueueDropsWhenQueueFull(t *testing.T) {
	dev := &fakeDeviceClient{hung: true}
	a, _ := newQueueApplier(t, dev, nil, nil)

	for i := 0; i < pushQueueCapacity; i++ {
		a.pushQ <- validSpecPush("spec-fill")
	}
	done := make(chan struct{})
	go func() {
		a.enqueuePush(validSpecPush("spec-overflow"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueuePush blocked on a full queue")
	}
}

// TestDeleteFrameWithoutTypeStillDetaches guards the frame routing fix:
// delete frames omit connector_type and must bypass the type registry yet
// still delete the EdgeX device and fire the detach hook.
func TestDeleteFrameWithoutTypeStillDetaches(t *testing.T) {
	dev := &fakeDeviceClient{notFound: true}
	detachCh := make(chan string, 4)
	a, rec := newQueueApplier(t, dev, nil, func(edgexName, _ string) {
		detachCh <- edgexName
	})

	a.dispatchPush(context.Background(), validSpecPush("spec-del"))
	rec.waitFor(t, device.ConnectorStateApplied, "")

	deletePush := device.ConnectorPush{
		Kind:     device.ConnectorKindDelete,
		SpecID:   "spec-del",
		DeviceID: "07704cf9-f0fc-4876-8955-64e6141540c7",
		Revision: 2,
		// ConnectorType intentionally empty, as NL sends delete frames.
	}
	a.dispatchPush(context.Background(), deletePush)

	select {
	case name := <-detachCh:
		if name != "camera-109" {
			t.Fatalf("detach device = %q, want camera-109", name)
		}
	case <-time.After(time.Second):
		t.Fatal("detach hook not fired for delete frame")
	}
	if got := dev.deleteCount(); got != 1 {
		t.Errorf("DeleteDeviceByName calls = %d, want 1", got)
	}
}
