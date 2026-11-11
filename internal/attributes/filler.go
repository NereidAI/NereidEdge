// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package attributes materializes the platform-managed (SERVER/SHARED) device
// attributes NereidLink holds for the gateway's own identity and for every
// attached sub-device.
//
// The edge keeps a local cache per scope: pushes arriving on
// v1/me/attributes/down (gateway self) and v1/gateway/attributes/push
// (sub-device) carry only the changed key, and a JSON null value means the key
// was deleted platform-side and must be removed locally. After every (re)connect
// the edge pulls the full per-scope state once (v1/me/attributes/get and the
// gateway-proxied v1/gateway/attributes/get) to recover pushes missed while
// offline; the reply topic is /get/ack and replaces the scope wholesale.
//
// For sub-devices the effective cache is mirrored into the EdgeX device Tags so
// the local device definition reflects what the platform holds; the gateway's
// own attributes have no EdgeX device and stay in the cache.
package attributes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/requests"
	edgexErrors "github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	"github.com/nereidai/nereidedge/internal/binding"
	"github.com/nereidai/nereidlink/sdk/device/go"
	"github.com/nereidai/nereidlink/sdk/device/go/devicemqtt"
	nlgateway "github.com/nereidai/nereidlink/sdk/device/go/gateway"
)

// requestRateLimit spaces consecutive platform-managed attribute pulls so a
// gateway with many attached sub-devices does not burst the broker on every
// (re)connect.
const requestRateLimit = 200 * time.Millisecond

// requestSweepTimeout bounds one full pull sweep (gateway self plus every
// attached sub-device). The sweep runs detached from the caller's context so a
// reconnect replay that cancels its own deadline does not abort it.
const requestSweepTimeout = 2 * time.Minute

// Filler owns the attribute cache, the downlink subscriptions and the
// on-connect pull for the gateway itself and its attached sub-devices.
type Filler struct {
	// mq is the direct-device (v1/me) MQTT surface used for the gateway's own
	// identity; gw is the gateway (v1/gateway) surface used for sub-devices.
	mq       *devicemqtt.Client
	gw       *nlgateway.Client
	resolver *binding.Resolver
	svc      interfaces.ApplicationService
	lc       logger.LoggingClient

	// attached returns the sub-device UUIDs currently attached to this
	// connection; only those may be pulled (the platform rejects the rest).
	attached func() []string

	cache *attrCache

	// edgexMu serializes the read-modify-write of an EdgeX device's Tags so
	// concurrent sub-device pushes cannot lose one another's update.
	edgexMu sync.Mutex
	// applied is the effective attribute map last written to EdgeX per device,
	// used to diff the next write and delete keys removed platform-side.
	applied map[string]map[string]any

	// reqMu guards the request-sweep coalescing flag.
	reqMu   sync.Mutex
	reqBusy bool

	// onSelf, when set, is invoked with the gateway's own per-scope cache
	// after every self change. It lets the semantic applier reconcile the
	// well-known gateway-scope attributes (log.level, forward.unmapped, ...)
	// without this package knowing what any of them mean.
	onSelf func(self map[string]map[string]any)
}

// SetSelfObserver registers a callback for gateway-own attribute changes. The
// callback receives a snapshot of the SERVER_SCOPE/SHARED_SCOPE maps and runs
// on the calling (paho/downlink) goroutine, so it must be quick and must not
// publish.
func (f *Filler) SetSelfObserver(observer func(self map[string]map[string]any)) {
	f.onSelf = observer
}

// ReapplySelf re-emits the current gateway-own cache to the observer. It is
// called once the semantic applier is wired so attributes pulled during the
// post-hello sweep — which can arrive before the observer exists — are still
// applied.
func (f *Filler) ReapplySelf() {
	f.emitSelf()
}

// emitSelf invokes the self observer with a deep copy of the current
// gateway-own cache. The copy keeps the callback from holding the cache lock
// or observing a concurrent mutation.
func (f *Filler) emitSelf() {
	if f.onSelf == nil {
		return
	}
	f.onSelf(f.cache.snapshotSelf())
}

// New creates a Filler. attached may be nil, in which case no sub-device pull
// is issued.
func New(
	mq *devicemqtt.Client,
	gw *nlgateway.Client,
	resolver *binding.Resolver,
	svc interfaces.ApplicationService,
	attached func() []string,
	lc logger.LoggingClient,
) *Filler {
	return &Filler{
		mq:       mq,
		gw:       gw,
		resolver: resolver,
		svc:      svc,
		lc:       lc,
		attached: attached,
		cache:    newAttrCache(),
		applied:  make(map[string]map[string]any),
	}
}

// Start registers the four attribute subscriptions. It must run before hello
// and be re-issued after every reconnect: the SDK uses a clean session, so a
// transport reconnect drops every broker-side subscription and paho does not
// restore them.
func (f *Filler) Start(ctx context.Context, qos byte) error {
	if err := f.mq.SubscribeAttributesDown(ctx, qos, f.onSelfPush); err != nil {
		return err
	}
	if err := f.mq.SubscribeAttributesAck(ctx, qos, f.onSelfAck); err != nil {
		return err
	}
	if err := f.gw.SubscribeAttributePushes(ctx, qos, f.onSubPush); err != nil {
		return err
	}
	return f.gw.SubscribeAttributeAcks(ctx, qos, f.onSubAck)
}

// RequestAttributes issues one pull for the gateway's own identity followed by
// one per attached sub-device, spaced by requestRateLimit. It runs on its own
// goroutine and coalesces overlapping calls (a burst of reconnects triggers a
// single sweep).
func (f *Filler) RequestAttributes() {
	f.reqMu.Lock()
	if f.reqBusy {
		f.reqMu.Unlock()
		f.lc.Debugf("attribute pull already in progress; coalescing")
		return
	}
	f.reqBusy = true
	f.reqMu.Unlock()

	go func() {
		defer func() {
			f.reqMu.Lock()
			f.reqBusy = false
			f.reqMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), requestSweepTimeout)
		defer cancel()
		scopes := []string{device.AttributeScopeServer, device.AttributeScopeShared}
		if err := f.mq.RequestAttributes(ctx, scopes, 1); err != nil {
			f.lc.Errorf("pull gateway attributes failed: %s", err.Error())
		}
		if f.attached == nil {
			return
		}
		for _, dev := range f.attached() {
			if ctx.Err() != nil {
				return
			}
			if err := f.gw.RequestAttributes(ctx, dev, scopes, 1); err != nil {
				f.lc.Errorf("pull attributes for sub-device %s failed: %s", dev, err.Error())
			}
			if !sleepCtx(ctx, requestRateLimit) {
				return
			}
		}
	}()
}

// RequestDevice pulls one sub-device's platform-managed attributes. It is used
// when a sub-device attaches after the initial post-hello sweep, so its cache
// is populated even though only changed keys are pushed thereafter.
func (f *Filler) RequestDevice(nlUUID string) {
	if nlUUID == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), requestSweepTimeout)
		defer cancel()
		scopes := []string{device.AttributeScopeServer, device.AttributeScopeShared}
		if err := f.gw.RequestAttributes(ctx, nlUUID, scopes, 1); err != nil {
			f.lc.Errorf("pull attributes for sub-device %s failed: %s", nlUUID, err.Error())
		}
	}()
}

// onSelfPush applies one SERVER/SHARED change for the gateway's own identity.
func (f *Filler) onSelfPush(push device.AttributesDown) {
	if !f.validScope(push.Scope, "gateway") {
		return
	}
	f.cache.applySelf(push.Scope, push.Data)
	f.emitSelf()
}

// onSelfAck replaces the gateway's own per-scope cache from a pull reply.
func (f *Filler) onSelfAck(ack device.AttributesGetAck) {
	for scope, data := range ack.Scopes {
		if !f.validScope(scope, "gateway") {
			continue
		}
		f.cache.replaceSelf(scope, data)
	}
	f.emitSelf()
}

// onSubPush applies one SERVER/SHARED change for an attached sub-device and
// mirrors the effective state into its EdgeX device. The cache update is
// synchronous (it must preserve push order); the EdgeX metadata call is
// offloaded because the paho callback goroutine is shared with every other
// downlink handler.
func (f *Filler) onSubPush(push device.GatewayAttributesPush) {
	if push.Device == "" {
		f.lc.Debugf("skipping attribute push: missing device")
		return
	}
	if !f.validScope(push.Scope, push.Device) {
		return
	}
	f.cache.applySub(push.Device, push.Scope, push.Data)
	go f.syncDevice(push.Device)
}

// onSubAck replaces one sub-device's per-scope cache from a pull reply.
func (f *Filler) onSubAck(ack device.GatewayAttributesGetAck) {
	if ack.Device == "" {
		return
	}
	for scope, data := range ack.Scopes {
		if !f.validScope(scope, ack.Device) {
			continue
		}
		f.cache.replaceSub(ack.Device, scope, data)
	}
	go f.syncDevice(ack.Device)
}

// validScope reports whether scope is a platform-managed scope. CLIENT is
// device-owned: the platform rejects a pull for it and never pushes it, so a
// frame carrying it is a protocol violation the edge logs and drops.
func (f *Filler) validScope(scope, subject string) bool {
	switch scope {
	case device.AttributeScopeServer, device.AttributeScopeShared:
		return true
	case device.AttributeScopeClient:
		f.lc.Warnf("ignoring CLIENT-scope platform attribute for %s", subject)
	default:
		f.lc.Warnf("ignoring unknown attribute scope %q for %s", scope, subject)
	}
	return false
}

// syncDevice mirrors a sub-device's effective attribute cache into its EdgeX
// device Tags, deleting keys removed platform-side.
func (f *Filler) syncDevice(nlUUID string) {
	edgexName, ok := f.resolver.Name(nlUUID)
	if !ok {
		f.lc.Debugf("attribute sync skipped for unmapped NL device %s", nlUUID)
		return
	}

	f.edgexMu.Lock()
	defer f.edgexMu.Unlock()

	eff := f.cache.subEffective(nlUUID)
	prev := f.applied[nlUUID]

	var upsert map[string]any
	for k, v := range eff {
		if old, seen := prev[k]; !seen || !reflect.DeepEqual(old, v) {
			if upsert == nil {
				upsert = make(map[string]any)
			}
			upsert[k] = v
		}
	}
	var remove []string
	for k := range prev {
		if _, still := eff[k]; !still {
			remove = append(remove, k)
		}
	}
	if len(upsert) == 0 && len(remove) == 0 {
		return
	}
	if err := f.updateDeviceAttributes(context.Background(), edgexName, upsert, remove); err != nil {
		f.lc.Errorf("failed to update attributes for device %s: %s", edgexName, err.Error())
		return
	}
	f.applied[nlUUID] = eff
	f.lc.Debugf("synced attributes to device %s: upsert=%d remove=%d", edgexName, len(upsert), len(remove))
}

// updateDeviceAttributes merges upsert keys into and removes remove keys from
// the EdgeX device Tags, preserving every value the edge did not manage.
func (f *Filler) updateDeviceAttributes(ctx context.Context, deviceName string, upsert map[string]any, remove []string) error {
	devResp, err := f.svc.DeviceClient().DeviceByName(ctx, deviceName)
	if err != nil {
		if edgexErrors.Kind(err) == edgexErrors.KindEntityDoesNotExist {
			return fmt.Errorf("device not found: %s", deviceName)
		}
		return err
	}
	dev := devResp.Device
	if dev.Tags == nil {
		dev.Tags = make(map[string]any)
	}
	for k, v := range upsert {
		dev.Tags[k] = v
	}
	for _, k := range remove {
		delete(dev.Tags, k)
	}
	name := dev.Name
	adminState := dev.AdminState
	operatingState := dev.OperatingState
	patch := dtos.UpdateDevice{
		Name:           &name,
		Description:    &dev.Description,
		ServiceName:    &dev.ServiceName,
		ProfileName:    &dev.ProfileName,
		Labels:         dev.Labels,
		AdminState:     &adminState,
		OperatingState: &operatingState,
		Protocols:      dev.Protocols,
		AutoEvents:     dev.AutoEvents,
		Tags:           dev.Tags,
		Properties:     dev.Properties,
		Location:       dev.Location,
	}
	req := requests.UpdateDeviceRequest{
		BaseRequest: dtoCommon.NewBaseRequest(),
		Device:      patch,
	}
	_, err = f.svc.DeviceClient().Update(ctx, []requests.UpdateDeviceRequest{req})
	return err
}

// sleepCtx sleeps for d, returning false if ctx is canceled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// attrCache is the in-memory platform-managed attribute state: one map per
// scope for the gateway's own identity, and device -> scope -> keys for
// sub-devices.
type attrCache struct {
	mu   sync.RWMutex
	self map[string]map[string]any
	subs map[string]map[string]map[string]any
}

func newAttrCache() *attrCache {
	return &attrCache{
		self: make(map[string]map[string]any),
		subs: make(map[string]map[string]map[string]any),
	}
}

func (c *attrCache) applySelf(scope string, data map[string]json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.self[scope] == nil {
		c.self[scope] = make(map[string]any)
	}
	apply(c.self[scope], data)
}

func (c *attrCache) replaceSelf(scope string, data map[string]json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.self[scope] = decodeScope(data)
}

// snapshotSelf deep-copies the gateway-own per-scope cache so the observer can
// read it without holding the lock.
func (c *attrCache) snapshotSelf() map[string]map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]map[string]any, len(c.self))
	for scope, kv := range c.self {
		cp := make(map[string]any, len(kv))
		for k, v := range kv {
			cp[k] = v
		}
		out[scope] = cp
	}
	return out
}

func (c *attrCache) applySub(device, scope string, data map[string]json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subs[device] == nil {
		c.subs[device] = make(map[string]map[string]any)
	}
	if c.subs[device][scope] == nil {
		c.subs[device][scope] = make(map[string]any)
	}
	apply(c.subs[device][scope], data)
}

func (c *attrCache) replaceSub(device, scope string, data map[string]json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subs[device] == nil {
		c.subs[device] = make(map[string]map[string]any)
	}
	c.subs[device][scope] = decodeScope(data)
}

// subEffective flattens a sub-device's platform scopes into one map for the
// EdgeX tag write. SERVER_SCOPE is applied first, then SHARED_SCOPE, so a key
// present in both resolves to its shared value.
func (c *attrCache) subEffective(nlUUID string) map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]any)
	scopes := c.subs[nlUUID]
	for _, scope := range []string{device.AttributeScopeServer, device.AttributeScopeShared} {
		for k, v := range scopes[scope] {
			out[k] = v
		}
	}
	return out
}

// apply merges one change into dst: a JSON null deletes the key, any other
// value upserts it. Only the changed keys are touched, so a push never clears
// unrelated keys.
func apply(dst map[string]any, data map[string]json.RawMessage) {
	for k, raw := range data {
		if isNull(raw) {
			delete(dst, k)
			continue
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		dst[k] = v
	}
}

// decodeScope decodes a full per-scope key/value set returned by a get/ack,
// dropping null entries (a null carries no value to store).
func decodeScope(data map[string]json.RawMessage) map[string]any {
	out := make(map[string]any, len(data))
	for k, raw := range data {
		if isNull(raw) {
			continue
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		out[k] = v
	}
	return out
}

// isNull reports whether a raw JSON value is the null literal.
func isNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
