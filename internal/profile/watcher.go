// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

// Defaults for the EdgeX message bus watcher. EdgeX publishes core-metadata
// lifecycle notifications on its internal bus (MQTT in the reference
// deployment) under the base topic prefix "edgex".
const (
	defaultWatchType      = "mqtt"
	defaultWatchProtocol  = "tcp"
	defaultWatchHost      = "edgex-mqtt-broker"
	defaultWatchPort      = 1883
	defaultWatchBaseTopic = "edgex"

	// watchClientID must differ from the app-service MessageBus ClientId so
	// the two MQTT connections to the EdgeX broker do not evict each other.
	watchClientID = "nereid-edge-profile-watch"

	// deviceProfileEventTopic is the wildcard topic for core-metadata device
	// profile lifecycle events. The concrete published topic is
	// <base>/system-events/core-metadata/deviceprofile/<action>/<owner>/<name>.
	deviceProfileEventTopic = "system-events/core-metadata/deviceprofile/#"

	watchPublishTimeout = 10 * time.Second
	watchReconnectMin   = time.Second
	watchReconnectMax   = 30 * time.Second
)

// WatchConfig describes how to reach the EdgeX message bus so device profile
// change notifications can be consumed the moment they happen.
type WatchConfig struct {
	// Type is the message bus implementation. Empty selects "mqtt".
	Type string
	// Protocol is the broker protocol. Empty selects "tcp".
	Protocol string
	// Host is the broker host. Empty selects "edgex-mqtt-broker".
	Host string
	// Port is the broker port. Zero selects 1883.
	Port int
	// BaseTopic is the EdgeX base topic prefix. Empty selects "edgex".
	BaseTopic string
}

// withDefaults fills unset fields with the EdgeX reference defaults.
func (c WatchConfig) withDefaults() WatchConfig {
	if c.Type == "" {
		c.Type = defaultWatchType
	}
	if c.Protocol == "" {
		c.Protocol = defaultWatchProtocol
	}
	if c.Host == "" {
		c.Host = defaultWatchHost
	}
	if c.Port == 0 {
		c.Port = defaultWatchPort
	}
	if c.BaseTopic == "" {
		c.BaseTopic = defaultWatchBaseTopic
	}
	return c
}

// Watcher subscribes to EdgeX core-metadata device profile lifecycle system
// events and pushes each added or changed profile to NereidLink immediately.
// It complements the periodic full scan rather than replacing it: if the bus
// is unreachable the scan still converges, and content-hash de-duplication
// keeps the two paths from double publishing.
type Watcher struct {
	syncer *Syncer
	busCfg types.MessageBusConfig
	topic  string
	log    logger.LoggingClient
}

// NewWatcher builds a watcher for the given EdgeX message bus. The syncer
// publishes the profiles it observes to NereidLink.
func NewWatcher(cfg WatchConfig, syncer *Syncer, log logger.LoggingClient) (*Watcher, error) {
	if syncer == nil {
		return nil, fmt.Errorf("profile watcher requires a syncer")
	}
	cfg = cfg.withDefaults()
	return &Watcher{
		syncer: syncer,
		busCfg: types.MessageBusConfig{
			Broker: types.HostInfo{Host: cfg.Host, Port: cfg.Port, Protocol: cfg.Protocol},
			Type:   cfg.Type,
			// A distinct client id and auto-reconnect keep this side
			// connection independent of the app-service bus session.
			Optional: map[string]string{
				"ClientId":      watchClientID,
				"AutoReconnect": "true",
				"KeepAlive":     "30",
			},
		},
		topic: strings.Join([]string{cfg.BaseTopic, deviceProfileEventTopic}, "/"),
		log:   log,
	}, nil
}

// Start connects and consumes device profile events until ctx is cancelled,
// rebuilding the connection with bounded backoff after a transport failure.
func (w *Watcher) Start(ctx context.Context) {
	if w == nil {
		return
	}
	backoff := watchReconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		client, err := messaging.NewMessageClient(w.busCfg)
		if err == nil {
			err = client.Connect()
		}
		if err != nil {
			w.log.Warnf("profile watcher connect to EdgeX message bus failed: %v (retry in %s)", err, backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		w.log.Infof("profile watcher listening on '%s'", w.topic)
		backoff = watchReconnectMin
		err = w.consume(ctx, client)
		_ = client.Disconnect()
		if ctx.Err() != nil {
			return
		}
		w.log.Warnf("profile watcher subscription ended: %v (retry in %s)", err, backoff)
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// consume runs the receive loop for one connected client.
func (w *Watcher) consume(ctx context.Context, client messaging.MessageClient) error {
	messages := make(chan types.MessageEnvelope)
	errs := make(chan error)
	if err := client.Subscribe([]types.TopicChannel{{Topic: w.topic, Messages: messages}}, errs); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err, ok := <-errs:
			if !ok {
				return fmt.Errorf("message error channel closed")
			}
			w.log.Errorf("profile watcher bus error: %v", err)
		case msg, ok := <-messages:
			if !ok {
				return fmt.Errorf("message channel closed")
			}
			w.handle(ctx, msg)
		}
	}
}

// handle processes one system event envelope, publishing the profile when it
// is a device-profile add or update.
func (w *Watcher) handle(ctx context.Context, env types.MessageEnvelope) {
	prof, ok, err := profileFromSystemEvent(env.Payload)
	if err != nil {
		w.log.Warnf("profile watcher: undecodable system event on '%s': %v", env.ReceivedTopic, err)
		return
	}
	if !ok {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, watchPublishTimeout)
	defer cancel()
	if err := w.syncer.SyncDTO(pctx, prof); err != nil {
		w.log.Warnf("profile watcher: publish profile %q failed: %v", prof.Name, err)
		return
	}
	w.log.Infof("profile watcher published profile %q", prof.Name)
}

// profileFromSystemEvent extracts a device profile from a core-metadata
// system event payload. It reports ok=false for any event that is not a
// device-profile add or update notification, so unrelated events on the same
// topic subtree are ignored. A device-profile delete is intentionally not an
// error: NereidLink has no descriptor-delete uplink, so the profile is left
// for the next full inventory scan to reconcile.
func profileFromSystemEvent(payload any) (dtos.DeviceProfile, bool, error) {
	var evt dtos.SystemEvent
	if err := decodeSystemEvent(payload, &evt); err != nil {
		return dtos.DeviceProfile{}, false, err
	}
	if evt.Type != common.DeviceProfileSystemEventType {
		return dtos.DeviceProfile{}, false, nil
	}
	switch evt.Action {
	case common.SystemEventActionAdd, common.SystemEventActionUpdate:
	default:
		return dtos.DeviceProfile{}, false, nil
	}
	var prof dtos.DeviceProfile
	if err := evt.DecodeDetails(&prof); err != nil {
		return dtos.DeviceProfile{}, false, err
	}
	return prof, true, nil
}

// decodeSystemEvent unmarshals a message-bus payload into a SystemEvent.
// The payload type depends on how the bus client decoded the envelope (raw
// bytes for JSON payloads, a map for already-decoded values), so each shape
// is normalised to JSON before decoding.
func decodeSystemEvent(payload any, out *dtos.SystemEvent) error {
	switch v := payload.(type) {
	case []byte:
		return json.Unmarshal(v, out)
	case json.RawMessage:
		return json.Unmarshal(v, out)
	case string:
		return json.Unmarshal([]byte(v), out)
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, out)
	}
}

// sleepCtx sleeps for d unless ctx is cancelled first, reporting whether the
// full sleep elapsed.
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

// nextBackoff doubles cur up to watchReconnectMax.
func nextBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > watchReconnectMax {
		return watchReconnectMax
	}
	return next
}
