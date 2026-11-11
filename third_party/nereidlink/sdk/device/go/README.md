<!--
 Copyright (C) 2026 YIQISOFT
 SPDX-License-Identifier: Apache-2.0
-->

# NereidLink Device SDK for Go

Device and gateway (NereidEdge) firmware SDK for the NereidLink southbound
ingresses. It is a **thin protocol library**: typed frames, credentials,
topic bindings and MQTT/HTTP transport helpers. It owns no goroutines,
runs no loops and makes no policy decisions — firmware keeps full control
of poll cadence, reconnects, hello negotiation and manifest timing.

For the adapter SDK used to add a new southbound wire protocol *inside or
alongside the platform*, see [`sdk/`](../../README.md) instead.

## Packages

| Package | Purpose |
|---|---|
| `device` (root) | Credentials (token / id+secret / X.509), frame models for telemetry, commands, shadow, attributes, domain events, OTA, hello/manifest/connector/discovery/probe/link-episode, topic constants, canonical config hash |
| `devicehttp` | HTTP ingress client: telemetry, long-poll commands, command response, shadow, OTA list/range-download/progress |
| `devicemqtt` | MQTT ingress client on Eclipse Paho v1: connect, LWT, telemetry, attributes, domain events, command/shadow/attribute subscriptions, raw/JSON publish |
| `gateway` | Gateway surface on top of `devicemqtt`: hello, heartbeat/goodbye, link-episode reports, sub-devices, manifest, batched telemetry/attributes, relayed sub-device commands, connector, discovery, probe, attribute push, OTA, gateway operations (advertise/invoke/respond) |
| `examples/` | Direct device (`simple`) and gateway (`gateway`) reference programs |

## Install

```bash
go get github.com/nereidai/nereidlink/sdk/device/go@latest
```

The module is independent of the server module: it does not pull NATS,
PostgreSQL drivers or any other server dependency.

## Directly connected device

### MQTT

```go
client, _ := devicemqtt.Connect(ctx, devicemqtt.Options{
    BrokerURL:  "tcp://nereidlink.example:1883", // or tls://...:8883
    Credential: device.Token("device-access-token"),
    ClientID:   "sensor-001",
})
defer client.Disconnect(200)

_ = client.SubscribeCommands(ctx, 1, func(cmd device.Command) {
    // Downlink topic is v1/me/cmd/<name>; reply topic is v1/me/cmdres/<id>.
    _ = client.RespondCommand(ctx, cmd.ID, map[string]any{"ok": true}, 1)
})

_ = client.PublishTelemetry(ctx,
    device.NewFrame().SetFloat("temperature", 21.5).SetBool("online", true), 1)
```

Credentials: `device.Token(t)` (MQTT username=t, empty password),
`device.Basic(id, secret)` (username/password), `device.ClientCert(...)`
(mTLS). The MQTT client bypasses environment HTTP/SOCKS proxies because
field brokers are reached directly; TLS still verifies the server
certificate unless you explicitly opt out.

### HTTP

```go
httpClient, _ := devicehttp.New(devicehttp.Options{
    BaseURL:    "https://nereidlink.example",
    Credential: device.Token("device-access-token"),
})

_, _ = httpClient.PublishTelemetry(ctx, device.NewFrame().SetFloat("temperature", 21.5))

for {
    cmd, _ := httpClient.PollCommands(ctx) // one long poll; firmware loops
    if cmd != nil {
        _ = httpClient.RespondCommand(ctx, cmd.ID, map[string]any{"ok": true})
    }
}
```

Telemetry frames are flat JSON objects of numbers/booleans/strings; null
and nested objects/arrays are rejected locally before upload. One
Envelope is published per key server-side.

## OTA (direct device)

```go
list, _ := httpClient.ListOTAAssignments(ctx)
for _, job := range list.Assignments {
    var f *os.File // firmware decides where bytes go and verifies sha256
    res, _ := httpClient.DownloadOTAPackage(ctx, job.TaskID,
        devicehttp.DownloadOptions{Destination: f}) // Range resume supported
    _ = httpClient.ReportOTAProgress(ctx, job.TaskID,
        device.OTAProgress{State: device.OTAStateDownloading, ProgressPct: 50, Bytes: res.Written})
}
```

The SDK streams bytes but deliberately does not flash or verify firmware:
persist the stream, check `X-Firmware-Sha256` (returned as
`DownloadResult.SHA256`), then drive the ordered state machine
(`initiated → downloading → downloaded → verified → updating → updated
→ rebooting → confirmed/succeeded/failed`).

## Gateway

A gateway is a device with the gateway capability. It uses the device
topics and, after one hello per connection, the `v1/gateway/*` topics.

```go
mq, _ := devicemqtt.Connect(ctx, devicemqtt.Options{
    BrokerURL: *broker, Credential: device.Basic(id, secret),
    ClientID: edgeID, Will: devicemqtt.GoodbyeWill("transport_down"),
})
gw := gateway.Wrap(mq)

_ = gw.SubscribeConnectors(ctx, 0, func(push device.ConnectorPush) { /* apply */ })
_ = gw.SubscribeProbes(ctx, 0, func(req device.ProbeRequest) { /* answer per device */ })
_ = gw.SubscribeOTATasks(ctx, 0, func(task device.GatewayOTATask) { /* download external URL */ })

// Whole-gateway operations (ADR-0035): subscribe before hello, then
// advertise the catalog in Hello.Operations and answer invokes.
_ = gw.SubscribeOperations(ctx, 0, func(inv device.OperationInvoke) {
    // Execute only operations you advertised; answer within inv.TimeoutMs.
    _ = gw.RespondOperation(ctx, device.OperationResult{
        ID: inv.ID, OK: true, Result: json.RawMessage(`{"loss":0}`),
    }, 1)
})

ack, err := gw.NegotiateHello(ctx, device.Hello{
    EdgeID: edgeID, AgentVersion: "2.3.1",
    Proto:    device.ProtoRange{Min: "1.0", Max: "1.4"},
    Features: device.FeatureSet{Required: []string{device.FeatureTelemetry, device.FeaturePresence}},
    Operations: []device.OperationAdvert{{
        Operation:   "diag.ping",
        TypeVersion: "1",
        DisplayName: map[string]string{"en": "Ping host", "zh": "Ping probe"},
        Interaction: device.OperationInteractionRequestResponse,
        TimeoutMs:   10000,
        Risk:        device.OperationRiskRead,
        ParamSchema: json.RawMessage(`{"type":"object","required":["host"],` +
            `"properties":{"host":{"type":"string"},"count":{"type":"integer"}}}`),
    }},
}, 10*time.Second)

// Check each advertised entry; a rejected entry never fails the handshake.
for _, r := range ack.Operations {
    if !r.Accepted {
        log.Printf("operation %s rejected: %s", r.Operation, r.Reason)
    }
}

_ = gw.ConnectDevices(ctx, []string{subDeviceUUID}, 1)
_ = gw.PublishTelemetry(ctx, device.GatewayFrame{
    DeviceID: subDeviceUUID, TS: time.Now().UnixMilli(),
    Measurements: device.NewFrame().SetFloat("temperature", 21.5),
})
// gw.Heartbeat(...) on ack.Limits.HeartbeatS; gw.SendManifest(...) every 5 min.
```

Full working program: [`examples/gateway`](examples/gateway/main.go).

### Gateway responsibilities (not automated)

- Re-run hello after every reconnect (server state resets to
  UNIDENTIFIED; frames sent before ack are discarded).
- Heartbeat on the negotiated interval; the LWT fires automatically on
  crash, publish a planned `Goodbye` on graceful shutdown.
- Run a local link watchdog and report each unplanned down/up interval
  with `ReportLinkEpisode` (`reason: "transport_down"`); subscribe the ack
  once and stop retrying on `accepted`/`duplicate`. Backlog replay after a
  restart is just oldest-first QoS1 reports; the platform de-duplicates by
  `(edge_id, down_at)`.
- Full-state `Manifest` at the wall-clock aligned cadence; include the
  connectors segment explicitly (`&[]`) when reporting zero connectors.
- De-duplicate connector pushes by `(spec_id, revision)`; the full desired
  set is re-sent after every hello.
- Use `device.CanonicalConfigHash` for the manifest `config_hash` (compact,
  key-sorted `{"config":...,"mappings":...}` sha256).

## Gateway operations (ADR-0035)

Whole-gateway, menu-driven RPC (ping/reboot/…), distinct from sub-device
commands. The platform ships **no operation names**: the firmware declares
the catalog in hello and executes only what it advertised.

- **Advertise** with `Hello.Operations []device.OperationAdvert`
  (`operation`, `type_version`, bilingual `display_name`/`description`,
  `interaction` `request-response`/`one-way`, `timeout_ms` 100..60000,
  `risk` `read`/`normal`/`dangerous`, self-contained inline
  `param_schema`/`result_schema` — no external `$ref`/URL).
- The hello **ack** reports each entry in
  `ack.Operations []device.OperationAdvertResult` (`accepted`, `reason`);
  one bad entry never fails the handshake. Accepted descriptors are stored
  **per gateway** by `(gateway, operation, type_version)`: two gateways
  advertising the same key never overwrite each other, and an invocation is
  validated against the target gateway's own descriptor.
- `Hello.Operations` is the gateway's **full current set** on every hello;
  keys absent from a later hello are reconciled away. Omit the field (nil)
  when firmware does not speak operations; an explicit empty slice clears
  it. When one gateway advertises several versions of an operation, the
  platform invokes the newest one.
- Same key + `type_version` is a contract: keep params/result/risk identical
  across gateways. Use `diag.*`/`sys.*`/`service.*` for common actions and
  `vendor.<vendor>.*` for vendor ones; a breaking change needs a new version
  or key (e.g. `diag.ping.v2`).
- **Execute** via `gw.SubscribeOperations(ctx, qos, func(device.OperationInvoke))`
  (topic `v1/gateway/operations/invoke`) and answer request-response ops
  with `gw.RespondOperation(ctx, device.OperationResult{ID, OK, Result|Error}, qos)`
  on `v1/gateway/operations/result`, within `inv.TimeoutMs`. For `one-way`
  operations send no result.
- `params` arrive already validated against `param_schema`; `secret://`
  references are resolved to plaintext at the last mile (never log/persist).
  The result JSON must stay under 64KB and must never contain secrets — the
  platform does not inspect result content.
- Risk (`read`/`normal`/`dangerous`) and the human-only/TOTP gates are
  platform-side REST concerns; they do not affect MQTT advertising or
  invoke/result. A late/duplicate/unknown-id result is dropped server-side.

There is no feature flag: operations ride the gateway capability. Full
sample: [`examples/gateway`](examples/gateway/main.go).

## Protocol notes / gaps

The SDK follows implemented server behavior:

- Gateway-relayed commands for attached sub-devices are supported:
  downlink `v1/gateway/command` (frame fields `command_id`/`device`/
  `interaction`/`timeliness`/`method`/`params`) and uplink
  `v1/gateway/command/response`; use `gateway.SubscribeCommands` /
  `gateway.RespondCommand`. Persistent offline queueing through a gateway
  is platform behavior; the edge only sees live frames.
- Attributes: direct devices report CLIENT scope on `v1/me/attributes`
  and receive SERVER/SHARED pushes on `v1/me/attributes/down`; gateways
  use `v1/gateway/attributes` (`{device:{key:value}}`) and receive
  `v1/gateway/attributes/push`.
- Domain events: direct devices publish a single event object or an
  array on `v1/me/events` (`event_type`/`severity`/`ts_ms`/`payload`).
- The blob-upload relay (edge -> platform object storage) is still
  deferred and not exposed. Gateway-published domain events are also
  not implemented server-side.
- Hello `resume.last_cursor` is accepted by the server but no cursor
  replay exists; desired state is full-state and idempotently re-driven.
- Downlink MQTT frames are QoS 0, non-retained; uplink QoS is chosen by
  firmware (QoS 1 recommended).

## Development

```bash
cd sdk/device/go
go test ./...      # unit + in-process mochi-mqtt integration tests
go vet ./...
```
