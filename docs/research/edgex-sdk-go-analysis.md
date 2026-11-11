<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# EdgeX app-functions-sdk-go v4 Source Code Analysis

- Date: 2026-10-04
- Target: `app-functions-sdk-go` in a local workspace scratch directory (not committed) (shallow clone of main, HEAD 35e0c4d, 2026-09-02; stable tag v4.0.2, main is v4.1.0-dev)
- License: Apache-2.0; module `github.com/edgexfoundry/app-functions-sdk-go/v4`

## 1. Versions and Dependencies

- `go.mod`: Go 1.25.0; core dependencies: go-mod-bootstrap/v4 v4.1.0-dev.75, go-mod-core-contracts/v4 v4.1.0-dev.40, go-mod-messaging/v4 v4.1.0-dev.30, go-mod-registry/v4 v4.1.0-dev.14.
- Production builds should pin the stable tag **v4.0.2** (corresponding to EdgeX 4.0.2 Odessa, 2026-05): the template's own go.mod already references v4.0.2 (`app-service-template/go.mod`), with a local replace to `../`.
- MQTT client: paho v1.5.1 (`go.mod:8`, the same library and major version as NereidLink); S&F databases: pgx v5 and redigo; echo v4.15.4; nats.go v1.50.0 is pulled in indirectly via go-mod-messaging.
- Version numbers are injected at build time: `internal/constant.go:21-25` (SDKVersion/ApplicationVersion default to 0.0.0, via -ldflags).
- CI: Jenkinsfile, Go 1.25; the SDK repository itself does not produce a container image (`buildImage: false`); lint via golangci-lint v2.5.0; `make test-sdk` = `go test -race ./...`.

## 2. Programming Model

Entry points: `pkg.NewAppService(serviceKey)` / `NewAppServiceWithTargetType` (`pkg/factory.go:34-48`), which call Initialize internally. The public interface is defined in `pkg/interfaces/service.go:88-198`.

The official skeleton lives in `app-service-template/main.go` (the only in-repo example; the legacy example/ directory has been removed; online documentation is at docs.edgexfoundry.org):

```go
service, ok := pkg.NewAppService("my-app-service")          // main.go:53
// default pipeline
service.SetDefaultFunctionsPipeline(
    transforms.NewFilterFor(names).FilterByDeviceName,
    myCustomTransform,
    sender.MQTTSend,
)
// per-topic pipeline (wildcard +/#)
service.AddFunctionsPipelineForTopics("Floats",
    []string{"events/device/device-virtual/+/Random-Float-Device/#"}, fns...)
// custom REST route (echo.HandlerFunc, can be marked Unauthenticated)
service.AddCustomRoute("/api/v4/hello", interfaces.Unauthenticated, handler, http.MethodGet)
service.Run()
```

- Custom function signature: `type AppFunction func(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{})`; example at `app-service-template/functions/sample.go:53`.
- The default target type is `dtos.Event`, with adaptive JSON/CBOR decoding (`internal/runtime/runtime.go:235-297`); raw triggers use `[]byte`; metrics use `dtos.Metric`.
- The trigger is determined by **configuration** `Trigger.Type`, not by a code API.

### Trigger (`internal/app/triggerfactory.go:32-74`)

| Type | Constant/File | Description |
|---|---|---|
| EDGEX-MESSAGEBUS | `triggerfactory.go:34`, `internal/trigger/messagebus/messaging.go:52` | MessageClient injected by bootstrap; subscribed topics automatically get the `edgex/` prefix; SetResponseData content is sent back to PublishTopic |
| EXTERNAL-MQTT | `triggerfactory.go:35`, `internal/trigger/mqtt/mqtt.go:55` | paho connects directly to an external broker; **BackgroundPublisher is not supported** (`mqtt.go:86-88`) |
| HTTP | `triggerfactory.go:36`, `internal/trigger/http/rest.go:61` | Fixed `POST /api/v4/trigger` |
| custom | `RegisterCustomTriggerFactory` (`triggerfactory.go:77-106`) | The Trigger interface only requires Initialize; the framework injects TriggerConfig (including ConfigLoader) |

## 3. Built-in transforms (pkg/transforms/)

| File | Constructor | Pipeline Function |
|---|---|---|
| filter.go | NewFilterFor/NewFilterOut | FilterByProfileName/DeviceName/SourceName/ResourceName (supports regular expressions) |
| conversion.go | NewConversion | TransformToXML/TransformToJSON |
| compression.go | NewCompression | CompressWithGZIP/ZLIB (base64) |
| aesprotection.go | NewAESProtection(secretName, key) | Encrypt (the key can come from the secret store) |
| http.go | NewHTTPSender (WithSecretHeader/WithOptions) | HTTPPost/HTTPPut; custom headers, persistOnError, multi-target chains |
| **mqttsecret.go** | **NewMQTTSecretSender / WithTopicFormatter** | **MQTTSend** |
| batch.go | NewBatchByTime/Count/TimeAndCount | Batch |
| tags.go | NewTags | AddTags |
| event.go | NewEventWrapper{Simple,Binary,Object}Reading | Wrap (wraps payloads back into an Event) |
| jsonlogic.go | NewJSONLogic | Evaluate |
| metrics.go | NewMetricsProcessor | ToLineProtocol |
| responsedata.go | NewResponseData | SetResponseData |

**There is no NATS export, gRPC export, or AWS/Azure/GCP export** (zero matches across the repository; cloud export was removed before v2).

### Key Capabilities

- Multiple per-topic pipelines: `service.go:425-439`; v4.0.2 adds runtime topic changes via `SetFunctionsPipelineTopics`.
- BackgroundPublisher: `AddBackgroundPublisher(WithTopic)` (`service.go:151-176`); it only publishes to the EdgeX MessageBus and is unavailable with the http/external-mqtt triggers.
- Store & Forward: `Writable.StoreAndForward{Enabled,RetryInterval,MaxRetryCount}`; **v4 supports redisdb and postgres, with postgres as the default** (`internal/store/db/db.go:24-27`, `store/db/postgres/embed/sql/`); credentials are obtained from SecretProvider; on export-function failure call `ctx.SetRetryData`, and on success `ctx.TriggerRetryFailedData`; `RegisterCustomStoreFactory` is available.

## 4. Message Bus (go-mod-messaging/v4)

The factory only supports `mqtt` / `nats-core` / `nats-jetstream`; **Redis Streams and ZeroMQ have been removed**. In EdgeX 4.x compose, the default edgex-broker is NATS Core. NATS Optional supports QueueGroup, ExactlyOnce, JetStream Durable/AutoProvision, and more. `MessageBus.Disabled: true` turns off the message bus client entirely.

## 5. MQTT Export (the Core Integration Point with NereidLink)

`pkg/transforms/mqttsecret.go`:

- `MQTTSecretConfig` (:57-87): BrokerAddress (`tcp://host:1883` / `mqtts://...`), ClientId, SecretName, Topic, QoS, Retain, AuthMode, AutoReconnect, KeepAlive, ConnectTimeout, PublishTimeout, MaxReconnectInterval, SkipCertVerify, **Will**.
- `MQTTSend` (:249-328): lazy initialization/connection; topic placeholders are replaced from the ctx values DeviceName/ProfileName/SourceName/ReceivedTopic (fixed topics are passed through unchanged); 30s publish timeout; failures trigger SetRetryData and are re-sent after recovery; byte/error metrics are exported automatically.
- TLS/Auth in `pkg/secure/mqttfactory.go`: four modes, **none / usernamepassword / clientcert / cacert**; credentials come from SecretProvider (Vault or InsecureSecrets); hot-reload of the CA pool and secrets rebuilds the connection.
- `ConnectToBroker(lc, sp, retryCount, retryInterval)` (:344-377, pre-connect added in v4.0), `SetOnConnectHandler` (:380).
- Data handoff: upstream `[]byte`/string/json.Marshaler values are directly usable via `util.CoerceType`.

There is no built-in support for custom NATS/gRPC export; the pattern is a custom AppFunction struct that holds the client (see `functions/sample.go:40-49`), uses `ctx.SecretProvider()` to fetch secrets, `AppContext()` to observe shutdown, and SetRetryData to reuse S&F.

## 6. Configuration System

- YAML (replacing TOML since v3). A private configuration sample is at `app-service-template/res/configuration.yaml`; the structs are in `internal/common/config.go` (ConfigurationStruct:43, TriggerInfo:64, ExternalMqttConfig:113, PipelineInfo:144, StoreAndForwardInfo:178).
- Common config (Trigger defaults to EDGEX-MESSAGEBUS plus SubscribeTopics events/#, and MessageBus defaults) is fetched via `-cp keeper.http://core-keeper:59890`, or from a local file via `-cc`; missing common config is a hard error (`triggerfactory.go:67`). v4.0 replaces Consul with core-keeper.
- Explicit external-mqtt configuration: `Trigger.Type=external-mqtt` plus `Trigger.ExternalMqtt.{Url,ClientId,AuthMode,...}`.
- Environment variables: inherited from go-mod-bootstrap (WRITABLE_*); since v3, SecretStore configuration can only be overridden via environment variables.
- **Full security mode**: SecretProvider (default Vault at localhost:8200, SPIFFE token provider), JWT microservice authentication; standard routes require authentication in secure mode; non-secure mode uses Writable.InsecureSecrets.

## 7. Standalone Operation / Deployment

- Can run without EdgeX core: external-mqtt/http trigger plus `MessageBus.Disabled: true` requires no keeper/broker; edgex-messagebus can also use a fully local Trigger+MessageBus configuration to connect directly to a standalone NATS.
- Container image: the template's `make docker` uses a multi-stage build, golang:1.25-alpine3.22 → alpine:3.22 + ca-certificates + dumb-init, PIE/RELRO, CGO_ENABLED=0; the default CMD is `-cp=keeper.http://edgex-core-keeper:59890 --registry`.
- Standard form: join edgex-compose, pull configuration via core-keeper, and use edgex-broker (NATS) for the event stream.

## 8. Maturity

- v3→v4 highlights (CHANGELOG): v3 moved to YAML, added JWT, removed ZeroMQ, and added the edgex/ topic prefix; v4.0 replaced Consul with core-keeper, introduced a new MessageEnvelope (generic payload, breaking), switched S&F to postgres and made it the default, added OpenZiti (no_openziti build tag), PIE/RELRO, MQTT pre-connect, and replay; v4.0.2 adds runtime pipeline topic changes and Go 1.25.
- Tests: 40 `*_test.go` files (vs. 76 non-test Go files), including 15 for transforms, 6 for app, 4 for store, 1 each for the three triggers, etc.; run with `go test -race -coverprofile`.
- The LTS policy is in the README: non-LTS releases get community support only for the latest release; Odessa 4.x is the current mainline.

## 9. Direct Application to NereidEdge

The simplest path for "NATS in → custom JSON → MQTT out to v1/gateway/telemetry" is **a custom transform function plus the built-in MQTTSecretSender; do not manage paho yourself**:

```go
service, _ := pkg.NewAppService("nereid-edge")
sender := transforms.NewMQTTSecretSender(transforms.MQTTSecretConfig{
    BrokerAddress: "tls://nereidlink:8883", ClientId: "nereid-edge-xxx",
    AuthMode: "clientcert", Topic: "v1/gateway/telemetry",
    AutoReconnect: true, Will: &models.WillConfig{/* v1/gateway/goodbye */},
}, true) // persistOnError, together with Writable.StoreAndForward + postgres
service.SetDefaultFunctionsPipeline(convertEventToGatewayJSON, sender.MQTTSend)
service.Run()
```

Control-plane and downlink subscribe flows such as hello/heartbeat/manifest/connect are implemented outside the SDK as a separate protocol state machine component (which can reuse the same paho connection or use an independent client).
