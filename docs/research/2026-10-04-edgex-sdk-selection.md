<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# EdgeX App Functions SDK: Go vs Python — Technology Selection Research

- Date: 2026-10-04
- Question: NereidEdge is built on the latest EdgeX Foundry release (4.0.2 / Odessa) and integrates with NereidLink. Should the application service SDK be `app-functions-sdk-go` or `app-functions-sdk-python`?
- Analysis targets (all source checkouts live in a local workspace scratch directory, not committed):
  - `NereidLink` (integration target; Go 1.26; module `github.com/nereidai/nereidlink`)
  - `app-functions-sdk-go` (main = v4.1.0-dev.74; stable tag **v4.0.2**)
  - `app-functions-sdk-python` (main; stable tag **v4.0.2**)
- Supporting detail: [nereidlink-integration-surface.md](nereidlink-integration-surface.md) and [edgex-sdk-go-analysis.md](edgex-sdk-go-analysis.md)

## Conclusion (TL;DR)

**Choose the Go SDK (`github.com/edgexfoundry/app-functions-sdk-go/v4`, pinned to v4.0.2).** This is not a generic "Go is more mature than Python" claim: three lines of evidence — the integration contract, ecosystem alignment, and SDK maturity — all point to the same answer:

1. **NereidLink already has an explicit decision and a frozen protocol**: ADR-0026 (`NereidLink/docs/adr/0026-edge-connector-lifecycle.md:9`) states in writing that
   > "NereidEdge = the application service we build on an **EdgeX App Service (Go)** (not a standalone agent); to NL it is a connector of type `nereid-edge`."
   The decision is backed by a frozen wire protocol, `docs/protocols/nereid-edge/v1.md` (Draft v1, 585 lines), and the NL server side already implements the full semantics: handshake, heartbeat, manifest, commands, OTA, probe, and the declarative connector lifecycle.
2. **The maturity gap between the Go and Python SDKs is large.** The Python SDK is a new repository rewritten from scratch for Odessa 4.0 (2025-03): first commit on 2024-12-05, only 30 commits and 8 contributors. Its core pipeline works, but **the Vault secure-secret subsystem is not implemented, BackgroundPublisher is missing, the configurable pipeline is not wired up, the trigger layer has zero integration tests, and it has never been published to PyPI**. The Go SDK is the reference implementation that has shipped with EdgeX for years, with 40 test files and the full feature set: security, S&F, postgres, OpenZiti, and more.
3. **The Go SDK covers exactly everything the integration needs.** NereidLink embeds a mochi-mqtt broker, and the Go SDK's built-in `MQTTSecretSender` supports all four authentication modes — none, usernamepassword, clientcert, cacert — plus automatic reconnection, Will (LWT), persistOnError, and Store & Forward (postgres). These match the protocol requirements precisely: mTLS, the LWT `v1/gateway/goodbye`, and resume-after-disconnect (resume/S&F).

That MQTT baseline transport "works in both Go and Python" only means the NereidLink **server-side protocol is language-agnostic** (ADR-0026:10); it does not mean the EdgeX-side SDKs are equivalent — the same ADR still specifies the EdgeX app service itself as Go.

## Version baseline

| Item | Value | Basis |
|---|---|---|
| Latest stable EdgeX release | **4.0.2 Odessa**, 2026-05-29 | edgex-go GitHub releases; the latest stable tag of both the Go and Python SDKs is v4.0.2 |
| Next release | 4.1.0-dev (the Go SDK dev tag is already at v4.1.0-dev.74; **do not use it as a production baseline**) | `git ls-remote --tags` |
| Go SDK module | `github.com/edgexfoundry/app-functions-sdk-go/v4`, pin **v4.0.2** | `app-functions-sdk-go/app-service-template/go.mod` |
| Go version | 1.25 (required by v4.0.2; NereidLink itself uses go 1.26.0) | SDK `go.mod:3`; NL `go.mod:3` |
| Python SDK | `app_functions_sdk_py`, Python 3.10 only (hard-locked to `>=3.10,<3.11`) | `pyproject.toml:14` |
| License | Apache-2.0 for both | each repository's LICENSE |
| Default EdgeX 4.x message bus | **NATS Core** (Redis Streams removed; ZeroQ removed) | go-mod-messaging factory; Python SDK `messaging/__init__.py:40-64` |

> Note: NereidLink itself also uses NATS (Core + JetStream + KV) as its internal bus, but **EdgeX's NATS and NL's NATS are two independent deployments**; the baseline integration does not connect directly to the other side's NATS (see below).

## Integration surface: why the MQTT gateway protocol, and not gRPC / direct NATS / HTTP

NereidLink exposes a purpose-built connector surface for edge-side application services; a full walkthrough is in [nereidlink-integration-surface.md](nereidlink-integration-surface.md). Key points:

- **There is no northbound gRPC usable as a data plane**: the entire proto set contains only one `RegistryAuth` service (a device-authentication callback, default 127.0.0.1:19090); neither telemetry nor commands have RPCs (`NereidLink/api/proto/nereidlink/v1/registry.proto:40-43`).
- **Direct connection to NL's internal NATS** is the pattern for server-side adapters (standalone protocol-ingest processes). It requires a platform-issued NATS account and a self-implemented full semantics stack: Envelope, 1024 partitions, profile conversion, command sessions, and presence. The **NATS leafnode mentioned in ADR-0026 is only an optional Go enhancement, and the NL server has not implemented it yet** (a single comment in the whole codebase, at `internal/edge/model.go:13`).
- **HTTP `/v1/me/*`** is the direct-connect device model: it has no gateway batching, hello negotiation, or manifest reconciliation semantics, and cannot express an EdgeX gateway hosting multiple sub-devices.
- **The MQTT gateway protocol `v1/gateway/*` is the only frozen and fully implemented surface**: the protocol lives at `NereidLink/docs/protocols/nereid-edge/v1.md`, topic constants at `internal/mqttapi/topics.go:50-124`, ACL at `internal/mqttapi/acl.go`, handshake at `internal/edge/negotiator.go`, telemetry-to-bus landing at `internal/ingestmqtt/server.go:1190-1261`, downlink relay at `internal/ingestmqtt/*relay*.go`, and there is a directly reusable Python reference implementation at `deploy/simulator/gateway_sim.py`.

Protocol requirements on the EdgeX side (all met by the Go SDK):

| Protocol requirement | Go SDK capability |
|---|---|
| MQTT 3.1.1/5.0 over TLS, optional mTLS | `MQTTSecretSender` + `pkg/secure/mqttfactory.go`: four modes — none, usernamepassword, **clientcert**, cacert |
| Pre-registered LWT `v1/gateway/goodbye` | `MQTTSecretConfig.Will` field (supported since 3.1) |
| Long-lived connection, automatic reconnection | `AutoReconnect`, lazy connect, connection rebuild after hot secret refresh |
| No data sent before hello; fixed topic | A custom in-pipeline transform gates on handshake completion; the topic is fixed at `v1/gateway/telemetry` |
| 30s heartbeat / 5min manifest / connect/disconnect | Requires custom functions plus timers (**the Python SDK needs the same hand-written code**; this part is language-independent) |
| No data loss on network outage | `persistOnError=true` plus Store & Forward (Go supports postgres/redis; **Python only sqlite**) |
| Subscribing to downlink command/ota/probe/shadow/connectors | Go: a custom AppFunction struct holding the client (the same paho v1.5.1 as NL); the external-mqtt trigger is another option to evaluate |
| Secure production operation (Vault-managed certs/credentials) | Go: full Vault/SecretProvider integration; **Python: secure mode is simply unavailable** |

## Item-by-item comparison of the two SDKs

| Dimension | Go SDK v4.0.2 | Python SDK v4.0.2 |
|---|---|---|
| Status | EdgeX's primary reference implementation for years | A from-scratch rewrite started 2024-12, a "1.0-class newcomer" (the old Python SDK was archived in 2021 with no code continuity) |
| Go/Python version | Go 1.25 | Locked to the single minor version Python 3.10 |
| Module/package | Go module (template pins v4.0.2) | Not published to PyPI; source-only `pip install .`; the version still reads 0.0.2 |
| Trigger | edgex-messagebus / external-mqtt / http / custom | The same four |
| Message bus | mqtt / **nats-core / nats-jetstream** | mqtt / nats-core (no jetstream) |
| Pipelines, custom functions, per-topic multi-pipeline | Yes | Yes |
| MQTT/HTTP export + Will + auto-reconnect | Yes | Yes |
| Store & Forward | **postgres, redis** | **sqlite only** |
| BackgroundPublisher (publish messages to the bus in the background) | Yes | **No** |
| app-service-configurable (assemble pipelines purely from configuration) | Complete (including MQTTExport) | Code exists but is **not wired up**, and the function registry lacks mqttExport |
| Vault / secure mode | Complete (SPIFFE tokens, JWT microservice authentication) | **Secure Provider not implemented; no-secty is the only run mode** (`bootstrap/secret/secret.py:36-38`) |
| HTTPS webserver / CORS | Yes | Config fields exist but are **not implemented** |
| Tests | 40 `*_test.go` files (full `-race` run) | 142 test functions, but **zero tests for trigger / messaging / the main Service flow** |
| Examples | `app-service-template/` (buildable and runnable) plus official documentation examples | Template plus four one-liner examples (the custom-trigger example even has a bug) |
| Official image | Template `make docker` (golang:1.25-alpine → alpine:3.22, PIE/RELRO, CGO_ENABLED=0) | No published image; no Python service in the official compose |
| Activity | Continuously high-maintenance; PR #1828 (2026-09) | 30 commits / 8 people; after 4.0.0 each patch release has only 2–4 commits, mostly LTS fixes |
| Synergy with NereidLink | **Same language**: NL's edge protocol implementation, paho v1.5.1, and topic/frame structures can be directly referenced or reused; the team's Go stack stays uniform | Cross-language; protocol simulation can reference NL's bundled `gateway_sim.py`, but product code starts a separate Python stack |

## Risks (Go approach)

1. **A substantial part of the protocol still has to be hand-written**: the hello handshake state machine, the 30s heartbeat, the 5min manifest, connect/disconnect, downlink subscriptions and command acks, and the OTA/probe flows. The SDK only provides the "consume Event → transform → publish over MQTT" skeleton; the gateway protocol state machine must be built in-house (the same is true in either language). NL's `internal/edge/` and `internal/ingestmqtt/*relay*.go` are the best implementation references.
2. **Device identity mapping**: NL uniquely validates sub-devices by UUID, so EdgeX device names must be mapped to NL device ids (via connector spec/mappings or auto-provision, which is disabled by default).
3. **Do not count on leafnode for resume-after-disconnect**: NL has not implemented it; v1 outage resilience is handled first with the Go SDK's postgres Store & Forward.
4. **Pin-version discipline**: use the stable v4.0.2 tag; do not chase v4.1.0-dev on main. NL's go 1.26 and EdgeX's go 1.25 toolchains do not conflict (a single build machine can install 1.26; go.mod selects the toolchain automatically).
5. **Device services are outside this SDK selection**: southbound collection such as Modbus/ONVIF uses EdgeX device services (the Go family, e.g. device-modbus) or NL's connector system; this conclusion covers only the app service (northbound uplink) part.

## If Python were chosen instead (the not-recommended case)

Reconsider only if all of the following hold: the deployment explicitly forbids Go binaries, only non-secure (no-secty) mode is ever run, a single-file sqlite S&F is acceptable, BackgroundPublisher/configurable are not needed, and the team has no Go capability but is willing to follow a low-activity new SDK. **None of these conditions holds in the current NereidEdge/NereidLink situation**, and ADR-0026 already decided the opposite.

## Recommended minimal implementation path (Go)

1. Copy `app-functions-sdk-go/app-service-template/` as the NereidEdge repository skeleton, with go.mod pinning `app-functions-sdk-go/v4 v4.0.2`.
2. Trigger: `EDGEX-MESSAGEBUS` plus `MessageBus.Type=nats-core` (matching the edgex-broker of the EdgeX 4.x compose), subscribing to the default `events/device/#`.
3. Custom transform: `dtos.Event` → NL gateway batch telemetry JSON `{"<nl-device-uuid>": {"ts": <epoch ms>, "<resource>": <value>}}` (refer to `internal/ingestmqtt/server.go:1190-1261` and `deploy/simulator/gateway_sim.py:41-43`).
4. Egress: `transforms.NewMQTTSecretSender(transforms.MQTTSecretConfig{BrokerAddress: "tls://nereidlink:8883", AuthMode: "clientcert", Topic: "v1/gateway/telemetry", Will: {...goodbye...}, AutoReconnect: true}, true)` with Store & Forward (postgres) enabled. **Do not manage a paho client yourself inside a custom function** — that re-implements the roughly 200 lines of resilience the SDK already provides and cannot reuse S&F metrics or replay.
5. Build a separate protocol state-machine component: hello-negotiation gating, heartbeat/manifest timers, connect/disconnect, downlink topic subscriptions (command/ota/probe/shadow/connectors) and their responses.
6. Credentials: in production, gateway X.509 certificates issued by the NL PKI (the `nereidlink pki init` system), injected through EdgeX Vault/SecretProvider; during development, username/password plus port 1883 is acceptable.
7. Add the container to the EdgeX compose (aligned with core-keeper:59890), and on the NL side register a device with `is_gateway=true` and bind the sub-devices to it.

## References

- NereidLink ADR-0026: `NereidLink/docs/adr/0026-edge-connector-lifecycle.md`
- Frozen wire protocol: `NereidLink/docs/protocols/nereid-edge/v1.md`
- EdgeX 4.0.2 releases: https://github.com/edgexfoundry/edgex-go/releases (the Odessa LTS designation still needs to be cross-checked against the edgex wiki later)
- Key Go SDK files: `pkg/transforms/mqttsecret.go`, `pkg/secure/mqttfactory.go`, `app-service-template/{main.go,functions/sample.go,res/configuration.yaml}`
- Evidence of key Python SDK gaps: `src/app_functions_sdk_py/bootstrap/secret/secret.py:36-40`, `internal/store/sqlite/`, `functions/configurable.py` (no callers)
