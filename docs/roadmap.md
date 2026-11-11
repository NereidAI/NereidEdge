<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# NereidEdge Milestone Roadmap (M0–M7)

- Date: 2026-10-04
- Scope: a Go Application Service based on EdgeX 4.0.2 (Odessa), bridging to the NereidLink gateway protocol `v1/gateway/*` (ADR-0026, `docs/protocols/nereid-edge/v1.md`)
- M8 (multi-gateway scale/quotas/bulk PKI) is deferred; **distribution and delivery rely on an Open Horizon management hub portal**
- Reference sources are consolidated in a local workspace scratch directory (not committed): NereidLink, app-functions-sdk-go (v4.0.2), and the Open Horizon management hub
- Selection rationale: [research/2026-10-04-edgex-sdk-selection.md](research/2026-10-04-edgex-sdk-selection.md)

## Milestone Overview

| Milestone | Theme | Exit criteria |
|---|---|---|
| M0 | Engineering skeleton and integration environment | Empty service running in edgex-compose |
| M1 | Tracer Bullet: handshake + telemetry uplink | ds-virtual data lands in the NL `ts_telemetry` table |
| M2 | Online semantics and reconciliation | Correct gateway/sub-device online state and manifest reconciliation |
| M3 | Security and network-outage resilience | mTLS + Vault + postgres S&F + resume |
| M4 | Downlink control plane | Commands, shadow, and connector lifecycle working bidirectionally |
| M5 | Edge autonomy | Real southbound devices, auto-provision, probe |
| M6 | Protocol completion and compatibility | Full topic coverage, blob relay, N-k compatibility tests |
| M7 | Distribution and operability | Horizon service definition published; CI/observability/version matrix complete |

---

## M0 — Engineering Skeleton and Integration Environment

- Initialize the Go module (`go 1.25`, aligned with SDK v4.0.2; NL's 1.26 toolchain can coexist), copy `app-service-template/` as the skeleton, and pin `app-functions-sdk-go/v4 v4.0.2` (do not track v4.1.0-dev).
- Engineering files: Makefile (build/test with `-race`/golangci-lint lint), a two-stage Dockerfile (golang:1.25-alpine → alpine:3.22, PIE/RELRO, CGO_ENABLED=0), and `res/configuration.yaml` plus common config.
- Establish NereidEdge's own AGENTS.md (Apache-2.0 copyright header, English comments, DCO, verification commands, commit identity, etc., trimmed from the NL conventions).
- Local integration environment (documentation + docker compose):
  - EdgeX side: core-keeper + edgex-broker (NATS core) + core-data/metadata/command + ds-virtual (4.0.2 compose, security mode initially disabled);
  - NL side: `make infra-up` (PG+TimescaleDB, NATS), `migrate up`, `seed`, `make dev`.
- Acceptance: `make build/test/lint` all pass; the empty-pipeline service starts and trigger initialization appears in the logs; the README documents one-command environment startup.

## M1 — Tracer Bullet: hello Handshake + Telemetry Uplink (Plaintext + Password Authentication)

Vertically slice the thinnest end-to-end path first, without any optional features.

- Pipeline: `EDGEX-MESSAGEBUS` trigger (nats-core, subscribing to `events/device/#`) → custom transform (`dtos.Event` → NL gateway batch JSON `{"<nl-device-uuid>": {"ts": <ms>, "<resource>": <value>}}`) → `transforms.MQTTSecretSender` publishing to `v1/gateway/telemetry` (tcp://nl:1883, AuthMode=usernamepassword).
- **Protocol pre-gating components** (NL drops all data frames before hello negotiation):
  - A standalone paho client (the same connection as the egress, or a controlled client), with LWT pre-registered on `v1/gateway/goodbye`;
  - `hello` frame: `v/edge_id/agent_version/proto.min,max/features.required=["telemetry","presence"]/connectors[]/client_time`; subscribe to `hello/ack` with a 10s timeout; state machine `UNIDENTIFIED → NEGOTIATED → CLOSED`;
  - 30s heartbeat; pipeline egress blocks/queues until NEGOTIATED.
- NL-side preparation: create a gateway device with `is_gateway=true`, create sub-devices bound via `gateway_id`, and issue TOKEN credentials; align measurement keys with the NL Profile.
- Frame format references: `internal/ingestmqtt/server.go:1190-1261`, `deploy/simulator/gateway_sim.py`.
- Acceptance: the gateway shows online in the NL console/WS; ds-virtual values enter `ts_telemetry` in real time; re-negotiation succeeds after a NereidEdge restart.

## M2 — Device Mapping and Online-Semantics Reconciliation

- EdgeX device name → NL sub-device UUID mapping module (locally persisted; initially manual configuration/static mappings; replaced by connector-spec delivery after M4).
- Sub-device presence: EdgeX device online/offline transitions → `v1/gateway/connect` / `disconnect` (batched).
- 5-minute manifest: assemble the full online inventory ahead of absolute wall-clock boundaries and report missed ticks; 16-minute lease.
- Other uplinks: `v1/gateway/attributes` (client attributes), `v1/gateway/events` (lifecycle).
- Joint validation with the NL watchdog: missed-heartbeat SUSPECT/DOWN cascades, no alerting on planned goodbye downtime, and flapping thresholds.
- Acceptance: under cable-pull/process-kill scenarios, NL presence state and alerts match chapters 5–6 of `v1.md`; manifest drift is detectable on the NL side.

## M3 — Security and Network-Outage Resilience (Production Hardening)

- mTLS: AuthMode=clientcert on port 8883; gateway X.509 certificates are issued by the NL PKI hierarchy (`nereidlink pki init`) and injected through the **EdgeX Vault/SecretProvider** (hot secret refresh rebuilds the MQTT connection).
- Enable Store & Forward: `persistOnError=true` + `Writable.StoreAndForward` + a **postgres** database; verify that events are persisted to the database during disconnection and replayed after recovery, along with the relevant metrics.
- `resume`: hello carries session_id/last_cursor, working with S&F to provide reconnect resume/backfill semantics.
- Security mode: the image joins compose with `-cp=keeper.http://edgex-core-keeper:59890 --registry`; SecretStore configuration is supplied through environment variables; standard route authorization applies.
- Acceptance: no data points lost and no duplicates during 30 minutes of poor/no network (judged by NL deduplication/cursor semantics); certificate rotation takes effect without a service restart.

## M4 — Downlink Control Plane

- Commands: subscribe to `v1/gateway/command`, dispatch to devices through EdgeX core-command (messagebus or client), and return results on `v1/gateway/command/response`; support one-way/request-response and timeout semantics; handle `v1/gateway/attributes/push`.
- Shadow: the gateway's own direct-connect identity reports its actual runtime state to `v1/me/shadow/report` (reported on (re)connect and on change); subscribe to `v1/me/shadow/desired`, only tracking and echoing back the version (gateway-level keys are delivered via attributes; desired state is not persisted); sub-device shadows (`v1/gateway/shadow/*`) are not implemented for now.
- Connector lifecycle (declarative per ADR-0026):
  - Self-describe via hello `connectors[]` (JSON Schema);
  - Subscribe to `v1/gateway/connectors/#` to handle spec/delete and manage local southbound plugin instances;
  - Reply on `v1/gateway/connectors/status` (APPLIED/FAILED/DEGRADED plus revision, timestamps, and idempotent retries);
  - Accept `secret://` encrypted credentials and use them for plugin connections.
- Acceptance: when a connector spec is delivered from the NL console, NereidEdge automatically establishes the connection and reports APPLIED; deletions and modifications converge under strict-reconcile; command round-trips succeed end to end.

## M5 — Edge Autonomy

- Real southbound: integrate EdgeX device services (starting with modbus, then ONVIF/custom MQTT as needed), linking M4 connector instances to EdgeX devices/profiles create/update/delete operations; the NL centrally manages configuration schemas and plugin versions.
- ~~auto-provision~~: **Shelved (2026-10-08)** — too few use cases. Local scan discovery → `v1/gateway/discovery` → handling `discovery/result` (including tenant guardrails: Profile matching, naming rules, quantity caps, automatic assignment, and audit; disabled by default) is not implemented for now; `internal/discovery/handler.go` has been removed (recoverable from Git history), and the `binding.Resolver.provisioned` path is not enabled. Revive the original design if use cases emerge later.
- ~~OTA~~: **Removed (2026-10-10)** — gateway and sub-device firmware upgrades are out of scope for this project. `internal/ota/` has been deleted; hello no longer advertises the `ota` optional capability, does not subscribe to `v1/gateway/ota/task`, and does not report `ota/progress`; firmware upgrades are instead carried by Horizon / the Open Horizon management hub service version management, at a layer outside NE.
- On-demand probing: subscribe to `v1/gateway/probe` (cached/fresh/strict, 10s default, aggregation) and reply on `probe/response`.
- Acceptance: probe latency/coalescing behavior matches protocol chapter 9.

## M6 — Protocol Completion and Compatibility

- Cover the remaining topics: the full attributes set, events, and all advisory handling; protocol field allowlists/validation and rejection paths.
- Blob uplink relay: **depends on the NL `POST /v1/connector/<edge-id>/uploads` endpoint being implemented (currently deferred)**; this item stays suspended until NL is ready.
- Compatibility: matrix tests for hello `proto.min/max` version-intersection negotiation (simulating server versions 1.0–2.0); required-feature rejection and optional-feature degradation paths.
- Protocol conformance test suite: build an NL-side behavior simulator by inverting `gateway_sim.py` (or test against real NL directly), and formalize it as CI integration tests.
- Acceptance: 100% protocol topic coverage (deferred items marked with their dependencies); the compatibility matrix passes automatically.

## M7 — Distribution (the Open Horizon management hub) and Operability

- Artifacts: image publication (GHCR/private registry), multi-architecture (amd64/arm64), SBOM and signatures.
- **Horizon listing**: create new service definitions and Business Policies for the EdgeX 4.x stack in the Open Horizon management hub / Open Horizon —
  - Current state: the Horizon repository only contains `docs/edgex-3.1-deployment.md` (Redis bus, core-common-config-bootstrapper); **the 4.x topology has changed (the NATS edgex-broker replaces redis, core-keeper, postgres for S&F)**, so a 4.0.2 service definition chain must be produced on the Horizon side (broker → core-* → device services → nereid-edge app service, recursive requiredServices, reusing host-network lessons);
  - Deliverables: the NereidEdge service definition (including configuration/secret injection conventions), deployment policy templates, and updated deployment documentation (referencing the Open Horizon management hub repository or this repository's docs, with a PR opened in Horizon).
- Agent self-upgrade: the M5 OTA item was removed (2026-10-10); agent upgrades go through the single Horizon service version management channel.
- Observability: Prometheus metrics (reusing SDK-exported bytes/errors plus protocol state-machine metrics), structured logging, `/healthz`, and readiness (negotiation state).
- CI: lint/test -race, integration tests (compose brings up EdgeX+NL), image builds, and documentation of the EdgeX/NL version compatibility matrix.
- Acceptance: a clean edge node brings up the full stack with one Horizon policy command and auto-registers with NL; the network-outage upgrade/rollback drill passes.

## Cross-Milestone Constraints

- Pin EdgeX v4.0.2; do not upgrade to v4.1 until it passes the compatibility matrix after release.
- The NATS leafnode enhancement is not scheduled until it is implemented on the NL server side (`internal/edge/model.go:13` is comment-only); network-outage resilience relies on postgres S&F.
- The NL-side deferred item (the blob relay endpoint) is an external dependency for M6 and must be tracked.
- The NL UUID is the canonical unique device identifier; the EdgeX name exists only in the local mapping.
- M8 (bulk gateway registration/large-scale PKI, quotas, fleet-level alerting) is deferred, but the M2–M4 data models must not block future extension.
