<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# NereidEdge Milestones

Last reviewed: 2026-11-11.

This document tracks implementation and acceptance separately. A checked item
means the corresponding implementation is present in this repository, with unit
tests where stated, or that a historical E2E result was explicitly recorded.
It does not imply a new live acceptance run for this documentation update.

The early M3-M5 stub checklist has been superseded by the capability milestones
below. Early OTA and discovery stubs are no longer treated as delivered product
features.

## Initial Engineering Baseline

### M0-M2: Service, gateway, and device lifecycle

- [x] EdgeX App Functions SDK Go v4.0.2 service structure, configuration, and
  logging baseline.
- [x] NereidLink gateway connection, connect/negotiate/heartbeat lifecycle, and
  remote configuration support.
- [x] First-class TLS materials for the NereidLink connection: `RootCAFile`
  pins a private-CA bundle, paired `ClientCertFile`/`ClientKeyFile` select mTLS,
  and `InsecureSkipVerify` is an explicit lab-only opt-in. Each material also
  accepts inline PEM text (env-overridable), with file delivery preferred and
  inline PEM taking precedence. `CredentialID` stays mandatory in every mode,
  including mTLS, and must match the registered NereidLink credential ID. PEM
  files are read fail-fast at startup; credential/TLS selection is unit-tested
  without a broker.
- [x] Protocol negotiation with required telemetry, presence, and command
  capabilities; optional capabilities are negotiated only when enabled.
- [x] EdgeX-name to NereidLink-UUID mapping, binding resolver integration,
  device attach/detach, presence, and five-minute manifest reconciliation.
- [x] Connect-on-first-telemetry, disconnect-on-shutdown, and
  `ForwardUnmapped` filtering.
- [x] Event/Reading conversion, including pointer-based EdgeX events and
  nanosecond-to-millisecond timestamp conversion.
- [x] Historical E2E result: Random-Boolean-Device telemetry reached
  NereidLink during the initial telemetry milestone.

### Initial test, build, and deployment baseline

- [x] Unit-test baseline established for gateway mapping, metrics, and binding
  resolution; subsequent capability sections track its expansion.
- [x] Makefile targets for formatting, vet, lint, tests, builds, and image
  packaging.
- [x] Multi-stage Dockerfile, Docker Compose development profile, operator
  environment examples, and deployment documentation.
- [x] Reviewed NereidLink device SDK mirror is the single SDK source for
  local, CI, and release builds; no adjacent checkout or named Docker build
  context is required.

## Delivered Capability Milestones

### Phase 1: Connector spec downlink and device reconciliation

- [x] Unified binding resolver with static/spec precedence, reverse lookup,
  binding ownership, and conflicting-name detection.
- [x] Connector applier for idempotent EdgeX Device upsert/delete, revision and
  config-hash deduplication, and APPLIED/FAILED status reporting.
- [x] Canonical `nereid-edge` connector type plus compatibility aliases and
  EdgeX v4 API request compatibility.
- [x] Connector inventory in manifests, debounced immediate reports after
  changes, and mixed device/node instance bookkeeping.
- [x] Self-healing after out-of-band EdgeX Device deletion and correct handling
  of renames and live updates.
- [x] Historical E2E result: connector spec apply/update/delete CRUD was
  validated during the initial NL-first integration.

### Phase 1.1: Node-level connectors and MQTT export

- [x] **P0 - Handler architecture**: `Handler` and `Registry` route connector
  types, preserve the southbound Device flow, and allow node-level specs with
  an empty `device_id`.
- [x] **P1 - Schema and manifest support**: Connector-type schema
  advertisements, mixed applied sets, and node-level reconciliation.
- [x] **P2 - `edge-export-mqtt` implementation**: Independent export runtimes
  are managed through an `ExportStage` fanout in the default pipeline, followed
  by the NereidLink telemetry stage. Filtering, JSON/raw payloads, topic
  templates, authentication/TLS, apply/delete, and lifecycle cleanup are
  implemented and unit-tested with fake connections.
- [x] **P3 - Runtime resilience**: CREATING/APPLIED/DEGRADED/FAILED reporting,
  bounded queues, reconnect backoff and maximum attempts, idempotent redrive,
  hot swap, graceful shutdown, and secret-value log masking.
- [ ] Resolve `secret://` references for MQTT export credentials; masking alone
  does not provide secret resolution.
- [ ] Implement batching if the wire-compatible `buffer` field is retained;
  the field is currently accepted without batching behavior.
- [ ] Validate against a real external broker, including APPLIED status,
  manifest reporting, filtering, reconnects, and broker-side receipt.
- [ ] Complete NODE_CONNECTOR_DESIGN.md AC1-AC10, southbound/node-level
  coexistence, stress/stability checks, and final acceptance documentation.

### Attributes, shadow, commands, health, and gateway operations

- [x] SERVER/SHARED attribute caches for the gateway and attached sub-devices,
  full pulls after reconnect and attach, JSON-null deletion, and scope-aware
  replacement.
- [x] EdgeX Tags synchronization for sub-device attributes and live gateway
  `log.level` application with configured-default fallback.
- [x] Real gateway reported shadow with build/protocol/features, effective
  settings, health, link statistics, attached-device counts, deduplication,
  retries, and desired-version tracking.
- [x] Direction-aware command handling, read/write validation, one-way and
  request/response behavior, and on-demand cached/fresh/strict probes.
- [x] Global and per-instance reachability monitoring, failure thresholds,
  presence withdrawal/recovery, and probe-policy replacement.
- [x] Whole-gateway administrative, network, diagnostic, and redacted log
  operations (ADR-0035), with explicit allowlists for privileged actions.
- [x] Planned goodbye, reconnect resubscription/replay, local link-health
  episode tracking, closed-episode reporting, acknowledgements, and flap
  detection.

### Phase 1.3 and ADR-0038: Device profile downlink

- [x] NereidLink device SDK capability negotiation, profile formats, spec and
  status frames, topics, subscriptions, and reporting support.
- [x] Format-specific landing with `(name, revision)` idempotency and
  APPLIED/FAILED/REMOVED status reports. Invalid or unsupported input reports
  FAILED; REJECTED is not currently emitted.
- [x] Idempotent EdgeX `edgex-device-profile/3` upsert/delete and persistent
  `nereid-neutral/1` document landing.
- [x] Explicit-removal semantics, in-use handling, ownership fencing, and no
  implicit deletion for entries missing from a frame.
- [x] Durable `profiles-down.json` revision/format/status/document ledger and
  unit tests for persistence, negotiation, retry, removal, and landing paths.
- [ ] Run production profile-downlink acceptance against NereidLink and EdgeX.

### Phase 2: EdgeX profile uplink

- [x] **P2a - Fetch and publish**: Core-metadata fetcher, hash-based
  deduplication, and QoS1 gateway profile publication.
- [x] Post-hello and reconnect replay, five-minute full scans, immediate
  publication after connector attach, and EdgeX add/update message-bus watch.
- [x] Unit tests for conversion, deduplication, watcher events, and replay
  behavior.
- [ ] **P2b - Cross-repository acceptance**: Confirm final payloads with
  NereidLink, ingest and convert them to descriptors, verify Bool persistence
  in `v_bool`, and validate cross-product hello replay.
- [ ] Extend the current minimal descriptor projection if full-fidelity
  synchronization is required. Uplink currently emits name, value type, units,
  and description; profile deletion uplink is not implemented.

### ADR-0034: Edge secret delivery

- [x] Full-set synchronization, revision deduplication, explicit removal,
  per-entry status reporting, encrypted-at-rest storage, and atomic persistence.
- [x] AES-256-GCM storage codec with operator-provided or ephemeral keys and
  reconnect redelivery semantics.
- [x] ONVIF username/password delivery, camera-MAC/default-credential binding,
  credential-type advertisement, and inference stream-secret fanout.
- [x] Unit tests for storage, cryptography, reconciliation, ONVIF binding, and
  inference credential delivery.
- [ ] Run production secret-delivery and credential-rotation acceptance.

### Phase 3: Generic SecretInjector

The ADR-0034 secret core above is implemented, but it does not make every
advertised credential type a consuming-service integration.

- [ ] Define the generic `secrets[]` connector configuration and binding model.
- [ ] Support the remaining credential forms beyond ONVIF and inference secret
  delivery; other catalog entries currently remain encrypted local copies.
- [ ] Cover insecure and secure EdgeX secret-store paths across supported API
  versions, including device-service reload/restart behavior.
- [ ] Add compatibility and end-to-end validation for each supported device
  service.

### ADR-0039 and ADR-0040: Edge inference bridge

- [x] Negotiated model inventory, neutral `edge-inference` bindings, Job device
  translation, binding status/reason mapping, and inference shadow projection.
- [x] Model and expanded pipeline bindings, source/stream options, camera
  profile discovery, VAAPI settings pass-through, and stream-secret fanout.
- [x] `edge-manager-resource` durable ownership/delete ledgers, retries,
  readback verification, orphan reconciliation, and optional job statistics.
- [x] `edge-northbound-filter` with mandatory `PipelineJSON`/`Embedding*`
  privacy guards on both NereidLink and auxiliary MQTT egress.
- [x] Detection telemetry normalization, binary media markers, and the
  `inference.diag` operation.
- [x] Unit tests for lifecycle, recovery, privacy, translation, health,
  credentials, manager resources, and telemetry.
- [ ] Execute and record the complete production
  NereidLink -> NereidEdge -> EdgeX -> NereidInference acceptance chain,
  including real cameras and target inference hardware.

### Build, CI, and publication

- [x] Binary and amd64/arm64 OCI build paths with tag-sourced versions, a
  rolling development channel, and no `latest` tag.
- [x] Fork-safe CI with source verification, BuildKit GHA cache integration,
  OCI platform validation, and seven-day artifacts; CI never pushes images.
- [x] Independent main-push development publication and tag-only SemVer release
  publication through organization reusable workflows.
- [x] Build-network endpoints remain operator-injected, and the Makefile has no
  registry push target.

## Deferred, Removed, and Remaining Work

### EdgeX-first discovery and auto-provisioning

- [x] Research, resolver storage, and initial design work.
- **Parked:** `ReportDiscovery`, `SubscribeDiscoveryResults`, and
  auto-provisioning are not wired. Dormant resolver storage and vendored SDK
  APIs do not constitute a delivered discovery flow. Resume only when a concrete
  use case justifies it, then coordinate with profile synchronization.

### OTA

- **Removed from NereidEdge scope:** Gateway and sub-device OTA handlers and
  hello advertisement were removed. Agent and firmware rollout belongs to the
  Open Horizon service-management layer, not this repository.

### Later production and acceptance work

- [ ] Implement the planned Vault/mTLS rotation and durable Store-and-Forward /
  resume path; these remain roadmap items, not current capabilities.
- [ ] Add automated cross-version protocol and clean-node integration gates,
  and track the externally dependent blob-relay endpoint.
- [ ] Remove legacy connector aliases after all consumers migrate.
- [ ] Finish configuration/example cleanup, including reconciling the base
  DEBUG log-level default with the Compose INFO default.
