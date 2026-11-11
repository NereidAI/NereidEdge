<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# NereidLink Integration Surface Analysis (NereidEdge Perspective)

- Date: 2026-10-04
- Subject: NereidLink source checked out in a local workspace scratch directory (not committed); module `github.com/nereidai/nereidlink`, VERSION v0.0.1, Go 1.26
- Summary: the designated edge-side integration channel is the **MQTT gateway protocol `v1/gateway/*`** (JSON over MQTT 3.1.1/5.0, TLS/mTLS). The protocol is frozen by ADR-0026 and `docs/protocols/nereid-edge/v1.md` and is fully implemented on the NereidLink (NL) side. gRPC has no data plane; direct NATS access is a server-side adapter pattern whose leafnode is not yet implemented; the HTTP device surface cannot express gateway semantics.

## 1. Platform Positioning and Architecture

An enterprise on-premises IoT pipeline platform: multi-protocol device ingress → unified message bus (NATS) → rule engine → persistence/egress/backflow; the platform itself contains zero business logic. Stack: Go, gRPC, NATS (Core + JetStream + KV), PostgreSQL + TimescaleDB, S3-compatible object storage, embedded mochi-mqtt; all real-time push goes over WebSocket and polling is forbidden; Redis is not used.

Two deployment forms (one codebase, ADR-0007):

- **All-in-one single binary** (`nereidlink serve`, the currently complete MVP implementation): embeds HTTP/WS + mochi-mqtt + registry gRPC; only PG + NATS are external (MinIO optional).
- Kubernetes microservices: the same code split into role-based containers via the Helm chart (`deploy/kubernetes/helm/nereidlink/`, currently replicaCount=1), with mTLS between services.

## 2. Directory Responsibilities

| Directory | Responsibility |
|---|---|
| `cmd/nereidlink/` | cobra entry point: `serve/migrate/seed/simulate/license/ops/pki/doctor/backup`; wiring assembled in `*_wiring.go` |
| `internal/` | All business implementation (54 subpackages): gatewayapi, registry, ingestmqtt, ingesthttp, telemetry, rule, command, presence, shadow, connector, manifest, linkhealth, edge, ota, probe, discovery, blob, identity, machineauth, rbac, accounting, streams, store, etc. |
| `api/proto/nereidlink/v1/` | Only 5 protos: envelope/point/presence/command/registry; only registry.proto contains a gRPC service; `api/gen/` holds buf-generated code |
| `sdk/` | **Go-only** server-side southbound Adapter SDK (adapter/telemetry/registry/profile/partition/contract/scriptruntime), for standalone protocol-ingest processes; not a device-side SDK |
| `adapters/` | Standalone process adapters: `coap/` (complete reference, ships its own cmd/Dockerfile), `template/`, `noop/` |
| `configs/` | Full dev.yaml sample, adapters/*.example.yaml, PKI output, license public key |
| `deploy/` | Helm, Dockerfile, hardened NATS config, backups, CoAP compose, `simulator/gateway_sim.py` (Python gateway simulator) |
| `web/` | React 18 + Vite + TS + AntD5; build output is embedded into the binary |
| `docs/` | 31 ADRs, architecture, protocol specs, openapi.yaml (~3000 lines), FEATURES.md, LESSONS_LEARNED.md |

## 3. External Interfaces

### 3.1 gRPC: A Single Service, RegistryAuth

`api/proto/nereidlink/v1/registry.proto:40-43`:

```protobuf
service RegistryAuth {
  rpc Authenticate(AuthenticateRequest) returns (AuthenticateResponse);
  rpc Authorize(AuthorizeRequest)     returns (AuthorizeResponse);
}
```

- Called back by the ingress layer/adapter to authenticate devices at connection time; supports TOKEN/PASSWORD/X.509 fingerprints; returns `DeviceIdentity{tenant_id, device_id, capabilities[]}` (the only capability today is `gateway`).
- Listens by default on 127.0.0.1:19090 (`configs/dev.yaml:12`), authenticated by a Bearer module token (`sdk/registry/client.go:24-72`); cluster deployments can enable TLS 1.3/mTLS.
- **There is no northbound gRPC data plane for telemetry or the rule engine**. The Command in command.proto is a NATS payload, not an RPC.

### 3.2 HTTP REST: Two Surfaces

- Management/user surface gateway-api, default 0.0.0.0:8080; routes in `internal/gatewayapi/server.go:569-907`, contract in `docs/api/openapi.yaml`. Key route families:
  - Authentication: `/v1/auth/login|refresh|logout`, `POST /v1/auth/token` (OAuth2 client_credentials for machine applications), MFA, bootstrap;
  - Devices/profiles/credentials: per-tenant devices/profiles/credentials, bulk-import, claim/transfer/decommission/quarantine;
  - Read-only telemetry queries, command dispatch, command tasks;
  - **Edge connectors**: `/v1/tenants/{t}/gateways/{g}/connectors[/{spec}]`, discovery-policy/candidates, `/secrets`;
  - probe, OTA/firmware, rules/alerts/dashboards, audit, quotas, retention policies, service accounts, ops orders;
  - Adapter registration (no tenant): `POST /v1/modules/register`, `/v1/modules/{id}/heartbeat`;
  - `/healthz`, `/metrics`, `/api/docs`, embedded SPA.
- Device southbound ingest-http (same port in development, under `/v1/me/`): `POST /v1/me/telemetry`, `GET /v1/me/commands` (25s long polling), `POST /v1/me/cmdres/{command}`, shadow/report, the ota family; Bearer or Basic authentication. The `POST /v1/connector/<edge-id>/uploads` blob relay endpoint described in the protocol spec **is not yet implemented in code**.

### 3.3 WebSocket

Entry point `GET ws://host:8080/wws` (config `server.ws_path`, default /ws); the token is passed in the query string `?access_token=`; multiple subscriptions per connection, server push only; control-frame ops are subscribe/unsubscribe/resume/reauth; resource types are telemetry/presence/alarm/notification/command_task/probe_task/ota_task. The server fans out using Core NATS subscriptions.

### 3.4 NATS Design (authoritative doc: `docs/architecture/messaging.md`)

Prefix `nl.`; hierarchy `nl.<domain>.<tenant>.<partition-or-device>[.rest]`.

- Uplink: `nl.telemetry.s<N>.<tenant>.<partition>` (JetStream; 1024 partitions mapped onto 16 physical streams TL_S0..S15, `sdk/partition/partition.go:20-42`), `nl.attribute.sN.*`, `nl.event.sN.*`; presence goes over Core; alarm over Core+JS.
- Downlink: `nl.command.dispatch.<ingress-id>` (Core), `nl.command.response.<t>.<d>`, `nl.attribute.down.<ingress-id>`.
- Side channels: `nl.egress.<tenant>.<forwarder>` (dedicated stream, ADR-0018), audit, linkhealth, manifest, connector.desired, ota.task, probe, etc.
- KV buckets: presence/activity/topology (45s lease)/**gw-lease (edge heartbeat lease, 90s)**/partitions/sessions/creds.
- Third-party integration contracts: (1) push mode — HTTP webhook or direct JetStream publish of Envelope; (2) auth-callback mode — gRPC Authenticate/Authorize. Third parties use a dedicated NATS account/leafnode with minimal subject permissions (example `deploy/nats/server.hardened.conf`: can only pub `nl.telemetry.>` and sub their own dispatch).

### 3.5 MQTT: Embedded mochi-mqtt (Focus Area)

- The mochi-mqtt v2.7.9 library is embedded (`internal/ingestmqtt/server.go`), supporting 3.1.1/5.0; listens on plaintext 1883 and TLS/mTLS 8883 (K8s NodePort 31883/31884); `tls_require_client_cert` controls whether mTLS is mandatory.
- Two hooks: AuthHook (calls the registry gRPC for authentication/ACL) and PublishHook (PUBLISH → Envelope → JetStream).
- Topics are frozen in `internal/mqttapi/topics.go`:
  - Directly connected devices: `v1/me/{telemetry,attributes,events,shadow/report,cmdres/<id>}`, subscribing to `v1/me/cmd/#`, `v1/me/attributes/down`, `v1/me/shadow/desired`.
  - **Gateway (NereidEdge) uplink**: `v1/gateway/{hello,heartbeat,goodbye,connect,disconnect,manifest,telemetry,attributes,shadow/report,command/response,ota/progress,probe/response,connectors/status,discovery}`;
  - **Gateway downlink (Edge subscriptions)**: `v1/gateway/hello/ack`, `command`, `attributes/push`, `shadow/desired`, `ota/task`, `probe`, `connectors/#`, `discovery/result`.
- Gateway batch telemetry frame (ThingsBoard style): `{"<sub-device-id>": {"ts": 1727..., "temperature": 21.4}}` (`server.go:1190-1261`; simulator `deploy/simulator/gateway_sim.py:41-43`).
- ACL: deny-by-default allowlist; the device record must have `is_gateway=true` and hold the gateway capability; it can only publish on behalf of sub-devices attached to this gateway that have completed a connect (`mqttapi/acl.go:43-125`).

### 3.6 Other Protocol Adapters

CoAP is a standalone-process adapter (`adapters/coap/`, UDP 5683/DTLS 5684) that never touches PG; its pattern is HTTP enrollment registration + heartbeat + publishing data over NATS. LwM2M/SNMP/Modbus follow the same standalone-adapter form (example YAMLs and reserved NATS accounts already exist under configs/adapters).

## 4. Data Model and Telemetry Path

```
Device frame (MQTT/HTTP/CoAP, gateway or direct)
 → AuthHook authentication (local X509/JWT verification; TOKEN/PASSWORD via gRPC + L1/L2 cache)
 → JSON split by measurement key
 → sdk/telemetry.Publish: applies DeviceProfile descriptors for unit conversion,
    generates a TelemetryPoint (pb) per point → wrapped in Envelope (pb)
 → JetStream nl.telemetry.sN.<tenant>.<partition> (TL_S0..15, retention 7d)
 → sharded consumption by telemetry.Server → TimescaleDB ts_telemetry narrow table (EAV)
 → parallel consumption by rule.Processor; gateway-api Core subscriptions fan out to WS
```

- `Envelope` (api/proto/.../envelope.proto:42-58): schema_version=1, tenant/device/gateway_id, MsgType, content_type, payload, dual event_time/ingress_time timestamps, ingress_id, protocol, BlobRef. It is an internal contract and is not exposed directly to devices.
- `TelemetryPoint`: measurement_key + v_double/raw_double/v_bool/v_string + value_type + quality (GOOD/UNCERTAIN/BAD) + unit + descriptor_version.
- The model borrows from Eclipse Hono (explicitly noted in messaging.md). Mapping to EdgeX: DeviceProfile≈DeviceProfile, measurement descriptor≈DeviceResource/PropertyValue, Envelope+Point≈Event/Reading (but NL emits one Envelope per point), Command≈Command. Differences: NL mandates TimescaleDB, profile-version pinning, and per-device calibration.

## 5. Four Layers of Authentication and Authorization

1. Users: OIDC/OAuth2 or local argon2id + TOTP; JWT access tokens 30 min / refresh tokens 7 d; WS uses the same JWT.
2. Machine applications: mTLS certificates preferred, or OAuth2 client_credentials (`POST /v1/auth/token`); NATS uses NKey/JWT.
3. **Devices/gateways (MQTT/HTTP/CoAP)**: multiple per-device credentials of type TOKEN/PASSWORD/X509/JWT; X509/JWT are verified purely locally on the hot path (CRL fail-closed, JWKS); TOKEN/PASSWORD go through gRPC + cache + a credential_epoch global revocation gate; NL ships a built-in lightweight CA (`nereidlink pki init`).
4. Internal/adapter: gRPC module token bearer; cluster mTLS; NATS per-account subject allowlists.

NereidEdge uses layer 3: the gateway is registered as a device with `is_gateway=true`, with username/password or X.509 credentials; sub-devices are bound via `device.gateway_id`.

## 6. Deployment and Default Ports

| Component | Port |
|---|---|
| gateway-api (HTTP/WS) | 8080 (WS path /wws) |
| mochi-mqtt | 1883 / 8883; K8s NodePort 31883/31884 |
| RegistryAuth gRPC | 127.0.0.1:19090 |
| ops dual-control management endpoint | 127.0.0.1:19190 |
| PostgreSQL+TimescaleDB (external, required) | 5432 (timescale/timescaledb:2.17.2-pg16) |
| NATS (-js) | 4222, monitor 8222; leafnode 6222 is documented planning only |
| S3/SeaweedFS (optional) | Development: seaweedfs-dev.example.internal:8333, bucket nereidlink |
| CoAP adapter | UDP 5683 / DTLS 5684 |

Migrations are run explicitly with `nereidlink migrate up` (currently up to 0053); the Helm chart ships bundled PG+NATS and also supports byo-infra; images uniformly use public coordinates, and internal registries are resolved transparently through node-level containerd mirrors.

## 7. NereidEdge Integration Plan (Protocol Contract Details)

Identity: on the NL side, create a gateway device with `is_gateway=true`, bind sub-devices, and issue credentials (token in development, X.509 in production).

Connection and session (`docs/protocols/nereid-edge/v1.md`):

1. Connect to `tcp://nl:1883` or mTLS `tls://nl:8883` with a unique ClientID; **pre-register the LWT as `v1/gateway/goodbye`**; keepalive 60s.
2. Subscribe to `v1/gateway/hello/ack` (or use the MQTT5 ResponseTopic), then send `v1/gateway/hello` (`v/edge_id/agent_version/proto.min,max/features.required=["telemetry","presence"]/connectors[](self-describing JSON Schema)/resume/caps/client_time`) with a 10s timeout; all data frames before hello are discarded (`ingestmqtt/server.go:1215-1218`); the server protocol range is 1.0–2.0 (`internal/edge/model.go:274`), and the response returns session_id, the version intersection, and clock skew; the state machine is `UNIDENTIFIED → NEGOTIATED → CLOSED`.
3. Keepalive trio: application heartbeat every 30s (one missed beat → SUSPECT at 45s; three missed beats → DOWN at 90s, cascading sub-device offline); batch connect/disconnect declarations; every 5 minutes, aligned to wall clock, send a manifest with the full online inventory (16min lease).
4. Data is sent to `v1/gateway/telemetry`; the key must be the **NL sub-device UUID** (EdgeX device names must be mapped; automatic provisioning is off by default); measurement keys must align with the Profile.
5. Subscribe to downlinks: `v1/gateway/connectors/#` (declarative spec; reply on connectors/status: APPLIED/FAILED + revision), `command` (reply on command/response), `shadow/desired`, `ota/task` (HTTPS relay download, mandatory sha256, reply on ota/progress), `probe` (reply on probe/response); to report locally discovered devices, send `discovery` (requires the tenant to enable auto_provision).

Implementation reference index:

| Purpose | Location |
|---|---|
| Full protocol text | `docs/protocols/nereid-edge/v1.md` (585 lines; topic table at 100-133) |
| Topic constants / ACL | `internal/mqttapi/topics.go:50-124`, `internal/mqttapi/acl.go` |
| Handshake state machine | `internal/edge/model.go`, `internal/edge/negotiator.go`, `internal/ingestmqtt/hello.go:150-215` |
| Gateway telemetry onto the bus | `internal/ingestmqtt/server.go:1190-1261` |
| Downlink relays | `internal/ingestmqtt/commandrelay.go`, `ota_relay.go`, `probe_relay.go`, `discovery_relay.go` |
| Heartbeat/watchdog/inventory reconciliation | `internal/linkhealth/`, `internal/manifest/`, `internal/connector/` |
| Runnable reference (auth+connect+telemetry) | `deploy/simulator/gateway_sim.py` |

## 8. Alternative Channel Trade-offs

| Approach | Conclusion |
|---|---|
| MQTT gateway protocol | **The only frozen and fully implemented surface**; complete coverage of hello/manifest/presence/watchdog/connector lifecycle/OTA/probe/shadow/commands; language-agnostic, JSON only |
| gRPC | Auth callbacks only; no data plane |
| Direct NATS (adapter mode) | Feasible but requires reimplementing the full Envelope/partition/profile/command-session/presence stack and a platform-issued NATS account; **the leafnode server side is not implemented** (only a single comment at `internal/edge/model.go:13`), so it cannot be relied on |
| HTTP `/v1/me/*` | No gateway semantics; not recommended |
| HTTPS blob relay | Documented but the endpoint is unimplemented; OTA currently uses external URLs + sha256 |
