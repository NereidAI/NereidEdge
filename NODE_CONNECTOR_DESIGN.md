<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# NereidEdge Node-Level Connector Design (Empty `device_id`)

> Status: This document began as a research and design proposal before any
> implementation existed. P0-P3 are now substantially implemented. This
> revision records the implemented architecture and the remaining deviations:
> `secret://` resolution for MQTT export, batching, and real-broker AC1-AC10
> acceptance are still open. See [MILESTONES.md](MILESTONES.md) for the summary
> implementation and acceptance status.

## 1. Feasibility Conclusion

**Conclusion: feasible. Use separate connector types, a handler registry, and
independently managed export runtimes.**

Rationale:

- The original rejection of an empty `device_id` was a precondition of the
  southbound EdgeX Device creation path, not a protocol restriction. The SDK
  push frame marks `device_id` as `omitempty`; the business semantics of Device
  creation require a NereidLink sub-device binding, while a node-level
  connector does not.
- Node-level capabilities and EdgeX Device creation are orthogonal
  responsibilities. A node-level connector does not create a core-metadata
  Device, require a NereidLink sub-device UUID, or produce device-owned
  telemetry.
- The current implementation uses an `ExportStage` fanout inside the default
  App Functions SDK pipeline. Each connector spec still owns an independent
  runtime, MQTT client, queue, cancellation context, revision, and health
  state. It does not create an independent SDK trigger/pipeline for every
  spec.
- Manifest reconciliation depends on `spec_id`, `revision`, and `config_hash`,
  not on a non-empty `device_id`. Device- and node-level instances therefore
  share the same applied inventory.

**Risk level: medium.** The main risks are runtime lifecycle management,
bounded-queue behavior, connection recovery, hot-swap ordering, and accurate
status reporting. Fake-connection unit tests reduce these risks but do not
replace real-broker acceptance.

## 2. Connector Types and Plugin Architecture

### 2.1 Use a separate connector type instead of reusing `nereid-edge`

| Option | Advantages | Disadvantages | Decision |
|---|---|---|---|
| Reuse `nereid-edge` with a config `mode` | Fewer types and simpler initial migration | Mixed responsibilities, complicated schema validation, unclear Device versus node status semantics | Rejected |
| Add a separate type for each capability | Clear ownership, self-contained schemas, independent handlers, no regression to the southbound path | Slightly more connector types | **Selected** |

Initial and reserved types:

- `edge-export-mqtt`: implemented as the first node-level MQTT northbound
  export connector.
- `edge-export-http`: reserved for a later iteration; it is not currently
  registered or implemented.

The `edge-` prefix distinguishes node-level capabilities from `nereid-edge`,
which creates and manages southbound EdgeX Devices.

### 2.2 Route by connector type through a handler registry

The original static type switch and monolithic apply/delete path have been
replaced by a registry. The implemented interface is:

```go
// Handler handles a specific connector type.
type Handler interface {
    Type() string
    Apply(ctx context.Context, push device.ConnectorPush) error
    Delete(ctx context.Context, push device.ConnectorPush) error
}

// Registry maps a connector type to its handler.
type Registry interface {
    Register(t string, h Handler)
    Get(t string) (Handler, bool)
}
```

Routing rules:

- `applySpec` performs common validation, then resolves the handler by type.
- A device-level handler such as `nereid-edge` continues to require a non-empty
  `device_id` and follows the EdgeX Device upsert path.
- A node-level handler such as `edge-export-mqtt` accepts an empty `device_id`
  and manages a node runtime instead of an EdgeX Device.
- An unregistered type retains the existing ignore behavior.
- Delete follows the same type-based routing and cleans up the corresponding
  Device or node runtime.
- Manifest reconciliation walks the complete applied set by `spec_id`; it does
  not filter instances by the presence of a Device name or ID.

This preserves the existing southbound contract while adding node-level types
at one routing point.

## 3. Config Contract and Schema Registration

### 3.1 Hello connector schema registration

NereidLink validates connector configs against a self-contained JSON Schema
advertised by the edge during hello negotiation. Each supported connector type
owns its schema.

Requirements:

- JSON Schema draft 2020-12.
- Self-contained documents without cross-document `$ref` dependencies.
- Strict types and explicit required fields.
- Defensive edge-side parsing even when the platform has already validated the
  config.

The P1 schema-registration framework and mixed Device/node manifest support
are implemented. The advertised schema and the defensive parser must remain
aligned when fields change.

### 3.2 `edge-export-mqtt`

**Purpose:** Export selected EdgeX events to an external MQTT broker other
than the NereidLink broker. The connector does not create an EdgeX Device or
bind a NereidLink sub-device.

Config contract:

```json
{
  "type": "object",
  "required": [
    "broker_url",
    "topic_template"
  ],
  "properties": {
    "enabled": { "type": "boolean", "default": true },
    "broker_url": { "type": "string", "format": "uri", "pattern": "^(mqtt|mqtts|tcp|ssl)://.+" },
    "client_id": { "type": "string", "minLength": 1 },
    "qos": { "type": "integer", "minimum": 0, "maximum": 2, "default": 0 },
    "retained": { "type": "boolean", "default": false },
    "keep_alive_s": { "type": "integer", "minimum": 1, "maximum": 300, "default": 30 },
    "username": { "type": "string" },
    "password": { "type": "string", "description": "A secret:// reference is reserved by the design but is not resolved by the current exporter" },
    "ca_cert": { "type": "string", "description": "Optional PEM CA certificate" },
    "client_cert": { "type": "string" },
    "client_key": { "type": "string" },
    "insecure_skip_verify": { "type": "boolean", "default": false },
    "topic_template": {
      "type": "string",
      "minLength": 1,
      "description": "Supported placeholders: {deviceName}, {profileName}, {sourceName}, {serviceName}, {eventId}, {origin}"
    },
    "payload_format": {
      "type": "string",
      "enum": ["json", "raw"],
      "default": "json",
      "description": "json exports a compact Event/Reading envelope; raw exports reading values"
    },
    "filter": {
      "type": "object",
      "properties": {
        "device_names": { "type": "array", "items": { "type": "string" } },
        "profile_names": { "type": "array", "items": { "type": "string" } },
        "resource_names": { "type": "array", "items": { "type": "string" } },
        "min_readings": { "type": "integer", "minimum": 0, "default": 0 }
      },
      "description": "Operator selection policy. The NE-owned mandatory PipelineJSON and Embedding* privacy guard is applied before this filter and cannot be overridden."
    },
    "buffer": {
      "type": "object",
      "description": "Accepted for wire compatibility; batching is not implemented and publication is per event",
      "properties": {
        "max_batch": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 1 },
        "flush_ms": { "type": "integer", "minimum": 10, "maximum": 5000, "default": 100 }
      }
    },
    "reconnect": {
      "type": "object",
      "properties": {
        "min_backoff_ms": { "type": "integer", "minimum": 100, "maximum": 60000, "default": 500 },
        "max_backoff_ms": { "type": "integer", "minimum": 1000, "maximum": 300000, "default": 5000 },
        "max_attempts": { "type": "integer", "minimum": 0, "default": 0, "description": "0 means retry forever" }
      }
    },
    "tags": { "type": "object", "additionalProperties": { "type": "string" } }
  },
  "additionalProperties": false
}
```

Behavior notes:

- `broker_url` and `topic_template` are required by the defensive parser.
- Supported topic placeholders are `{deviceName}`, `{profileName}`,
  `{sourceName}`, `{serviceName}`, `{eventId}`, and `{origin}`.
- Unknown placeholders fall back to
  `nereid-edge/{deviceName}/{profileName}/{sourceName}` and emit a bounded
  warning rather than suppressing every export.
- JSON output uses a compact event envelope to avoid exposing unnecessary
  internal fields. Raw output exports selected reading values.
- Empty filter lists match everything, subject to the mandatory privacy guard.
- `buffer` is accepted for wire compatibility only. `max_batch` and `flush_ms`
  currently have no batching behavior.
- The runtime uses a bounded queue per instance. A full queue drops the event,
  records the drop, and reports DEGRADED rather than blocking the default
  pipeline.
- `secret://` references are masked in logs, but the current MQTT exporter
  passes configured credential values to Paho without resolving those
  references. Section 5 and AC10 therefore remain open.

### 3.3 Reserved `edge-export-http`

A future HTTP connector may define `url`, `method`, `headers`, `timeout_ms`,
`retry`, `auth`, `tls`, `filter`, and `payload_format`. Sensitive basic,
bearer, and token fields should support the same secret-reference contract.
Its self-contained draft 2020-12 schema must be registered only when an actual
handler is available.

## 4. Runtime Lifecycle

### 4.1 Implemented pipeline model

The implementation uses one shared EdgeX trigger and default App Functions SDK
pipeline, with a side-channel fanout before NereidLink telemetry publication:

```text
EdgeX Event
  -> mandatory NE privacy guard
  -> ExportStage.Fanout
       -> per-spec bounded queue
       -> per-spec filter
       -> topic and payload rendering
       -> external MQTT publish
  -> NereidLink TelemetryPublisher
```

`ExportStage` always returns `(true, data)`, so an auxiliary export cannot
stall or short-circuit the NereidLink path. Fanout is non-blocking. Each spec
owns the following runtime state:

```text
per spec_id:
  context/cancel and done signal
  external MQTT connection
  bounded event queue
  applied revision/config hash
  current state and reason
  reconnect attempt/backoff state
  overflow/drop state
```

There is no independent EdgeX message-bus subscription per export spec and no
implemented batch stage. Event duplication between NereidLink and auxiliary
MQTT is intentional: each destination receives the same accepted event under
its own policy.

### 4.2 Apply, hot update, and delete

| Action | Implemented behavior |
|---|---|
| Apply | Validate config defensively, report CREATING through the caller, construct the replacement runtime, and start it. The initial connection outcome selects APPLIED or DEGRADED; invalid config reports FAILED. |
| Idempotent redrive | If the live runtime has the same config hash and an equal or newer revision, keep it running. A terminal FAILED runtime is rebuilt even for the same config. |
| Hot update | Start the new instance before stopping the old one. Revision/hash changes replace the runtime without changing the southbound Device path. |
| Delete | Cancel the runtime, make a bounded best-effort attempt to drain queued events, close the external connection, remove the spec from the runtime map, and release resources. |
| Shutdown | Stop every export runtime through the same bounded cleanup path. |

State semantics:

- `CREATING`: Apply has started runtime construction; reported by the applier.
- `APPLIED`: The external connection is ready, or a successful publish has
  recovered a previously overflowed runtime.
- `DEGRADED`: Connection failure/loss, repeated publish failure, reconnect in
  progress, or queue overflow with dropping.
- `FAILED`: Invalid config or exhausted non-zero `max_attempts`.
- Delete removes the active runtime and applied inventory entry; no persistent
  `DECOMMISSIONED` runtime state is required.

Connection failures use exponential backoff. `max_attempts=0` retries forever;
a positive value transitions to terminal FAILED after exhaustion. Repeated
reports of the same state are deduplicated to avoid status spam.

### 4.3 Manifest reconciliation

Node-level instances must appear in the connectors section of the five-minute
manifest and event-triggered manifest reports.

The implemented rules are:

1. The applied set carries both device-level and node-level instances and does
   not require a Device name or ID for node-level entries.
2. Reconciliation walks every applied spec and computes `config_hash` using
   the SDK canonicalization contract. Node-level mappings are normally empty.
3. Empty `device_id` validation belongs to the device-level handler, not the
   common applier.
4. `markApplied`/`finishApply` can persist a node-level entry without a Device
   ID.
5. Delete cleans up the node runtime and applied entry without deleting an
   EdgeX Device.

The full applied set therefore supports mixed Device and node inventories,
manifest drift detection, and hello redrive.

## 5. Credentials and `secret://`

The platform's `secret://<name>` convention identifies a tenant-managed secret
that must be resolved before the value is used by a connector.

Target design:

- Sensitive MQTT/HTTP fields such as `password`, `bearer_token`, `client_key`,
  `client_cert`, and `ca_cert` should support `secret://` where appropriate.
- Resolve references during runtime creation or hot update, close to actual
  use, rather than placing plaintext in connector specs or durable logs.
- Re-resolve when a connection must be rebuilt after credential rotation.
- Redact both literal secrets and secret references from diagnostic output.

**Current implementation status:**

- Secret-reference log masking is implemented.
- ADR-0034 provides a separate encrypted-at-rest secret-delivery path for
  supported consuming services.
- MQTT export does not yet resolve `secret://`; it currently passes the
  configured value directly to the MQTT client.
- AC10 remains open and the P3 credential work must not be described as fully
  complete until resolution and real redelivery/rotation behavior are verified.

## 6. Multiple Instances, Conflicts, and Idempotency

| Scenario | Design and current behavior |
|---|---|
| Multiple instances | A gateway can run multiple node-level connectors keyed by different `spec_id` values. Each owns an independent runtime and queue. |
| Same broker/topic in multiple specs | Allowed. Different filters may intentionally export to the same destination; operators own duplicate/loop prevention. |
| Hello redrive | Equal revision/hash redrive keeps a healthy runtime alive. A missing or terminal FAILED runtime is rebuilt for self-healing. |
| Revision update | Start the replacement before stopping the old runtime, then update applied state after the initial outcome. |
| Reconnect convergence | Five-minute manifests plus event-triggered reports expose the complete desired/applied inventory. |
| Resource uniqueness | `ExportManager` owns one active runtime per `spec_id`; Delete and StopAll release its resources. |
| Overload | A full bounded queue drops instead of blocking the shared pipeline and reports DEGRADED until publication recovers. |

The platform owns the desired set. The edge does not globally deduplicate
connector specs by broker or topic.

## 7. Event Source and Coexistence with NereidLink Telemetry

### 7.1 Event source

Node-level export consumes the same accepted EdgeX Events as the existing
message-bus trigger. The current implementation deliberately reuses the
default pipeline rather than creating an independent trigger subscription for
every spec.

This gives all export runtimes the same event input and privacy baseline while
keeping per-instance filtering and publication independent.

### 7.2 Coexistence strategy

- **Default NereidLink path:** Continues through `TelemetryPublisher` after the
  export fanout.
- **Node-level export path:** Fans a copy out to every matching export runtime
  and publishes according to each spec's filter, payload, topic, and connection
  policy.
- **Failure isolation:** Export fanout does not return an error to the default
  pipeline. Queue overflow and connection failures affect the corresponding
  export status, not NereidLink telemetry delivery.

Receiving the same event on NereidLink and an external broker is expected. An
export-only mode that disables NereidLink telemetry is not part of this design.

## 8. Feasibility, Effort, and Risks

| Dimension | Assessment | Notes |
|---|---|---|
| Feasibility | High and implemented | The registry, schema, mixed manifest, and MQTT runtime paths exist. |
| Implementation effort | Medium | Main work was the handler refactor, schema/manifest support, export runtime, status state machine, and reconnect/hot-swap behavior. |
| Main risks | Runtime leaks, reconnect storms, secret leakage, event loss during overload/update | Cancellation and StopAll bound lifecycle; backoff limits reconnects; masking reduces disclosure; queues are explicitly bounded and lossy under overload. |
| Remaining acceptance risk | Medium | Fake dialers do not verify real Paho/broker behavior, TLS, ACLs, network interruption, or credential rotation. |
| Backward compatibility | High | The non-empty `device_id` southbound flow remains intact; wire frames retain `omitempty` semantics. |

A hot swap reduces the replacement window, but it is not a durable event log.
Neither hot swap nor the bounded queue provides Store-and-Forward guarantees.

## 9. Phased Implementation Plan and Status

| Phase | Scope | Current status and remaining acceptance |
|---|---|---|
| **P0 - Refactor** | Add `Handler` and `Registry`; wrap southbound Device creation; route Device and node specs by type | **Implemented and unit-tested.** Preserve southbound regression coverage. |
| **P1 - Schema and manifest** | Register schemas by type; support mixed applied sets and node-level manifest entries | **Implemented and unit-tested.** Real manifest/broker acceptance remains part of P4. |
| **P2 - MQTT export** | Add the handler, shared-pipeline fanout, filter/transform/publish, and Apply/Delete lifecycle | **Implemented with fake-connection unit tests.** A real external broker has not closed AC1-AC5. |
| **P3 - Resilience** | Add states, backoff/max attempts, hot swap, bounded queues, and log masking | **Substantially implemented and unit-tested.** `secret://` resolution is still missing; batching is not implemented. |
| **P4 - Acceptance** | Execute AC1-AC10 and Device/node coexistence, stability, and recovery tests | **Open.** Do not infer completion from unit tests alone. |

## 10. Required Acceptance Cases

These are acceptance tests, not statements of completed E2E validation. Unit
tests with fake connections cover several mechanics, but every case still
requires its stated external-system validation. AC10 also requires the missing
secret-resolution implementation.

| Case | Procedure | Expected result |
|---|---|---|
| **AC1: Node-level creation** | Push `connector_type=edge-export-mqtt` with `device_id=""` and valid config targeting a test broker | The edge accepts the spec, reports APPLIED, and includes its `spec_id`, revision, and config hash in the manifest. |
| **AC2: External export** | Produce a matching device-virtual event after AC1 | The test broker receives the filtered message with the expected JSON payload and rendered topic. |
| **AC3: Southbound coexistence** | Run `nereid-edge` with a non-empty `device_id` beside `edge-export-mqtt` with an empty `device_id` | Both operate independently; telemetry reaches NereidLink and the external broker; the manifest contains both instances. |
| **AC4: Hot update** | Update `topic_template` and bump revision | The new topic takes effect, the old runtime stops, state returns to APPLIED, and no stale exporter remains. |
| **AC5: Delete** | Delete the node-level spec | The runtime stops, the broker receives no new messages, the manifest entry disappears, and resources are released. |
| **AC6: DEGRADED/FAILED** | Use an unreachable broker, then provide syntactically invalid config | Connection failure reports DEGRADED with backoff; invalid config reports FAILED with a safe, actionable reason. |
| **AC7: Recovery** | Restore broker availability after AC6 | Reconnect succeeds and status returns to APPLIED without manual recreation. |
| **AC8: Hello redrive** | Restart the edge or force hello redrive | Equal revision/hash specs do not create duplicate connections; missing/FAILED runtimes self-heal; manifest state converges. |
| **AC9: Multiple instances and conflicts** | Run several node-level specs with different `spec_id` values | Instances operate independently and do not enter Device-name conflict handling. |
| **AC10: `secret://`** | Set `password` to `secret://test-mqtt-pass` and rotate the underlying secret | The resolved credential connects successfully, rotation is picked up by the defined reconnect path, and logs never expose plaintext or the raw secret reference. |

Stress acceptance should additionally measure queue drops, reconnect status,
hot-swap behavior, and shutdown cleanup under sustained event volume.

## 11. Compatibility and Contract Notes

- **No connector wire-frame redesign:** `spec`, `delete`, and `status` retain
  their existing shapes. `device_id` remains `omitempty`; new capabilities are
  expressed through connector type and schema.
- **Hello schemas:** Self-contained schemas allow platform validation and can
  also support schema-driven console forms.
- **Different `device_id` semantics:** Device-level connectors bind a
  NereidLink sub-device; node-level connectors represent gateway-wide
  capability. Type-based routing keeps those semantics explicit.
- **Canonical config hashes:** Device and node specs use the same SDK
  canonicalization contract so manifest drift detection is consistent.
- **No delivery guarantee upgrade:** The export side channel does not add
  persistent queuing, exactly-once delivery, or NereidLink Store-and-Forward
  semantics.

## 12. Governed Connectors Positioning and Follow-Up

The connector mechanism itself is not unique: declarative reconciliation,
protocol adapters, and gateway integrations are established industry patterns.
The potential differentiator is the combination of declarative governance,
schema-driven extension without a platform protocol change, canonical
edge/platform contracts, and explicit security boundaries. The value is
governance and safety, not merely connector count or throughput.

### 12.1 Existing foundations

| Foundation | Current status | Evidence and limit |
|---|---|---|
| Platform-neutral extension | A new type requires an edge handler and an advertised schema; config remains opaque to the platform kernel | Demonstrated by `edge-export-mqtt`; each new type still needs real implementation and acceptance. |
| Canonical contract and reconciliation | Shared SDK hashing, applied inventories, and manifest drift detection support convergence | See Sections 4.3 and 11. |
| Secret-delivery foundation | ADR-0034 provides encrypted-at-rest copies and redacted diagnostics for supported consumers | Generic `secret://` resolution for export is not delivered and must not be claimed as complete. |
| EdgeX as the execution plane | EdgeX App/Device Services can be governed without moving southbound business logic into NereidLink | The southbound and inference paths use this model. |

### 12.2 Recommended follow-up items

| # | Item | Purpose and ownership |
|---|---|---|
| **R1** | Add a deep Modbus TCP or OPC UA connector | Turn schema-driven extensibility into a second substantial catalog example. NE owns the handler; NereidLink owns any console/schema presentation. |
| **R2** | Schema-driven console forms | Use self-contained hello schemas as the basis for connector forms and future catalog packaging. NereidLink owns the console work. |
| **R3** | Automatic template-change propagation | Ensure a template change recomputes and repushes effective configs with an explicit revision contract instead of leaving instances stale. NereidLink owns merge/push behavior. |
| **R4** | Complete P3 gaps and P4 acceptance | Implement export `secret://` resolution, then execute AC1-AC10 against a real broker, including AC3 coexistence and recovery. NE owns this work. |
| **R5** | Maintain public design and examples | Keep this document, README, MILESTONES, and operator examples aligned with actual behavior and limitations. Both repositories own their respective documentation. |

### 12.3 Recommended order

1. Complete the remaining P3 credential behavior and R4 acceptance so the
   existing MQTT connector is verified rather than merely implemented.
2. Add R1 as a visible second example of governed connector extensibility.
3. Schedule R2 and R3 after the runtime and acceptance contracts are stable.

Positioning statement:

> **Governed Connectors:** Declare edge connectors, detect and repair drift,
> and extend capabilities through typed schemas and handlers without changing
> the platform connector wire protocol. Credential-resolution claims apply only
> after the corresponding consuming path has implemented and accepted them.
