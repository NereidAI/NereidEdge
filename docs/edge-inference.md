<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# Edge model inference bridge (ADR-0039)

NereidEdge (NE) is the **neutral protocol bridge** for the edge inference
plane. It never acquires model packages, never touches frames/ffmpeg and
never calls an inference runtime. All inference execution, package/media
handling, runtime readiness and job lifecycle live in the independent
**NereidInference (NI)** EdgeX Device Service (`service=device-inference`),
which NE treats exactly like every other Device Service (modbus, onvif, …).

Wire contract: NereidLink edge protocol v1 §15 (states, reason codes §15.6,
shadow block §15.5, `inference.diag` §15.7, alerts §15.8).

## Negotiation gate

Everything in this feature is gated on BOTH conditions:

1. the static config `Inference.Enabled: true`, AND
2. a **runtime existence probe** proving the NI plane (singleton manager
   device) is actually deployed on this node.

Only then does hello **offer** `inference` (contract + backend families) and
self-register the `edge-inference` connector. Runtime gating distinguishes
three deployment shapes:

| Shape | Detection | Behavior |
| --- | --- | --- |
| no NI installed | manager device `404` (definitive) | hello carries no `inference` block/feature and no edge-inference schema; pollers idle; shadow has no `inference` key; byte-identical to old firmware. NL shows no inference capability and the console offers no binding UI |
| NI present, temporarily unreachable | last good read exists, current read fails | session stays negotiated; `models[]` segment **omitted**, last good inventory cached (never `[]`); shadow `OFFLINE`; recovery resumes on the next poll |
| NI present, zero models | successful read of `[]` | explicit `"models": []` every manifest cycle; NL hard-prunes inventory with per-model audit; no stale alert (this is explicit zero, not silence) |

The presence probe is one core-metadata `DeviceByName(nereid-inference)`
before the first hello and every 30s thereafter:

- `200` → present; `404` (`KindEntityDoesNotExist`) → absent; any other
  metadata error is inconclusive and keeps the previous state (a metadata
  outage must neither withdraw a live plane nor register an absent one);
- **rising edge** (NI installed mid-session) triggers an immediate re-hello
  over the still-open MQTT session: the capability block and edge-inference
  schema register, NL re-drives desired state, the models segment appears
  without a process restart;
- **falling edge** (NI uninstalled mid-session) does not tear down the
  negotiated session; read-failure rules apply and the next transport
  (re)hello withdraws the advertisement.

Even with the feature enabled and present, everything additionally requires
the hello ack to carry the non-empty `inference` block with contract
`model/1`:

- the manager-device poller performs **zero** reads before negotiation;
- the manifest `models[]` segment is omitted when un-negotiated;
- the binding-status poller skips every edge-inference spec;
- `inference.diag` returns "not negotiated" if somehow invoked.

An older platform therefore sees byte-identical behavior to firmware without
the feature, apart from the optional-feature offer in hello.

## Configuration (`res/configuration.yaml`, `NereidEdge.Inference`)

| Key | Default | Meaning |
| --- | --- | --- |
| `Enabled` | `true` | Offer the `inference` feature in hello |
| `Backends` | `[kserve, openai]` | Protocol families in preference order; never vendor/silicon names |
| `DeviceService` | `device-inference` | NI EdgeX service name |
| `ManagerDevice` | `nereid-inference` | NI singleton manager device |
| `ModelsResource` | `ModelsJSON` | Model inventory String resource on the manager profile |
| `InventoryPollS` | `30` | ModelsJSON poll interval (manifest inventory) |
| `ShadowPollS` | `30` | Shadow freshness/`checked_at` refresh interval |
| `BindingPollS` | `10` | Per-job state/health poll and manager-resource retry interval |
| `ManagerStoreDir` | `/app/data` | Durable manager-resource identity/tombstone ledger; opaque definitions stay in NI |
| `RuntimeEndpoint` | `edgex-ovms:9000` | Display-only endpoint carried in the shadow block |

## Inventory bridge (`internal/inferencebridge`)

Polls the manager device through ordinary core-command reads every
`InventoryPollS`, with `ds-pushevent=false&ds-returnevent=true` so polls stay
off the message bus:

```
GET {CoreCommandURL}/api/v3/device/name/nereid-inference/ModelsJSON
```

The String reading is a JSON array mapped field-by-field to
`device.ManifestModel` (`id, version, revision, task, family, backend,
model_name, state, reason, labels, active_bindings`).

Failure semantics are deliberately conservative:

- read failure / non-200 / malformed payload → the cached inventory is kept
  in memory but **not provided** to the manifest builder, so the `models[]`
  segment is **omitted** (a transient outage can never prune the NL
  inventory); an explicit successful `[]` *is* authoritative and prunes to
  zero;
- every successful or failed read notifies the manifest/shadow on change and
  an immediate refresh is forced right after each (re)hello;
- the periodic full manifest still rides the existing 5-minute boundary
  loop, so inventory also converges without change notifications.

The three wire shapes are never conflated (§15.2):

| Segment in frame | Meaning |
| --- | --- |
| `"models"` key absent | not negotiated, NI absent, or this read failed — NL keeps stored inventory untouched |
| `"models": []` | authoritative zero inventory after a successful read — NL hard-prunes every row, one prune audit line per model, no stale alert |
| `"models": [...]` | full replacement; rows missing from the list are pruned |

`[]` is therefore produced as a non-nil empty Go slice end to end
(`bridge.Models()` sizes the copy explicitly; `BuildManifest` attaches only
non-nil provider returns), so the frame can never accidentally encode
`"models": null`.

### `active_bindings` is NE-authoritative

NI's `ModelsJSON` counts **every** Job device it hosts, including hand-built
test jobs and orphans left by crashed/old NE processes. NE overrides the
per-model number in the manifest provider from its live legacy `model_ref`
specs (grouped by id/version; a floating spec is attributed only when exactly
one installed version matches that id). Pipeline specs do **not** synthesize
per-model counts in this override: NE transports only their `PipelineId`, even
though NI can attribute its own resolved pipeline model references internally.

The shadow block's `active_bindings` is instead the total of all live
NE-owned edge-inference specs, legacy and pipeline alike. FAILED bindings
still exist and count; broken specs whose Job is missing do not.

### Orphan-job reconciliation

After every (re)hello/process start a 30-second replay-grace window lets NL
re-drive desired state; then, throttled to one metadata scan per 2 minutes
and piggy-backed on manifest reconciliation, NE lists Job devices under
`device-inference` and deletes any carrying an NE ownership tag
(`nereid-spec`) whose spec is no longer applied. Its virtual sub-device
presence is withdrawn through the same detach path as an explicit unbind.

- a Job **with** `nereid-spec` whose spec is gone → swept;
- a Job **without** ownership tags (hand-built/test leftovers, e.g.
  `inference-yolov8n-*`) is **never** deleted automatically: NE cannot prove
  ownership. Such jobs do not reach the cloud under `ForwardUnmapped:false`
  and do not count in `active_bindings`; remove them manually if needed.

> EdgeX note: EdgeX Odesa 4.0.2 keeps the `/api/v3` device-command route
> (`go-mod-core-contracts` v4 `ApiBase`); there is no `/api/v4` core-command
> route in this release.

## `edge-inference` connector (`internal/connector`)

The connector is self-advertised with `type_version=1` and a self-contained
draft 2020-12 schema (no external `$ref`). `edge-manager-resource` shares the
inference advertisement gate; `edge-northbound-filter` is advertised
independently. The inference schema has two mutually exclusive desired forms.
The legacy form requires nested `source` + `model_ref` and forbids
`PipelineId`. The expanded pipeline form requires `PipelineId`, forbids
`model_ref`, and rejects any non-empty legacy nested `source` values. Omitting
both selection forms is invalid.

Legacy single-model config:

```json
{
  "source":   {"device": "<camera sub-device UUID>", "resource": "snapshot_jpeg", "mode": "snapshot"},
  "model_ref": {"id": "yiqisoft-yolov8n", "version": "1.0.0", "revision": "r2"},
  "trigger": "event",
  "period_ms": 1000,
  "max_idle_ms": 60000,
  "error_threshold": 5,
  "conf_threshold": 0.4,
  "target_device": "GPU",
  "decode_accel": "vaapi",
  "vaapi_device": "renderD129"
}
```

### Expanded NL pipeline binding

The neutral NL → NE transport uses NI's case-sensitive top-level property
names. NE decodes only this binding envelope; graph definitions and emitted
reading values remain opaque.

```json
{
  "PipelineId": "face-recognition-v1",
  "PublishMode": "all",
  "PublishReadings": ["PipelineJSON", "InferLatencyMs"],
  "SourceKind": "onvif-camera",
  "SourceDevice": "<camera sub-device UUID>",
  "SecretName": "optional-edge-secret",
  "StreamProfile": "main",
  "DecodeAccel": "vaapi",
  "VAAPIDevice": "renderD129",
  "TargetDevice": "GPU.0",
  "FPS": 5,
  "trigger": "continuous",
  "period_ms": 1000,
  "max_idle_ms": 60000,
  "error_threshold": 5,
  "conf_threshold": 0.4,
  "ffmpeg_extra": "-fflags nobuffer -flags low_delay"
}
```

| Expanded field(s) | Transport to the NI Job |
| --- | --- |
| `PipelineId` | Trimmed and validated as `^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`, then forwarded as the string property `PipelineId`. `PipelineID` and JSON-document spelling `pipelineId` are not binding-property aliases. Pipeline Jobs never receive `ModelID` or `ModelVersion`; model selection belongs to the registered graph. |
| `PublishMode` | Optional `all`, `nonempty`, `none`, or empty. A non-empty value is forwarded; absent/empty is omitted so NI defaults to `all`. This controls NI's edge-local bus emission, not NL upload. |
| `PublishReadings` | Optional unique string array. Entries are trimmed and must match `^[A-Za-z][A-Za-z0-9_]{0,63}$`; duplicates after trimming are invalid. NE joins the array into NI's CSV string (for example, `PipelineJSON,InferLatencyMs`). A null/omitted array omits the property; an empty array forwards an empty string. |
| `SourceKind` | `onvif-camera` (default), `onvif-snapshot`, `rtsp`, `rtsp-udp`, `mpjpeg`, or `jpeg`. `none` is explicitly rejected for pipelines because they require image input. |
| `SourceDevice` / `SourceURL` | ONVIF kinds require `SourceDevice`, an attached NL camera UUID that NE resolves to the EdgeX device name. Direct kinds require a trimmed `SourceURL` and use the same scheme, host, length, and no-userinfo validation as legacy `mode=url`; no `SourceDevice` is emitted. |
| `SecretName` / `StreamProfile` | Trimmed and passed through after the same safe-token checks as legacy sources. An expanded ONVIF stream with no explicit secret uses the camera's mapped onvif-basic replica. |
| `DecodeAccel` / `VAAPIDevice` | Passed through as NI string properties. These are the expanded counterparts of legacy `decode_accel`/`vaapi_device`; NI remains the authoritative source/device validator. |
| `TargetDevice` / `ServableID` | `ServableID`, when present, is passed through for an external-servable reference. A managed pipeline with no target gets `TargetDevice=CPU`. With `ServableID` and no target, NE instead **omits** `TargetDevice` rather than inventing CPU; NL also rejects an explicit target on an external-servable binding. |
| `FPS` / `trigger` | An omitted pipeline trigger defaults to `continuous` (legacy defaults to `event`). Continuous Jobs forward `FPS` (0–60 in the schema) as a string, defaulting to `5` when omitted or non-positive. Periodic/event mapping and the shared period, idle, error, and confidence fields are otherwise unchanged. |

Pipeline Jobs always use profile **`nereid-inference-pipeline`**, not
`nereid-inference-detection` and not a configured legacy profile override. NI's
profile exposes `PipelineJSON`, `InferLatencyMs`, `State`, `LastError`,
`SuccessTotal`, `ErrorTotal`, `P50Ms`, `P95Ms`, and writable `Trigger`.
Detection aliases and evidence-JPEG defaults do not turn this into a detection
Job; in particular, NE does not add default `EmitFrame` or `AnnotatePreview`
properties to a pipeline binding.

### Legacy nested-source behavior

`source.mode` is optional (`snapshot` | `stream` | `url`, absent =
`snapshot`); `source.profile` optionally names an ONVIF media ProfileToken
(main/sub/third stream) for stream mode only, absent = camera default.
`mode=url` is the direct-URL escape hatch for cameras without an ONVIF
registration: it carries `url` (≤1024), `transport`
(`rtsp`|`rtsp-udp`|`mpjpeg`|`jpeg`) and the optional `secret_name`
(≤128, `^[A-Za-z0-9_.:,-]+$`) of an **edge-provisioned** NI secret, and no
`device`/`resource` (NE skips the binding resolver entirely). The URL scheme
must match the transport (`rtsp://` for rtsp/rtsp-udp, `http(s)://` for
mpjpeg/jpeg) and embedded userinfo is rejected with `config-invalid` — the
URL is stored in the platform (so ONVIF modes remain preferred) but no
password ever transits the config.
`trigger` is `event` | `periodic` | `continuous`; `continuous` carries
`fps` (0.1–60, default 5). All capabilities are advertised in this
self-registered schema, so older platforms simply never send them (the
source object forbids additional properties). The selectable profile
tokens are enumerated by the read-only `camera.profiles` v1 gateway
operation (`{source: <camera sub-device UUID>}` →
`{profiles:[{token,name,encoding,width,height,fps}]}`, read risk, no
step-up); NE resolves the camera through the binding resolver and reads
core-command `MediaProfiles` (the device-onvif-camera 4.x resource name;
fallbacks `GetProfiles`/`Profiles`/`Media2Profiles` — the parser tolerates
both full Media1 documents (`VideoEncoderConfiguration` carries
resolution/rate) and slim Media2 lists). Tokens must match
`^[A-Za-z0-9_.:,-]+$` (≤64); a profile on a snapshot binding or an unsafe
token is rejected with `config-invalid`.

`source.device` is resolved through the existing `binding.Resolver` to the
ONVIF camera's EdgeX device name. If that camera sub-device is not attached
yet, the spec fails with `config-invalid` and no EdgeX device is created.
The failed instance stays out of the observed manifest, so NL's
desired/observed reconciliation re-drives the spec once the camera has
attached (or on a spec revision). **Ordering dependency:** the camera
sub-device must be attached before the edge-inference spec.

Legacy single-model translation to the NI Job device:

| EdgeX property | Value |
| --- | --- |
| service / profile | `device-inference` / `nereid-inference-detection` |
| device name | `inference-<specID>` with every non-`[A-Za-z0-9-_.~]` rune → `-` (EdgeX names forbid `:`) |
| `SourceKind` / `SourceDevice` / `SourceURL` | resolved from `source.mode`: absent/`snapshot` → `onvif-snapshot` + managed camera EdgeX name; `stream` → `onvif-camera` + camera name (NI federation resolves RTSP locally via core-metadata/`StreamUri` read and the camera's own EdgeX secret, then ffmpeg); `url` → the chosen direct kind (`rtsp`/`rtsp-udp`/`mpjpeg`/`jpeg`) + `SourceURL=<trimmed url>` and **no** `SourceDevice` (resolver skipped). Unknown mode/transport, scheme/transport mismatch, missing host, >1024 chars, or embedded userinfo → `config-invalid`. A dead feed reports `source-no-frame` from the DS, never a silent acquisition fallback. `source.profile` (stream only) → NI `StreamProfile`: federation calls the read-only `StreamUri`/`GetStreamUri` resource with a base64 `jsonObject` query carrying `{ProfileToken, StreamSetup}` (device-onvif-camera 4.x rejects PUT); main/sub resolve and cache independently |
| `ModelID` / `ModelVersion` | `model_ref.id` / `model_ref.version` (empty = floating latest) |
| `TargetDevice` | `target_device` passed through verbatim (string); absent/empty → `CPU`. The DS normalizes case and validates the value (`CPU`, `GPU`, `GPU.0`, `NPU`, …); NE never guesses or silently remaps a requested target |
| `Trigger` | `trigger=periodic` → `periodic` + `PeriodMs`; `event` → `command` (NI event-bus wake is not implemented; do not invent a period); `continuous` → `continuous` + `FPS` (default 5). Unknown trigger → `config-invalid` |
| `FPS` | continuous jobs only: `fps` as a string scalar, default `5`; absent for other triggers |
| `MaxIdleMs` / `ErrorThreshold` / `ConfThreshold` | passed through (string scalars; NI job profile requires strings) |
| `EmitFrame` / `AnnotatePreview` | `emit_frame`/`annotate_preview` booleans serialized as strings; absent config fields default both to `true` so pre-upgrade bindings stay byte-identical |
| `DecodeAccel` | `decode_accel` passed through as a string: `software` or `vaapi`; absent/empty omits the property and preserves software decoding for legacy bindings. VA-API is valid only for ONVIF RTSP (`mode=stream`) and direct `rtsp`/`rtsp-udp`; NI authoritatively rejects other source combinations. NE never rewrites VA-API to software |
| `VAAPIDevice` | non-empty `vaapi_device` passed through verbatim as a string; both `renderD129` and `/dev/dri/renderD129` are accepted by the strict schema pattern `^(?:/dev/dri/)?renderD\d+$`. Absent/empty omits the property so NI uses its `/dev/dri/renderD128` default. NE does not add paths or defaults. A device supplied without `decode_accel=vaapi` is still forwarded for NI to reject explicitly |
| `FFmpegExtra` | ffmpeg-backed sources (`mode=stream`, or direct `rtsp`/`rtsp-udp`/`mpjpeg`/`jpeg` via `mode=url`): `ffmpeg_extra` trimmed and passed through (≤256 chars). Allowlist enforced authoritatively by the DS (`-re`, or `-r/-ss/-t/-threads/-fflags/-flags` each with a value); invalid tokens fail the job with `config-invalid`. Never sent for snapshot jobs |
| `SecretName` | **stream jobs**: the onvif-basic secret name mapped to the camera MAC (`AppCustom/CredentialsMap`, fallback `AppCustom/DefaultSecretName`). Snapshot jobs never carry this property, so pre-stream bindings stay byte-identical. **Direct-URL jobs (`mode=url`)** instead carry `source.secret_name` verbatim: the name of a secret the operator provisioned directly into NI's secret store (username/password); NI injects it into the URL. No URL password is ever translated through the cloud config |
| tags | `local: true`, plus `nereid-spec`/`nereid-device` correlation — presence attaches immediately, no health probe |

`decode_accel` and `vaapi_device` configure only the site-local acquisition
pipeline. They are independent of `FFmpegExtra` and never enter the model
package or model metadata. VA-API requires suitable host drivers and access to
`/dev/dri`; an unsupported source, missing device, or unavailable driver makes
NI fail explicitly, with no automatic software-decoding fallback.

Changing `target_device`, `decode_accel`, or `vaapi_device` changes the
canonical config hash, so the spec revision upserts the EdgeX job device. The
DS rebuilds the job on `UpdateDevice`; a target change also leases a different
OVMS servable.

### Legacy served name vs package `model_name`

The KServe request does **not** use the package's `runtime.model_name`
directly. The DS derives one OVMS servable per
`(model id, model version, TargetDevice)` and generates the served name for
the KServe call from that triple. Also note the package's OVMS numeric
model directory (typically `1`) is an OVMS layout convention, not the
Nereid semantic version (`model_ref.version`): do not confuse the two.

### Legacy binding availability vs model inventory

`ModelsJSON` state `READY` means a package is installed and validation
passed; it no longer proves a servable is loaded on a particular target.
Whether a binding is actually usable on its requested target is reported by
the Job device: `APPLIED` means success, while an OVMS load/ready failure on
that target (e.g. a CPU-only artifact requested on GPU) must surface as
`FAILED` — ideally reason `target-device-unavailable` (§15.3/§15.6) — and is
never treated as success or silently rewritten to CPU.

> **NI dependency (known gap):** NE passes the target verbatim and its reason
> sanitizer already whitelists `target-device-unavailable`, so a DS that
> emits it reaches the cloud unchanged. The NereidInference build currently
> emits `config-invalid` (selector string rejected by `NormalizeTargetDevice`)
> or `model-not-ready` (OVMS readiness bool) instead of distinguishing
> "target device missing/unavailable"; KServe ModelReady itself returns only
> a bool, so NE cannot synthesize the finer reason without guessing. Producing
> `target-device-unavailable` at the source is an NI-side change.

The generic upsert/ledger/resolver/rename/delete paths are reused; after
creation `resolver.SetSpec(specID, jobName, childUUID)` makes NI bus events
land on the NL virtual output sub-device, which then connects and appears in
manifest `devices[]`. No NL device is created by NE (the console wizard
already created the child).

**Unbind convergence.** A `connectors/delete` frame deletes the NI Job and
withdraws the virtual output sub-device end to end: the local-only presence
state is tracked from the optimistic attach (so detach works even though
local devices are never probed), the remembered `device_id` drives the
detach even when resolver state was cleared by a reconnect, and the resulting
path publishes `v1/gateway/connect` disconnect and removes the UUID from the
managed set — the next manifest `devices[]` (full or debounced) no longer
contains it. Post-reconnect reconciliation additionally sweeps orphaned
NE-owned Jobs (see "Orphan-job reconciliation" above).

### Correlating NL and EdgeX device names

The mapping is deterministic and survives an NE restart:

| Identity | Value |
| --- | --- |
| EdgeX Job device name | `inference-<connector spec UUID>` (UUID hyphens are EdgeX-name-safe, so the spec UUID is legible verbatim) |
| EdgeX tag `nereid-spec` | the NL connector spec ID |
| EdgeX tag `nereid-device` | the NL virtual output sub-device UUID (`spec.device_id`) |
| EdgeX label set | `nereid`, `inference`, `edge-inference` |

Query from either side:

```bash
# EdgeX name -> NL sub-device
curl -s http://edgex-core-metadata:59881/api/v3/device/name/inference-<spec-uuid> \
  | jq '.device.tags["nereid-device"]'
# Find the EdgeX job for an NL sub-device UUID
curl -s 'http://edgex-core-metadata:59881/api/v3/device/all?limit=200' \
  | jq '.devices[] | select(.tags["nereid-device"]=="<subdevice-uuid>") | .name'
```

The runtime resolver additionally logs `attach edgex=<job> nl=<subdevice>` at
startup/spec application. `detections_json` and the `frame` blob point are
keyed to the sub-device, and `frame.blob_key`/blob object keys embed the
sub-device UUID.

### Stream credentials

The cloud delivers `onvif-basic` secrets only to device-onvif-camera (ADR-0034),
but NI's `onvif-camera` federation authenticates ffmpeg from **NI's own**
secret store under the job's `SecretName`. The edge bridges the same credential
without any URL/password entering a connector config, the NI job properties or
a log line:

1. **name resolution** — for a stream binding the applier resolves the secret
   name from the camera MAC via core-keeper
   `edgex/v4/device-onvif-camera/AppCustom/CredentialsMap` (CSV membership;
   deterministic precedence), falling back to `AppCustom/DefaultSecretName`;
2. **dual delivery** — every inference job carrying a `SecretName` (both
   federated `stream` jobs and direct-URL `mode=url` rtsp/mpjpeg/jpeg jobs)
   gets its secret POSTed **synchronously before the job device is upserted**,
   so NI never observes an unresolvable name. The same document is also
   re-posted by the debounced reconciler (apply/delete/onvif-redelivery/
   hello-redrive triggers), by a steady **30s replay cadence**, and for any
   name referenced by an NI Job device even when the NE snapshot does not
   include it. Only `username`/`password` cross services (no `mode`); the
   target is `http://edgex-device-inference:59992/api/v3/secret`. Idempotent
   replays log at DEBUG, the first delivery per name at INFO;
3. **job property** — `SecretName` lands on stream and direct-URL jobs (NI
   resolves rtsp and onvif-camera sources through the same named-secret
   injection path); snapshot jobs never get it and pre-stream bindings stay
   byte-identical;
4. **cleanup** — on unbind / stream→snapshot / mapping change, the NI replica
   is keeper-prefix-deleted (`edgex/v4/device-inference/Writable/
   InsecureSecrets/<name>`) only when no live NE inference binding (stream OR
   direct-URL) AND no NI Job device (including untagged hand-built ones)
   still references that name, and only after the reference count has stayed
   zero for a **60s prune grace window** — a delete-old/create-new update
   burst therefore never removes the secret at its zero-reference
   intermediate state, and a new reference re-posts immediately. Prune
   candidates are additionally restricted to names existing on the onvif
   source side, so foreign NI secrets (OpenAI keys, hand-provisioned RTSP
   secrets) are never touched;
5. **ordering** — a credential delivered after the binding resolves
   retroactively: the onvif binder success path notifies the reconciler,
   which retranslates and `UpdateDevice`s only the affected jobs (no spec
   revision, no churn when the mapping is unchanged);
6. **NI restart self-heal** — in non-secure mode NI's secret store is
   memory-only and a `docker restart edgex-device-inference` loses every
   replica. The 30s replay (plus the pre-apply Ensure on any later apply)
   repopulates every referenced secret, so jobs recover within tens of
   seconds without a manual POST.

A mapping miss or keeper/metadata hiccup is soft: the job keeps no
`SecretName`, the failure surfaces honestly as `source-no-frame` from NI, and
the next delivery/periodic pass heals it. A pre-apply delivery failure never
blocks the job upsert (the debounced pass and replay cadence retry it).

### Legacy source camera requirements by acquisition mode

The console offers a mode only when this gateway's **self-registered
schema** advertises it (`source.properties.mode.enum` for acquisition,
`trigger.enum` for `continuous`); firmware whose schema lacks the enum hides
the option and shows the "only supports snapshot acquisition" hint. The
capability is therefore unlocked by deploying firmware that advertises it
and letting the next (re)hello recompile the schema in NL.

| `source.mode` | Required camera capability | Failure when unsupported |
| --- | --- | --- |
| `snapshot` (default) | ONVIF `GetSnapshotUri` (device-onvif-camera `Snapshot` core-command) | `GetSnapshotUri ... ActionNotSupported` → job `FAILED/source-no-frame` |
| `stream` | ONVIF `GetStreamUri` with a usable RTSP feed plus the camera's EdgeX secret (URL/credentials resolved inside the NI federation, never sent to NL) | no usable frame in window → `FAILED/source-no-frame`; NE never silently downgrades to snapshot |
| `url` | nothing ONVIF: a reachable direct `rtsp://`/`http(s)://` URL and, if the camera requires auth, a NI secret provisioned edge-side under `secret_name` | bad scheme/transport, embedded userinfo, or >1024 chars → `config-invalid` at apply; unreachable/401 feed → `FAILED/source-no-frame` |

Check the chosen camera's Snapshot/StreamUri support through core-command
before binding. A camera currently usable for snapshots (e.g. one returning
`ActionNotSupported` on `GetSnapshotUri` but serving RTSP) becomes usable by
selecting `stream` rather than changing firmware. Cameras without any ONVIF
registration use `mode=url` below.

#### Non-ONVIF / non-compliant firmware: direct-URL escape hatch (`mode=url`)

The Hikvision DS-IPC-T12HV3-IA rig (V5.7.3) answers `GetStreamUri`/`StreamUri`
with `rtsp://.../Streaming/Channels/0?transportmode=unicast&profile=Profile_1`,
which ffmpeg rejects with `400 Bad Request` even with credentials, while the
standard `Channels/101` path answers `401` without and is expected to work
with the replicated credential. Cameras like this, and anything without an
ONVIF registration at all, bind through the direct-URL mode:

```json
"source": {
  "mode": "url",
  "transport": "rtsp",
  "url": "rtsp://192.0.2.10:554/Streaming/Channels/101",
  "secret_name": "cam-101-direct"
}
```

NE translates this to NI `SourceKind=<transport>` + `SourceURL` (trimmed) and
skips the camera binding resolver entirely (no `SourceDevice`). Rules:

- **URL only, no credentials.** Embedded userinfo (`rtsp://user:pass@…`) is
  rejected `config-invalid`; the URL is nevertheless platform-stored, so the
  managed ONVIF modes remain preferred whenever available.
- **Passwords stay edge-side.** `secret_name` references a username/password
  secret by name only; the platform stores the name, never the password.
  NE delivers exactly like a federated stream secret — pre-apply Ensure,
  debounced reconcile, 30s replay, 60s prune grace (see "Stream
  credentials" above) — by reading the named document from the edge
  secret-delivery region and POSTing username/password into NI's own
  store. Today the document must be deliverable there as the onvif-basic
  secret type (a generic direct-URL secret type is tracked as an ADR-0040
  follow-up). A wrong/missing secret surfaces honestly as
  `source-no-frame`/auth failure from NI, never as a fallback.
  Design/security rationale: ADR-0040 (edge-inference direct-URL source).
- scheme must match `transport` (`rtsp` → `rtsp://`; `mpjpeg`/`jpeg` →
  `http(s)://`); `rtsp-udp` is the UDP-transport RTSP variant. Length ≤1024.
- `ffmpeg_extra` is available on these ffmpeg-backed sources with the same
  DS-side allowlist as `mode=stream`; `profile` is rejected (`config-invalid`)
  because there is no ONVIF profile concept.

The earlier alternative — an untagged edge-local hand-built NI Job
(`SourceKind=rtsp`, `SourceURL=…`, `SecretName=<name>`,
`ForwardUnmapped:false`) — still works for one-offs, but its telemetry stays
off the cloud and it bypasses the NL lifecycle entirely; prefer `mode=url`
when cloud-managed status/events are wanted.

### Binding status

Every `BindingPollS` (10s), each applied edge-inference spec is polled for
`State` + `LastError`. When the state is readable, NE also reads the optional
`SuccessTotal`, `ErrorTotal`, `P50Ms`, and `P95Ms` resources exposed by both
current NI detection and pipeline profiles. State/reason mapping is:

| NI Job state | Connector status | reason |
| --- | --- | --- |
| `APPLIED` | `APPLIED` | empty |
| `DEGRADED` | `DEGRADED` | `LastError` (typically `idle-no-inference`) |
| `FAILED` | `FAILED` | `LastError` (`source-no-frame`, `model-not-ready`, `target-device-unavailable`, …) |
| DS unreadable | `FAILED` | `runtime-unreachable` |

`target-device-unavailable` is passed through verbatim once NI emits it
(see known gap above); NE never fabricates it from ambiguous KServe bools and
never falls back to CPU.

Only state/reason/stats changes are reported; a metrics-only change therefore
produces a fresh status, while an unchanged sample does not. All reasons pass
through a sanitizer that accepts only the §15.6 kebab-case vocabulary and maps
anything else to `runtime-error`, so DS error text, paths and addresses can
never reach NL.

NI owns the two-level error ladder (NE only relays): consecutive acquisition
or inference errors ≥ `error_threshold` (NE defaults to 5) put the job in
`DEGRADED`, and 6× that (30 consecutive) escalates to `FAILED`; one
successful frame returns it to `APPLIED`; the idle watchdog no longer
overwrites an error state. `LastError` always carries a §15.6 constant
(`source-no-frame` for a dead stream). The manager device's
`ObjectDiagnosticsJSON` free English text is never used as a reason.

Valid job metrics add an optional `stats` object to the existing
connector-status JSON frame on the unchanged MQTT topic:

```json
"stats": {
  "success_total": 18234,
  "error_total": 12,
  "p50_ms": 47.5,
  "p95_ms": 88.25
}
```

The JSON fields correspond respectively to NI's `SuccessTotal`, `ErrorTotal`,
`P50Ms`, and `P95Ms`; counters are unsigned integers and latencies are numbers
in milliseconds. Every field is independently optional. Read failures, missing
readings, negative/non-integral counters, invalid numbers, NaN, and infinity
are omitted rather than converted to zero, and an unavailable later sample
does not reuse a stale earlier value. `stats` itself is omitted when no valid
field remains, which keeps the legacy typed status path. Zero-valued counters
remain meaningful and are reported. When `SuccessTotal` is zero, NE suppresses
`P50Ms`/`P95Ms` as NI startup placeholders, not measurements; it does not
report startup zero latency as health evidence.

## Opaque NI manager arrays (`edge-manager-resource`)

This node-level connector (`type_version=1`, empty `device_id`) delivers one
identity into a manager resource whose String reading contains a JSON array of
objects. It is advertised only with the inference plane. NE interprets just
the envelope identity and, for pipelines, `version`; graph nodes, descriptors,
and every other definition field remain opaque JSON.

```json
{
  "manager_device": "nereid-inference",
  "resource": "PipelinesJSON",
  "pipeline_id": "face-recognition-v1",
  "definition": {
    "version": "1.0.0",
    "nodes": [{"id": "source", "kind": "source"}]
  }
}
```

The schema requires `manager_device` + `resource` and exactly one identity
form. `manager_device`, `resource`, and `entry_id` are at most 128 characters;
`resource` must match `^[A-Za-z0-9_.-]+$`.

- `pipeline_id` selects the pipeline shorthand, with `id_field=pipelineId`
  implicit (an explicit `id_field` may only be `pipelineId`). Both the id and a
  managed definition's mandatory string `version` must match
  `^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`.
- `entry_id` + `id_field` is the resource-neutral form for other symmetric
  tables, for example `entry_id=face-external`, `id_field=servableId`, and a
  runtime-only external-servable resource. `id_field` matches
  `^[A-Za-z][A-Za-z0-9_]*$` and is at most 64 characters.
- `definition` is an optional JSON object. If its identity field is present,
  it must agree with the desired identity; otherwise NE inserts that field.
  Unknown envelope fields, arbitrary manager devices, unsafe resource names,
  and ambiguous identity forms are `config-invalid`. `manager_device` must
  equal the configured `Inference.ManagerDevice` write boundary.

NI owns the resource semantics: `PipelinesJSON` is its complete accepted
pipeline table, replaced atomically only after full validation. Accepted
`pipelineId` + `version` definitions are immutable, including after deletion
and restart, and a Job resolves the latest accepted version of its
`PipelineId`. `ExternalServablesJSON` GET currently returns the merged
provisioned/runtime table with probe state, while PUT replaces only the runtime
table. Those asymmetric views are **not** safe for generic GET→merge→PUT:
NE therefore permits reference-only reads of `ExternalServablesJSON` but rejects
managed definitions and defensively blocks every PUT to that resource. Managed
external registration awaits an NI-owned runtime-only symmetric resource;
copying the merged status view back would convert provisioned rows into runtime
rows and make override deletion ambiguous. NE does not validate opaque entry
schemas.

### GET → merge → PUT → GET

For a managed definition on a symmetric resource, NE uses ordinary
core-command APIs:

```text
GET /api/v3/device/name/{manager_device}/{resource}?ds-pushevent=false&ds-returnevent=true
PUT /api/v3/device/name/{manager_device}/{resource}?ds-pushevent=false&ds-returnevent=false
GET /api/v3/device/name/{manager_device}/{resource}?ds-pushevent=false&ds-returnevent=true
```

The PUT body is `{ "<resource>": "<canonical JSON array>" }`: the array is a
JSON string, as required by the EdgeX String resource. Neither management
operation emits the opaque table onto the device-event bus.

NE decodes the complete array with JSON-number preservation, replaces or
inserts only the matching identity, preserves every non-target JSON value,
and PUTs the complete resulting array. A pipeline target is
`pipelineId` + string `version`; a generic target is its declared identity
field alone. Opacity is semantic rather than a promise to preserve the whole
array's original whitespace or object-key bytes.

The post-PUT GET is authoritative. Readback verification requires exactly one
owned target, the expected table size, and preservation of all non-target
entries independent of array order. NI may normalize the owned definition and
sort the table; NE accepts that normalization, records the normalized owned
entry as its baseline, and avoids a repeated PUT while that baseline remains
unchanged. Missing/invalid readbacks and mismatches are retryable `DEGRADED`
states (`readback-invalid`/`readback-mismatch`), not false `APPLIED` results.

A pipeline registration spec may advance its string version without changing
its spec id. NE retains the versions previously landed by that spec, so older
versions and unrelated entries survive an update and remain available for
rollback. Rolling back to an already accepted id+version does not rewrite its
immutable definition. Deleting the spec removes only versions that spec owns;
versions it never claimed and versions owned by other specs survive.

Without `definition`, the spec is **reference-only**: NE GETs the table and
reports `APPLIED` when a matching entry exists, or
`DEGRADED/manager-entry-missing` when none exists. It never invents a missing
definition and never PUTs during either apply or delete, so it cannot delete
the referenced entry. A managed→reference-only update releases that spec's
ownership metadata without changing NI, so a later delete remains non-writing.

Per-spec locks and durable per-spec revision high-water marks fence delayed
applies/deletes, including duplicates arriving after a successful delete,
unknown node-level deletes racing their first spec, and newer rejected desired
revisions. An explicit same-revision delete may retire a rejection without
removing that revision fence. Per-manager/resource locks serialize each
GET/PUT/GET transaction **inside one NE process**, preventing local concurrent
writers and stale retries from losing entries or resurrecting deletes. There is
no ETag/revision token or cross-process compare-and-swap: NI's PUT is an atomic
table replacement, but NE does not claim fenced read-modify-write across
multiple NE/NL writer processes.

### Durable replay and observed status

`Inference.ManagerStoreDir` (default `/app/data`) contains
`manager-resources.json`, an atomically replaced identity/lifecycle ledger. It
stores desired/tombstone identity, owned versions, revision high-water marks,
rejection state/reason, hash/status metadata, and delete state, but **never**
the opaque definition, observed entry content, config mappings, or health-check
document. Startup probes that the directory is actually writable before
advertising the connector. A desired revision that cannot be persisted does not
advance NE's applied manifest revision/hash. A rejected revision reports
terminal `FAILED/config-invalid` only after its high-water mark and reason are
durable; until then it reports retryable `DEGRADED/runtime-unreachable`. Every
successful full-ledger write promotes all still-pending rejections, and durable
terminal rejections are republished at least once after process reconstruction
or same-revision redrive.

A newer but **undurable** desired revision also fences older work in memory so
its retryable `DEGRADED` report cannot be overwritten by the lifecycle it
rolled back. No implementation can restore that observation after a process
restart when every write containing it failed. Restart ordering is therefore
based on the last committed ledger until NL redrives the newest desired. NL
must supersede and stop retrying an older delete outbox as soon as it creates a
newer desired revision for the same spec; NE does not claim to remember an
undurable desired revision across reconstruction.

Transient operations retry on the `BindingPollS` cadence; failed deletes remain
tombstones, and failed status publications remain dirty for republication.
Per-spec publication generations, current-desired status checks, committed
revision-only delete markers, and lifecycle identity checks prevent an older network
completion, delayed same-revision result, or recovered rejection from replacing
a newer pending report or publishing after its delete has retired that
lifecycle. Successful deletion also invalidates a pending failure report for
that same delete; an unrelated later ledger failure does not make an already
committed delete undurable again.

After process reconstruction, non-delete records await NL's desired redrive
because their definitions are deliberately absent from the ledger; only
tombstones may execute before that replay. **Redrive absence never authorizes
deletion.** Queue overflow, decode failure, invalid config, partial publication,
and NL outage are all indistinguishable from an offline deletion if absence
alone were used. Persisted entries therefore remain awaiting replay until an
explicit spec arrives, and only an explicit, revision-fenced delete removes
owned NI entries. NL must retain delete tombstones/outbox state and retry those
explicit deletes after reconnect so deletions made while NE was offline
eventually converge. Manifest absence alone is not a delete acknowledgement:
an NE record awaiting opaque redrive is also absent from the applied manifest.
Without a separate delete-ack contract, NL must retain and replay its delete
tombstones rather than retiring them from that absence signal.

An `APPLIED` manager status adds optional top-level
`observed_revision` (the connector desired revision confirmed by readback) and
`observed_hash` (lowercase SHA-256 of NE's canonical JSON encoding of the
complete GET array) to the same connector-status topic. The hash describes
actual normalized readback, not merely desired content; it is neither a
pipeline version nor a cross-process concurrency token.

## Northbound reading filter (`edge-northbound-filter`)

This independently advertised, node-level v1 connector owns the singleton
policy for the primary `v1/gateway/telemetry` publisher. A second owning spec
is rejected, avoiding ambiguous cross-spec rule order. Its config contains
optional `enabled`, `default_action` (`allow` by default), and ordered `rules`:

```json
{
  "enabled": true,
  "default_action": "allow",
  "rules": [
    {"action": "allow", "device_names": ["face-conclusions"],
     "resource_names": ["matched", "personId", "score"]},
    {"action": "deny", "device_names": ["face-conclusions"]},
    {"action": "deny", "profile_names": ["nereid-inference-detection"],
     "resource_names": ["DetectionCount", "FrameJPEG"]}
  ]
}
```

For each reading, empty selector lists match everything; otherwise all three
selector dimensions must match (device AND profile AND resource), with
`path.Match` patterns ORed within a list. The **first matching rule wins**;
if none matches, `default_action` applies. With no policy, non-guarded legacy
telemetry defaults to allow. `enabled:false` bypasses the rules but retains
the configured `default_action` (`allow` when omitted). Selectors use raw
EdgeX device/profile/resource names. This is a
whole-reading/resource whitelist, not a parser for fields inside JSON values.

Filtering runs **before** detection-resource normalization and before NL
device resolution, so rules must name `DetectionCount`/`FrameJPEG`, not the
later aliases `detection_count`/`frame`. If no readings survive, the complete
event stays off the uplink. These configurable rules do not control NI's
`PublishMode`/`PublishReadings` local-bus policy, nor the per-export
device/profile/resource selection of the auxiliary `edge-export-mqtt`
fanout; the mandatory privacy guard below is nevertheless enforced on that
fanout as well.

A mandatory NE-owned privacy guard runs before all configurable rules and
drops resource **`PipelineJSON`** and every resource whose name starts with
the case-sensitive prefix **`Embedding`**. It applies even when the filter spec is absent, disabled, or
contains an explicit `allow` for those names. Reading values are never decoded
for this decision. The same guard is enforced inside every
`edge-export-mqtt` fanout before its operator-configured `filter.resource_names`
selection, so an export allowlist naming `PipelineJSON` or `Embedding*` cannot
override it and an event whose readings are all guarded is never exported.
Consequently embedding-bearing pipeline facts remain on
the edge-local bus for companions and cannot be mirrored to NL or to any
external broker by policy;
only separately produced, explicitly allowed conclusion readings are eligible
for northbound transport.

## Runtime shadow block

NI exposes no aggregate runtime health block; NE derives `reported.inference`
(present only after negotiation) and refreshes it every `ShadowPollS`:

```json
"inference": {
  "state": "ONLINE|DEGRADED|OFFLINE",
  "endpoint": "edgex-ovms:9000",
  "protocol": "kserve",
  "models_ready": 1,
  "models_total": 2,
  "active_bindings": 3,
  "checked_at": "2026-10-26T08:00:00Z",
  "probe_interval_s": 30
}
```

- `OFFLINE` — manager device read failed (DS unreachable);
- `ONLINE` — DS reachable and at least one READY model, no DEGRADED binding;
- `DEGRADED` — reachable with zero READY models, or at least one DEGRADED
  binding;
- `protocol` — dominant backend family, `kserve` preferred over `openai`;
- `checked_at` is refreshed continuously; NL treats silence past
  `3 × probe_interval_s` as stale.

Phase A deliberately judges reachability from the manager-device read only;
`endpoint` is display-only and is not actively probed (to avoid false
downgrades).

## Detection telemetry (`internal/pipeline`)

Only events of profile `nereid-inference-detection` are renamed, so other
Device Services are unaffected:

| NI resource | detection/v1 point | type |
| --- | --- | --- |
| `DetectionJSON` | `detections_json` | string (payload already `nereid.detection/1`, passed through) |
| `DetectionCount` | `detection_count` | retagged Int32 → v_double |
| `Top1Label` | `top1_label` | string |
| `Top1Score` | `top1_score` | float |
| `InferLatencyMs` | `infer_latency_ms` | float |
| `FrameJPEG` | `frame` | binary media marker |

### Evidence JPEG via binary media marker

The annotated result JPEG is **not** sent as an inline base64 string point.
After renaming to `frame`, `readingsToFrame` wraps it in the NereidLink
*Binary Media and S3 Storage* marker and the frame is published raw on the
existing `v1/gateway/telemetry` topic (no new topic):

```json
{
  "<subdevice-uuid>": {
    "detection_count": 1,
    "detections_json": "{...}",
    "infer_latency_ms": 12.3,
    "frame": {"blob": true, "content_type": "image/jpeg",
              "filename": "<reading-id>.jpg", "data": "<base64 jpeg>"},
    "ts": 1792981234000
  }
}
```

NL decodes the marker under the sub-device identity, offloads the bytes to
its blob backend and stores the measurement as a `v_string`
`blob://media/<tenant>/<subdevice>/<uuid>` reference — the bytes never enter
the envelope, time-series rows or PostgreSQL bytea. The console downloads it
through the tenant blob sign URL. The marker field name **is** the final
measurement key (`frame`), aligned with the ADR-0039 `frame.blob_key`
vocabulary. Frames containing a marker are encoded locally with
`PublishRaw`; scalar-only frames keep the typed SDK `PublishTelemetry` path.
The edge never parses or rewrites `detections_json`, and the server
generates the object key, so the edge cannot populate
`detections_json.frame.blob_key` before sending; evidence and detection
detail are correlated by the same device + `ts`.

The marker applies to **any** Binary reading (not only inference events);
an empty Binary reading is skipped with a warning. When `FrameJPEG` omits
its media type, the content type falls back to `image/jpeg`; the display
filename is derived from the EdgeX reading UUID or the event origin time,
never from user-supplied paths.

## `inference.diag`

Registered as a read-risk request-response operation (30s deadline). Params
`{model_ref:{id,version}, source?}` return
`{reachable, model_ready, infer_ok, latency_ms?, frame_source:"snapshot_jpeg",
reason?}`:

1. `reachable` — a forced live manager-device read succeeds;
2. `model_ready` — the cached inventory entry is READY;
3. binding lookup by model (`source` narrows on `source.device`);
4. best-effort synchronous probe: PUT `Trigger` once, then poll `State` /
   `LastError` / `InferLatencyMs` for up to ~8s; failure reasons use the
   §15.6 vocabulary. Jobs are asynchronous, so "triggered but no fresh
   result within the window" returns `infer_ok=false`,
   `reason=source-no-frame`.

No new MQTT topic is used: everything rides the existing operations
invoke/result frames and core-command. This remains a legacy `model_ref`
diagnostic; it does not locate a `PipelineId`-only Job by the graph's internal
model references.

## Current limits / follow-ups

- **Event-bus wake**: NI does not implement bus-triggered wake; NL `event`
  trigger maps to NI `command` (manual Trigger PUT), used by `inference.diag`.
- **Runtime aggregation**: per-job counters/latencies now ride optional
  connector-status `stats`, but shadow reachability still derives from manager
  reads and `P50Ms` is not aggregated into shadow `latency_ms`. NI still has
  no aggregate `StatusJSON` runtime resource.
- **Generic node lifecycle**: the manager's durable revision/rejection fences do
  not cover `edge-export-mqtt` or `edge-northbound-filter`. Their generic
  node-runtime path still lacks a persistent high-water mark for an unknown
  type-less delete racing a delayed spec. That separately owned lifecycle gap
  is not hidden by the manager guarantees; the mandatory raw-resource privacy
  guard remains enabled regardless of filter-policy lifecycle state.
- **Revision pinning**: legacy `model_ref.revision` is advertised in the schema
  but NI currently selects single models by id/version only. Pipeline versions
  instead follow the NI registry's immutable id+version/latest-accepted rules.

## Cross-product sign-off and production gate

The NE implementation and its unit tests establish the transport behavior
above; they are not a cross-product production acceptance claim.

- **NI final sign-off** owns the authoritative registry and runtime contract:
  atomic validated table replacement, immutable id+version history, latest
  accepted-version resolution, pipeline/detection profiles and resources,
  readback normalization, job state/statistics, and real model/runtime
  validation. Managed external-servable delivery additionally requires an
  NI-owned runtime-only symmetric resource; the current merged
  `ExternalServablesJSON` status view is reference-only at NE. NE does not infer
  graph, decoder, model, or media semantics.
- **NL final sign-off** must map and actually deliver its declarative manager
  artifacts through `edge-manager-resource`, including generic
  `entry_id`+`id_field` identities on symmetric resources. It must persist and
  retry explicit versioned deletes across NE offline periods, deliver
  northbound policy and forwarder configuration, and retain
  preflight/dependency/rollback ownership. NL must also implement the readers
  that consume optional job `stats` and manager
  `observed_revision`/`observed_hash` status fields rather than treating an
  EdgeX command acknowledgement as end-to-end health.

**Production NL → NE → EdgeX → NI end-to-end validation has not been run.**
It remains blocked on NL manager-resource delivery and durable delete retry,
NL northbound delivery, NL optional-stats readers, and the final NI sign-offs
above. Managed external-servable E2E additionally waits for NI's runtime-only
resource and execution contract. NE unit tests and NI synthetic pipeline tests
are not evidence that this production path has executed.

## Operational pitfalls (lessons learned)

- **EdgeX multi-status (HTTP 207) validation rejections are silent at the
  SDK level.** When a device service's `AddDevice`/`UpdateDevice` validation
  callback fails (e.g. NI rejecting an allowlisted `ffmpeg_extra`),
  core-metadata answers overall 207 with a non-2xx **per-item** status and
  `go-mod-core-contracts` returns a nil error. Code that only checks the
  transport error reports a false APPLIED. Rule: always inspect every
  `BaseResponse.StatusCode` in the Add/Update response slice; a 3xx/4xx/5xx
  item is a rejected desired document, reported to the cloud as
  `config-invalid` (never `runtime-unreachable`). Files:
  `internal/connector/applier.go` (`deviceAdd`/`updateDevice`,
  `errDeviceRejected`), test `inference_reject_test.go`.
- **NI's non-secure secret store is memory-only.** `POST /api/v3/secret`
  merges into memory only; a `docker restart edgex-device-inference` loses
  every secret and GET config never lists them by design. NE therefore
  re-posts every referenced secret on a 30s cadence and synchronously
  before each job upsert; never assume a one-time manual POST survives an
  NI restart. Related: NI resolves `SecretName` for BOTH `SourceKind=rtsp`
  and `onvif-camera` through one injection path, so pruning must count
  references from direct-URL bindings too, and only after a 60s
  zero-reference grace (a delete-old/create-new update transiently shows
  zero refs). Files: `internal/inferencecred/cred.go`
  (`Ensure`/`Reconcile`), `internal/connector/inference_handler.go`
  (pre-apply delivery, replay ticker).
- **device-onvif-camera 4.x names its profile resources differently from
  the ONVIF operations.** The read resources are `MediaProfiles` (full
  Media1 documents) and `Media2Profiles` (slim Media2 list), not
  `GetProfiles`/`Profiles`; its `StreamUri` resource is read-only (PUT →
  405) and ignores `?ProfileToken=`, so profile-scoped URI requests need
  DS-version-aware command shapes. `camera.profiles` tries
  `MediaProfiles → GetProfiles → Profiles → Media2Profiles`
  (`internal/cameraprofiles`).

