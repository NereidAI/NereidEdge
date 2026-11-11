<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# NereidEdge

NereidEdge is an EdgeX Foundry Application Service, built with App Functions
SDK Go v4.0.2, that connects EdgeX southbound devices and companion services to
NereidLink through the MQTT gateway protocol `v1/gateway/*` (ADR-0026).

The service owns protocol bridging, reconciliation, and edge-local lifecycle
management. It does not run inference itself: NereidInference remains an
independent device service, and NereidEdge bridges its inventory, bindings,
state, and credentials when that integration is enabled.

## Implementation status

The gateway, connector, profile, secret-delivery, and inference bridges are
implemented and covered by repository unit tests. Real-broker MQTT export,
cross-repository profile acceptance, and the complete production inference
chain still require the end-to-end acceptance work tracked in
[MILESTONES.md](MILESTONES.md). A checked implementation milestone does not
imply current production or hardware acceptance.

## Features

- **Telemetry and presence**: Converts EdgeX Events/Readings, including binary
  media markers, to NereidLink gateway telemetry; maps EdgeX device names to
  NereidLink sub-device UUIDs; connects devices on first telemetry; withdraws
  presence on detach or shutdown; and sends a full manifest every five minutes.
- **Governed connector lifecycle**: Applies device- and node-level connector
  specs through typed handlers, schema advertisements, revision deduplication,
  ownership and conflict checks, status reports, and manifest reconciliation.
  Out-of-band EdgeX device deletion is repaired during reconciliation.
- **Node-level MQTT export**: Fans events out from the default pipeline to
  independently managed `edge-export-mqtt` instances with resource filters,
  JSON/raw payloads, topic templates, authentication/TLS, bounded queues,
  reconnect backoff, hot swap, and graceful deletion.
- **Device control and health**: Handles direction-aware commands, on-demand
  probes, global and per-instance health checks, platform-managed attributes,
  the gateway's reported shadow, and live `log.level` updates.
- **Gateway operations and link health**: Provides allowlisted administrative,
  network, diagnostic, and log operations; reports closed link episodes and
  handles acknowledgements; distinguishes planned goodbye from unplanned
  outages; and replays subscriptions and state after reconnect.
- **Bidirectional device profiles**: Lands negotiated ADR-0038 profile
  deliveries with durable revision ledgers and ownership fencing, and publishes
  a minimal descriptor projection of local EdgeX profiles after negotiation,
  on changes, and during periodic scans.
- **Secret delivery**: Stores ADR-0034 secret copies encrypted at rest,
  reconciles revisions and removals, delivers ONVIF credentials and camera
  bindings, and fans stream credentials out to NereidInference without placing
  them in connector specs or logs.
- **Edge inference bridge**: Bridges model inventory, model and expanded
  pipeline bindings, camera profiles, manager-owned resources, binding status,
  inference shadow state, optional job statistics, and detection telemetry.
  Northbound privacy filters apply to both NereidLink and auxiliary MQTT egress.
- **Operations baseline**: Exposes EdgeX service health and metrics endpoints,
  bounded container logs, rotated local log capture, and redacted diagnostic
  log tails.

## Architecture

```text
EdgeX devices/services ---> EdgeX message bus ---> NereidEdge ---> NereidLink MQTT
                                                         |
                                                         +---> auxiliary MQTT brokers

NereidLink downlinks ---> NereidEdge ---> EdgeX core services / NereidInference
NereidInference state ---> NereidEdge ---> NereidLink manifest/shadow/telemetry
```

Key packages:

- `cmd/nereid-edge/`: Process composition, protocol wiring, and replay.
- `internal/gateway/`, `internal/binding/`: Gateway protocol, device mapping,
  negotiation, manifest state, and binding ownership.
- `internal/connector/`, `internal/pipeline/`: Connector handlers, inference
  lifecycle, telemetry conversion, privacy filters, and MQTT export runtimes.
- `internal/profile/`, `internal/attributes/`, `internal/shadow/`: Bidirectional
  profile synchronization, platform attributes, and gateway shadow state.
- `internal/command/`, `internal/probe/`, `internal/healthmonitor/`: Device
  commands, probes, and reachability monitoring.
- `internal/secrets/`, `internal/onvifbind/`, `internal/inferencecred/`: Secret
  persistence and consuming-service credential delivery.
- `internal/inferencebridge/`, `internal/cameraprofiles/`: Neutral inference
  inventory/state projection and ONVIF camera-profile discovery.
- `internal/gatewayoperation/`, `internal/linkwatch/`: Whole-gateway operations
  and link-health episode tracking.

## Quick start

### Prerequisites

- Go 1.27.3; golangci-lint is also required by `make lint` and `make verify`.
- EdgeX Foundry 4.0.2 core services and the required device services.
- A reachable NereidLink MQTT broker and valid gateway credentials.
- The NereidLink device SDK is shipped as a reviewed in-repo mirror under
  `third_party/nereidlink`; no adjacent source checkout or private module
  credentials are required.

### Build and verify

```bash
# Build for the current platform.
make build-local

# Run formatting checks, go vet, lint, all Go tests, and a linux/amd64 build.
make verify
```

The `go.mod` replace directive points at the in-repo mirror, so a fresh
clone builds and runs its full test suite without any workspace setup or
external SDK checkout.

### Configure

Edit `res/configuration.yaml` under the `NereidEdge` section:

```yaml
NereidEdge:
  BrokerURL: "tcp://localhost:1883"
  CredentialID: "your-cred-id"
  CredentialSecret: "your-cred-secret"
  EdgeID: ""  # Optional: empty falls back to CredentialID.
  DeviceMap: "edgex-device-name:nl-subdevice-uuid"
  ForwardUnmapped: false
```

`DeviceMap` is a comma-separated static mapping. Static mappings take precedence
over connector-spec bindings; the resolver also enforces ownership and
name-conflict rules. `ForwardUnmapped: false` skips telemetry from devices
without a known mapping.

For persistent ADR-0034 secret copies, mount `/app/data` and provision a stable
32-byte `Secrets.KeyFile`. An empty key file uses an ephemeral key; the platform
must re-deliver the full secret set after a restart. Profile and inference
ledgers also use the data directory.

### TLS and certificate authentication

TLS is enabled by the broker URL scheme: use `tls://host:8883` instead of
`tcp://host:1883`. Three trust models are supported:

| Broker certificate | Authentication | Configuration |
|---|---|---|
| Public CA | id/secret | Only change `BrokerURL` to `tls://`; the image trust store verifies the server |
| Private CA | id/secret | Pin the CA PEM with `RootCAFile`/`NEREIDEDGE_ROOTCAFILE` (file) or `RootCAPEM`/`NEREIDEDGE_ROOTCAPEM` (inline) |
| Private CA / mTLS listener | X.509 client certificate | Pin the CA and set the client cert/key together (`ClientCertFile`+`ClientKeyFile`, or inline `ClientCertPEM`+`ClientKeyPEM`) |

`CredentialID` (the `NEREID_CRED_ID` / `NEREIDEDGE_CREDENTIALID` value) is
required in every row, including mTLS. It is the MQTT CONNECT username for
password auth and names the X509 credential for certificate auth, so it must
match a credential ID already registered in NereidLink; presenting a client
certificate without that registered ID is rejected. The service fails fast at
startup when it is empty. `CredentialSecret` stays empty in certificate mode.

The PEM files are read once at startup and a bad path fails the service before
it connects. With Compose, put the files in a host `certs/` directory, mount it
read-only (see `docker-compose.override.yaml.example`) and point the
`NEREID_*` variables from `.env.example` at the in-container paths. A
client-certificate pair must always be configured together and use the same
delivery mode. `InsecureSkipVerify` exists only for isolated lab brokers and
must never be enabled in production.

Each material also has an inline PEM alternative (`RootCAPEM`,
`ClientCertPEM`, `ClientKeyPEM`, overridable through `NEREIDEDGE_ROOTCAPEM` and
similar environment variables); inline PEM wins when both it and the file path
are set. File delivery is preferred: with `EDGEX_OVERWRITE_CONFIG=true` the
EdgeX config provider seeds env-overridden values into core-keeper, and
process environment is visible through `docker inspect` and
`/proc/<pid>/environ`. Inline PEM is intended for platforms that inject
secrets as environment variables.

**Writing inline PEM.** A PEM document is multi-line; each config carrier has
its own way of preserving the line breaks.

1. Compose YAML (`docker-compose.override.yaml`) — use a block scalar, which
   keeps the real newlines:

   ```yaml
   environment:
     - NEREIDEDGE_BROKERURL=tls://mosquitto:8883
     - NEREIDEDGE_CREDENTIALID=${NEREID_CRED_ID}
     - NEREIDEDGE_ROOTCAPEM: |
         -----BEGIN CERTIFICATE-----
         MIIB...CA bundle...
         -----END CERTIFICATE-----
     - NEREIDEDGE_CLIENTCERTPEM: |
         -----BEGIN CERTIFICATE-----
         MIIB...client cert...
         -----END CERTIFICATE-----
     - NEREIDEDGE_CLIENTKEYPEM: |
         -----BEGIN PRIVATE KEY-----
         MIIE...private key...
         -----END PRIVATE KEY-----
   ```

2. `res/configuration.yaml` — the same block scalar works under `NereidEdge`,
   or a double-quoted one-liner whose `\n` YAML converts into real newlines:

   ```yaml
   NereidEdge:
     RootCAPEM: |
       -----BEGIN CERTIFICATE-----
       MIIB...CA bundle...
       -----END CERTIFICATE-----
     # equivalent one-liner:
     # RootCAPEM: "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n"
   ```

3. `.env` — dotenv has no reliable multi-line syntax, so keep the PEM on one
   line with literal `\n` separators. The agent converts them to real
   newlines before handing the PEM to the TLS library:

   ```bash
   NEREID_ROOT_CA_PEM=-----BEGIN CERTIFICATE-----\nMIIB...CA bundle...\n-----END CERTIFICATE-----\n
   NEREID_CLIENT_CERT_PEM=-----BEGIN CERTIFICATE-----\nMIIB...client cert...\n-----END CERTIFICATE-----\n
   NEREID_CLIENT_KEY_PEM=-----BEGIN PRIVATE KEY-----\nMIIE...private key...\n-----END PRIVATE KEY-----\n
   ```

   Generate the one-liner from a PEM file with
   `awk 'NF {printf "%s\\n", $0}' ca.pem`. The certificate and key still have
   to use the same delivery mode (both inline or both file paths), and
   `NEREID_CRED_ID` remains mandatory.

   Both key PEM headers are accepted: `BEGIN PRIVATE KEY` (PKCS#8) and
   `BEGIN EC PRIVATE KEY` (SEC1, as emitted by `openssl ecparam`/`openssl genpkey`
   for EC keys). The TLS SNI/verification name is derived automatically from
   the `BrokerURL` host, so no separate ServerName setting exists.

**TLS troubleshooting.**

| Symptom in the log | Cause and fix |
|---|---|
| `CredentialID is required, including mTLS...` at startup | Even certificate auth needs `NEREID_CRED_ID`; it must match the X509 credential ID registered in NereidLink. `NEREID_CRED_SECRET` stays empty |
| `ClientCertPEM and ClientKeyPEM must be configured together` (or the `*File` variant) | Supply both halves of the pair through one delivery mode; mixing inline PEM and file paths is rejected |
| `read NereidEdge.*File: no such file` | A `*File` path points outside the container; mount the host PEM directory read-only and use the in-container path |
| `x509: certificate signed by unknown authority` | The broker CA is not in the pinned bundle or system store; set `RootCAFile`/`RootCAPEM` to the issuing CA PEM |
| `x509: certificate is valid for ...` | The broker certificate SAN does not match the hostname in `BrokerURL`; use the matching hostname or reissue the certificate |
| PEM parse / key pair errors after inline env delivery | The literal `\n` separators were mangled; regenerate the one-liner with the `awk` command above, or switch to file delivery |

This applies to the main NereidLink connection only. The optional
`edge-export-mqtt` auxiliary broker has its own `ca_cert`/`client_cert`
connector fields and is configured separately.

### Run

```bash
./nereid-edge
```

Run the binary from the repository root so it can find `res/configuration.yaml`,
or provide the normal EdgeX configuration-provider options. The service key is
`nereid-edge`, and its default HTTP port is `59750`.

## Docker and Compose

Build the multi-architecture OCI archive and a native image for local use:

```bash
# linux/amd64,linux/arm64 archive; this does not load or push an image.
make docker

# Native ghcr.io/nereidai/nereidedge:<version> tag for local Docker/Compose runs.
make docker-dev
```

Both targets are self-contained: the Dockerfile copies the SDK from the
in-repo `third_party/nereidlink` mirror and needs no named build contexts or
external checkouts.

The OCI archive is written to
`dist/images/nereidedge-<version>-multiarch.oci.tar`. Override `PLATFORMS` or
`IMAGE_ARCHIVE` when needed. The archive is neither loaded into Docker's local
image store nor pushed to GHCR. Docker's local image store cannot load the
multi-platform index directly; use `make docker-dev` for a local run.

Neither local Make target performs `docker login` or `docker push`, and neither
requires GHCR write permission. The full `ghcr.io/nereidai/...` coordinates are
the canonical image names embedded in the OCI archive or applied as local tags.
Actual GHCR publication is performed only by GitHub Actions using its
`packages: write` token permission.

The Compose file expects the external `edgex_edgex-network`, mounts `res/`
read-only, persists `/app/data`, pushes local configuration into core-keeper,
and rebuilds the native image on startup. Prepare operator values from the
template: the default `ENV_FILE` is the git-ignored `.env` in the repository
root, and `make up` passes `--env-file` only when that file exists, so a
fresh clone needs no external deployment directory.

```bash
cp .env.example .env
# Edit .env with the broker URL and gateway credentials, then:
make up
make logs
```

Use `.env.example` as the variable reference and keep credentials outside the
repository. To select another operator profile, set `ENV_FILE` explicitly,
for example `make up ENV_FILE=compose.k3s.env`. The base Compose file
consumes the broker and credential values; copy
`docker-compose.override.yaml.example` to `docker-compose.override.yaml`
when `NEREID_EDGE_ID` or `NEREID_DEVICE_MAP` environment overrides are needed.
See [docs/deployment.md](docs/deployment.md) for deployment details.

## Versioning, CI, and publication

There is no `VERSION` file. An exact `v*` git tag is the release version source
(`v0.0.1` -> `0.0.1`); every other checkout uses `0.0.0-dev`.

### Image build and publication runbook

| Flow | Trigger or command | Output | GHCR login/push |
|---|---|---|---|
| Local multi-arch package | `make docker` | One `linux/amd64,linux/arm64` OCI archive under `dist/images/`; three image tags are embedded in the archive | No |
| Local native image | `make docker-dev` | One local image named `ghcr.io/nereidai/nereidedge:<version>` for the host architecture | No |
| CI validation | Push to `main` or a pull request runs `.github/workflows/ci.yml` | Source verification, OCI platform validation, and a seven-day workflow artifact | No; the workflow has read-only contents permission and `push: false` |
| Development publication | Push to `main` independently runs `.github/workflows/publish-dev.yml` | GHCR tags `0.0.0-dev` and `0.0.0-dev-<sha>` | Yes; only this Actions workflow receives `packages: write` |
| Release publication | Push a SemVer `v*` tag to run `.github/workflows/release.yml` | Three GHCR tags: `<version>`, `<version>-<utc-timestamp>`, and `<version>-<git-short-sha>` | Yes; only this Actions workflow receives `packages: write` |

Local developers do not need GHCR write permission. The `ghcr.io/nereidai/...`
coordinates used by Make are canonical image names: they identify the OCI
archive contents or a local Docker tag, but a local tag does not mean the image
exists in GHCR. Make contains neither `docker login` nor `docker push`.

A push to `main` can therefore show two Actions runs: `CI` builds and validates
the artifact without pushing, while `Publish Dev Image` independently publishes
the development channel. Pull requests run only the non-publishing CI path. No
workflow creates a `latest` tag.

All builds use the reviewed NereidLink SDK mirror inside the repository, so no
private-repository token or adjacent checkout is required. Maintainers can run
`make sdk-mirror` from a clean NereidLink checkout to refresh the mirror and
its recorded `REVISION`.

Build network endpoints default to empty pristine upstream sources. Set
`GOPROXY` and `ALPINE_MIRROR` in the environment or in git-ignored `local.mk`
when a regional mirror is required; no repository workflow hardcodes them.

## Development

```bash
make test        # All Go unit tests
make test-cover  # Coverage report
make vet         # go vet
make lint        # golangci-lint
make build-all   # linux/amd64 and linux/arm64 binaries
make verify      # Complete local source gate
```

## Documentation

- [Milestones and remaining acceptance work](MILESTONES.md)
- [Deployment guide](docs/deployment.md)
- [Node-level connector design](NODE_CONNECTOR_DESIGN.md)
- [Edge inference integration and validation status](docs/edge-inference.md)
- [Logging and diagnostic capture](docs/logging.md)
- [Security reporting](SECURITY.md)
- [Code of conduct](CODE_OF_CONDUCT.md)

## License

NereidEdge is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE)
and [NOTICE](NOTICE) for the license and attribution notices.

## Acknowledgments

NereidEdge builds on [EdgeX Foundry](https://www.edgexfoundry.io/), the
vendor-neutral open-source edge IoT framework hosted by the Linux Foundation.
In particular, it is implemented as an EdgeX Application Service on top of
`app-functions-sdk-go` and the `go-mod-*` support libraries. The EdgeX source
code is available at [github.com/edgexfoundry](https://github.com/edgexfoundry).
