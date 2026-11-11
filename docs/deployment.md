<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# Deployment Guide

## Overview

This document covers deployment of NereidEdge as an EdgeX App Service (v4.0.2) connecting to NereidLink MQTT gateway.

## Prerequisites

- EdgeX Foundry stack v4.0.2 (core-keeper, NATS core-bus, core-data, core-metadata, command, device services)
- NereidLink MQTT broker (tcp:// or tls://)
- Valid gateway credentials (CredentialID, CredentialSecret, EdgeID)
- Network access between EdgeX and NereidLink

## Configuration

NereidEdge uses EdgeX custom configuration in `res/configuration.yaml` under section `NereidEdge`:

```yaml
NereidEdge:
  BrokerURL: "tcp://<nl-mqtt-host>:1883"
  CredentialID: "<your-cred-id>"
  CredentialSecret: "<your-cred-secret>"
  EdgeID: "<edge-instance-id>"
  DeviceMap: "edgex-device1:nl-uuid1,edgex-device2:nl-uuid2"
```

Notes:
- DeviceMap maps EdgeX DeviceName to NereidLink sub-device UUID
- Port: service HTTP port 59750 (EdgeX service registration)
- Trigger: defaults to EdgeX messagebus (NATS)

## Deployment Modes

### 1) Bare metal (systemd or direct)

Build binary:
```bash
make build-local  # current platform
./nereid-edge
```

Ensure res/configuration.yaml is accessible from working directory.

### 2) Docker

Build the multi-architecture release archive (default:
`linux/amd64,linux/arm64`):
```bash
make docker
```

The OCI archive is written under `dist/images/`; override `PLATFORMS` to select
a different target set. It is not loaded into the local Docker image store and
is not pushed to GHCR by Make. Build a native image before a local Docker run:
```bash
make docker-dev
```

The `make docker-dev` tag uses the canonical GHCR coordinates as a local image
name only. Make performs no login or push; GHCR images are published by the
dedicated GitHub Actions workflows.

Run (with host network or external EdgeX network):
```bash
docker run --rm -p 59750:59750 \
  -v $(pwd)/res:/app/res:ro \
  --name nereid-edge \
  ghcr.io/nereidai/nereidedge:0.0.0-dev
```

### 3) Docker Compose

Prerequisites: external network `edgex_edgex-network` exists (EdgeX compose setup).

Prepare Compose values from the template. The Makefile defaults `ENV_FILE`
to the git-ignored `.env` in the repository root and passes `--env-file`
only when that file exists, so a fresh clone requires no external deployment
directory. Use `.env.example` as the variable reference when preparing the
file; keep credentials outside version control.

Start the default profile:
```bash
cp .env.example .env
# edit .env with the broker URL and gateway credentials
make up
```

To select another operator profile, set `ENV_FILE` explicitly:
```bash
make up ENV_FILE=compose.k3s.env
```

The canonical Compose equivalent is:
```bash
docker compose --env-file .env up -d --build
```

Optional: use `docker-compose.override.yaml` to pass env vars to override config.

## Networking

- EdgeX services: communicate via internal EdgeX network
- NereidEdge needs outbound to NereidLink MQTT (1883/tcp or 8883/tls)
- Health: GET http://<host>:59750/api/v4/ping returns `pong`

## Security

- Use `tls://<host>:8883` in `BrokerURL` for an encrypted NereidLink connection.
- Public-CA broker certificates need no extra configuration: the image already
  contains the system CA bundle.
- Private CA: mount the PEM bundle read-only and set
  `NereidEdge.RootCAFile` (or the `NEREIDEDGE_ROOTCAFILE` env override) to its
  in-container path.
- mTLS: additionally mount the client certificate and key and set
  `ClientCertFile`/`ClientKeyFile` (`NEREIDEDGE_CLIENTCERTFILE` /
  `NEREIDEDGE_CLIENTKEYFILE`) together. `CredentialID`
  (`NEREID_CRED_ID`) remains mandatory even with a client certificate: it
  names the X509 credential and must match the credential ID registered in
  NereidLink. Leave `CredentialSecret` empty in certificate mode. PEM files
  are read once at startup and a bad path fails fast.
- Inline PEM text is also accepted via `RootCAPEM`/`ClientCertPEM`/
  `ClientKeyPEM` (`NEREIDEDGE_ROOTCAPEM` and related overrides); inline PEM
  wins over a configured file. Prefer files: env-overridden values are seeded
  into core-keeper and visible through `docker inspect`. In YAML use a block
  scalar; in a `.env` file keep one line with literal `\n` separators (the
  agent converts them to real newlines). The certificate and key must use the
  same delivery mode.
- `InsecureSkipVerify` disables broker certificate verification and is for
  isolated lab brokers only; never enable it in production.
- Store credentials securely (secrets management). Avoid committing CredentialSecret
- Container runs as non-root user (UID 1000)
- Use minimal base images (Alpine)

## Operations

- Heartbeat: automatic, interval from negotiation (ack.Limits.HeartbeatS)
- Manifest: sent every 5 minutes with attached device list
- Connect on first telemetry; disconnect all on shutdown
- Logs via EdgeX LoggingClient; the container also tees them to a size-rotated
  file on the data volume for `sys.logs_tail` (see docs/logging.md)

## Troubleshooting

- Connection fails: check BrokerURL, firewall, credentials
- TLS/mTLS fails: verify the tls:// scheme and port, that CredentialID matches
  the credential ID (X509 name) registered in NereidLink, the CA bundle pins
  the broker certificate, and the client cert/key pair is complete
- No telemetry forwarded: verify DeviceMap, EdgeX device events reaching app service
- Negotiation fails: check EdgeID, credential validity, feature compatibility

## Related

- [Roadmap](roadmap.md) — M0–M7 milestones
- [Node-level connector design](../NODE_CONNECTOR_DESIGN.md) — connector
  lifecycle and protocol surface
- [Edge inference bridge](edge-inference.md) — model bindings and media relay
- [Logging](logging.md) — log locations and rotation
