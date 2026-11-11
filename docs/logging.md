<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# Logging and Rotation

## Purpose

NereidEdge (the edge agent) logs to stdout; Docker captures that stream to its
`json-file` log on the host. The `sys.logs_tail` whole-gateway operation
(ADR-0035) needs a bounded, on-disk copy of the agent log **inside** the
container so an operator can pull recent lines over NereidLink without SSH.
This document is the standard for how that file is produced, bounded and
consumed.

## Design

- The agent keeps logging to stdout, unchanged. `docker compose logs`, `make
  logs` and the dev-mode `.tmp/NereidEdge/logs/backend.log` capture are
  unaffected.
- The container entrypoint (`docker/entrypoint.sh`) tees the agent's
  stdout/stderr into a size-rotated file on the persistent data volume
  (`/app/data/logs/nereid-edge.log`) while mirroring every line back to the
  container stdout. The agent stays PID 1 (`exec`), so signals propagate
  normally.
- Rotation caps total disk use at `MAX_BYTES x (KEEP + 1)`.
- `NereidEdge.Operations.LogFile` points at the same file, so `sys.logs_tail`
  reads it with secret redaction and a 64 KiB result cap. It only ever reads
  the configured file, never a caller-supplied path.

## Parameters

| Setting | Default | Meaning |
|---|---|---|
| `NereidEdge.Operations.LogFile` (config) / `NEREIDEDGE_OPERATIONS_LOGFILE` (env) | `/app/data/logs/nereid-edge.log` | rotated file path; empty disables `sys.logs_tail` and file capture |
| `NEREIDEDGE_LOG_MAX_BYTES` | `5242880` (5 MiB) | rotate once the active file reaches this size |
| `NEREIDEDGE_LOG_KEEP` | `3` | rotated backups retained (`nereid-edge.log.1` .. `.N`) |

The file path has a single authoritative definition: `res/configuration.yaml`
sets the default, and the entrypoint reads the same value through the
`NEREIDEDGE_OPERATIONS_LOGFILE` override, so the config and the writer cannot
drift apart.

## Why not /tmp or /dev/shm

- `/tmp` inside a container is **not** a tmpfs by default. It lives on the
  container writable layer (host disk) and survives `docker restart`, so it
  neither saves space nor self-clears.
- `/dev/shm` is memory-backed (default 64 MiB) and cleared on restart, but it
  costs RAM and a full buffer makes writes fail. Logs should survive a restart
  for postmortems, so RAM is the wrong medium.
- The named data volume is disk-backed, persistent, and its size is bounded by
  rotation — the right fit for a field gateway.

## Docker

- The image creates `/app/data/logs` owned by the runtime user and uses
  `docker/entrypoint.sh` as its entrypoint.
- `docker-compose.yaml` sets `json-file` `max-size` and `max-file` so the
  host-side stdout log is bounded too.

## Bare metal

Bare-metal (systemd or direct) deployments do not use the entrypoint. Point
`NereidEdge.Operations.LogFile` at the file the process supervisor already
writes (for example a systemd `StandardOutput=` file), or leave it empty to
disable `sys.logs_tail`.

## Verification

Build the image and confirm the file appears and rotates:

```bash
make docker-dev
docker run --rm ghcr.io/nereidai/nereidedge:0.0.0-dev --help   # start, then inspect in the stack
```

In a running stack, trigger `sys.logs_tail` from NereidLink and confirm it
returns recent redacted lines; check `ls -l /app/data/logs` in the container.

## Related

- docs/deployment.md - deployment modes and configuration
- docs/roadmap.md - M0-M8 milestones
