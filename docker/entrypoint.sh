#!/bin/sh
# Copyright (C) 2026 YIQISOFT
# SPDX-License-Identifier: Apache-2.0
#
# Container entrypoint for NereidEdge.
#
# The agent logs only to stdout, but the sys.logs_tail whole-gateway operation
# (ADR-0035) needs a bounded on-disk copy of those logs *inside* the container
# so an operator can pull recent lines over NereidLink without SSH. This
# wrapper tees the agent's stdout/stderr into a size-rotated file on the
# persistent data volume while still mirroring every line to the container's
# own stdout, so `docker compose logs` keeps working.
#
# The shell stays PID 1 as a tiny init: it forwards stop signals to the agent,
# waits for it to exit, and only then lets the reader drain the FIFO, so the
# final lines are neither lost from the file nor from `docker logs`.
#
# The reader loop below is the only writer of the log file; it never opens a
# long-lived descriptor, so rotation is a plain rename with no unlink race.
#
# Environment:
#   NEREIDEDGE_OPERATIONS_LOGFILE  rotated log file path. This is the same
#                                  variable the EdgeX config override uses for
#                                  NereidEdge.Operations.LogFile, so the path
#                                  has a single source of truth; empty disables
#                                  both file capture and the operation.
#   NEREIDEDGE_LOG_MAX_BYTES       rotate once the active file reaches this
#                                  size in bytes (default 5242880 = 5 MiB).
#   NEREIDEDGE_LOG_KEEP            rotated backups to retain (default 3).
set -eu

LOG_FILE="${NEREIDEDGE_OPERATIONS_LOGFILE:-/app/data/logs/nereid-edge.log}"
LOG_MAX_BYTES="${NEREIDEDGE_LOG_MAX_BYTES:-5242880}"
LOG_KEEP="${NEREIDEDGE_LOG_KEEP:-3}"
APP_BIN="/app/nereid-edge"

# log_stream mirrors its stdin to the container stdout and appends it to a
# size-rotated file. It runs in the background, fed by a FIFO the agent writes
# to. LC_ALL=C is scoped to this subshell so ${#line} counts bytes rather than
# characters; it must not leak into the agent's environment.
log_stream() {
    LC_ALL=C
    export LC_ALL
    bytes=0
    if [ -f "$LOG_FILE" ]; then
        bytes=$(wc -c < "$LOG_FILE")
    fi
    while IFS= read -r line || [ -n "$line" ]; do
        printf '%s\n' "$line"
        printf '%s\n' "$line" >> "$LOG_FILE"
        bytes=$(( bytes + ${#line} + 1 ))
        if [ "$bytes" -ge "$LOG_MAX_BYTES" ]; then
            i="$LOG_KEEP"
            rm -f "$LOG_FILE.$i"
            while [ "$i" -gt 1 ]; do
                prev=$(( i - 1 ))
                if [ -f "$LOG_FILE.$prev" ]; then
                    mv -f "$LOG_FILE.$prev" "$LOG_FILE.$i"
                fi
                i="$prev"
            done
            mv -f "$LOG_FILE" "$LOG_FILE.1"
            : > "$LOG_FILE"
            bytes=0
        fi
    done
}

if [ -z "$LOG_FILE" ]; then
    exec "$APP_BIN" "$@"
fi

LOG_DIR=$(dirname "$LOG_FILE")
mkdir -p "$LOG_DIR"
FIFO="/tmp/nereid-edge-log.fifo"
rm -f "$FIFO"
mkfifo "$FIFO"

log_stream < "$FIFO" &
READER_PID=$!

"$APP_BIN" "$@" > "$FIFO" 2>&1 &
APP_PID=$!

# Forward the container stop signal to the agent, then keep waiting until it
# has actually exited (a trapped signal makes `wait` return early, so re-wait).
# set -e must be off here: the interrupted `wait` returns non-zero.
trap 'kill -TERM "$APP_PID" 2>/dev/null || true' TERM INT
set +e
while :; do
    wait "$APP_PID"
    status=$?
    if kill -0 "$APP_PID" 2>/dev/null; then
        continue
    fi
    break
done

# The agent closed the FIFO, so the reader has seen EOF; let it drain.
wait "$READER_PID"
exit "$status"
