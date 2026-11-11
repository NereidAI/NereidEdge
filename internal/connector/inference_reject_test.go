// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"testing"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// TestDSValidationRejectionReportsConfigInvalid pins the multi-status
// contract: a device-service validation refusal (EdgeX HTTP 207 with a
// non-2xx per-item status, e.g. NI rejecting an allowlisted FFmpegExtra)
// resolves at transport level and the SDK client returns no transport
// error. NereidEdge must inspect the per-item statuses and report
// config-invalid, never a false APPLIED and never runtime-unreachable.
func TestDSValidationRejectionReportsConfigInvalid(t *testing.T) {
	a, dev, rec := newInferenceApplier(t)
	dev.addReject = `Device validation failed: ffmpeg extra argument "-y" is not in the allowlist`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.runPushWorker(ctx)

	cfg := streamInferenceConfig("periodic")
	cfg["ffmpeg_extra"] = "-y /tmp/not-allowed"
	a.enqueuePush(inferencePushRev("spec-ds-reject", 1, cfg))
	rec.waitFor(t, device.ConnectorStateFailed, "config-invalid")

	dev.mu.Lock()
	stored := dev.created["inference-spec-ds-reject"]
	dev.mu.Unlock()
	if stored {
		t.Fatal("rejected device must never be marked created")
	}
}
