// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

// Device OTA phase and reason vocabulary. They mirror the platform ota
// package and are accepted by POST /v1/me/ota/{taskId}/progress and the
// gateway v1/gateway/ota/progress topic.
const (
	// OTAStateInitiated marks the device starting the task.
	OTAStateInitiated = "initiated"

	// OTAStateDownloading marks package download in progress.
	OTAStateDownloading = "downloading"

	// OTAStateDownloaded marks the package fully received.
	OTAStateDownloaded = "downloaded"

	// OTAStateVerified marks checksum verification success.
	OTAStateVerified = "verified"

	// OTAStateUpdating marks the flash/apply phase.
	OTAStateUpdating = "updating"

	// OTAStateUpdated marks apply success before a reboot-confirm policy.
	OTAStateUpdated = "updated"

	// OTAStateRebooting marks the device rebooting into the new firmware.
	OTAStateRebooting = "rebooting"

	// OTAStateConfirmed is sent after reboot with the running version under
	// the confirmed_after_reboot policy.
	OTAStateConfirmed = "confirmed"

	// OTAStateSucceeded finalizes success (SUCCESS is accepted too).
	OTAStateSucceeded = "succeeded"

	// OTAStateFailed finalizes failure; pair with a ResultCode.
	OTAStateFailed = "failed"
)

// Structured OTA failure/reason codes. They are orthogonal to phases:
// progress frames omit them, failed frames carry one.
const (
	OTAResultFlashFull            = "flash_full"
	OTAResultOutOfRAM             = "out_of_ram"
	OTAResultConnectionLost       = "connection_lost"
	OTAResultIntegrityCheckFailed = "integrity_check_failed"
	OTAResultUnsupportedType      = "unsupported_type"
	OTAResultModelMismatch        = "model_mismatch"
	OTAResultVersionMismatch      = "version_mismatch"
	OTAResultInvalidURI           = "invalid_uri"
	OTAResultApplyFailed          = "apply_failed"
	OTAResultRebootTimeout        = "reboot_timeout"
	OTAResultDownloadNoProgress   = "download_no_progress"
	OTAResultAlreadyCurrent       = "already_current"
	OTAResultUnknown              = "unknown"
)

// OTAAssignment is one open platform-hosted firmware job for a direct
// device, returned by GET /v1/me/ota/tasks.
type OTAAssignment struct {
	// TaskID identifies the rollout task.
	TaskID string `json:"task_id"`

	// Version is the target firmware version.
	Version string `json:"version"`

	// URL is the relative authenticated download path (/v1/me/ota/...).
	URL string `json:"url"`

	// SHA256 is the lowercase hex digest the device must verify.
	SHA256 string `json:"sha256"`

	// Size is the package byte length.
	Size int64 `json:"size"`

	// State is the current per-device phase.
	State string `json:"state"`
}

// OTAProgress is the body of POST /v1/me/ota/{taskId}/progress.
type OTAProgress struct {
	// State is one of the OTAState* constants; lower-case.
	State string `json:"state"`

	// ProgressPct is 0..100 and must not move backwards.
	ProgressPct int `json:"progress_pct,omitempty"`

	// Message carries an optional human-readable detail.
	Message string `json:"message,omitempty"`

	// ResultCode is the structured failure reason.
	ResultCode string `json:"result_code,omitempty"`

	// Bytes is the downloaded byte count.
	Bytes int64 `json:"bytes,omitempty"`

	// FirmwareVersion is the running version on a post-reboot confirm.
	FirmwareVersion string `json:"firmware_version,omitempty"`
}

// OTATaskList is the assignments envelope. The platform always emits an
// array, never null.
type OTATaskList struct {
	// Assignments lists open jobs, newest first; empty when none.
	Assignments []OTAAssignment `json:"assignments"`
}

// GatewayOTATask is the descriptor pushed on v1/gateway/ota/task for the
// sub-devices of one gateway. The gateway fetches URL directly (with
// Headers when present) and must verify SHA256 before applying.
type GatewayOTATask struct {
	// TaskID identifies the rollout.
	TaskID string `json:"task_id"`

	// Devices are the target sub-device UUIDs.
	Devices []string `json:"devices"`

	// Version is the target firmware version.
	Version string `json:"version"`

	// URL is the absolute external package URL.
	URL string `json:"url"`

	// SHA256 is the lowercase hex digest.
	SHA256 string `json:"sha256"`

	// Size is the package byte length.
	Size int64 `json:"size"`

	// Headers optionally carries download headers (e.g. authorization).
	Headers map[string]string `json:"headers,omitempty"`

	// SuccessPolicy is device_reported (default) or confirmed_after_reboot.
	SuccessPolicy string `json:"success_policy,omitempty"`
}

// GatewayOTAProgress is one per-device frame on v1/gateway/ota/progress.
type GatewayOTAProgress struct {
	// TaskID identifies the rollout.
	TaskID string `json:"task_id"`

	// Device is the target sub-device UUID.
	Device string `json:"device"`

	// State is one of the OTAState* constants.
	State string `json:"state"`

	// ProgressPct is 0..100.
	ProgressPct int `json:"progress_pct"`

	// Message carries an optional detail.
	Message string `json:"message,omitempty"`

	// ResultCode is the structured failure reason.
	ResultCode string `json:"result_code,omitempty"`

	// Bytes is the downloaded byte count.
	Bytes int64 `json:"bytes,omitempty"`

	// FirmwareVersion is the running version on a confirm frame.
	FirmwareVersion string `json:"firmware_version,omitempty"`
}
