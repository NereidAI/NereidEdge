// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"

	"github.com/nereidai/nereidedge/internal/corecmd"
	device "github.com/nereidai/nereidlink/sdk/device/go"
)

const (
	// TypeEdgeInference is the ADR-0039 connector type. NereidEdge only
	// translates it to a generic NereidInference EdgeX job device; it never
	// calls an inference runtime or handles media/tensors.
	TypeEdgeInference = "edge-inference"

	inferenceTypeVersion     = "1"
	defaultInferenceDS       = "device-inference"
	inferenceProfile         = "nereid-inference-detection"
	inferencePipelineProfile = "nereid-inference-pipeline"
	inferenceProtocol        = "Inference"
	// defaultTargetDevice matches the device-service default; applied only
	// when the binding spec omits target_device.
	defaultTargetDevice = "CPU"
	// jobReadQuery keeps status polls off the message bus.
	jobReadQuery = "ds-pushevent=false&ds-returnevent=true"
	// bindingPollInterval is the default job-state polling interval.
	bindingPollInterval = 10 * time.Second
)

var errInferenceConfig = errors.New("invalid edge-inference config")

// safeTokenPattern bounds cloud-derived tokens (ONVIF media ProfileToken) to
// the same transport-safe character set as target_device: no whitespace,
// quotes or punctuation that could carry injection into command bodies.
var safeTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_.:,-]+$`)

// NI pipeline IDs and reading names have narrower contracts than generic
// EdgeX tokens. Keep the runtime patterns identical to the advertised schema.
var inferencePipelineIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)
var inferencePublishReadingPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

// directTransportPattern lists the NI direct-source kinds accepted through
// the mode=url escape hatch (cameras without an ONVIF registration).
var directTransportPattern = regexp.MustCompile(`^(rtsp|rtsp-udp|mpjpeg|jpeg)$`)

// validateDirectURL guards the mode=url SourceURL. The URL is cloud-stored,
// so it must identify a host only: scheme has to match the chosen NI
// transport and embedded userinfo (username/password in the URL) is rejected
// — credentials enter NI through an edge-provisioned named secret instead.
func validateDirectURL(raw, transport string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > 1024 {
		return errInferenceConfig
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return errInferenceConfig
	}
	switch transport {
	case "rtsp", "rtsp-udp":
		if u.Scheme != "rtsp" {
			return errInferenceConfig
		}
	case "mpjpeg", "jpeg":
		if u.Scheme != "http" && u.Scheme != "https" {
			return errInferenceConfig
		}
	default:
		return errInferenceConfig
	}
	if u.User != nil {
		return errInferenceConfig
	}
	return nil
}

// InferenceSecretBinding names one stream binding's camera and the onvif
// secret name its NI job must authenticate ffmpeg with.
type InferenceSecretBinding struct {
	CameraEdgeXName string
	SecretName      string
}

// InferenceCredentialManager resolves the onvif-basic secret name mapped to a
// camera and converges the NI secret replica set. It is optional: a nil
// manager keeps stream jobs credential-less (NI then reports source-no-frame
// honestly) and every snapshot binding byte-identical.
type InferenceCredentialManager interface {
	ResolveSecretName(ctx context.Context, cameraEdgeXName string) (string, error)
	// Ensure makes every named secret exist in NI immediately; it runs on
	// the apply hot path before the job device becomes visible to NI.
	Ensure(ctx context.Context, names []string) error
	Reconcile(ctx context.Context, live []InferenceSecretBinding) error
}

// inferenceSchema is the self-contained draft 2020-12 config schema
// registered through hello connectors[]; external $refs are forbidden.
var inferenceSchema = json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "oneOf": [
    {"required": ["source", "model_ref"], "not": {"required": ["PipelineId"]}},
    {
      "required": ["PipelineId"],
      "not": {"required": ["model_ref"]},
      "properties": {
        "SourceKind": {"enum": ["onvif-camera", "onvif-snapshot", "rtsp", "rtsp-udp", "mpjpeg", "jpeg"]}
      }
    }
  ],
  "properties": {
    "source": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "device": {"type": "string", "minLength": 1, "description": "Managed camera sub-device UUID; required for snapshot/stream (ONVIF) modes."},
        "resource": {"type": "string", "enum": ["snapshot_jpeg"]},
        "mode": {
          "type": "string",
          "enum": ["snapshot", "stream", "url"],
          "default": "snapshot",
          "description": "snapshot: one ONVIF Snapshot JPEG GET per cycle; stream: federated RTSP via ffmpeg from the camera's ONVIF registration; url: direct camera URL for non-ONVIF hardware (transport selects rtsp/rtsp-udp/mpjpeg/jpeg)."
        },
        "profile": {
          "type": "string",
          "minLength": 1,
          "maxLength": 64,
          "pattern": "^[A-Za-z0-9_.:,-]+$",
          "description": "ONVIF media ProfileToken (main/sub/third) for mode=stream."
        },
        "url": {
          "type": "string",
          "minLength": 8,
          "maxLength": 1024,
          "description": "Direct camera URL for mode=url: rtsp://host/path (transport rtsp/rtsp-udp) or http(s)://host/path (mpjpeg/jpeg). Credentials MUST NOT be embedded; reference an edge-side secret via secret_name. The URL is stored in the platform, so prefer ONVIF modes when available."
        },
        "transport": {
          "type": "string",
          "enum": ["rtsp", "rtsp-udp", "mpjpeg", "jpeg"],
          "description": "Acquisition transport for mode=url."
        },
        "secret_name": {
          "type": "string",
          "minLength": 1,
          "maxLength": 128,
          "pattern": "^[A-Za-z0-9_.:,-]+$",
          "description": "Optional name of an edge-provisioned secret (username/password) the device service injects into the direct URL; used for mode=url only."
        }
      }
    },
    "model_ref": {
      "type": "object",
      "required": ["id", "version"],
      "additionalProperties": false,
      "properties": {
        "id": {"type": "string", "minLength": 1},
        "version": {"type": "string", "minLength": 1},
        "revision": {"type": "string"}
      }
    },
    "PipelineId": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$"},
    "PublishMode": {"type": "string", "enum": ["", "all", "nonempty", "none"]},
    "PublishReadings": {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 64, "pattern": "^[A-Za-z][A-Za-z0-9_]{0,63}$"}, "uniqueItems": true},
    "SourceKind": {"type": "string", "enum": ["none", "onvif-camera", "onvif-snapshot", "rtsp", "rtsp-udp", "mpjpeg", "jpeg"]},
    "SourceDevice": {"type": "string", "minLength": 1},
    "SourceURL": {"type": "string", "maxLength": 1024},
    "SecretName": {"type": "string", "maxLength": 128, "pattern": "^[A-Za-z0-9_.:,-]+$"},
    "StreamProfile": {"type": "string", "maxLength": 64, "pattern": "^[A-Za-z0-9_.:,-]+$"},
    "DecodeAccel": {"type": "string", "enum": ["software", "vaapi"]},
    "VAAPIDevice": {"type": "string", "pattern": "^(?:/dev/dri/)?renderD\\d+$"},
    "TargetDevice": {"type": "string", "maxLength": 64, "pattern": "^[A-Za-z0-9_.:,-]+$"},
    "FPS": {"type": "number", "minimum": 0, "maximum": 60},
    "ServableID": {"type": "string", "minLength": 1},
    "trigger": {"type": "string", "enum": ["event", "periodic", "continuous"], "default": "event"},
    "period_ms": {"type": "integer", "minimum": 200},
    "max_idle_ms": {"type": "integer", "minimum": 1000},
    "fps": {"type": "number", "minimum": 0.1, "maximum": 60, "description": "Inference rate for trigger=continuous (frames per second, capped by the source stream)."},
    "error_threshold": {"type": "integer", "minimum": 1},
    "conf_threshold": {"type": "number", "minimum": 0, "maximum": 1},
    "target_device": {
      "type": "string",
      "default": "CPU",
      "minLength": 1,
      "maxLength": 64,
      "pattern": "^[A-Za-z0-9_.:,-]+$",
      "description": "Inference target passed through verbatim to the device service (CPU, GPU, GPU.0, NPU, AUTO, MULTI:..., HETERO:...). The DS validates and interprets it; absent means CPU and the edge must never silently fall back to CPU on failure."
    },
    "emit_frame": {
      "type": "boolean",
      "default": true,
      "description": "Attach the evidence JPEG (FrameJPEG reading) to every result event. Off saves uplink bandwidth/storage; the cloud then has no evidence image for drill-down. Absent defaults to true to match pre-upgrade bindings."
    },
    "annotate_preview": {
      "type": "boolean",
      "default": true,
      "description": "Draw detection boxes/labels on the evidence JPEG and the local diagnostic preview. Independent of emit_frame: the preview port still reflects it."
    },
    "ffmpeg_extra": {
      "type": "string",
      "maxLength": 256,
      "description": "Optional extra ffmpeg argv tokens for stream acquisition (space-separated, no shell, no quotes). Allowlisted flags only: -re and -r/-ss/-t/-threads/-fflags/-flags each with a value, e.g. \"-fflags nobuffer -flags low_delay\". The device service performs the authoritative validation and rejects anything else."
    },
    "decode_accel": {
      "type": "string",
      "enum": ["software", "vaapi"],
      "default": "software",
      "description": "Site-local ffmpeg decode pipeline. VA-API is valid only for ONVIF RTSP and direct rtsp/rtsp-udp sources."
    },
    "vaapi_device": {
      "type": "string",
      "pattern": "^(?:/dev/dri/)?renderD\\d+$",
      "description": "Optional DRM render node for decode_accel=vaapi; absent uses the NI default."
    }
  }
}`)

// inferenceModelRef is the legacy single-model binding. Pipeline bindings use
// PipelineId instead; the two desired forms are mutually exclusive.
type inferenceModelRef struct {
	ID       string `json:"id"`
	Version  string `json:"version"`
	Revision string `json:"revision"`
}

// inferenceConfig accepts both the original nested source+model_ref document
// and NL's expanded pipeline binding, whose property names already follow NI's
// wire contract. Only this transport envelope is decoded; pipeline definitions
// and emitted readings remain opaque to NE.
type inferenceConfig struct {
	Source struct {
		Device   string `json:"device"`
		Resource string `json:"resource"`
		// Mode selects snapshot JPEG polling (default, onvif-snapshot) or
		// the federated RTSP/ffmpeg subscription (onvif-camera). The RTSP
		// URL and credentials stay edge-local: NI resolves them from the
		// ONVIF camera's own core-metadata protocol properties.
		Mode string `json:"mode"`
		// Profile is an optional ONVIF media ProfileToken (main/sub stream)
		// used only in stream mode.
		Profile string `json:"profile"`
		// URL/Transport/SecretName carry the direct-URL escape hatch for
		// non-ONVIF cameras (mode=url). The URL is cloud-stored by
		// necessity but must never embed credentials; SecretName references
		// an edge-provisioned secret.
		URL        string `json:"url"`
		Transport  string `json:"transport"`
		SecretName string `json:"secret_name"`
	} `json:"source"`
	ModelRef *inferenceModelRef `json:"model_ref"`

	// Expanded NL pipeline binding (PascalCase is intentional and signed off
	// against NL's business expansion). PublishReadings arrives as an array
	// and is flattened to NI's comma-separated string property below.
	PipelineId          string   `json:"PipelineId"`
	PublishMode         string   `json:"PublishMode"`
	PublishReadings     []string `json:"PublishReadings"`
	DirectSourceKind    string   `json:"SourceKind"`
	DirectSourceDevice  string   `json:"SourceDevice"`
	DirectSourceURL     string   `json:"SourceURL"`
	DirectSecretName    string   `json:"SecretName"`
	DirectStreamProfile string   `json:"StreamProfile"`
	DirectDecodeAccel   string   `json:"DecodeAccel"`
	DirectVAAPIDevice   string   `json:"VAAPIDevice"`
	DirectTargetDevice  string   `json:"TargetDevice"`
	DirectFPS           float64  `json:"FPS"`
	ServableID          string   `json:"ServableID"`

	Trigger   string `json:"trigger"`
	PeriodMS  int    `json:"period_ms"`
	MaxIdleMS int    `json:"max_idle_ms"`
	// FPS bounds the inference rate of trigger=continuous jobs.
	FPS float64 `json:"fps"`
	// EmitFrame toggles the per-event evidence JPEG. nil keeps the bridge
	// default (true), matching bindings created before the property existed.
	EmitFrame *bool `json:"emit_frame,omitempty"`
	// AnnotatePreview toggles box/label drawing on evidence and preview.
	AnnotatePreview *bool `json:"annotate_preview,omitempty"`
	// DecodeAccel selects the site-local ffmpeg input decoder.
	DecodeAccel string `json:"decode_accel"`
	// VAAPIDevice optionally selects the DRM render node for VA-API.
	VAAPIDevice string `json:"vaapi_device"`
	// FFmpegExtra is the allowlisted extra argv string for ffmpeg sources;
	// the device service performs the authoritative allowlist validation.
	FFmpegExtra    string  `json:"ffmpeg_extra"`
	ErrorThreshold int     `json:"error_threshold"`
	ConfThreshold  float64 `json:"conf_threshold"`
	// TargetDevice selects the OVMS inference target (CPU/GPU/GPU.0/NPU/...).
	// It is passed through verbatim; the device service normalizes and
	// validates it. Empty selects the DS default CPU.
	TargetDevice string `json:"target_device"`
}

// CommandReader is the core-command subset used by the binding status poller.
type CommandReader interface {
	Read(ctx context.Context, edgexName, commandName, rawQuery string) (responses.EventResponse, int, error)
}

// InferenceHandler translates edge-inference specs into ordinary edgex-device
// lifecycle operations against NereidInference. It contains no inference
// semantics beyond protocol-property name translation.
type InferenceHandler struct {
	applier *Applier
}

// NewInferenceHandler creates the edge-inference connector handler.
func NewInferenceHandler(applier *Applier) *InferenceHandler {
	return &InferenceHandler{applier: applier}
}

// Type returns the connector type implemented by this handler.
func (h *InferenceHandler) Type() string { return TypeEdgeInference }

// Apply translates one edge-inference spec and runs the generic device path.
func (h *InferenceHandler) Apply(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	if push.DeviceID == "" {
		h.applier.reportFailed(ctx, push, "config-invalid")
		return nil
	}
	cfg, err := h.applier.inferenceDeviceConfig(push)
	if err != nil {
		h.applier.reportFailed(ctx, push, "config-invalid")
		return nil
	}
	// Deliver referenced secrets BEFORE the job device becomes visible to
	// NI: both federated stream jobs and direct-rtsp jobs authenticate
	// through the same named secret, so NI must never observe a
	// SecretName it cannot resolve (it would climb DEGRADED -> FAILED).
	// A delivery failure is soft (the debounced/periodic reconcile and
	// NI's own self-healing retry cover it); the job still lands so the
	// control plane converges once delivery catches up.
	if name, _ := cfg.Protocols[inferenceProtocol]["SecretName"].(string); name != "" {
		h.applier.ensureInferenceSecret(ctx, name)
	}
	h.applier.applyDeviceSpec(ctx, push, cfg)
	return nil
}

// Delete removes the translated job device through the generic delete path.
func (h *InferenceHandler) Delete(ctx context.Context, push device.ConnectorPush) error {
	if h.applier == nil {
		return nil
	}
	h.applier.applyDelete(ctx, push)
	return nil
}

// inferenceDeviceName renders the deterministic EdgeX job-device name. NL's
// local_id convention uses "inference:<spec_id>" but EdgeX names forbid colon,
// so every non-EdgeX-name rune becomes a hyphen.
func inferenceDeviceName(specID string) string {
	var b strings.Builder
	b.WriteString("inference-")
	for _, r := range specID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.', r == '~':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// inferenceDeviceConfig maps the semantic connector document to NereidInference
// Inference protocol properties. The virtual output device UUID is only used
// to resolve telemetry ownership; it is never the EdgeX device name.
func (a *Applier) inferenceDeviceConfig(push device.ConnectorPush) (SpecConfig, error) {
	var cfg inferenceConfig
	if err := json.Unmarshal(push.Config, &cfg); err != nil {
		return SpecConfig{}, err
	}
	cfg.PipelineId = strings.TrimSpace(cfg.PipelineId)
	if cfg.ModelRef != nil {
		if cfg.ModelRef.ID == "" || cfg.PipelineId != "" {
			return SpecConfig{}, errInferenceConfig
		}
	} else if !inferencePipelineIDPattern.MatchString(cfg.PipelineId) {
		return SpecConfig{}, errInferenceConfig
	}
	pipelineBinding := cfg.ModelRef == nil
	if pipelineBinding {
		seenReadings := make(map[string]struct{}, len(cfg.PublishReadings))
		for i, reading := range cfg.PublishReadings {
			reading = strings.TrimSpace(reading)
			if !inferencePublishReadingPattern.MatchString(reading) {
				return SpecConfig{}, errInferenceConfig
			}
			if _, duplicate := seenReadings[reading]; duplicate {
				return SpecConfig{}, errInferenceConfig
			}
			seenReadings[reading] = struct{}{}
			cfg.PublishReadings[i] = reading
		}
	}
	mode := strings.TrimSpace(cfg.Source.Mode)
	sourceKind := "onvif-snapshot"
	sourceID := cfg.Source.Device
	sourceURL := ""
	streamProfile := strings.TrimSpace(cfg.Source.Profile)
	directSecret := strings.TrimSpace(cfg.Source.SecretName)
	directMode := false
	if pipelineBinding {
		// NL's expanded pipeline binding carries NI source properties directly.
		// Do not silently mix them with the legacy nested source envelope.
		if mode != "" || cfg.Source.Device != "" || cfg.Source.Resource != "" ||
			cfg.Source.Profile != "" || cfg.Source.URL != "" || cfg.Source.Transport != "" ||
			cfg.Source.SecretName != "" {
			return SpecConfig{}, errInferenceConfig
		}
		sourceKind = strings.TrimSpace(cfg.DirectSourceKind)
		if sourceKind == "" {
			sourceKind = "onvif-camera"
		}
		sourceID = strings.TrimSpace(cfg.DirectSourceDevice)
		sourceURL = strings.TrimSpace(cfg.DirectSourceURL)
		streamProfile = strings.TrimSpace(cfg.DirectStreamProfile)
		directSecret = strings.TrimSpace(cfg.DirectSecretName)
		switch sourceKind {
		case "onvif-camera", "onvif-snapshot":
		case "rtsp", "rtsp-udp", "mpjpeg", "jpeg":
			directMode = true
			if err := validateDirectURL(sourceURL, sourceKind); err != nil {
				return SpecConfig{}, err
			}
		default:
			return SpecConfig{}, errInferenceConfig
		}
	} else {
		// Legacy nested source acquisition. Absent/empty mode means snapshot;
		// stream uses NI's ONVIF federation and url is the direct escape hatch.
		if mode != "url" && (sourceID == "" || cfg.Source.Resource == "") {
			return SpecConfig{}, errInferenceConfig
		}
		switch mode {
		case "", "snapshot":
		case "stream":
			sourceKind = "onvif-camera"
		case "url":
			directMode = true
			sourceKind = strings.TrimSpace(cfg.Source.Transport)
			sourceURL = strings.TrimSpace(cfg.Source.URL)
			if !directTransportPattern.MatchString(sourceKind) {
				return SpecConfig{}, errInferenceConfig
			}
			if err := validateDirectURL(sourceURL, sourceKind); err != nil {
				return SpecConfig{}, err
			}
		default:
			return SpecConfig{}, errInferenceConfig
		}
	}
	if streamProfile != "" {
		if sourceKind != "onvif-camera" || !safeTokenPattern.MatchString(streamProfile) ||
			len(streamProfile) > 64 {
			return SpecConfig{}, errInferenceConfig
		}
	}
	if directSecret != "" {
		if (!pipelineBinding && !directMode) || sourceKind == "none" || sourceKind == "onvif-snapshot" ||
			!safeTokenPattern.MatchString(directSecret) || len(directSecret) > 128 {
			return SpecConfig{}, errInferenceConfig
		}
	}
	sourceName := ""
	if sourceKind == "onvif-snapshot" || sourceKind == "onvif-camera" {
		if sourceID == "" {
			return SpecConfig{}, errInferenceConfig
		}
		var ok bool
		sourceName, ok = a.resolver.Name(sourceID)
		if !ok {
			a.lc.Warnf("edge-inference spec %s source camera %s is not attached", push.SpecID, sourceID)
			return SpecConfig{}, errInferenceConfig
		}
	}
	// An explicit expanded SecretName is passed through. Otherwise federated
	// streams resolve the camera's onvif-basic replica exactly as legacy jobs do.
	streamSecretName := directSecret
	if sourceKind == "onvif-camera" && streamSecretName == "" && a.inferenceCreds != nil {
		resolveCtx, cancel := context.WithTimeout(context.Background(), a.metadataTimeout)
		name, err := a.inferenceCreds.ResolveSecretName(resolveCtx, sourceName)
		cancel()
		if err != nil {
			a.lc.Warnf("edge-inference spec %s stream secret unresolved for camera %s: %s",
				push.SpecID, sourceName, err.Error())
		} else {
			streamSecretName = name
		}
	}

	trigger := cfg.Trigger
	if trigger == "" {
		if pipelineBinding {
			trigger = "continuous"
		} else {
			trigger = "event"
		}
	}
	// NI has no event-bus wake path in this slice: the NL "event" semantics is
	// served by NI's command trigger (one Trigger PUT per inference), while
	// "periodic" maps directly to NI periodic scheduling. "continuous" maps
	// to NI's FPS-ticked continuous inference against a long-lived source.
	jobTrigger := "command"
	periodMS := 0
	switch trigger {
	case "event":
	case "periodic":
		jobTrigger = "periodic"
		periodMS = cfg.PeriodMS
		if periodMS == 0 {
			periodMS = 1000
		}
	case "continuous":
		jobTrigger = "continuous"
	default:
		return SpecConfig{}, errInferenceConfig
	}
	maxIdleMS := cfg.MaxIdleMS
	if maxIdleMS == 0 {
		maxIdleMS = 60000
	}
	errorThreshold := cfg.ErrorThreshold
	if errorThreshold == 0 {
		errorThreshold = 5
	}
	dsName := a.inferenceDSName
	if dsName == "" {
		dsName = defaultInferenceDS
	}
	// Pipeline jobs use NI's pipeline device profile and resources; configured
	// legacy profile overrides apply only to single-model detection jobs.
	profileName := a.inferenceProfileName()
	if pipelineBinding {
		profileName = inferencePipelineProfile
	}
	// TargetDevice is a passthrough selector, not a NE-side decision: only
	// fill the DS default when the spec omits it; never guess or silently
	// remap a requested target. The DS normalizes case/validates the value.
	targetDevice := cfg.TargetDevice
	if pipelineBinding && cfg.DirectTargetDevice != "" {
		targetDevice = cfg.DirectTargetDevice
	}
	if targetDevice == "" && (!pipelineBinding || cfg.ServableID == "") {
		targetDevice = defaultTargetDevice
	}
	// Every Inference protocol property is a string scalar: NereidInference
	// parses them through its job config and rejects typed values.
	//
	// Three acquisitions map onto NI sources: "snapshot" → onvif-snapshot
	// (one Snapshot JPEG per cycle); "stream" → onvif-camera federation
	// (RTSP resolved and authenticated edge-locally); "url" → the direct
	// rtsp/rtsp-udp/mpjpeg/jpeg source with the operator-supplied URL.
	props := map[string]any{
		"SourceKind":     sourceKind,
		"Trigger":        jobTrigger,
		"PeriodMs":       strconv.Itoa(periodMS),
		"MaxIdleMs":      strconv.Itoa(maxIdleMS),
		"ErrorThreshold": strconv.Itoa(errorThreshold),
		"ConfThreshold":  strconv.FormatFloat(cfg.ConfThreshold, 'f', -1, 64),
		"TargetDevice":   targetDevice,
	}
	if pipelineBinding {
		if cfg.ServableID != "" && targetDevice == "" {
			delete(props, "TargetDevice")
		}
		props["PipelineId"] = cfg.PipelineId
		if cfg.PublishMode != "" {
			props["PublishMode"] = cfg.PublishMode
		}
		if cfg.PublishReadings != nil {
			props["PublishReadings"] = strings.Join(cfg.PublishReadings, ",")
		}
		if cfg.ServableID != "" {
			props["ServableID"] = cfg.ServableID
		}
	} else {
		// Legacy single-model jobs keep their model lease/refcount identity.
		props["ModelID"] = cfg.ModelRef.ID
		props["ModelVersion"] = cfg.ModelRef.Version
	}
	if sourceName != "" {
		props["SourceDevice"] = sourceName
	}
	// Direct-URL source: URL carries no userinfo (passwords reference an
	// edge-provisioned secret and never transit this config).
	if directMode {
		props["SourceURL"] = sourceURL
	}
	if directSecret != "" {
		props["SecretName"] = directSecret
	}
	// Evidence JPEG is enabled by default at the bridge for bindings
	// created before the property existed; an explicit cloud choice is
	// passed through verbatim. The telemetry pipeline turns FrameJPEG into
	// a binary media marker that NL offloads to blob storage rather than an
	// inline base64 string.
	if !pipelineBinding || cfg.EmitFrame != nil {
		emitFrame := true
		if cfg.EmitFrame != nil {
			emitFrame = *cfg.EmitFrame
		}
		props["EmitFrame"] = strconv.FormatBool(emitFrame)
	}
	if !pipelineBinding || cfg.AnnotatePreview != nil {
		annotatePreview := true
		if cfg.AnnotatePreview != nil {
			annotatePreview = *cfg.AnnotatePreview
		}
		props["AnnotatePreview"] = strconv.FormatBool(annotatePreview)
	}
	// Optional allowlisted ffmpeg tuning for the stream source. The device
	// service owns the authoritative allowlist (-re/-r/-ss/-t/-threads/
	// -fflags/-flags); absent means the input template default.
	if extra := strings.TrimSpace(cfg.FFmpegExtra); extra != "" {
		props["FFmpegExtra"] = extra
	}
	// DecodeAccel and VAAPIDevice are site-local acquisition properties. NI
	// authoritatively validates source compatibility and device availability;
	// NE supplies no render-node default and never rewrites a failed request
	// to software decoding.
	decodeAccel := cfg.DecodeAccel
	vaapiDevice := cfg.VAAPIDevice
	if pipelineBinding {
		decodeAccel = cfg.DirectDecodeAccel
		vaapiDevice = cfg.DirectVAAPIDevice
	}
	if decodeAccel != "" {
		props["DecodeAccel"] = decodeAccel
	}
	if vaapiDevice != "" {
		props["VAAPIDevice"] = vaapiDevice
	}
	// FPS only governs continuous jobs; NI defaults other triggers itself.
	if jobTrigger == "continuous" {
		fps := cfg.FPS
		if pipelineBinding {
			fps = cfg.DirectFPS
		}
		if fps <= 0 {
			fps = 5
		}
		props["FPS"] = strconv.FormatFloat(fps, 'f', -1, 64)
	}
	// ProfileToken selects main/sub/third stream on the federated source;
	// absent lets the camera service pick its default profile. Changing it
	// changes the config hash and rebuilds the job.
	if streamProfile != "" {
		props["StreamProfile"] = streamProfile
	}
	// Stream jobs name the onvif-basic secret NI reads from its own store;
	// snapshot bindings never carry this property.
	if streamSecretName != "" {
		props["SecretName"] = streamSecretName
	}
	return SpecConfig{
		DSName:      dsName,
		ProfileName: profileName,
		DeviceName:  inferenceDeviceName(push.SpecID),
		Description: "NereidInference edge inference binding",
		Labels:      []string{"nereid", "inference", "edge-inference"},
		Protocols:   map[string]map[string]any{inferenceProtocol: props},
		// A local DS pipeline is brought online immediately without probing;
		// liveness is reported from the job state poller, not health_check.
		// The nereid-* tags persist the NL correlation on the EdgeX device so
		// operators can map the EdgeX job name back to the connector spec and
		// the NL virtual output sub-device without NE's in-memory resolver.
		Tags: map[string]any{
			"local":         true,
			"nereid-spec":   push.SpecID,
			"nereid-device": push.DeviceID,
		},
	}, nil
}

func (a *Applier) inferenceProfileName() string {
	if a.inferenceProfile != "" {
		return a.inferenceProfile
	}
	return inferenceProfile
}

// SanitizeReason enforces the ADR-0039 transport-safe vocabulary so DS error
// text, paths or addresses never reach NL.
func SanitizeReason(reason string) string {
	switch reason {
	case "model-not-ready", "model-not-available", "target-device-unavailable",
		"runtime-unreachable", "runtime-error",
		"inference-timeout", "source-no-frame", "preprocess-failed", "postprocess-failed",
		"idle-no-inference", "resource-exhausted", "config-invalid", "package-invalid",
		"checksum-mismatch":
		return reason
	default:
		return "runtime-error"
	}
}

// inferenceFailureReason maps local apply errors to §15.6 reason codes; generic
// connector types keep their raw error text.
func inferenceFailureReason(err error) string {
	if errors.Is(err, errInferenceConfig) || errors.Is(err, errDeviceRejected) {
		return "config-invalid"
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "core-metadata unreachable"), strings.Contains(text, "connection refused"),
		strings.Contains(text, "no such host"), strings.Contains(text, "timeout"):
		return "runtime-unreachable"
	case strings.Contains(text, "model"):
		return "model-not-ready"
	default:
		return "config-invalid"
	}
}

// ConfigureInference enables the edge-inference connector advert and supplies
// the reader used by the binding-status poller. negotiated gates all uplink
// status frames on the hello ack accepting the inference feature, so sessions
// against an older platform stay byte-identical to firmware without it.
func (a *Applier) ConfigureInference(dsName, profileName string, commands CommandReader, poll time.Duration, negotiated func() bool) {
	a.inferenceAdvert = true
	a.inferenceDSName = dsName
	a.inferenceProfile = profileName
	a.inferenceCommands = commands
	a.inferencePoll = poll
	a.inferenceNegotiated = negotiated
}

// SetInferencePresence wires the runtime NI presence probe. The edge
// self-registers edge-inference (and hello advertises the inference feature)
// only while the deployed inference device service/manager device actually
// exists; a nil predicate keeps the advert unconditionally (tests/legacy).
func (a *Applier) SetInferencePresence(present func() bool) {
	a.inferencePresent = present
}

// SetInferenceCredentials wires the stream credential bridge: it resolves the
// onvif-basic secret name mapped to a stream camera and converges the secret
// replica set inside NI. It must be called before specs start applying; a nil
// manager leaves every binding byte-identical.
func (a *Applier) SetInferenceCredentials(m InferenceCredentialManager) {
	a.inferenceCreds = m
}

// NotifyInferenceCredentials requests a debounced reconciliation of the NI
// secret replica set; the call is non-blocking and bursts coalesce to one run.
func (a *Applier) NotifyInferenceCredentials() {
	if a.inferenceCreds == nil {
		return
	}
	select {
	case a.inferenceCredPing <- struct{}{}:
	default:
	}
}

// inferenceCredApplyTimeout bounds the pre-apply secret delivery; it must
// finish (or fail soft) before the job device upsert.
const inferenceCredApplyTimeout = 15 * time.Second

// inferenceCredReplayInterval re-posts every referenced secret on a steady
// cadence so an NI restart (non-secure secret store is memory-only) heals
// within tens of seconds without an edge restart or manual POST. It is a var
// so tests can collapse the cadence.
var inferenceCredReplayInterval = 30 * time.Second

// ensureInferenceSecret delivers one named secret to NI synchronously. It is
// best-effort: failures are logged and left to the debounced reconcile and
// the periodic replay instead of blocking the control-plane apply.
func (a *Applier) ensureInferenceSecret(ctx context.Context, name string) {
	if a.inferenceCreds == nil || name == "" {
		return
	}
	deliverCtx, cancel := context.WithTimeout(ctx, inferenceCredApplyTimeout)
	defer cancel()
	if err := a.inferenceCreds.Ensure(deliverCtx, []string{name}); err != nil {
		a.lc.Warnf("pre-apply delivery of inference secret %q failed (reconcile will retry): %s",
			name, err.Error())
	}
}

// inferenceCredSettleDelay lets full secret-set redrives (one Apply per
// secret after every hello) and multi-spec applies coalesce into one pass.
// It is a var so tests can collapse the debounce window.
var inferenceCredSettleDelay = 2 * time.Second

// inferenceCredRunTimeout bounds one credential reconciliation pass.
const inferenceCredRunTimeout = 60 * time.Second

// runInferenceCredentialReconciler converges NI stream credentials off the
// apply hot path: it refreshes the SecretName job property when the camera
// mapping changed and then reconciles the NI replica set.
func (a *Applier) runInferenceCredentialReconciler(ctx context.Context) {
	replay := time.NewTicker(inferenceCredReplayInterval)
	defer replay.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-replay.C:
			// Steady-cadence full re-ensure: NI's non-secure secret store
			// is memory-only, so an NI restart loses every replica until
			// the edge posts them again.
		case <-a.inferenceCredPing:
		}
	drain:
		for {
			select {
			case <-a.inferenceCredPing:
			case <-replay.C:
			default:
				break drain
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(inferenceCredSettleDelay):
		}
		runCtx, cancel := context.WithTimeout(ctx, inferenceCredRunTimeout)
		a.reconcileInferenceCredentials(runCtx)
		cancel()
	}
}

// reconcileInferenceCredentials updates SecretName properties on stream jobs
// when the camera -> onvif secret mapping changed, strips it after a
// stream -> snapshot switch, and converges the NI secret replica set.
func (a *Applier) reconcileInferenceCredentials(ctx context.Context) {
	if a.observe || !a.inferenceAdvert || a.inferenceCreds == nil {
		return
	}
	// Same replay guard as the orphan sweep: desired state re-uploads after
	// (re)hello and secret delivery redrives the full set, so never reconcile
	// before the redrive settles.
	a.mu.Lock()
	if time.Since(a.settledAt) < inferenceReplayGrace {
		a.mu.Unlock()
		return
	}
	snapshot := make(map[string]appliedSpec)
	for id, sp := range a.applied {
		if sp.connectorType == TypeEdgeInference {
			snapshot[id] = sp
		}
	}
	a.mu.Unlock()

	var bindings []InferenceSecretBinding
	for specID, sp := range snapshot {
		var cfg inferenceConfig
		if sp.deviceName == "" || json.Unmarshal(sp.config, &cfg) != nil {
			continue
		}
		mode := strings.TrimSpace(cfg.Source.Mode)
		if mode == "" {
			mode = "snapshot"
		}
		current := a.currentJobSecretName(ctx, sp.deviceName)
		if cfg.ModelRef == nil {
			current = a.reconcileExpandedInferenceCredential(ctx, specID, sp, cfg, current)
			if current != "" {
				cameraName := ""
				if cfg.DirectSourceKind == "" || cfg.DirectSourceKind == "onvif-camera" {
					cameraName, _ = a.resolver.Name(cfg.DirectSourceDevice)
				}
				bindings = append(bindings, InferenceSecretBinding{
					CameraEdgeXName: cameraName,
					SecretName:      current,
				})
			}
			continue
		}
		switch mode {
		case "stream":
			cameraName, ok := a.resolver.Name(cfg.Source.Device)
			if !ok {
				continue
			}
			want, err := a.inferenceCreds.ResolveSecretName(ctx, cameraName)
			if err != nil {
				a.lc.Warnf("stream secret for spec %s unresolved: %s", specID, err.Error())
			}
			if want != current {
				if name := a.retranslateInferenceJob(ctx, specID, sp); name != "" {
					current = name
				}
			}
			if current != "" {
				bindings = append(bindings, InferenceSecretBinding{
					CameraEdgeXName: cameraName,
					SecretName:      current,
				})
			}
		case "url":
			// Direct-URL jobs reference an edge-provisioned secret by name
			// through source.secret_name; the replica is protected and
			// replayed exactly like a federated stream's secret, and
			// dropping secret_name strips the job property.
			want := strings.TrimSpace(cfg.Source.SecretName)
			if want != current {
				retranslated := a.retranslateInferenceJob(ctx, specID, sp)
				if want != "" {
					// The desired name drives the binding even while the
					// job update is pending; delivery converges first.
					current = want
				} else {
					// Successfully stripped -> "". A failed strip leaves
					// the live NI job still carrying the name, which the
					// NI-job protection scan keeps safe until retry.
					current = retranslated
				}
			}
			if current != "" {
				bindings = append(bindings, InferenceSecretBinding{SecretName: current})
			}
		default:
			// stream/url -> snapshot: the property must disappear so the
			// job stops authenticating through a stale NI secret.
			if current != "" {
				a.retranslateInferenceJob(ctx, specID, sp)
			}
		}
	}

	if err := a.inferenceCreds.Reconcile(ctx, bindings); err != nil {
		a.lc.Warnf("inference stream credential reconcile failed: %s", err.Error())
	}
}

// reconcileExpandedInferenceCredential applies the same SecretName refresh
// lifecycle to NL's direct pipeline binding without reinterpreting its source.
func (a *Applier) reconcileExpandedInferenceCredential(
	ctx context.Context, specID string, sp appliedSpec, cfg inferenceConfig, current string,
) string {
	sourceKind := strings.TrimSpace(cfg.DirectSourceKind)
	if sourceKind == "" {
		sourceKind = "onvif-camera"
	}
	want := strings.TrimSpace(cfg.DirectSecretName)
	cameraName := ""
	if sourceKind == "onvif-camera" {
		var ok bool
		cameraName, ok = a.resolver.Name(cfg.DirectSourceDevice)
		if !ok {
			return current
		}
		if want == "" && a.inferenceCreds != nil {
			resolved, err := a.inferenceCreds.ResolveSecretName(ctx, cameraName)
			if err != nil {
				a.lc.Warnf("pipeline stream secret for spec %s unresolved: %s", specID, err.Error())
			} else {
				want = resolved
			}
		}
	}
	if want != current {
		retranslated := a.retranslateInferenceJob(ctx, specID, sp)
		if want == "" {
			return retranslated
		}
		return want
	}
	return current
}

// currentJobSecretName reads the live NI job's Inference.SecretName property.
func (a *Applier) currentJobSecretName(ctx context.Context, deviceName string) string {
	resp, err := a.deviceByName(ctx, deviceName)
	if err != nil {
		return ""
	}
	props, ok := resp.Device.Protocols[inferenceProtocol]
	if !ok {
		return ""
	}
	name, _ := props["SecretName"].(string)
	return name
}

// retranslateInferenceJob rebuilds one inference job device from its stored
// desired config and updates it in core-metadata, returning the resulting
// SecretName property ("" for snapshot).
func (a *Applier) retranslateInferenceJob(ctx context.Context, specID string, sp appliedSpec) string {
	push := device.ConnectorPush{
		SpecID:        specID,
		DeviceID:      sp.deviceID,
		ConnectorType: TypeEdgeInference,
		Revision:      sp.revision,
		Config:        sp.config,
		Mappings:      sp.mappings,
	}
	cfg, err := a.inferenceDeviceConfig(push)
	if err != nil {
		a.lc.Warnf("retranslate inference job %s failed: %s", specID, err.Error())
		return ""
	}
	if err := a.upsertDevice(ctx, cfg); err != nil {
		a.lc.Warnf("update inference job %s for secret change failed: %s", specID, err.Error())
		return ""
	}
	a.lc.Infof("inference job %s refreshed for stream credential change", specID)
	name, _ := cfg.Protocols[inferenceProtocol]["SecretName"].(string)
	return name
}

// StartInferenceStatusPoller translates NI job State/LastError readings to
// connector status frames on transitions only.
func (a *Applier) StartInferenceStatusPoller(ctx context.Context) {
	if a.inferenceCommands == nil {
		return
	}
	interval := a.inferencePoll
	if interval <= 0 {
		interval = bindingPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if a.inferenceNegotiated != nil && !a.inferenceNegotiated() {
				continue
			}
			if a.inferencePresent != nil && !a.inferencePresent() {
				continue
			}
			a.pollInferenceStatus(ctx)
		}
	}
}

// DegradedInferenceBindings counts bindings currently reported DEGRADED; the
// gateway shadow uses it to downgrade ONLINE to DEGRADED.
func (a *Applier) DegradedInferenceBindings() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, state := range a.inferenceState {
		if strings.HasPrefix(state, device.ConnectorStateDegraded) {
			n++
		}
	}
	return n
}

// InferenceBindingCounts returns the number of live NE-created edge-inference
// jobs per (model id, model version), keyed id+"\x00"+version. The device
// service counts every Job device it hosts, including hand-built and orphaned
// jobs; §15.2 active_bindings must reflect only cloud bindings this NE still
// serves. FAILED jobs still exist and are counted; broken specs (device
// missing and unrestorable) are not.
func (a *Applier) InferenceBindingCounts() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]int)
	for specID, sp := range a.applied {
		if sp.connectorType != TypeEdgeInference {
			continue
		}
		if _, bad := a.broken[specID]; bad {
			continue
		}
		var cfg inferenceConfig
		if err := json.Unmarshal(sp.config, &cfg); err != nil {
			continue
		}
		if cfg.ModelRef == nil || cfg.ModelRef.ID == "" {
			continue
		}
		out[cfg.ModelRef.ID+"\x00"+cfg.ModelRef.Version]++
	}
	return out
}

// ActiveInferenceBindings returns the total number of live edge-inference
// jobs owned by this NE. It overrides the DS-side sum in the shadow block so
// orphan/hand-built jobs on the DS cannot inflate it.
func (a *Applier) ActiveInferenceBindings() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for specID, sp := range a.applied {
		if sp.connectorType != TypeEdgeInference {
			continue
		}
		if _, bad := a.broken[specID]; !bad {
			n++
		}
	}
	return n
}

const (
	// inferenceReplayGrace holds the orphan sweep after (re)hello/process
	// start until NL's desired-state redrive has settled, so a live job
	// whose spec simply has not replayed yet can never be deleted.
	inferenceReplayGrace = 30 * time.Second
	// inferenceSweepThrottle bounds the cost of the full metadata scan; the
	// manifest provider fires on every debounced change.
	inferenceSweepThrottle = 2 * time.Minute
	inferenceSweepPageSize = 100
)

// SweepOrphanInferenceJobs converges NI Job devices to the cloud spec set
// after reconnect/redrive: any Job carrying NE ownership tags whose spec is
// no longer applied is deleted and its virtual presence withdrawn. Jobs
// without the nereid-spec tag (hand-built/test leftovers) are never removed:
// ownership cannot be proven for them, and ForwardUnmapped:false already
// keeps their telemetry off the cloud. Such leftovers are surfaced in docs
// for manual cleanup.
func (a *Applier) SweepOrphanInferenceJobs(ctx context.Context) {
	if a.observe || !a.inferenceAdvert {
		return
	}
	a.mu.Lock()
	if time.Since(a.settledAt) < inferenceReplayGrace ||
		time.Since(a.lastSweep) < inferenceSweepThrottle {
		a.mu.Unlock()
		return
	}
	live := make(map[string]struct{}, len(a.applied))
	for specID, sp := range a.applied {
		if sp.connectorType == TypeEdgeInference {
			live[specID] = struct{}{}
		}
	}
	dsName := a.inferenceDSName
	a.lastSweep = time.Now()
	a.mu.Unlock()
	if dsName == "" {
		dsName = defaultInferenceDS
	}

	var orphans []dtos.Device
	for offset := 0; ; offset += inferenceSweepPageSize {
		listCtx, cancel := context.WithTimeout(ctx, a.metadataTimeout)
		resp, err := a.service.DeviceClient().AllDevices(listCtx, nil, offset, inferenceSweepPageSize)
		cancel()
		if err != nil {
			a.lc.Warnf("inference orphan sweep cannot list EdgeX devices: %s", err.Error())
			return
		}
		for i := range resp.Devices {
			d := resp.Devices[i]
			if d.ServiceName != dsName {
				continue
			}
			specID, ok := d.Tags["nereid-spec"].(string)
			if !ok || specID == "" {
				continue
			}
			if _, alive := live[specID]; alive {
				continue
			}
			orphans = append(orphans, d)
		}
		if len(resp.Devices) < inferenceSweepPageSize {
			break
		}
	}

	for _, d := range orphans {
		specID, _ := d.Tags["nereid-spec"].(string)
		childUUID, _ := d.Tags["nereid-device"].(string)
		if err := a.deviceDelete(ctx, d.Name); err != nil {
			a.lc.Errorf("sweep: failed to delete orphan inference job %s (spec=%s): %s",
				d.Name, specID, err.Error())
			continue
		}
		a.resolver.ReleaseSpec(specID, d.Name)
		a.lc.Infof("swept orphan inference job: edgex=%s spec=%s virtual=%s", d.Name, specID, childUUID)
		if a.onDetach != nil {
			a.onDetach(d.Name, childUUID)
		}
	}
}

func (a *Applier) pollInferenceStatus(ctx context.Context) {
	if a.inferenceNegotiated != nil && !a.inferenceNegotiated() {
		return
	}
	type binding struct {
		specID     string
		revision   int64
		deviceName string
	}
	var bindings []binding
	a.mu.Lock()
	for specID, sp := range a.applied {
		if sp.connectorType == TypeEdgeInference && sp.deviceName != "" {
			bindings = append(bindings, binding{specID: specID, revision: sp.revision, deviceName: sp.deviceName})
		}
	}
	active := make(map[string]bool, len(bindings))
	a.mu.Unlock()

	for _, b := range bindings {
		active[b.specID] = true
		state, reason, stats := a.readInferenceHealth(ctx, b.deviceName)
		a.mu.Lock()
		previous := a.inferenceState[b.specID]
		next := state + "\x00" + reason
		if stats != nil {
			next += "\x00" + stats.key()
		}
		changed := previous != next
		if changed {
			a.inferenceState[b.specID] = next
		}
		a.mu.Unlock()
		if changed {
			if err := a.reportStatusFrame(connectorStatusFrame{
				ConnectorStatus: device.ConnectorStatus{
					SpecID: b.specID, State: state, Revision: b.revision, Reason: reason,
				},
				Stats: stats,
			}); err != nil {
				// Keep the health sample dirty so the next poll republishes it even
				// when state, reason, and every metric remain unchanged.
				a.mu.Lock()
				if a.inferenceState[b.specID] == next {
					a.inferenceState[b.specID] = previous
				}
				a.mu.Unlock()
			}
		}
	}
	a.mu.Lock()
	for specID := range a.inferenceState {
		if !active[specID] {
			delete(a.inferenceState, specID)
		}
	}
	a.mu.Unlock()
}

func (a *Applier) readInferenceStateAndError(ctx context.Context, deviceName string) (string, string, bool) {
	stateEvent, status, err := a.inferenceCommands.Read(ctx, deviceName, "State", jobReadQuery)
	if err != nil || status != 200 {
		return device.ConnectorStateFailed, "runtime-unreachable", false
	}
	state, _ := corecmd.ReadingsToMap(stateEvent.Event.Readings)["State"].(string)
	reason := ""
	if reasonEvent, reasonStatus, reasonErr := a.inferenceCommands.Read(ctx, deviceName, "LastError", jobReadQuery); reasonErr == nil && reasonStatus == 200 {
		reason, _ = corecmd.ReadingsToMap(reasonEvent.Event.Readings)["LastError"].(string)
	}
	switch state {
	case device.ConnectorStateApplied:
		return device.ConnectorStateApplied, "", true
	case device.ConnectorStateDegraded:
		if reason == "" {
			reason = "idle-no-inference"
		}
		return device.ConnectorStateDegraded, SanitizeReason(reason), true
	case device.ConnectorStateFailed:
		if reason == "" {
			reason = "runtime-error"
		}
		return device.ConnectorStateFailed, SanitizeReason(reason), true
	default:
		return device.ConnectorStateFailed, "runtime-error", true
	}
}

func (a *Applier) readInferenceHealth(ctx context.Context, deviceName string) (string, string, *InferenceJobStats) {
	state, reason, readable := a.readInferenceStateAndError(ctx, deviceName)
	if !readable {
		return state, reason, nil
	}
	var stats *InferenceJobStats
	for _, command := range []string{"SuccessTotal", "ErrorTotal", "P50Ms", "P95Ms"} {
		event, status, err := a.inferenceCommands.Read(ctx, deviceName, command, jobReadQuery)
		if err != nil || status != 200 {
			continue
		}
		value, exists := corecmd.ReadingsToMap(event.Event.Readings)[command]
		if !exists {
			continue
		}
		if stats == nil {
			stats = &InferenceJobStats{}
		}
		switch command {
		case "SuccessTotal":
			stats.SuccessTotal = inferenceCounter(value)
		case "ErrorTotal":
			stats.ErrorTotal = inferenceCounter(value)
		case "P50Ms":
			stats.P50MS = inferenceLatency(value)
		case "P95Ms":
			stats.P95MS = inferenceLatency(value)
		}
	}
	// NI emits numeric-zero latency readings before the first successful job.
	// They are placeholders, not measurements; zero-valued counters remain
	// meaningful and must still be reported.
	if stats != nil && stats.SuccessTotal != nil && *stats.SuccessTotal == 0 {
		stats.P50MS = nil
		stats.P95MS = nil
	}
	if stats != nil && stats.SuccessTotal == nil && stats.ErrorTotal == nil && stats.P50MS == nil && stats.P95MS == nil {
		return state, reason, nil
	}
	return state, reason, stats
}

// inferenceCounter accepts NI's Int64 readings and their string transport.
// Invalid or negative values are unavailable, never a fabricated zero.
func inferenceCounter(value any) *uint64 {
	var parsed uint64
	switch typed := value.(type) {
	case uint64:
		parsed = typed
	case uint32:
		parsed = uint64(typed)
	case int64:
		if typed < 0 {
			return nil
		}
		parsed = uint64(typed)
	case int32:
		if typed < 0 {
			return nil
		}
		parsed = uint64(typed)
	case int:
		if typed < 0 {
			return nil
		}
		parsed = uint64(typed)
	case float64:
		if typed < 0 || typed >= 1<<64 || typed != math.Trunc(typed) || math.IsInf(typed, 0) || math.IsNaN(typed) {
			return nil
		}
		parsed = uint64(typed)
	case string:
		value, err := strconv.ParseUint(strings.TrimSpace(typed), 10, 64)
		if err != nil {
			return nil
		}
		parsed = value
	default:
		return nil
	}
	return &parsed
}

func inferenceLatency(value any) *float64 {
	var parsed float64
	switch typed := value.(type) {
	case float64:
		parsed = typed
	case float32:
		parsed = float64(typed)
	case int64:
		parsed = float64(typed)
	case int32:
		parsed = float64(typed)
	case int:
		parsed = float64(typed)
	case uint64:
		parsed = float64(typed)
	case string:
		value, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return nil
		}
		parsed = value
	default:
		return nil
	}
	if parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return nil
	}
	return &parsed
}

// ResolveManagedName maps a cloud-managed sub-device UUID to its EdgeX
// device name; used by the camera.profiles operation.
func (a *Applier) ResolveManagedName(uuid string) (string, bool) {
	return a.resolver.Name(uuid)
}

// FindInferenceBinding locates the translated job device for one model and
// optional source sub-device, used by inference.diag.
func (a *Applier) FindInferenceBinding(modelID, modelVersion, sourceUUID string) (string, bool) {
	name, _, ok := a.FindInferenceBindingWithMode(modelID, modelVersion, sourceUUID)
	return name, ok
}

// FindInferenceBindingWithMode additionally reports the binding's acquisition
// mode ("snapshot" or "stream") so diagnostics can say where the frame came
// from.
func (a *Applier) FindInferenceBindingWithMode(modelID, modelVersion, sourceUUID string) (string, string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, sp := range a.applied {
		if sp.connectorType != TypeEdgeInference || sp.deviceName == "" {
			continue
		}
		var cfg inferenceConfig
		if err := json.Unmarshal(sp.config, &cfg); err != nil {
			continue
		}
		if cfg.ModelRef == nil || cfg.ModelRef.ID != modelID ||
			(modelVersion != "" && cfg.ModelRef.Version != modelVersion) {
			continue
		}
		if sourceUUID != "" && cfg.Source.Device != sourceUUID {
			continue
		}
		mode := cfg.Source.Mode
		if mode == "" {
			mode = "snapshot"
		}
		return sp.deviceName, mode, true
	}
	return "", "", false
}
