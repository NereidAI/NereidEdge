// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

const (
	infCameraUUID = "11111111-2222-3333-4444-555555555555"
	infChildUUID  = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	infCameraName = "onvif-camera-7"
)

func inferencePush(specID string, cfg map[string]any) device.ConnectorPush {
	raw, _ := json.Marshal(cfg)
	return device.ConnectorPush{
		Kind:          device.ConnectorKindSpec,
		SpecID:        specID,
		DeviceID:      infChildUUID,
		ConnectorType: TypeEdgeInference,
		Revision:      1,
		Config:        raw,
	}
}

func inferencePushRev(specID string, rev int64, cfg map[string]any) device.ConnectorPush {
	push := inferencePush(specID, cfg)
	push.Revision = rev
	return push
}

func validInferenceConfig(trigger string) map[string]any {
	cfg := map[string]any{
		"source":    map[string]any{"device": infCameraUUID, "resource": "snapshot_jpeg"},
		"model_ref": map[string]any{"id": "person", "version": "1.0.0", "revision": "r2"},
		"trigger":   trigger,
	}
	if trigger == "periodic" {
		cfg["period_ms"] = 500
	}
	return cfg
}

func newInferenceApplier(t *testing.T) (*Applier, *fakeDeviceClient, *statusRecord) {
	t.Helper()
	dev := &fakeDeviceClient{notFound: true}
	a, rec := newQueueApplier(t, dev, nil, nil)
	a.resolver.SetProvisioned(infCameraName, infCameraUUID)
	a.ConfigureInference("device-inference", inferenceProfile,
		nil, 50*time.Millisecond, func() bool { return true })
	return a, dev, rec
}

func TestInferenceDeviceNameSanitizesSpecID(t *testing.T) {
	got := inferenceDeviceName("07704cf9-f0fc-4876-8955-64e6141540c7")
	want := "inference-07704cf9-f0fc-4876-8955-64e6141540c7"
	if got != want {
		t.Fatalf("name=%q, want %q", got, want)
	}
	if bad := inferenceDeviceName("a:b/c"); !strings.HasPrefix(bad, "inference-a-b-c") {
		t.Fatalf("colon/slash not sanitized: %q", bad)
	}
}

func TestTranslateInferenceSpecToJobProperties(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	cfg, err := a.inferenceDeviceConfig(inferencePush("spec-1", validInferenceConfig("periodic")))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if cfg.DSName != "device-inference" || cfg.ProfileName != inferenceProfile {
		t.Fatalf("service/profile=%s/%s", cfg.DSName, cfg.ProfileName)
	}
	if cfg.DeviceName != "inference-spec-1" {
		t.Fatalf("job device name=%q", cfg.DeviceName)
	}
	if local, _ := cfg.Tags["local"].(bool); !local {
		t.Fatalf("tags=%v, want local:true for immediate presence", cfg.Tags)
	}
	// The NL identities are stamped on the EdgeX device, giving an
	// out-of-band mapping that survives an NE restart.
	if spec, _ := cfg.Tags["nereid-spec"].(string); spec != "spec-1" {
		t.Fatalf("nereid-spec tag=%q, want spec-1", spec)
	}
	if child, _ := cfg.Tags["nereid-device"].(string); child != infChildUUID {
		t.Fatalf("nereid-device tag=%q, want %s", child, infChildUUID)
	}
	props := cfg.Protocols[inferenceProtocol]
	assertProp := func(key, want string) {
		t.Helper()
		if got, _ := props[key].(string); got != want {
			t.Errorf("property %s=%q, want %q", key, got, want)
		}
	}
	assertProp("SourceKind", "onvif-snapshot")
	assertProp("SourceDevice", infCameraName)
	assertProp("ModelID", "person")
	assertProp("ModelVersion", "1.0.0")
	assertProp("Trigger", "periodic")
	assertProp("PeriodMs", "500")
	assertProp("MaxIdleMs", "60000")
	assertProp("ErrorThreshold", "5")
	assertProp("ConfThreshold", "0")
	assertProp("TargetDevice", "CPU")
	assertProp("EmitFrame", "true")

	// event semantics maps to NI's command trigger and must NOT invent a period.
	eventCfg, err := a.inferenceDeviceConfig(inferencePush("spec-2", validInferenceConfig("event")))
	if err != nil {
		t.Fatalf("translate event: %v", err)
	}
	eventProps := eventCfg.Protocols[inferenceProtocol]
	if got, _ := eventProps["Trigger"].(string); got != "command" {
		t.Fatalf("event trigger=%q, want command", got)
	}
	if got, _ := eventProps["PeriodMs"].(string); got != "0" {
		t.Fatalf("command job PeriodMs=%q, want \"0\"", got)
	}
}

// TestTranslateStreamSourceAndContinuousTrigger pins the §15.3 acquisition
// expansion: source.mode=stream maps to NI's onvif-camera federated RTSP
// source, and trigger=continuous maps to NI continuous with an FPS property.
func TestTranslateStreamSourceAndContinuousTrigger(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	cfg := validInferenceConfig("continuous")
	cfg["source"] = map[string]any{
		"device": infCameraUUID, "resource": "snapshot_jpeg", "mode": "stream",
	}
	cfg["fps"] = 12.5

	translated, err := a.inferenceDeviceConfig(inferencePush("spec-stream", cfg))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	props := translated.Protocols[inferenceProtocol]
	assert := func(key, want string) {
		t.Helper()
		if got, _ := props[key].(string); got != want {
			t.Errorf("property %s=%q, want %q", key, got, want)
		}
	}
	assert("SourceKind", "onvif-camera")
	assert("SourceDevice", infCameraName)
	assert("Trigger", "continuous")
	assert("FPS", "12.5")

	// Absent mode keeps the legacy snapshot path and emits no FPS for
	// non-continuous jobs.
	legacy, err := a.inferenceDeviceConfig(inferencePush("spec-legacy", validInferenceConfig("event")))
	if err != nil {
		t.Fatalf("translate legacy: %v", err)
	}
	legacyProps := legacy.Protocols[inferenceProtocol]
	if got, _ := legacyProps["SourceKind"].(string); got != "onvif-snapshot" {
		t.Fatalf("default SourceKind=%q, want onvif-snapshot", got)
	}
	if _, has := legacyProps["FPS"]; has {
		t.Fatalf("FPS must be absent for non-continuous jobs, got %v", legacyProps["FPS"])
	}

	// continuous without explicit fps gets NI's 5 fps default.
	defaults := validInferenceConfig("continuous")
	cont, err := a.inferenceDeviceConfig(inferencePush("spec-cont", defaults))
	if err != nil {
		t.Fatalf("translate continuous: %v", err)
	}
	if got, _ := cont.Protocols[inferenceProtocol]["FPS"].(string); got != "5" {
		t.Fatalf("default FPS=%q, want 5", got)
	}
}

func TestTranslateFrameAndFFmpegOptions(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	cfg := validInferenceConfig("periodic")
	cfg["source"] = map[string]any{
		"device": infCameraUUID, "resource": "snapshot_jpeg",
		"mode": "stream", "profile": "Profile_2",
	}
	cfg["emit_frame"] = false
	cfg["annotate_preview"] = true
	cfg["ffmpeg_extra"] = "-fflags nobuffer -flags low_delay"

	translated, err := a.inferenceDeviceConfig(inferencePush("spec-opts", cfg))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	props := translated.Protocols[inferenceProtocol]
	if got, _ := props["EmitFrame"].(string); got != "false" {
		t.Fatalf("EmitFrame=%q, want false", got)
	}
	if got, _ := props["AnnotatePreview"].(string); got != "true" {
		t.Fatalf("AnnotatePreview=%q, want true", got)
	}
	if got, _ := props["FFmpegExtra"].(string); got != "-fflags nobuffer -flags low_delay" {
		t.Fatalf("FFmpegExtra=%q", got)
	}

	// Absent options keep the bridge defaults and never send FFmpegExtra.
	legacy, err := a.inferenceDeviceConfig(inferencePush("spec-legacy", validInferenceConfig("event")))
	if err != nil {
		t.Fatalf("translate legacy: %v", err)
	}
	lp := legacy.Protocols[inferenceProtocol]
	if got, _ := lp["EmitFrame"].(string); got != "true" {
		t.Fatalf("default EmitFrame=%q, want true", got)
	}
	if got, _ := lp["AnnotatePreview"].(string); got != "true" {
		t.Fatalf("default AnnotatePreview=%q, want true", got)
	}
	if _, has := lp["FFmpegExtra"]; has {
		t.Fatal("FFmpegExtra must be absent when not configured")
	}
}

// TestTranslateDecodeAccelPassthrough pins the site-local decoder contract:
// legacy bindings omit both properties, while every configured value reaches
// NI as a string without changing the independent FFmpegExtra property.
func TestTranslateDecodeAccelPassthrough(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	tests := []struct {
		name         string
		decodeAccel  string
		vaapiDevice  string
		directRTSP   bool
		wantDecode   string
		wantVAAPIDev string
	}{
		{name: "legacy defaults omit both"},
		{name: "explicit software", decodeAccel: "software", wantDecode: "software"},
		{name: "vaapi uses NI default render node", decodeAccel: "vaapi", wantDecode: "vaapi"},
		{
			name: "short render node stays verbatim", decodeAccel: "vaapi", vaapiDevice: "renderD129",
			wantDecode: "vaapi", wantVAAPIDev: "renderD129",
		},
		{
			name: "absolute render node stays verbatim on direct RTSP", decodeAccel: "vaapi",
			vaapiDevice: "/dev/dri/renderD129", directRTSP: true,
			wantDecode: "vaapi", wantVAAPIDev: "/dev/dri/renderD129",
		},
		{
			name: "device without vaapi is still passed to NI", vaapiDevice: "renderD129",
			wantVAAPIDev: "renderD129",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validInferenceConfig("periodic")
			cfg["source"] = map[string]any{
				"device": infCameraUUID, "resource": "snapshot_jpeg", "mode": "stream",
			}
			if tt.directRTSP {
				cfg["source"] = map[string]any{
					"mode": "url", "transport": "rtsp", "url": "rtsp://cam.local/stream",
				}
			}
			cfg["ffmpeg_extra"] = "-threads 2"
			if tt.decodeAccel != "" {
				cfg["decode_accel"] = tt.decodeAccel
			}
			if tt.vaapiDevice != "" {
				cfg["vaapi_device"] = tt.vaapiDevice
			}

			translated, err := a.inferenceDeviceConfig(inferencePush("spec-decode", cfg))
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			props := translated.Protocols[inferenceProtocol]
			assertOptionalString := func(key, want string) {
				t.Helper()
				value, exists := props[key]
				if want == "" {
					if exists {
						t.Fatalf("%s must be absent, got %#v", key, value)
					}
					return
				}
				got, ok := value.(string)
				if !ok {
					t.Fatalf("%s type=%T, want string", key, value)
				}
				if got != want {
					t.Fatalf("%s=%q, want %q", key, got, want)
				}
			}
			assertOptionalString("DecodeAccel", tt.wantDecode)
			assertOptionalString("VAAPIDevice", tt.wantVAAPIDev)
			if got, ok := props["FFmpegExtra"].(string); !ok || got != "-threads 2" {
				t.Fatalf("FFmpegExtra=%#v, want unchanged string %q", props["FFmpegExtra"], "-threads 2")
			}
		})
	}
}

// TestTranslateDecodeAccelLeavesSourceValidationToNI ensures NE never turns
// an unsupported VA-API/source combination into a silent software fallback.
func TestTranslateDecodeAccelLeavesSourceValidationToNI(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	tests := []struct {
		name       string
		source     map[string]any
		sourceKind string
	}{
		{
			name: "snapshot",
			source: map[string]any{
				"device": infCameraUUID, "resource": "snapshot_jpeg", "mode": "snapshot",
			},
			sourceKind: "onvif-snapshot",
		},
		{
			name: "direct mpjpeg",
			source: map[string]any{
				"mode": "url", "transport": "mpjpeg", "url": "http://cam.local/video",
			},
			sourceKind: "mpjpeg",
		},
		{
			name: "direct jpeg",
			source: map[string]any{
				"mode": "url", "transport": "jpeg", "url": "http://cam.local/snapshot.jpg",
			},
			sourceKind: "jpeg",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validInferenceConfig("periodic")
			cfg["source"] = tt.source
			cfg["decode_accel"] = "vaapi"
			cfg["vaapi_device"] = "renderD129"
			translated, err := a.inferenceDeviceConfig(inferencePush("spec-invalid-vaapi-source", cfg))
			if err != nil {
				t.Fatalf("NE must leave source compatibility validation to NI: %v", err)
			}
			props := translated.Protocols[inferenceProtocol]
			if got, _ := props["SourceKind"].(string); got != tt.sourceKind {
				t.Fatalf("SourceKind=%q, want unchanged %q", got, tt.sourceKind)
			}
			if got, ok := props["DecodeAccel"].(string); !ok || got != "vaapi" {
				t.Fatalf("DecodeAccel=%#v, want vaapi without software fallback", props["DecodeAccel"])
			}
			if got, ok := props["VAAPIDevice"].(string); !ok || got != "renderD129" {
				t.Fatalf("VAAPIDevice=%#v, want renderD129", props["VAAPIDevice"])
			}
		})
	}
}

func TestTranslateDirectURLSource(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	// Direct RTSP URL: no managed camera sub-device and no resolver lookup;
	// the URL and a named edge-side secret map straight to NI properties.
	cfg := validInferenceConfig("continuous")
	cfg["fps"] = 4
	cfg["source"] = map[string]any{
		"mode": "url", "transport": "rtsp",
		"url":         "rtsp://cam-01.local:554/Streaming/Channels/101",
		"secret_name": "direct-cam-01",
	}
	translated, err := a.inferenceDeviceConfig(inferencePush("spec-url", cfg))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	props := translated.Protocols[inferenceProtocol]
	if got, _ := props["SourceKind"].(string); got != "rtsp" {
		t.Fatalf("SourceKind=%q, want rtsp", got)
	}
	if got, _ := props["SourceURL"].(string); got != "rtsp://cam-01.local:554/Streaming/Channels/101" {
		t.Fatalf("SourceURL=%q", got)
	}
	if got, _ := props["SecretName"].(string); got != "direct-cam-01" {
		t.Fatalf("SecretName=%q, want direct-cam-01", got)
	}
	if _, has := props["SourceDevice"]; has {
		t.Fatalf("direct URL must not carry SourceDevice: %v", props["SourceDevice"])
	}
	if _, has := props["StreamProfile"]; has {
		t.Fatal("direct URL must not carry StreamProfile")
	}

	// HTTP MJPEG URL maps to the mpjpeg kind.
	mjpeg := validInferenceConfig("periodic")
	mjpeg["period_ms"] = 1000
	mjpeg["source"] = map[string]any{
		"mode": "url", "transport": "mpjpeg",
		"url": "http://cam-02.local/video.cgi",
	}
	got2, err := a.inferenceDeviceConfig(inferencePush("spec-mjpeg", mjpeg))
	if err != nil {
		t.Fatalf("translate mpjpeg: %v", err)
	}
	if kind, _ := got2.Protocols[inferenceProtocol]["SourceKind"].(string); kind != "mpjpeg" {
		t.Fatalf("SourceKind=%q, want mpjpeg", kind)
	}
}

func TestTranslateDirectURLRejectsUnsafe(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	base := func(source map[string]any) map[string]any {
		cfg := validInferenceConfig("periodic")
		cfg["source"] = source
		return cfg
	}
	cases := map[string]map[string]any{
		"embedded credentials": {
			"mode": "url", "transport": "rtsp",
			"url": "rtsp://admin:hunter2@cam.local/stream",
		},
		"scheme/transport mismatch": {
			"mode": "url", "transport": "rtsp",
			"url": "http://cam.local/stream",
		},
		"unknown transport": {
			"mode": "url", "transport": "webrtc",
			"url": "rtsp://cam.local/stream",
		},
		"missing host": {
			"mode": "url", "transport": "jpeg",
			"url": "https:///snap.jpg",
		},
		"empty url": {
			"mode": "url", "transport": "rtsp", "url": "",
		},
		"secret on onvif mode": {
			"device": infCameraUUID, "resource": "snapshot_jpeg",
			"mode": "stream", "profile": "Profile_1",
			"secret_name": "sneaky",
		},
	}
	for name, source := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := a.inferenceDeviceConfig(
				inferencePush("spec-bad-url", base(source))); err == nil {
				t.Fatalf("%s: want config-invalid", name)
			}
		})
	}
}

func TestTranslateStreamProfilePassthrough(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	cfg := validInferenceConfig("periodic")
	cfg["source"] = map[string]any{
		"device": infCameraUUID, "resource": "snapshot_jpeg",
		"mode": "stream", "profile": "Profile_2",
	}
	translated, err := a.inferenceDeviceConfig(inferencePush("spec-sub", cfg))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if got, _ := translated.Protocols[inferenceProtocol]["StreamProfile"].(string); got != "Profile_2" {
		t.Fatalf("StreamProfile=%q, want Profile_2", got)
	}

	// A profile token only makes sense on the federated stream source.
	snapCfg := validInferenceConfig("periodic")
	snapCfg["source"] = map[string]any{
		"device": infCameraUUID, "resource": "snapshot_jpeg", "profile": "Profile_2",
	}
	if _, err := a.inferenceDeviceConfig(inferencePush("spec-snap-prof", snapCfg)); err == nil {
		t.Fatal("StreamProfile with snapshot mode must be config-invalid")
	}

	// Unsafe tokens are rejected before they can reach a command body.
	badCfg := validInferenceConfig("periodic")
	badCfg["source"] = map[string]any{
		"device": infCameraUUID, "resource": "snapshot_jpeg",
		"mode": "stream", "profile": "Profile 2; rm -rf /",
	}
	if _, err := a.inferenceDeviceConfig(inferencePush("spec-bad-prof", badCfg)); err == nil {
		t.Fatal("unsafe StreamProfile must be config-invalid")
	}
}

func TestTranslateRejectsUnknownModeAndTrigger(t *testing.T) {
	a, _, _ := newInferenceApplier(t)

	badMode := validInferenceConfig("periodic")
	badMode["source"] = map[string]any{
		"device": infCameraUUID, "resource": "snapshot_jpeg", "mode": "rtsp-url",
	}
	if _, err := a.inferenceDeviceConfig(inferencePush("spec-bad-mode", badMode)); err == nil {
		t.Fatal("unknown source mode must be config-invalid, not mapped")
	}

	badTrigger := validInferenceConfig("manual")
	if _, err := a.inferenceDeviceConfig(inferencePush("spec-bad-trigger", badTrigger)); err == nil {
		t.Fatal("unknown trigger must be config-invalid, not mapped")
	}
}

func TestTranslateTargetDevicePassthrough(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	gpuCfg := validInferenceConfig("periodic")
	gpuCfg["target_device"] = "GPU"

	translated, err := a.inferenceDeviceConfig(inferencePush("spec-gpu", gpuCfg))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if got, _ := translated.Protocols[inferenceProtocol]["TargetDevice"].(string); got != "GPU" {
		t.Fatalf("TargetDevice=%q, want GPU verbatim (no NE-side remap)", got)
	}

	// Omitted selects the DS default CPU; whitespace-only is treated absent.
	empty, err := a.inferenceDeviceConfig(inferencePush("spec-cpu", validInferenceConfig("periodic")))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if got, _ := empty.Protocols[inferenceProtocol]["TargetDevice"].(string); got != "CPU" {
		t.Fatalf("default TargetDevice=%q, want CPU", got)
	}
}

// TestTargetDeviceChangeRebuildsJobDevice verifies the existing hash-based
// dedup treats target_device as part of desired state: a revision changing
// only target_device must update the EdgeX job device (the DS rebuilds the
// OVMS servable lease on UpdateDevice).
func TestTargetDeviceChangeRebuildsJobDevice(t *testing.T) {
	a, dev, rec := newInferenceApplier(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.runPushWorker(ctx)

	cpuCfg := validInferenceConfig("periodic")
	a.enqueuePush(inferencePushRev("spec-target", 1, cpuCfg))
	rec.waitFor(t, device.ConnectorStateApplied, "")
	dev.mu.Lock()
	adds := dev.addCalls
	dev.mu.Unlock()
	if adds != 1 {
		t.Fatalf("initial Add calls=%d, want 1", adds)
	}

	gpuCfg := validInferenceConfig("periodic")
	gpuCfg["target_device"] = "GPU"
	a.enqueuePush(inferencePushRev("spec-target", 2, gpuCfg))
	rec.waitFor(t, device.ConnectorStateApplied, "")
	deadline := time.Now().Add(time.Second)
	var upds int
	for time.Now().Before(deadline) {
		dev.mu.Lock()
		upds = dev.updCalls
		dev.mu.Unlock()
		if upds >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	dev.mu.Lock()
	defer dev.mu.Unlock()
	if dev.updCalls < 1 {
		t.Fatal("target_device change did not update the job device")
	}
}

// TestDecodeConfigChangeRebuildsJobDevice verifies each new NL field participates
// in the existing canonical-config update path rather than being deduplicated.
func TestDecodeConfigChangeRebuildsJobDevice(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		property string
	}{
		{name: "decode accel", key: "decode_accel", value: "vaapi", property: "DecodeAccel"},
		{name: "VAAPI device", key: "vaapi_device", value: "renderD129", property: "VAAPIDevice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, dev, rec := newInferenceApplier(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go a.runPushWorker(ctx)

			base := validInferenceConfig("periodic")
			base["source"] = map[string]any{
				"device": infCameraUUID, "resource": "snapshot_jpeg", "mode": "stream",
			}
			a.enqueuePush(inferencePushRev("spec-decode-update", 1, base))
			rec.waitFor(t, device.ConnectorStateApplied, "")

			changed := validInferenceConfig("periodic")
			changed["source"] = base["source"]
			changed[tt.key] = tt.value
			a.enqueuePush(inferencePushRev("spec-decode-update", 2, changed))
			for {
				st := rec.waitFor(t, device.ConnectorStateApplied, "")
				if st.Revision == 2 {
					break
				}
			}

			dev.mu.Lock()
			defer dev.mu.Unlock()
			if dev.updCalls != 1 {
				t.Fatalf("Update calls=%d, want 1 after changing only %s", dev.updCalls, tt.key)
			}
			props := dev.devices["inference-spec-decode-update"].Protocols[inferenceProtocol]
			if got, ok := props[tt.property].(string); !ok || got != tt.value {
				t.Fatalf("updated %s=%#v, want string %q", tt.property, props[tt.property], tt.value)
			}
		})
	}
}

// TestInferencePollReportsModelNotReady verifies an OVMS target that fails to
// load (e.g. a CPU artifact requested on GPU) is surfaced verbatim as
// FAILED/model-not-ready, never as success and never rewritten to CPU.
func TestInferencePollReportsModelNotReady(t *testing.T) {
	a, _, rec := newInferenceApplier(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := &fakeInferenceReader{state: "FAILED", lastError: "model-not-ready"}
	a.inferenceCommands = reader
	a.mu.Lock()
	a.applied["spec-gpu-target"] = appliedSpec{
		revision:      1,
		deviceID:      infChildUUID,
		deviceName:    "inference-spec-gpu-target",
		connectorType: TypeEdgeInference,
	}
	a.mu.Unlock()

	a.pollInferenceStatus(ctx)
	st := rec.waitFor(t, device.ConnectorStateFailed, "model-not-ready")
	if st.Revision != 1 {
		t.Fatalf("revision=%d", st.Revision)
	}
}

func TestInferenceSpecMissingCameraIsConfigInvalid(t *testing.T) {
	dev := &fakeDeviceClient{notFound: true}
	a, rec := newQueueApplier(t, dev, nil, nil)
	// No provisioned camera: the resolver cannot map the NL camera UUID.
	h := NewInferenceHandler(a)
	push := inferencePush("spec-missing-cam", validInferenceConfig("event"))

	if err := h.Apply(context.Background(), push); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	st := rec.waitFor(t, device.ConnectorStateFailed, "config-invalid")
	if st.Revision != 1 {
		t.Fatalf("revision=%d", st.Revision)
	}
	if dev.addCalls != 0 {
		t.Fatalf("metadata Add called %d times for untranslatable spec", dev.addCalls)
	}
}

func TestInferenceApplyLifecycleAndStatusPoll(t *testing.T) {
	a, dev, rec := newInferenceApplier(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.runPushWorker(ctx)

	reader := &fakeInferenceReader{state: "APPLIED"}
	a.inferenceCommands = reader
	a.enqueuePush(inferencePush("spec-life", validInferenceConfig("periodic")))

	applied := rec.waitFor(t, device.ConnectorStateApplied, "")

	// The translated Job device carries the Inference protocol properties.
	dev.mu.Lock()
	if len(dev.added) != 1 {
		t.Fatalf("created devices=%d, want 1", len(dev.added))
	}
	job := dev.added[0]
	dev.mu.Unlock()
	if job.ProfileName != inferenceProfile || job.ServiceName != "device-inference" {
		t.Fatalf("job profile/service=%s/%s", job.ProfileName, job.ServiceName)
	}
	props := job.Protocols[inferenceProtocol]
	if props == nil || props["ModelID"] != "person" {
		t.Fatalf("job protocols=%v", job.Protocols)
	}
	// NI events for the job resolve to the NL virtual output sub-device.
	if uuid, ok := a.resolver.Resolve(job.Name); !ok || uuid != infChildUUID {
		t.Fatalf("resolver maps %s to %q,%v, want %s", job.Name, uuid, ok, infChildUUID)
	}
	_ = applied

	// First poll at the same APPLIED state emits nothing.
	a.pollInferenceStatus(ctx)
	if n := a.DegradedInferenceBindings(); n != 0 {
		t.Fatalf("degraded=%d, want 0", n)
	}

	// DEGRADED edge is reported once with the DS reason verbatim; an unchanged
	// second poll must not repeat the frame.
	reader.set("DEGRADED", "idle-no-inference")
	a.pollInferenceStatus(ctx)
	st := rec.waitFor(t, device.ConnectorStateDegraded, "idle-no-inference")
	if st.Revision != 1 {
		t.Fatalf("degraded revision=%d", st.Revision)
	}
	if n := a.DegradedInferenceBindings(); n != 1 {
		t.Fatalf("degraded bindings=%d, want 1", n)
	}
	a.pollInferenceStatus(ctx)
	if drained := drainStatus(rec); drained != 0 {
		t.Fatalf("unchanged state produced %d duplicate reports", drained)
	}

	// FAILED edge with another §15.6 reason.
	reader.set("FAILED", "source-no-frame")
	a.pollInferenceStatus(ctx)
	rec.waitFor(t, device.ConnectorStateFailed, "source-no-frame")

	// DS unreachable maps to FAILED/runtime-unreachable.
	reader.fail = true
	a.pollInferenceStatus(ctx)
	rec.waitFor(t, device.ConnectorStateFailed, "runtime-unreachable")

	// Binding lookup serves the diag operation.
	// diag "source" is the camera sub-device UUID carried in source.device.
	name, ok := a.FindInferenceBinding("person", "1.0.0", infCameraUUID)
	if !ok || name != job.Name {
		t.Fatalf("FindInferenceBinding=%q,%v", name, ok)
	}
	if _, ok := a.FindInferenceBinding("person", "9.9.9", ""); ok {
		t.Fatal("unknown model version must not resolve a binding")
	}
}

func TestInferenceStatusPollerHonorsNegotiationGate(t *testing.T) {
	a, _, _ := newInferenceApplier(t)
	reader := &fakeInferenceReader{state: "FAILED", lastError: "runtime-error"}
	a.inferenceCommands = reader
	a.inferenceNegotiated = func() bool { return false }

	a.mu.Lock()
	a.applied["spec-gated"] = appliedSpec{
		revision:      1,
		deviceID:      infChildUUID,
		deviceName:    "inference-spec-gated",
		connectorType: TypeEdgeInference,
	}
	a.mu.Unlock()

	a.pollInferenceStatus(context.Background())
	if _, ok := a.inferenceState["spec-gated"]; ok {
		t.Fatal("status poller updated state while inference un-negotiated")
	}
}

func TestSanitizeReason(t *testing.T) {
	cases := map[string]string{
		"model-not-ready":           "model-not-ready",
		"target-device-unavailable": "target-device-unavailable",
		"source-no-frame":           "source-no-frame",
		"dial tcp 10.0.0.1:9000: connect: connection refused": "runtime-error",
		"":     "runtime-error",
		"PATH": "runtime-error",
	}
	for in, want := range cases {
		if got := SanitizeReason(in); got != want {
			t.Errorf("SanitizeReason(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestEdgeInferenceAdvertGating(t *testing.T) {
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	for _, adv := range a.ConnectorAdverts() {
		if adv.Type == TypeEdgeInference {
			t.Fatal("edge-inference advertised before ConfigureInference")
		}
	}
	a.ConfigureInference("device-inference", inferenceProfile, &fakeInferenceReader{},
		time.Millisecond, func() bool { return true })
	var found bool
	for _, adv := range a.ConnectorAdverts() {
		if adv.Type == TypeEdgeInference {
			found = true
			if adv.TypeVersion != inferenceTypeVersion {
				t.Fatalf("type_version=%q", adv.TypeVersion)
			}
			if !json.Valid(adv.ConfigSchema) {
				t.Fatal("advertised schema is not valid JSON")
			}
		}
	}
	if !found {
		t.Fatal("edge-inference missing from adverts after configure")
	}
}

// TestInferenceUnbindDeletesJobWithdrawsPresence pins defect 3: deleting an
// edge-inference spec must delete the NI Job device and invoke detach with
// the remembered virtual output UUID, even if resolver state is stale.
func TestInferenceUnbindDeletesJobWithdrawsPresence(t *testing.T) {
	dev := &fakeDeviceClient{notFound: true}
	type detachCall struct{ edgex, uuid string }
	var detached []detachCall
	a, rec := newQueueApplier(t, dev, nil, func(edgex, uuid string) {
		detached = append(detached, detachCall{edgex, uuid})
	})
	a.resolver.SetProvisioned(infCameraName, infCameraUUID)
	a.ConfigureInference("device-inference", inferenceProfile, nil, 50*time.Millisecond,
		func() bool { return true })
	h := NewInferenceHandler(a)

	push := inferencePush("spec-unbind", validInferenceConfig("periodic"))
	if err := h.Apply(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	drainStatus(rec)

	if err := h.Delete(context.Background(), device.ConnectorPush{
		Kind: device.ConnectorKindDelete, SpecID: "spec-unbind",
		DeviceID: infChildUUID, Revision: 2,
	}); err != nil {
		t.Fatal(err)
	}

	var deleted bool
	for _, name := range dev.delCalls {
		if name == "inference-spec-unbind" {
			deleted = true
		}
	}
	if !deleted {
		t.Fatalf("NI Job not deleted: delCalls=%v", dev.delCalls)
	}
	if len(detached) != 1 || detached[0].edgex != "inference-spec-unbind" ||
		detached[0].uuid != infChildUUID {
		t.Fatalf("detach=%v, want one (inference-spec-unbind, %s)", detached, infChildUUID)
	}
}

// TestInferenceBindingCountsOnlyLiveSpecs pins defect 4: counts derive from
// live NE-owned edge-inference specs, not from DS-side job devices.
func TestInferenceBindingCountsOnlyLiveSpecs(t *testing.T) {
	dev := &fakeDeviceClient{notFound: true}
	a, rec := newQueueApplier(t, dev, nil, nil)
	a.resolver.SetProvisioned(infCameraName, infCameraUUID)
	a.ConfigureInference("device-inference", inferenceProfile, nil, time.Hour,
		func() bool { return true })
	h := NewInferenceHandler(a)

	personCfg := validInferenceConfig("periodic")
	carCfg := validInferenceConfig("periodic")
	carCfg["model_ref"] = map[string]any{"id": "car", "version": "2.0"}
	if err := h.Apply(context.Background(), inferencePush("spec-person", personCfg)); err != nil {
		t.Fatal(err)
	}
	if err := h.Apply(context.Background(), inferencePush("spec-car", carCfg)); err != nil {
		t.Fatal(err)
	}
	drainStatus(rec)

	counts := a.InferenceBindingCounts()
	if counts["person\x001.0.0"] != 1 || counts["car\x002.0"] != 1 || a.ActiveInferenceBindings() != 2 {
		t.Fatalf("counts=%v total=%d, want person=1 car=1 total=2", counts, a.ActiveInferenceBindings())
	}

	// Unbind one: the count must drop immediately.
	if err := h.Delete(context.Background(), device.ConnectorPush{
		Kind: device.ConnectorKindDelete, SpecID: "spec-person", DeviceID: infChildUUID,
	}); err != nil {
		t.Fatal(err)
	}
	counts = a.InferenceBindingCounts()
	if _, has := counts["person\x001.0.0"]; has || a.ActiveInferenceBindings() != 1 {
		t.Fatalf("after unbind counts=%v total=%d, want car only", counts, a.ActiveInferenceBindings())
	}
}

// TestSweepOrphanInferenceJobs verifies post-reconnect convergence: only
// NE-owned jobs (nereid-spec tag) whose spec is gone are deleted and
// withdrawn; live jobs and hand-built untagged jobs are left untouched.
func TestSweepOrphanInferenceJobs(t *testing.T) {
	dev := &fakeDeviceClient{notFound: true, allDevices: []dtos.Device{
		{Name: "inference-orphan", ServiceName: "device-inference",
			Tags: map[string]any{"nereid-spec": "spec-gone", "nereid-device": "uuid-gone"}},
		{Name: "inference-spec-live", ServiceName: "device-inference",
			Tags: map[string]any{"nereid-spec": "spec-live", "nereid-device": infChildUUID}},
		{Name: "inference-yolov8n-handbuilt", ServiceName: "device-inference"},
		{Name: "onvif-camera-7", ServiceName: "device-onvif-camera",
			Tags: map[string]any{"nereid-spec": "spec-gone"}},
	}}
	type detachCall struct{ edgex, uuid string }
	var detached []detachCall
	a, rec := newQueueApplier(t, dev, nil, func(edgex, uuid string) {
		detached = append(detached, detachCall{edgex, uuid})
	})
	a.resolver.SetProvisioned(infCameraName, infCameraUUID)
	a.ConfigureInference("device-inference", inferenceProfile, nil, time.Hour,
		func() bool { return true })
	h := NewInferenceHandler(a)
	if err := h.Apply(context.Background(),
		inferencePush("spec-live", validInferenceConfig("periodic"))); err != nil {
		t.Fatal(err)
	}
	drainStatus(rec)
	// Clear the startup replay-grace window.
	a.mu.Lock()
	a.settledAt = time.Now().Add(-time.Hour)
	a.mu.Unlock()

	a.SweepOrphanInferenceJobs(context.Background())

	if len(dev.delCalls) != 1 || dev.delCalls[0] != "inference-orphan" {
		t.Fatalf("delCalls=%v, want only inference-orphan", dev.delCalls)
	}
	if len(detached) != 1 || detached[0].edgex != "inference-orphan" || detached[0].uuid != "uuid-gone" {
		t.Fatalf("detach=%v, want orphan presence withdrawn with its virtual UUID", detached)
	}

	// Throttle: an immediate second sweep must not re-list.
	dev.delCalls = nil
	a.SweepOrphanInferenceJobs(context.Background())
	if len(dev.delCalls) != 0 {
		t.Fatalf("throttled sweep deleted %v", dev.delCalls)
	}
}

// TestOrphanSweepWaitsReplayGrace verifies a freshly (re)started NE never
// deletes jobs before NL's desired-state redrive could re-apply their specs.
func TestOrphanSweepWaitsReplayGrace(t *testing.T) {
	dev := &fakeDeviceClient{notFound: true, allDevices: []dtos.Device{{
		Name: "inference-orphan", ServiceName: "device-inference",
		Tags: map[string]any{"nereid-spec": "spec-gone"},
	}}}
	a, _ := newQueueApplier(t, dev, nil, nil)
	a.ConfigureInference("device-inference", inferenceProfile, nil, time.Hour,
		func() bool { return true })
	a.SweepOrphanInferenceJobs(context.Background())
	if len(dev.delCalls) != 0 {
		t.Fatalf("sweep ran inside replay grace: delCalls=%v", dev.delCalls)
	}
}

// TestInferenceAdvertPresenceGate verifies the NI runtime existence gate: the
// edge-inference self-registration appears only while NI is present.
func TestInferenceAdvertPresenceGate(t *testing.T) {
	a, _ := newQueueApplier(t, &fakeDeviceClient{}, nil, nil)
	a.ConfigureInference("device-inference", inferenceProfile, &fakeInferenceReader{},
		time.Millisecond, func() bool { return true })

	hasInference := func() bool {
		for _, adv := range a.ConnectorAdverts() {
			if adv.Type == TypeEdgeInference {
				return true
			}
		}
		return false
	}
	if !hasInference() {
		t.Fatal("edge-inference advert missing with NI present (default)")
	}
	a.SetInferencePresence(func() bool { return false })
	if hasInference() {
		t.Fatal("edge-inference advertised without a deployed NI plane")
	}
	a.SetInferencePresence(func() bool { return true })
	if !hasInference() {
		t.Fatal("edge-inference advert did not return with NI present")
	}
}

// TestInferenceSchemaTargetDeviceShape pins the §15.3 self-registration
// contract so NL/console validation stays in sync.
func TestInferenceSchemaTargetDeviceShape(t *testing.T) {
	var schema struct {
		Properties map[string]struct {
			Type      string          `json:"type"`
			Default   json.RawMessage `json:"default"`
			MinLength int             `json:"minLength"`
			MaxLength int             `json:"maxLength"`
			Pattern   string          `json:"pattern"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(inferenceSchema, &schema); err != nil {
		t.Fatal(err)
	}
	td, ok := schema.Properties["target_device"]
	if !ok {
		t.Fatal("schema lacks target_device")
	}
	if td.Type != "string" || string(td.Default) != `"CPU"` || td.MinLength != 1 || td.MaxLength != 64 ||
		td.Pattern != "^[A-Za-z0-9_.:,-]+$" {
		t.Fatalf("target_device schema=%+v, want §15.3 shape", td)
	}

	// source.mode, the continuous trigger value and fps must be advertised.
	var full struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(inferenceSchema, &full); err != nil {
		t.Fatal(err)
	}
	var source struct {
		Properties map[string]struct {
			Type      string   `json:"type"`
			Enum      []string `json:"enum"`
			Default   string   `json:"default"`
			Pattern   string   `json:"pattern"`
			MaxLength int      `json:"maxLength"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(full.Properties["source"], &source); err != nil {
		t.Fatal(err)
	}
	mode, ok := source.Properties["mode"]
	if !ok || mode.Type != "string" || mode.Default != "snapshot" ||
		len(mode.Enum) != 3 || mode.Enum[0] != "snapshot" ||
		mode.Enum[1] != "stream" || mode.Enum[2] != "url" {
		t.Fatalf("source.mode schema=%+v, want snapshot|stream|url default snapshot", mode)
	}
	profile, ok := source.Properties["profile"]
	if !ok || profile.Type != "string" || profile.MaxLength != 64 ||
		profile.Pattern != "^[A-Za-z0-9_.:,-]+$" {
		t.Fatalf("source.profile schema=%+v, want transport-safe string <=64", profile)
	}
	// Direct-URL escape hatch shape.
	directURL, ok := source.Properties["url"]
	if !ok || directURL.Type != "string" || directURL.MaxLength != 1024 {
		t.Fatalf("source.url schema=%+v, want string maxLength 1024", directURL)
	}
	transport, ok := source.Properties["transport"]
	if !ok || strings.Join(transport.Enum, ",") != "rtsp,rtsp-udp,mpjpeg,jpeg" {
		t.Fatalf("source.transport schema=%+v, want enum rtsp|rtsp-udp|mpjpeg|jpeg", transport)
	}
	secret, ok := source.Properties["secret_name"]
	if !ok || secret.Type != "string" || secret.MaxLength != 128 ||
		secret.Pattern != "^[A-Za-z0-9_.:,-]+$" {
		t.Fatalf("source.secret_name schema=%+v, want transport-safe string <=128", secret)
	}
	var trigger struct {
		Enum []string `json:"enum"`
	}
	if err := json.Unmarshal(full.Properties["trigger"], &trigger); err != nil {
		t.Fatal(err)
	}
	wantTriggers := map[string]bool{"event": false, "periodic": false, "continuous": false}
	for _, v := range trigger.Enum {
		if _, known := wantTriggers[v]; known {
			wantTriggers[v] = true
		}
	}
	for v, seen := range wantTriggers {
		if !seen {
			t.Fatalf("trigger enum=%v missing %q", trigger.Enum, v)
		}
	}
	var fps struct {
		Type    string  `json:"type"`
		Minimum float64 `json:"minimum"`
		Maximum float64 `json:"maximum"`
	}
	if err := json.Unmarshal(full.Properties["fps"], &fps); err != nil {
		t.Fatal(err)
	}
	if fps.Type != "number" || fps.Minimum <= 0 || fps.Maximum != 60 {
		t.Fatalf("fps schema=%+v, want number in (0, 60]", fps)
	}

	// Evidence-frame, annotation and ffmpeg tuning properties.
	var schemaWrap struct {
		Properties struct {
			EmitFrame struct {
				Type    string `json:"type"`
				Default bool   `json:"default"`
			} `json:"emit_frame"`
			AnnotatePreview struct {
				Type    string `json:"type"`
				Default bool   `json:"default"`
			} `json:"annotate_preview"`
			FFmpegExtra struct {
				Type      string `json:"type"`
				MaxLength int    `json:"maxLength"`
			} `json:"ffmpeg_extra"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(inferenceSchema, &schemaWrap); err != nil {
		t.Fatal(err)
	}
	toggles := schemaWrap.Properties
	if toggles.EmitFrame.Type != "boolean" || !toggles.EmitFrame.Default {
		t.Fatalf("emit_frame schema=%+v, want boolean default true", toggles.EmitFrame)
	}
	if toggles.AnnotatePreview.Type != "boolean" || !toggles.AnnotatePreview.Default {
		t.Fatalf("annotate_preview schema=%+v, want boolean default true", toggles.AnnotatePreview)
	}
	if toggles.FFmpegExtra.Type != "string" || toggles.FFmpegExtra.MaxLength != 256 {
		t.Fatalf("ffmpeg_extra schema=%+v, want string maxLength 256", toggles.FFmpegExtra)
	}
}

// TestInferenceSchemaDecodeAccelShape validates the advertised decoder enum
// and exercises the actual VAAPIDevice pattern with valid and invalid nodes.
func TestInferenceSchemaDecodeAccelShape(t *testing.T) {
	var schema struct {
		AdditionalProperties *bool `json:"additionalProperties"`
		Properties           struct {
			DecodeAccel struct {
				Type    string   `json:"type"`
				Enum    []string `json:"enum"`
				Default string   `json:"default"`
			} `json:"decode_accel"`
			VAAPIDevice struct {
				Type    string `json:"type"`
				Pattern string `json:"pattern"`
			} `json:"vaapi_device"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(inferenceSchema, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.AdditionalProperties == nil || *schema.AdditionalProperties {
		t.Fatal("edge-inference schema must keep additionalProperties=false")
	}
	decode := schema.Properties.DecodeAccel
	if decode.Type != "string" || decode.Default != "software" ||
		strings.Join(decode.Enum, ",") != "software,vaapi" {
		t.Fatalf("decode_accel schema=%+v, want software|vaapi default software", decode)
	}
	deviceSchema := schema.Properties.VAAPIDevice
	if deviceSchema.Type != "string" || deviceSchema.Pattern != `^(?:/dev/dri/)?renderD\d+$` {
		t.Fatalf("vaapi_device schema=%+v, want strict render-node pattern", deviceSchema)
	}
	// Go regexp has no non-capturing groups; capturing the same optional group
	// preserves its acceptance semantics for these schema-pattern test cases.
	pattern, err := regexp.Compile(strings.Replace(deviceSchema.Pattern, "(?:", "(", 1))
	if err != nil {
		t.Fatalf("compile vaapi_device pattern: %v", err)
	}
	for _, value := range []string{"renderD129", "/dev/dri/renderD129"} {
		if !pattern.MatchString(value) {
			t.Errorf("schema pattern rejects valid render node %q", value)
		}
	}
	for _, value := range []string{
		"", "../dri/x", "/tmp/renderD129", "/dev/dri/card0", "renderD",
		"renderD129 ", " renderD129", "renderD129\t", "/dev/dri/renderD129/extra",
	} {
		if pattern.MatchString(value) {
			t.Errorf("schema pattern accepts invalid render node %q", value)
		}
	}
}

// fakeInferenceReader emulates NereidInference job State/LastError reads.
type fakeInferenceReader struct {
	state          string
	lastError      string
	fail           bool
	metrics        map[string]any
	metricFailures map[string]bool
}

func (f *fakeInferenceReader) set(state, lastError string) {
	f.state, f.lastError = state, lastError
}

func (f *fakeInferenceReader) Read(_ context.Context, _, command, _ string) (responses.EventResponse, int, error) {
	if f.fail {
		return responses.EventResponse{}, 0, errors.New("device service unreachable")
	}
	var value any = f.state
	valueType := common.ValueTypeString
	if command == "LastError" {
		value = f.lastError
	} else if command != "State" {
		if f.metricFailures[command] {
			return responses.EventResponse{}, 503, errors.New("metric unavailable")
		}
		metric, exists := f.metrics[command]
		if !exists {
			metric = ""
		}
		value = metric
		switch metric.(type) {
		case int64:
			valueType = common.ValueTypeInt64
		case uint64:
			valueType = common.ValueTypeUint64
		case float64:
			valueType = common.ValueTypeFloat64
		}
	}
	reading, err := dtos.NewSimpleReading(inferenceProfile, "inference-job", command, valueType, value)
	if err != nil {
		return responses.EventResponse{}, 0, err
	}
	return responses.NewEventResponse("req", "", 200,
		dtos.Event{ProfileName: inferenceProfile, Readings: []dtos.BaseReading{reading}}), 200, nil
}

func drainStatus(rec *statusRecord) int {
	n := 0
	for {
		select {
		case <-rec.ch:
			n++
		case <-time.After(50 * time.Millisecond):
			return n
		}
	}
}
