// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	device "github.com/nereidai/nereidlink/sdk/device/go"

	"github.com/nereidai/nereidedge/internal/binding"
)

func TestNormalizeInferenceReadingsUsesDetectionV1Names(t *testing.T) {
	jsonReading, err := dtos.NewSimpleReading("p", inferenceDetectionProfile, "DetectionJSON", common.ValueTypeString, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	// DS emits DetectionCount as Int32; normalization must retag it to the
	// detection/v1 v_double seed point so the frame carries a float value.
	countReading, err := dtos.NewSimpleReading("p", inferenceDetectionProfile, "DetectionCount", common.ValueTypeInt32, int32(2))
	if err != nil {
		t.Fatal(err)
	}
	labelReading, err := dtos.NewSimpleReading("p", inferenceDetectionProfile, "Top1Label", common.ValueTypeString, "person")
	if err != nil {
		t.Fatal(err)
	}
	frameReading := dtos.NewBinaryReading("p", inferenceDetectionProfile, "FrameJPEG", []byte("jpeg"), "image/jpeg")
	event := &dtos.Event{
		ProfileName: inferenceDetectionProfile,
		DeviceName:  "inference-job-1",
		Readings:    []dtos.BaseReading{jsonReading, countReading, labelReading, frameReading},
	}

	normalizeInferenceReadings(event)

	if len(event.Readings) != 4 {
		t.Fatalf("readings=%d, want 3 scalar points plus the frame marker reading", len(event.Readings))
	}
	if event.Readings[0].ResourceName != "detections_json" || event.Readings[1].ResourceName != "detection_count" {
		t.Fatalf("resource names=%q,%q", event.Readings[0].ResourceName, event.Readings[1].ResourceName)
	}
	if event.Readings[1].ValueType != common.ValueTypeFloat64 {
		t.Fatalf("detection_count ValueType=%q, want Float64", event.Readings[1].ValueType)
	}
	if event.Readings[2].ResourceName != "top1_label" {
		t.Fatalf("third resource=%q, want top1_label", event.Readings[2].ResourceName)
	}
	// The evidence JPEG is kept and renamed to the "frame" media key.
	last := event.Readings[3]
	if last.ResourceName != frameMediaKey || last.ValueType != common.ValueTypeBinary {
		t.Fatalf("evidence reading = %q/%q, want frame/Binary", last.ResourceName, last.ValueType)
	}

	// Other DS profiles pass through untouched: no resource may be renamed
	// merely for sharing a resource name.
	other := &dtos.Event{ProfileName: "onvif-camera", Readings: []dtos.BaseReading{frameReading}}
	normalizeInferenceReadings(other)
	if len(other.Readings) != 1 || other.Readings[0].ResourceName != "FrameJPEG" {
		t.Fatalf("non-inference event was modified: %+v", other.Readings)
	}
}

func TestReadingsToFrameBinaryBecomesMediaMarker(t *testing.T) {
	jpeg := []byte{0xff, 0xd8, 0xff, 0xe0, 0, 1, 2, 3}
	reading := dtos.NewBinaryReading("p", "d", "snapshot", jpeg, "image/jpeg")
	frame := readingsToFrame([]dtos.BaseReading{reading}, logger.NewMockClient())

	marker, ok := frame["snapshot"].(mediaMarker)
	if !ok {
		t.Fatalf("snapshot value = %T (%v), want mediaMarker", frame["snapshot"], frame["snapshot"])
	}
	if !marker.Blob {
		t.Fatal("marker.blob must be true")
	}
	if marker.ContentType != "image/jpeg" {
		t.Fatalf("content_type=%q", marker.ContentType)
	}
	decoded, err := base64.StdEncoding.DecodeString(marker.Data)
	if err != nil {
		t.Fatalf("data is not standard base64: %s", err)
	}
	if string(decoded) != string(jpeg) {
		t.Fatalf("decoded bytes = %v, want %v", decoded, jpeg)
	}
	if marker.Filename == "" || !safeFilename(marker.Filename) {
		t.Fatalf("filename=%q must be display-safe", marker.Filename)
	}
	if hasMediaMarker(frame) != true {
		t.Fatal("hasMediaMarker must detect the structured marker")
	}
}

func TestReadingsToFrameMarkerDefaultsJPEGContentType(t *testing.T) {
	// FrameJPEG readings arrive renamed to "frame"; a DS that omits mediaType
	// must still produce an image/jpeg marker.
	reading := dtos.BaseReading{
		DeviceName:   "inference-job",
		ProfileName:  inferenceDetectionProfile,
		ResourceName: frameMediaKey,
		ValueType:    common.ValueTypeBinary,
		BinaryReading: dtos.BinaryReading{
			BinaryValue: []byte("jpeg-bytes"),
		},
	}
	frame := readingsToFrame([]dtos.BaseReading{reading}, logger.NewMockClient())
	marker, ok := frame[frameMediaKey].(mediaMarker)
	if !ok {
		t.Fatalf("frame value = %T, want mediaMarker", frame[frameMediaKey])
	}
	if marker.ContentType != "image/jpeg" {
		t.Fatalf("content_type=%q, want image/jpeg fallback", marker.ContentType)
	}
}

func TestReadingsToFrameEmptyBinarySkipped(t *testing.T) {
	reading := dtos.NewBinaryReading("p", "d", "snapshot", nil, "image/jpeg")
	frame := readingsToFrame([]dtos.BaseReading{reading}, logger.NewMockClient())
	if len(frame) != 0 {
		t.Fatalf("empty binary produced a marker: %+v", frame)
	}
	if hasMediaMarker(frame) {
		t.Fatal("empty binary must not register as media marker")
	}
}

func TestEncodeGatewayTelemetryRawShape(t *testing.T) {
	measurements := device.NewFrame().
		SetFloat("detection_count", 1).
		SetString("top1_label", "mask")
	measurements[frameMediaKey] = mediaMarker{
		Blob:        true,
		ContentType: "image/jpeg",
		Filename:    "evidence.jpg",
		Data:        base64.StdEncoding.EncodeToString([]byte("jpeg")),
	}

	raw, err := encodeGatewayTelemetryRaw("subdev-uuid", 1792981234000, measurements)
	if err != nil {
		t.Fatalf("encode: %s", err)
	}
	var batch map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw, &batch); err != nil {
		t.Fatalf("decode batch: %s", err)
	}
	values, ok := batch["subdev-uuid"]
	if !ok {
		t.Fatalf("batch keys=%v, want subdev-uuid", batch)
	}
	var ts int64
	if err := json.Unmarshal(values["ts"], &ts); err != nil || ts != 1792981234000 {
		t.Fatalf("ts raw=%s err=%v", values["ts"], err)
	}
	var marker mediaMarker
	if err := json.Unmarshal(values[frameMediaKey], &marker); err != nil || !marker.Blob || marker.ContentType != "image/jpeg" {
		t.Fatalf("frame marker=%+v err=%v", marker, err)
	}

	// Zero ts omits the reserved key.
	rawNoTS, err := encodeGatewayTelemetryRaw("subdev-uuid", 0, measurements)
	if err != nil {
		t.Fatalf("encode no-ts: %s", err)
	}
	var noTS map[string]map[string]json.RawMessage
	if err := json.Unmarshal(rawNoTS, &noTS); err != nil {
		t.Fatal(err)
	}
	if _, present := noTS["subdev-uuid"]["ts"]; present {
		t.Fatal("ts key present when ts == 0")
	}

	if _, err := encodeGatewayTelemetryRaw("", 1, measurements); err == nil {
		t.Fatal("missing device id must fail encoding")
	}
	if _, err := encodeGatewayTelemetryRaw("x", 1, device.NewFrame()); err == nil {
		t.Fatal("empty frame must fail encoding")
	}
}

func TestHasMediaMarker(t *testing.T) {
	scalar := device.NewFrame().SetFloat("x", 1)
	if hasMediaMarker(scalar) {
		t.Fatal("scalar frame falsely detected marker")
	}
	scalar[frameMediaKey] = mediaMarker{Blob: true, Data: "AA=="}
	if !hasMediaMarker(scalar) {
		t.Fatal("marker frame not detected")
	}
}

// testAppContext satisfies only the LoggingClient method Publish uses.
type testAppContext struct {
	interfaces.AppFunctionContext
}

func (testAppContext) LoggingClient() logger.LoggingClient { return logger.NewMockClient() }

func TestPublishSelectsRawPathForMarker(t *testing.T) {
	jpeg := []byte("jpeg-bytes")
	resolver := binding.NewResolver(nil)
	resolver.SetProvisioned("inference-job-1", "subdev-uuid")

	var rawPayload []byte
	pub := &TelemetryPublisher{
		Resolver:        resolver,
		ForwardUnmapped: false,
		publishRaw: func(_ context.Context, raw []byte) error {
			rawPayload = raw
			return nil
		},
		publishScalar: func(context.Context, device.GatewayFrame) error {
			t.Fatal("marker frame must not use the scalar path")
			return nil
		},
	}
	event := &dtos.Event{
		DeviceName:  "inference-job-1",
		ProfileName: inferenceDetectionProfile,
		Origin:      1_792_981_234_000_000_000,
		Readings: []dtos.BaseReading{
			dtos.NewBinaryReading("inference-job-1", inferenceDetectionProfile, "FrameJPEG", jpeg, "image/jpeg"),
		},
	}
	normalizeInferenceReadings(event)
	if ok, _ := pub.Publish(testAppContext{}, event); !ok {
		t.Fatal("Publish returned false for a media event")
	}
	if rawPayload == nil {
		t.Fatal("raw publish was not invoked")
	}
	var batch map[string]map[string]json.RawMessage
	if err := json.Unmarshal(rawPayload, &batch); err != nil {
		t.Fatalf("raw payload not the gateway batch map: %s", err)
	}
	values := batch["subdev-uuid"]
	if values == nil {
		t.Fatalf("payload not keyed by sub-device: %s", rawPayload)
	}
	var marker mediaMarker
	if err := json.Unmarshal(values[frameMediaKey], &marker); err != nil || !marker.Blob {
		t.Fatalf("frame marker missing/!blob: %+v err=%v", marker, err)
	}
}

func TestPublishKeepsScalarPath(t *testing.T) {
	resolver := binding.NewResolver(nil)
	resolver.SetProvisioned("modbus-1", "subdev-uuid")

	var sawScalar bool
	pub := &TelemetryPublisher{
		Resolver: resolver,
		publishScalar: func(_ context.Context, frame device.GatewayFrame) error {
			sawScalar = true
			if frame.DeviceID != "subdev-uuid" || frame.Measurements["temp"] != float64(21.5) {
				t.Fatalf("scalar frame=%+v", frame)
			}
			return nil
		},
		publishRaw: func(context.Context, []byte) error {
			t.Fatal("scalar frame must not use the raw path")
			return errors.New("unexpected raw path")
		},
	}
	reading, err := dtos.NewSimpleReading("modbus-1", "modbus", "temp", common.ValueTypeFloat64, 21.5)
	if err != nil {
		t.Fatal(err)
	}
	event := &dtos.Event{DeviceName: "modbus-1", ProfileName: "modbus", Readings: []dtos.BaseReading{reading}}
	if ok, _ := pub.Publish(testAppContext{}, event); !ok {
		t.Fatal("Publish returned false for a scalar event")
	}
	if !sawScalar {
		t.Fatal("scalar publish was not invoked")
	}
}

func TestReadingsToFrameObjectEncodedAsJSONString(t *testing.T) {
	objValue := map[string]any{"manufacturer": "ONVIF-Cam", "firmware": "1.2.3"}
	want, err := json.Marshal(objValue)
	if err != nil {
		t.Fatalf("marshal want: %s", err)
	}
	strReading, err := dtos.NewSimpleReading("p", "d", "mode", common.ValueTypeString, "auto")
	if err != nil {
		t.Fatalf("new string reading: %s", err)
	}
	objReading := dtos.NewObjectReading("p", "d", "deviceInfo", objValue)

	frame := readingsToFrame([]dtos.BaseReading{strReading, objReading}, logger.NewMockClient())

	got, ok := frame["deviceInfo"].(string)
	if !ok {
		t.Fatalf("deviceInfo frame value = %T (%v), want string", frame["deviceInfo"], frame["deviceInfo"])
	}
	if got != string(want) {
		t.Errorf("deviceInfo = %s, want %s", got, want)
	}
	if mode := frame["mode"]; mode != "auto" {
		t.Errorf("mode = %v, want auto", mode)
	}
}

func TestReadingsToFrameObjectArrayEncodedAsJSONString(t *testing.T) {
	arr := []any{"a", 1, true}
	want, err := json.Marshal(arr)
	if err != nil {
		t.Fatalf("marshal want: %s", err)
	}
	reading := dtos.NewObjectReadingWithArray("p", "d", "tags", arr)

	frame := readingsToFrame([]dtos.BaseReading{reading}, logger.NewMockClient())

	got, ok := frame["tags"].(string)
	if !ok {
		t.Fatalf("tags frame value = %T (%v), want string", frame["tags"], frame["tags"])
	}
	if got != string(want) {
		t.Errorf("tags = %s, want %s", got, want)
	}
}

func TestReadingsToFrameScalarDefaults(t *testing.T) {
	strReading, err := dtos.NewSimpleReading("p", "d", "mode", common.ValueTypeString, "auto")
	if err != nil {
		t.Fatalf("new string reading: %s", err)
	}
	frame := readingsToFrame([]dtos.BaseReading{strReading}, logger.NewMockClient())

	if got, ok := frame["mode"].(string); !ok || got != "auto" {
		t.Errorf("mode = %v (%T), want string \"auto\"", frame["mode"], frame["mode"])
	}
}

func TestReadingsToFrameObjectValueType(t *testing.T) {
	reading := dtos.NewObjectReading("p", "d", "deviceInfo", map[string]any{"v": 42})
	if reading.ValueType != common.ValueTypeObject {
		t.Fatalf("constructed ValueType = %q, want %q", reading.ValueType, common.ValueTypeObject)
	}
}

func TestMediaFilenameSafe(t *testing.T) {
	reading := dtos.NewBinaryReading("p", "d", frameMediaKey, []byte("x"), "image/jpeg")
	name := mediaFilename(reading)
	if !safeFilename(name) || len(name) < 5 {
		t.Fatalf("filename=%q not a safe .jpg name", name)
	}
	// No path traversal possible even with a hostile reading id.
	hostile := dtos.BaseReading{
		Id:            "../../etc/passwd",
		ResourceName:  frameMediaKey,
		Origin:        1_792_981_234_000_000_000,
		BinaryReading: dtos.BinaryReading{BinaryValue: []byte("x")},
	}
	if got := mediaFilename(hostile); !safeFilename(got) {
		t.Fatalf("hostile id produced unsafe filename %q", got)
	}
}

// safeFilename mirrors the display-only charset contract: no separators or
// traversal sequences and a known image extension.
func safeFilename(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	dot := false
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_':
		case r == '.':
			dot = true
			if i > 0 && name[i-1] == '.' {
				return false
			}
		default:
			return false
		}
	}
	return dot
}
