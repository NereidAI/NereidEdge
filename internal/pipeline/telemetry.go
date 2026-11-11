// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package pipeline provides function pipeline transforms for NereidEdge.
package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	"github.com/nereidai/nereidedge/internal/binding"
	"github.com/nereidai/nereidedge/internal/gateway"
	device "github.com/nereidai/nereidlink/sdk/device/go"
	nlgateway "github.com/nereidai/nereidlink/sdk/device/go/gateway"
)

// inferenceDetectionProfile is the NereidInference profile translated to the
// protocol-defined detection/v1 scalar point names.
const inferenceDetectionProfile = "nereid-inference-detection"

var inferenceDetectionAliases = map[string]string{
	"DetectionJSON":  "detections_json",
	"DetectionCount": "detection_count",
	"Top1Label":      "top1_label",
	"Top1Score":      "top1_score",
	"InferLatencyMs": "infer_latency_ms",
}

// frameMediaKey is the measurement key under which the annotated evidence
// JPEG is carried as a binary media marker. The name aligns with the
// ADR-0039 frame.blob_key vocabulary; NL stores it as a blob:// reference
// scoped to the sub-device identity.
const frameMediaKey = "frame"

// mediaMarker is the one structured gateway-telemetry field NL accepts for
// binary media (NereidLink "Binary Media and S3 Storage"). NL decodes the
// base64, offloads the bytes to its blob backend and replaces the field with
// a v_string "blob://..." reference; the bytes never enter the envelope,
// time-series rows or PostgreSQL bytea.
type mediaMarker struct {
	Blob        bool   `json:"blob"`
	ContentType string `json:"content_type,omitempty"`
	Filename    string `json:"filename,omitempty"`
	Data        string `json:"data"`
}

// TelemetryPublisher publishes EdgeX events to NereidLink as gateway telemetry.
type TelemetryPublisher struct {
	Gateway         *nlgateway.Client
	Resolver        *binding.Resolver
	Manager         *gateway.Manager
	ForwardUnmapped bool
	UplinkFilters   *UplinkFilterManager

	// publishScalar/publishRaw indirect the two uplink paths so tests can
	// observe which one a frame took. Production leaves both nil.
	publishScalar func(context.Context, device.GatewayFrame) error
	publishRaw    func(context.Context, []byte) error
}

// Publish converts an EdgeX event to NereidLink gateway telemetry frame.
func (p *TelemetryPublisher) Publish(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	lc := ctx.LoggingClient()

	// The test seams replace the gateway uplink; production always has a
	// connected client.
	if p.Gateway == nil && p.publishScalar == nil && p.publishRaw == nil {
		lc.Warn("Gateway client not initialized, skipping telemetry publish")
		return false, nil
	}

	// The App Functions SDK runtime delivers *dtos.Event (pointer).
	event, ok := data.(*dtos.Event)
	if !ok {
		if valueEvent, okValue := data.(dtos.Event); okValue {
			event = &valueEvent
			ok = true
		}
	}
	if !ok {
		lc.Warnf("Received non-event data of type %T, skipping", data)
		return false, nil
	}
	if p.UplinkFilters != nil {
		readings, allowed := p.UplinkFilters.FilterReadings(event)
		if !allowed {
			lc.Debugf("NL uplink filter blocked EdgeX event: edgex-device=%s profile=%s",
				event.DeviceName, event.ProfileName)
			return false, nil
		}
		event.Readings = readings
	}
	normalizeInferenceReadings(event)

	deviceID, resolved := "", false
	if p.Resolver != nil {
		deviceID, resolved = p.Resolver.Resolve(event.DeviceName)
	}
	if !resolved {
		if !p.ForwardUnmapped {
			lc.Debugf("Skipping unbound EdgeX device %s", event.DeviceName)
			return false, nil
		}
		deviceID = event.DeviceName
	}
	lc.Debugf("EdgeX event received: edgex-device=%s nl-device=%s readings=%d",
		event.DeviceName, deviceID, len(event.Readings))

	if p.Manager != nil {
		if attached := p.Manager.EnsureDevice(context.Background(), p.Gateway, deviceID); attached {
			lc.Infof("Sub-device attached: edgex=%s nl=%s", event.DeviceName, deviceID)
		}
	}

	measurements := readingsToFrame(event.Readings, lc)

	if len(measurements) == 0 {
		lc.Warnf("No convertible measurements for device %s, skipping publish", deviceID)
		return false, nil
	}

	// EdgeX origin is Unix nanoseconds; NL gateway frame TS is epoch millis.
	// Zero lets the server stamp arrival time.
	var ts int64
	if event.Origin > 0 {
		ts = event.Origin / 1_000_000
	}

	// A media marker is a structured value the scalar-only SDK encoder
	// rejects. When one is present the batch is encoded locally in the exact
	// v1/gateway/telemetry map shape and published raw; scalar-only frames
	// keep using the typed SDK path unchanged.
	if hasMediaMarker(measurements) {
		raw, err := encodeGatewayTelemetryRaw(deviceID, ts, measurements)
		if err != nil {
			lc.Errorf("Failed to encode media telemetry for device %s: %s", deviceID, err.Error())
			return false, err
		}
		if err := p.publishRawPayload(context.Background(), raw); err != nil {
			lc.Errorf("Failed to publish media telemetry to NereidLink for device %s: %s", deviceID, err.Error())
			return false, err
		}
	} else {
		frame := device.GatewayFrame{
			DeviceID:     deviceID,
			TS:           ts,
			Measurements: measurements,
		}
		if err := p.publishScalarFrame(context.Background(), frame); err != nil {
			lc.Errorf("Failed to publish telemetry to NereidLink for device %s: %s", deviceID, err.Error())
			return false, err
		}
	}

	keys := make([]string, 0, len(measurements))
	for k := range measurements {
		keys = append(keys, k)
	}
	// Only measurement keys are logged; marker base64 data is never logged.
	lc.Debugf("Telemetry published: device=%s keys=%v", deviceID, keys)
	return true, data
}

// publishScalarFrame keeps scalar frames on the typed SDK path; tests
// override the seam.
func (p *TelemetryPublisher) publishScalarFrame(ctx context.Context, frame device.GatewayFrame) error {
	if p.publishScalar != nil {
		return p.publishScalar(ctx, frame)
	}
	return p.Gateway.PublishTelemetry(ctx, frame)
}

// publishRawPayload sends a locally encoded batch; tests override the seam.
func (p *TelemetryPublisher) publishRawPayload(ctx context.Context, raw []byte) error {
	if p.publishRaw != nil {
		return p.publishRaw(ctx, raw)
	}
	return p.Gateway.MQTT().PublishRaw(ctx, device.TopicGatewayTelemetry, raw, 1)
}

// normalizeInferenceReadings maps NereidInference EdgeX resource names to
// the ADR-0039 detection/v1 point keys. The annotated evidence JPEG becomes
// the "frame" measurement and is converted to a binary media marker by
// readingsToFrame; it never travels as an inline base64 string point.
func normalizeInferenceReadings(event *dtos.Event) {
	if event.ProfileName != inferenceDetectionProfile {
		return
	}
	for i := range event.Readings {
		reading := &event.Readings[i]
		if reading.ResourceName == "FrameJPEG" {
			reading.ResourceName = frameMediaKey
		}
		if reading.ResourceName == "DetectionCount" {
			// The detection/v1 seed point detection_count is v_double while
			// the DS resource is Int32; retag before conversion so the frame
			// carries a float rather than an integer measurement.
			reading.ValueType = common.ValueTypeFloat64
		}
		if key, ok := inferenceDetectionAliases[reading.ResourceName]; ok {
			reading.ResourceName = key
		}
	}
}

// readingsToFrame converts EdgeX readings into a flat NL telemetry frame.
// The wire protocol accepts only scalars, so Object/ObjectArray readings are
// JSON-encoded strings. Binary readings become structured binary media
// markers (offloaded to blob storage by NL), never inline base64 string
// points.
func readingsToFrame(readings []dtos.BaseReading, lc logger.LoggingClient) device.Frame {
	measurements := device.NewFrame()
	for _, reading := range readings {
		switch reading.ValueType {
		case "Bool":
			v, err := strconv.ParseBool(reading.Value)
			if err == nil {
				measurements.SetBool(reading.ResourceName, v)
			} else {
				lc.Warnf("Failed to parse bool reading %s=%q: %s", reading.ResourceName, reading.Value, err.Error())
			}
		case "Int8", "Int16", "Int32", "Int64":
			v, err := strconv.ParseInt(reading.Value, 10, 64)
			if err == nil {
				_ = measurements.Set(reading.ResourceName, v)
			} else {
				lc.Warnf("Failed to parse int reading %s=%q: %s", reading.ResourceName, reading.Value, err.Error())
			}
		case "Uint8", "Uint16", "Uint32", "Uint64":
			v, err := strconv.ParseUint(reading.Value, 10, 64)
			if err == nil {
				_ = measurements.Set(reading.ResourceName, v)
			} else {
				lc.Warnf("Failed to parse uint reading %s=%q: %s", reading.ResourceName, reading.Value, err.Error())
			}
		case "Float32", "Float64":
			v, err := strconv.ParseFloat(reading.Value, 64)
			if err == nil {
				measurements.SetFloat(reading.ResourceName, v)
			} else {
				lc.Warnf("Failed to parse float reading %s=%q: %s", reading.ResourceName, reading.Value, err.Error())
			}
		case common.ValueTypeObject, common.ValueTypeObjectArray:
			payload, err := json.Marshal(reading.ObjectValue)
			if err != nil {
				lc.Warnf("Failed to marshal object reading %s: %s", reading.ResourceName, err.Error())
				continue
			}
			measurements.SetString(reading.ResourceName, string(payload))
		case common.ValueTypeBinary:
			if len(reading.BinaryValue) == 0 {
				lc.Warnf("Skipping empty binary reading %s for device event", reading.ResourceName)
				continue
			}
			contentType := reading.MediaType
			if contentType == "" && reading.ResourceName == frameMediaKey {
				contentType = "image/jpeg"
			}
			measurements[reading.ResourceName] = mediaMarker{
				Blob:        true,
				ContentType: contentType,
				Filename:    mediaFilename(reading),
				Data:        base64.StdEncoding.EncodeToString(reading.BinaryValue),
			}
		default:
			measurements.SetString(reading.ResourceName, reading.Value)
		}
	}
	return measurements
}

// hasMediaMarker reports whether the frame carries at least one structured
// binary media marker, which forces the raw (locally encoded) publish path.
func hasMediaMarker(measurements device.Frame) bool {
	for _, value := range measurements {
		if _, ok := value.(mediaMarker); ok {
			return true
		}
	}
	return false
}

// encodeGatewayTelemetryRaw renders one v1/gateway/telemetry payload locally:
// a map of one sub-device UUID to its measurements with the optional "ts"
// key. It exists because the SDK's scalar-only encoder rejects the nested
// media marker; the wire shape itself is identical to EncodeGatewayBatch.
func encodeGatewayTelemetryRaw(deviceID string, ts int64, measurements device.Frame) ([]byte, error) {
	if deviceID == "" {
		return nil, fmt.Errorf("gateway telemetry block is missing device_id")
	}
	if len(measurements) == 0 {
		return nil, fmt.Errorf("gateway telemetry frame is empty")
	}
	values := make(map[string]any, len(measurements)+1)
	for key, value := range measurements {
		values[key] = value
	}
	if ts != 0 {
		values["ts"] = ts
	}
	raw, err := json.Marshal(map[string]map[string]any{deviceID: values})
	if err != nil {
		return nil, fmt.Errorf("encode gateway telemetry batch: %w", err)
	}
	return raw, nil
}

// mediaFilename builds a display-only, path-free filename for one binary
// reading. The EdgeX reading UUID is the preferred base (no user input);
// the origin timestamp is the fallback. The evidence frame key always gets
// a .jpg extension; other media types map a small allowlist.
func mediaFilename(reading dtos.BaseReading) string {
	base := sanitizeMediaName(reading.Id)
	if base == "" {
		base = fmt.Sprintf("frame-%d", reading.Origin/1_000_000)
	}
	ext := mediaExtension(reading.MediaType)
	if reading.ResourceName == frameMediaKey {
		ext = ".jpg"
	}
	return base + ext
}

func mediaExtension(mediaType string) string {
	switch mediaType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	default:
		return ".bin"
	}
}

// sanitizeMediaName reduces any identifier to the filename-safe charset so a
// reading id can never carry a path segment or traversal sequence.
func sanitizeMediaName(name string) string {
	name = strings.TrimSpace(name)
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	cleaned := strings.Trim(b.String(), ".-_")
	if cleaned == "." || cleaned == ".." {
		return ""
	}
	return cleaned
}
