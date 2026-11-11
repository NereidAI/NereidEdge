// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package devicehttp is the HTTP device ingress client for NereidLink.
//
// It covers the six authenticated /v1/me endpoints: telemetry uplink,
// long-poll command retrieval, command response, shadow report and the
// firmware OTA assignment/download/progress surface. The client is
// stateless apart from its http.Client and credential; it starts no
// goroutines and runs no poll loop. Firmware decides when to poll and
// how to react to a command.
package devicehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

// Default paths of the device ingress. They are constants because the
// server exposes no service-discovery indirection.
const (
	PathTelemetry    = "/v1/me/telemetry"
	PathCommands     = "/v1/me/commands"
	PathCommandReply = "/v1/me/cmdres/"
	PathShadowReport = "/v1/me/shadow/report"
	PathOTATasks     = "/v1/me/ota/tasks"
	PathOTAPackage   = "/v1/me/ota/"
	PathOTAProgress  = "/v1/me/ota/"
)

// Options configures a Client.
type Options struct {
	// BaseURL is the platform root, e.g. https://nl.example.com.
	BaseURL string

	// Credential authenticates every request.
	Credential device.Credential

	// HTTPClient overrides the transport; nil builds a default client with
	// a generous timeout for long polling (requests still honor ctx).
	HTTPClient *http.Client

	// UserAgent overrides the default SDK user agent.
	UserAgent string
}

// Client is a thin, stateless HTTP ingress client.
type Client struct {
	base      string
	cred      device.Credential
	http      *http.Client
	userAgent string
}

// New constructs a Client and validates its options.
func New(opts Options) (*Client, error) {
	base := strings.TrimRight(opts.BaseURL, "/")
	if base == "" {
		return nil, fmt.Errorf("devicehttp: BaseURL is required")
	}
	if _, err := url.Parse(base); err != nil {
		return nil, fmt.Errorf("devicehttp: invalid BaseURL: %w", err)
	}
	if opts.Credential == nil {
		return nil, fmt.Errorf("devicehttp: Credential is required")
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		tlsCfg, err := device.TLSConfig(opts.Credential)
		if err != nil {
			return nil, err
		}
		httpClient = &http.Client{
			// Long polling may hold a request open; context bounds each call.
			Timeout: 0,
			Transport: &http.Transport{
				TLSClientConfig: tlsCfg,
			},
		}
	}
	return &Client{
		base:      base,
		cred:      opts.Credential,
		http:      httpClient,
		userAgent: opts.UserAgent,
	}, nil
}

// do performs one authenticated JSON request. A nil body is allowed. A
// non-2xx response becomes *device.Error decoded from the envelope.
func (c *Client) do(ctx context.Context, method, path string, body any,
	headers map[string]string) (*http.Response, error) {
	var reader io.Reader
	contentType := "application/json"
	if body != nil {
		switch b := body.(type) {
		case []byte:
			reader = bytes.NewReader(b)
		case io.Reader:
			reader = b
		default:
			raw, err := json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("encode request body: %w", err)
			}
			reader = bytes.NewReader(raw)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	device.SetAuthorization(req, c.cred)
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("devicehttp request %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 300 {
		return nil, device.DecodeError(resp)
	}
	return resp, nil
}

// PublishTelemetry submits one flat telemetry frame and returns the
// number of measurement Envelopes the platform published.
func (c *Client) PublishTelemetry(ctx context.Context, frame device.Frame) (device.TelemetryAck, error) {
	if err := frame.Validate(); err != nil {
		return device.TelemetryAck{}, err
	}
	raw, err := frame.EncodeJSON()
	if err != nil {
		return device.TelemetryAck{}, err
	}
	resp, err := c.do(ctx, http.MethodPost, PathTelemetry, raw, nil)
	if err != nil {
		return device.TelemetryAck{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var ack device.TelemetryAck
	if err := json.NewDecoder(resp.Body).Decode(&ack); err != nil {
		return device.TelemetryAck{}, fmt.Errorf("decode telemetry ack: %w", err)
	}
	return ack, nil
}

// PollCommands performs one long-poll GET /v1/me/commands. The returned
// command is nil on an idle poll; firmware is expected to call this in a
// loop. The first authenticated request also opens the platform presence
// session, which expires after an idle interval on the server.
func (c *Client) PollCommands(ctx context.Context) (*device.Command, error) {
	resp, err := c.do(ctx, http.MethodGet, PathCommands, nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var envelope device.CommandsResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode commands envelope: %w", err)
	}
	return envelope.Command, nil
}

// RespondCommand submits the arbitrary JSON result of a request-response
// command to POST /v1/me/cmdres/{id}.
func (c *Client) RespondCommand(ctx context.Context, commandID string, result any) error {
	if commandID == "" {
		return fmt.Errorf("devicehttp: command id is required")
	}
	resp, err := c.do(ctx, http.MethodPost, PathCommandReply+url.PathEscape(commandID), result, nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// ReportShadow merges a reported-state patch (optionally acknowledging a
// desired version) via POST /v1/me/shadow/report.
func (c *Client) ReportShadow(ctx context.Context, patch device.ReportedPatch) error {
	resp, err := c.do(ctx, http.MethodPost, PathShadowReport, patch, nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// DownloadOptions controls one authenticated OTA package download.
type DownloadOptions struct {
	// Offset starts a ranged/resumed download at this byte position. Zero
	// requests the full object.
	Offset int64

	// Length bounds one range (0 with a non-zero Offset means open-ended
	// suffix, matching the single Range header the server accepts).
	Length int64

	// Destination receives the package bytes. The caller is responsible
	// for persisting and verifying them against the assignment SHA256.
	Destination io.Writer
}

// DownloadResult reports how the package stream was served.
type DownloadResult struct {
	// Status is 200 (full) or 206 (partial content).
	Status int

	// Size is the total package length advertised by the platform.
	Size int64

	// SHA256 is the X-Firmware-Sha256 header value.
	SHA256 string

	// ContentRange carries the "bytes start-end/total" header on 206.
	ContentRange string

	// Written is the number of bytes copied to Destination.
	Written int64
}

// otaPackagePath builds /v1/me/ota/{task}/package.
func otaPackagePath(taskID string) string {
	return PathOTAPackage + url.PathEscape(taskID) + "/package"
}

// otaProgressPath builds /v1/me/ota/{task}/progress.
func otaProgressPath(taskID string) string {
	return PathOTAProgress + url.PathEscape(taskID) + "/progress"
}

// ListOTAAssignments returns the caller's open platform-hosted firmware
// jobs, newest first.
func (c *Client) ListOTAAssignments(ctx context.Context) (device.OTATaskList, error) {
	resp, err := c.do(ctx, http.MethodGet, PathOTATasks, nil, nil)
	if err != nil {
		return device.OTATaskList{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var list device.OTATaskList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return device.OTATaskList{}, fmt.Errorf("decode ota assignments: %w", err)
	}
	if list.Assignments == nil {
		// API contract: arrays are emitted as [], never null.
		list.Assignments = []device.OTAAssignment{}
	}
	return list, nil
}

// DownloadOTAPackage streams an authorized firmware package, honoring one
// byte Range for resume. It re-checks authorization server-side on every
// request.
func (c *Client) DownloadOTAPackage(ctx context.Context, taskID string, opts DownloadOptions) (DownloadResult, error) {
	if opts.Destination == nil {
		return DownloadResult{}, fmt.Errorf("devicehttp: download Destination is required")
	}
	headers := map[string]string{}
	if opts.Offset > 0 || opts.Length > 0 {
		headers["Range"] = singleRangeHeader(opts.Offset, opts.Length)
	}
	resp, err := c.do(ctx, http.MethodGet, otaPackagePath(taskID), nil, headers)
	if err != nil {
		return DownloadResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return DownloadResult{}, fmt.Errorf("devicehttp: unexpected download status %d", resp.StatusCode)
	}
	written, err := io.Copy(opts.Destination, resp.Body)
	if err != nil {
		return DownloadResult{}, fmt.Errorf("stream firmware package: %w", err)
	}
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return DownloadResult{
		Status:       resp.StatusCode,
		Size:         size,
		SHA256:       resp.Header.Get("X-Firmware-Sha256"),
		ContentRange: resp.Header.Get("Content-Range"),
		Written:      written,
	}, nil
}

// singleRangeHeader renders one "bytes=start-end" range. A zero length
// produces the open-ended "bytes=start-" form.
func singleRangeHeader(offset, length int64) string {
	if length <= 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}

// ReportOTAProgress records one per-device firmware progress frame.
func (c *Client) ReportOTAProgress(ctx context.Context, taskID string, progress device.OTAProgress) error {
	if taskID == "" {
		return fmt.Errorf("devicehttp: task id is required")
	}
	resp, err := c.do(ctx, http.MethodPost, otaProgressPath(taskID), progress, nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}
