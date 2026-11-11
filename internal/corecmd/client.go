// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package corecmd is a thin REST client over the EdgeX core-command API,
// shared by the gateway command handler and the sub-device health monitor.
package corecmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	dtoCommon "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/responses"
)

// DefaultURL is the default base URL of the EdgeX core-command service as
// seen from inside the EdgeX docker network.
const DefaultURL = "http://edgex-core-command:59882"

// defaultTimeout bounds each core-command HTTP request.
const defaultTimeout = 10 * time.Second

// Client is a REST client over the EdgeX core-command API.
type Client struct {
	base string
	http *http.Client
}

// New creates a core-command REST client. The base URL must not carry the
// /api/v3 prefix; an empty URL selects DefaultURL. A non-positive timeout
// selects defaultTimeout.
func New(baseURL string, timeout time.Duration) *Client {
	if baseURL == "" {
		baseURL = DefaultURL
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &Client{
		base: strings.TrimSuffix(baseURL, "/"),
		http: &http.Client{Timeout: timeout},
	}
}

// DeviceCoreCommands fetches the device core command list for an EdgeX
// device name. The HTTP status of a failed call is returned alongside the
// error so callers can surface it in response frames.
func (c *Client) DeviceCoreCommands(ctx context.Context, edgexName string) (dtos.DeviceCoreCommand, int, error) {
	var resp responses.DeviceCoreCommandResponse
	body, status, err := c.do(ctx, http.MethodGet, c.url(edgexName, "", ""), nil)
	if err != nil {
		return dtos.DeviceCoreCommand{}, status, err
	}
	if status != http.StatusOK {
		return dtos.DeviceCoreCommand{}, status, StatusError(status, body)
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return dtos.DeviceCoreCommand{}, status, fmt.Errorf("decode device core command: %w", err)
	}
	return resp.DeviceCoreCommand, status, nil
}

// Read executes a read command and returns the event response envelope.
// rawQuery is an already-encoded query string appended verbatim (for example
// "ds-pushevent=false&ds-returnevent=true"); empty means no query. With
// ds-returnevent=false the device service answers 200 with no body, which is
// reported as a zero envelope rather than a decode error.
func (c *Client) Read(ctx context.Context, edgexName, commandName, rawQuery string) (responses.EventResponse, int, error) {
	var resp responses.EventResponse
	body, status, err := c.do(ctx, http.MethodGet, c.url(edgexName, commandName, rawQuery), nil)
	if err != nil {
		return resp, status, err
	}
	if status != http.StatusOK {
		return resp, status, StatusError(status, body)
	}
	if len(body) == 0 {
		return resp, status, nil
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return resp, status, fmt.Errorf("decode event response: %w", err)
	}
	return resp, status, nil
}

// Set executes a write command with the given settings body.
func (c *Client) Set(ctx context.Context, edgexName, commandName string, settings any) (dtoCommon.BaseResponse, int, error) {
	return c.SetWithQuery(ctx, edgexName, commandName, "", settings)
}

// SetWithQuery is Set with an already-encoded device-service query. Manager
// resources use ds-pushevent=false&ds-returnevent=false so configuration
// delivery never emits the opaque document onto the event bus.
func (c *Client) SetWithQuery(ctx context.Context, edgexName, commandName, rawQuery string, settings any) (dtoCommon.BaseResponse, int, error) {
	var resp dtoCommon.BaseResponse
	body, status, err := c.do(ctx, http.MethodPut, c.url(edgexName, commandName, rawQuery), settings)
	if err != nil {
		return resp, status, err
	}
	if status != http.StatusOK {
		return resp, status, StatusError(status, body)
	}
	if len(body) == 0 {
		return resp, status, nil
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return resp, status, fmt.Errorf("decode base response: %w", err)
	}
	return resp, status, nil
}

// url builds the REST path for a device core command list or a single
// command: /api/v3/device/name/{edgexName}[/{commandName}][?rawQuery].
func (c *Client) url(edgexName, commandName, rawQuery string) string {
	u := fmt.Sprintf("%s%s/name/%s", c.base, common.ApiDeviceRoute, url.PathEscape(edgexName))
	if commandName != "" {
		u += "/" + url.PathEscape(commandName)
	}
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	return u
}

// do performs one core-command request and returns the raw body and status.
func (c *Client) do(ctx context.Context, method, u string, payload any) ([]byte, int, error) {
	var rd io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return nil, 0, err
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return data, resp.StatusCode, nil
}

// ReadingsToMap flattens an EdgeX event's readings into {resourceName: value}
// with values typed from their ValueType, mirroring the telemetry uplink.
func ReadingsToMap(readings []dtos.BaseReading) map[string]any {
	out := make(map[string]any, len(readings))
	for _, r := range readings {
		out[r.ResourceName] = ReadingValue(r)
	}
	return out
}

// ReadingValue converts one reading to a JSON-friendly value by its type.
func ReadingValue(r dtos.BaseReading) any {
	switch r.ValueType {
	case common.ValueTypeBool:
		if v, err := strconv.ParseBool(r.Value); err == nil {
			return v
		}
	case common.ValueTypeInt8, common.ValueTypeInt16, common.ValueTypeInt32, common.ValueTypeInt64:
		if v, err := strconv.ParseInt(r.Value, 10, 64); err == nil {
			return v
		}
	case common.ValueTypeUint8, common.ValueTypeUint16, common.ValueTypeUint32, common.ValueTypeUint64:
		if v, err := strconv.ParseUint(r.Value, 10, 64); err == nil {
			return v
		}
	case common.ValueTypeFloat32, common.ValueTypeFloat64:
		if v, err := strconv.ParseFloat(r.Value, 64); err == nil {
			return v
		}
	case common.ValueTypeObject, common.ValueTypeObjectArray:
		return r.ObjectValue
	}
	return r.Value
}

// StatusError builds an error from a non-200 response body, preferring the
// EdgeX message field when present.
func StatusError(status int, body []byte) error {
	var base dtoCommon.BaseResponse
	if json.Unmarshal(body, &base) == nil && base.Message != "" {
		return errors.New(base.Message)
	}
	return fmt.Errorf("core-command returned HTTP %d", status)
}
