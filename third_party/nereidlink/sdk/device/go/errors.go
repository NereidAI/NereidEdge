// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// ErrorBody is one error in the platform error envelope.
type ErrorBody struct {
	// Code is the global upper-snake-case error code, e.g. UNAUTHORIZED.
	Code string `json:"code"`

	// Message is the human-readable explanation.
	Message string `json:"message"`

	// TraceID optionally carries the server-side correlation id.
	TraceID string `json:"trace_id,omitempty"`
}

// APIError is the standard error envelope returned by every device-facing
// HTTP endpoint: {"error":{"code":"...","message":"..."}}. It implements
// the error interface; use errors.As to inspect code and status.
type APIError struct {
	// Body carries the decoded error document.
	Body ErrorBody `json:"-"`

	// HTTPStatus is the response status code that carried the envelope.
	HTTPStatus int `json:"-"`
}

// Error implements the error interface.
func (e *APIError) Error() string {
	if e.Body.Code == "" {
		return "nereidlink: unexpected HTTP status " + strconv.Itoa(e.HTTPStatus)
	}
	return fmt.Sprintf("nereidlink: %s: %s", e.Body.Code, e.Body.Message)
}

// MarshalJSON renders the platform envelope shape {"error":{...}}.
func (e *APIError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Error ErrorBody `json:"error"`
	}{Error: e.Body})
}

// UnmarshalJSON parses the platform envelope shape.
func (e *APIError) UnmarshalJSON(raw []byte) error {
	var envelope struct {
		Error ErrorBody `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	e.Body = envelope.Error
	return nil
}

// DecodeError parses a non-success device HTTP response body into *APIError.
// It tolerates an empty or non-JSON body, in which case only HTTPStatus is
// set. The body is always closed.
func DecodeError(resp *http.Response) *APIError {
	defer func() { _ = resp.Body.Close() }()

	out := &APIError{HTTPStatus: resp.StatusCode}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || len(raw) == 0 {
		return out
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return out
	}
	if out.Body.Code == "" {
		out.Body.Code = http.StatusText(resp.StatusCode)
	}
	return out
}
