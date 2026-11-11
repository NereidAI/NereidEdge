// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package metrics provides basic observability metrics.
package metrics

import "sync/atomic"

// Metrics holds simple counters.
type Metrics struct {
	telemetryCount int64
	errorCount     int64
}

// New creates new metrics.
func New() *Metrics {
	return &Metrics{}
}

// IncTelemetry increments telemetry counter.
func (m *Metrics) IncTelemetry() {
	if m == nil {
		return
	}
	atomic.AddInt64(&m.telemetryCount, 1)
}

// IncError increments error counter.
func (m *Metrics) IncError() {
	if m == nil {
		return
	}
	atomic.AddInt64(&m.errorCount, 1)
}

// GetTelemetry returns telemetry count.
func (m *Metrics) GetTelemetry() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.telemetryCount)
}

// GetError returns error count.
func (m *Metrics) GetError() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.errorCount)
}
