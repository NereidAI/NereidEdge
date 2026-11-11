// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package metrics

import "testing"

func TestMetrics(t *testing.T) {
	m := New()
	m.IncTelemetry()
	m.IncTelemetry()
	m.IncError()
	if m.GetTelemetry() != 2 {
		t.Errorf("telemetry expected 2, got %d", m.GetTelemetry())
	}
	if m.GetError() != 1 {
		t.Errorf("error expected 1, got %d", m.GetError())
	}
}
