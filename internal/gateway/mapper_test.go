// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway

import "testing"

func TestNewDeviceMapper(t *testing.T) {
	m := NewDeviceMapper("dev1:uuid1,dev2:uuid2")
	if got := m.Map("dev1"); got != "uuid1" {
		t.Errorf("expected uuid1, got %s", got)
	}
	if got := m.Map("dev2"); got != "uuid2" {
		t.Errorf("expected uuid2, got %s", got)
	}
	if got := m.Map("unknown"); got != "unknown" {
		t.Errorf("expected unknown, got %s", got)
	}
}

func TestNewDeviceMapperEmpty(t *testing.T) {
	m := NewDeviceMapper("")
	if got := m.Map("x"); got != "x" {
		t.Errorf("expected x, got %s", got)
	}
}
