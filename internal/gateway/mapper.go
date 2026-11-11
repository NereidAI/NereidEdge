// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package gateway provides mapping utilities for device translation.
package gateway

import (
	"strings"
)

// DeviceMapper maps EdgeX device names to NereidLink sub-device UUIDs.
type DeviceMapper struct {
	mapping map[string]string
	reverse map[string]string
}

// NewDeviceMapper creates a new device mapper from config string.
// Format: "edgex1:uuid1,edgex2:uuid2"
func NewDeviceMapper(cfg string) *DeviceMapper {
	m := &DeviceMapper{
		mapping: make(map[string]string),
		reverse: make(map[string]string),
	}
	if cfg == "" {
		return m
	}
	parts := strings.Split(cfg, ",")
	for _, p := range parts {
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, ":", 2)
		if len(kv) == 2 {
			k := strings.TrimSpace(kv[0])
			v := strings.TrimSpace(kv[1])
			m.mapping[k] = v
			m.reverse[v] = k
		}
	}
	return m
}

// Map translates EdgeX device name to NL sub-device UUID.
func (m *DeviceMapper) Map(edgexDevice string) string {
	if m == nil {
		return edgexDevice
	}
	if uuid, ok := m.mapping[edgexDevice]; ok {
		return uuid
	}
	return edgexDevice
}

// ReverseMap translates NL sub-device UUID to EdgeX device name.
func (m *DeviceMapper) ReverseMap(nlUUID string) string {
	if m == nil {
		return nlUUID
	}
	if name, ok := m.reverse[nlUUID]; ok {
		return name
	}
	return nlUUID
}

// Has reports whether the EdgeX device has an explicit NL mapping.
func (m *DeviceMapper) Has(edgexDevice string) bool {
	if m == nil {
		return false
	}
	_, ok := m.mapping[edgexDevice]
	return ok
}

// HasReverse reports whether the NL UUID has an explicit reverse mapping.
func (m *DeviceMapper) HasReverse(nlUUID string) bool {
	if m == nil {
		return false
	}
	_, ok := m.reverse[nlUUID]
	return ok
}

// GetMappedDevices returns all mapped NL device UUIDs.
func (m *DeviceMapper) GetMappedDevices() []string {
	if m == nil {
		return nil
	}
	devices := make([]string, 0, len(m.mapping))
	for _, uuid := range m.mapping {
		devices = append(devices, uuid)
	}
	return devices
}

// StaticMap returns a copy of the EdgeX name -> NL UUID mapping.
func (m *DeviceMapper) StaticMap() map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m.mapping))
	for k, v := range m.mapping {
		out[k] = v
	}
	return out
}
