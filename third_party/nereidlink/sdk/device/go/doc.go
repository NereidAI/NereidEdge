// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package device is the device-side (southbound) Go SDK for NereidLink.
//
// It is a thin protocol library for device and gateway firmware: it supplies
// typed frame models, credential handling and frozen wire constants for the
// HTTP and MQTT ingresses. The package owns no goroutines and runs no
// background loops; the firmware drives every poll, heartbeat and reconnect.
//
// Directly connected devices use the devicehttp or devicemqtt sub-packages.
// Gateways (devices with the gateway capability) additionally use the
// gateway sub-package for hello negotiation, sub-device attachment and the
// connector/discovery/probe/OTA gateway frames.
package device
