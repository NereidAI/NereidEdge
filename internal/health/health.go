// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package health provides health check utilities.
package health

import (
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
)

// Checker provides health status.
type Checker struct {
	Service interfaces.ApplicationService
}

// New creates a new health checker.
func New(svc interfaces.ApplicationService) *Checker {
	return &Checker{
		Service: svc,
	}
}

// Status returns health status.
func (c *Checker) Status() map[string]string {
	if c == nil || c.Service == nil {
		return map[string]string{"status": "down"}
	}
	if c.Service.LoggingClient() == nil {
		return map[string]string{"status": "unknown"}
	}
	return map[string]string{"status": "up"}
}
