// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// Handler handles a specific connector type.
type Handler interface {
	// Type returns the connector type this handler implements.
	Type() string
	// Apply applies or updates a connector spec instance.
	Apply(ctx context.Context, push device.ConnectorPush) error
	// Delete removes a connector spec instance.
	Delete(ctx context.Context, push device.ConnectorPush) error
}
