// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"errors"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// PublishProfiles publishes profiles to NereidLink using the NL SDK gateway client.
func (c *Client) PublishProfiles(ctx context.Context, profiles []device.EdgeProfile) error {
	if c == nil || c.gw == nil {
		return errors.New("not connected")
	}
	return c.gw.PublishProfiles(ctx, profiles)
}
