// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

type aliasHandler struct {
	h Handler
	t string
}

func (a *aliasHandler) Type() string {
	return a.t
}

func (a *aliasHandler) Apply(ctx context.Context, push device.ConnectorPush) error {
	if a.h == nil {
		return nil
	}
	return a.h.Apply(ctx, push)
}

func (a *aliasHandler) Delete(ctx context.Context, push device.ConnectorPush) error {
	if a.h == nil {
		return nil
	}
	return a.h.Delete(ctx, push)
}

var _ Handler = (*aliasHandler)(nil)

var _ Handler = (*aliasHandler)(nil)
