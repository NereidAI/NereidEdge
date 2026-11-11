// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"context"
	"errors"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// ErrNoTarget marks an apply that could not resolve any onvif target device or
// MAC address. The status report maps it to the generic "target not found"
// reason; no selector value ever reaches the platform as a reason.
var ErrNoTarget = errors.New("secret target not found")

// Binder pushes an applied secret beyond the local encrypted copy into an
// EdgeX device service so edge devices can use the credential. It is optional:
// without one a secret exists only as the encrypted copy. Binders must be
// idempotent: the handler re-delivers a full desired set after every (re)hello
// and re-invokes Apply even for already-stored revisions.
type Binder interface {
	// Apply binds entry into the EdgeX device service. It must return nil for
	// secret types it does not own, so generic secrets are unaffected.
	Apply(ctx context.Context, entry device.SecretEntry) error
	// Remove undoes the EdgeX-side binding for name. It must be idempotent and
	// treat an already-missing binding as success.
	Remove(ctx context.Context, name string) error
}
