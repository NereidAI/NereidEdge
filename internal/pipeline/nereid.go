// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package pipeline provides function pipeline transforms for NereidEdge.
package pipeline

import (
	"errors"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

// Passthrough is a minimal transform that logs and passes events through.
// It is intended as the default pipeline for M0 skeleton.
func Passthrough(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
	event, ok := data.(dtos.Event)
	if !ok {
		ctx.LoggingClient().Warn("Received non-event data")
		return false, errors.New("non-event data type")
	}

	ctx.LoggingClient().Debugf(
		"NereidEdge passthrough: device=%s profile=%s source=%s readings=%d",
		event.DeviceName, event.ProfileName, event.SourceName, len(event.Readings),
	)
	return true, data
}
