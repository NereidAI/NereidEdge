// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
)

// ExportStage returns an App Function that fans accepted EdgeX events out to
// every node-level export pipeline before the NL telemetry stage runs. It
// always returns (true, data) so export cannot stall or short-circuit the main
// pipeline; export is a parallel side channel.
func (m *ExportManager) ExportStage() interfaces.AppFunction {
	return func(ctx interfaces.AppFunctionContext, data interface{}) (bool, interface{}) {
		if ev := toEvent(data); ev != nil {
			m.Fanout(ev)
		}
		return true, data
	}
}

// toEvent extracts a *dtos.Event from the SDK-shaped data.
func toEvent(data interface{}) *dtos.Event {
	switch v := data.(type) {
	case *dtos.Event:
		return v
	case dtos.Event:
		return &v
	default:
		return nil
	}
}
