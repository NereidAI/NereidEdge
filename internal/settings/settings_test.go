// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package settings

import (
	"errors"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func newApplier(t *testing.T, calls *[]string, err error) *Applier {
	t.Helper()
	return New(Knobs{
		SetLogLevel: func(level string) error {
			*calls = append(*calls, level)
			return err
		},
		Logger: logger.NewMockClient(),
	}, Defaults{LogLevel: "INFO"})
}

func shared(kv map[string]any) map[string]map[string]any {
	return map[string]map[string]any{device.AttributeScopeShared: kv}
}

func TestApplyLogLevel(t *testing.T) {
	var calls []string
	a := newApplier(t, &calls, nil)

	a.Apply(shared(map[string]any{KeyLogLevel: "debug"}))

	if len(calls) != 1 || calls[0] != "DEBUG" {
		t.Fatalf("log level = %v, want [DEBUG]", calls)
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	var calls []string
	a := newApplier(t, &calls, nil)

	state := shared(map[string]any{KeyLogLevel: "warn"})
	a.Apply(state)
	a.Apply(state)

	if len(calls) != 1 {
		t.Fatalf("log level applied %d times, want 1", len(calls))
	}
}

func TestDeleteRevertsToDefault(t *testing.T) {
	var calls []string
	a := newApplier(t, &calls, nil)

	a.Apply(shared(map[string]any{KeyLogLevel: "debug"}))
	a.Apply(shared(map[string]any{}))

	if len(calls) != 2 || calls[0] != "DEBUG" || calls[1] != "INFO" {
		t.Fatalf("log level = %v, want [DEBUG INFO]", calls)
	}
}

func TestInvalidValueKeepsDefault(t *testing.T) {
	var calls []string
	a := newApplier(t, &calls, nil)

	a.Apply(shared(map[string]any{KeyLogLevel: "verbose"}))

	if len(calls) != 0 {
		t.Fatalf("invalid value must not touch the knob: %v", calls)
	}
}

func TestServerScopeIsIgnored(t *testing.T) {
	var calls []string
	a := newApplier(t, &calls, nil)

	a.Apply(map[string]map[string]any{
		device.AttributeScopeServer: {KeyLogLevel: "debug"},
	})

	if len(calls) != 0 {
		t.Fatalf("SERVER_SCOPE must not drive the knob: %v", calls)
	}
}

func TestSetterErrorKeepsCurrent(t *testing.T) {
	var calls []string
	a := newApplier(t, &calls, errors.New("boom"))

	a.Apply(shared(map[string]any{KeyLogLevel: "debug"}))
	// The failed apply must not be recorded, so a retry still calls the knob.
	a.Apply(shared(map[string]any{KeyLogLevel: "debug"}))

	if len(calls) != 2 {
		t.Fatalf("log level calls = %d, want 2", len(calls))
	}
}
