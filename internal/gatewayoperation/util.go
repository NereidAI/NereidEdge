// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gatewayoperation

import (
	"encoding/json"
	"errors"
	"math"
)

// round2 rounds to two decimals for human-facing numeric fields.
func round2(v float64) float64 { return math.Round(v*100) / 100 }

// clampInt bounds v to [lo, hi].
func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// marshalResult encodes an operation result document.
func marshalResult(v any) (json.RawMessage, error) {
	buf, err := json.Marshal(v)
	if err != nil {
		return nil, errors.New("encode result")
	}
	return buf, nil
}

// requiredInt returns a required integer param within [lo, hi].
func requiredInt(params map[string]any, key string, lo, hi int) (int, error) {
	raw, ok := params[key]
	if !ok {
		return 0, errors.New("missing required param: " + key)
	}
	n, err := paramInt(raw)
	if err != nil {
		return 0, errors.New("param " + key + " must be an integer")
	}
	if n < lo || n > hi {
		return 0, errors.New("param " + key + " out of range")
	}
	return n, nil
}

// optionalInt returns an optional integer param clamped to [lo, hi], or def.
func optionalInt(params map[string]any, key string, lo, hi, def int) (int, error) {
	raw, ok := params[key]
	if !ok {
		return def, nil
	}
	n, err := paramInt(raw)
	if err != nil {
		return 0, errors.New("param " + key + " must be an integer")
	}
	return clampInt(n, lo, hi), nil
}
