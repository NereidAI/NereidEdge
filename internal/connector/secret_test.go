// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import "testing"

func TestSanitizeSecrets(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"no secret passthrough", `{"qos":1}`, `{"qos":1}`},
		{"masks value", `password=secret://abc123`, `password=secret://***`},
		{"masks json value", `{"password":"secret://hunter2"}`, `{"password":"secret://***"}`},
		{"masks multiple", `a=secret://one b=secret://two`, `a=secret://*** b=secret://***`},
		{"brace delimiter", `{"k":"secret://v"}`, `{"k":"secret://***"}`},
		{"comma delimiter", `x=secret://a,y=1`, `x=secret://***,y=1`},
		{"empty value terminates", `x=secret:// y=2`, `x=secret://*** y=2`},
		{"value at end of string", `secret://lonely`, `secret://***`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeSecrets(tc.in)
			if got != tc.want {
				t.Fatalf("sanitizeSecrets(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
