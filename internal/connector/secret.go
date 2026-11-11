// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import "strings"

// sanitizeSecrets masks the value of every secret:// reference in a
// shallow JSON-like string for logging. It consumes input left to right so a
// masked reference can never be re-matched (the previous implementation
// rescanned from the start and looped forever once a value was masked).
func sanitizeSecrets(s string) string {
	if !strings.Contains(s, "secret://") {
		return s
	}
	const prefix = "secret://"
	var b strings.Builder
	b.Grow(len(s))
	rest := s
	for {
		idx := strings.Index(rest, prefix)
		if idx < 0 {
			b.WriteString(rest)
			return b.String()
		}
		// The secret value ends at the next JSON/log delimiter.
		end := len(rest)
		for i := idx + len(prefix); i < len(rest); i++ {
			c := rest[i]
			if c == ' ' || c == '\t' || c == '\n' || c == '\r' ||
				c == ',' || c == '"' || c == '}' || c == ']' || c == ')' {
				end = i
				break
			}
		}
		b.WriteString(rest[:idx])
		b.WriteString(prefix)
		b.WriteString("***")
		rest = rest[end:]
	}
}
