// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"encoding/json"
	"testing"
)

func TestSecretTypeAdvertsCatalog(t *testing.T) {
	adverts := SecretTypeAdverts()
	if len(adverts) == 0 {
		t.Fatal("expected a non-empty catalog")
	}
	seen := map[string]bool{}
	for _, a := range adverts {
		if a.Type == "" || a.TypeVersion == "" {
			t.Fatalf("advert %+v missing type or version", a)
		}
		if seen[a.Type] {
			t.Fatalf("duplicate catalog type %q", a.Type)
		}
		seen[a.Type] = true
		if !json.Valid(a.FieldsSchema) {
			t.Fatalf("%s: fields_schema is not valid JSON: %s", a.Type, a.FieldsSchema)
		}
		var schema map[string]any
		if err := json.Unmarshal(a.FieldsSchema, &schema); err != nil {
			t.Fatalf("%s: %v", a.Type, err)
		}
		if schema["type"] != "object" {
			t.Fatalf("%s: schema root type = %v", a.Type, schema["type"])
		}
		if required, ok := schema["required"].([]any); !ok || len(required) == 0 {
			t.Fatalf("%s: schema.required missing", a.Type)
		}
		if a.DisplayName["en"] == "" || a.DisplayName["zh"] == "" {
			t.Fatalf("%s: display_name = %v", a.Type, a.DisplayName)
		}
	}
	for _, want := range []string{"basic-auth", "mqtt-basic", "coap-psk", "opcua-user", "snmp-v3"} {
		if !seen[want] {
			t.Fatalf("catalog missing %q (got %v)", want, seen)
		}
	}
}
