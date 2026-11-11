// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"encoding/json"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// SecretTypeAdverts returns the platform credential-type catalog entries that
// still lack a local binder. Every delivered secret is stored for any
// advertised type as an encrypted local copy, but only types with a real
// device-service binding (onvif-basic from onvifbind.SecretTypeAdverts) also
// configure the consuming service. Announcing them registers the descriptors
// in the tenant secret catalog now so operators can record credentials ahead
// of the device service that consumes them (ADR-0034). Callers must join them
// with the implemented binder adverts.
func SecretTypeAdverts() []device.SecretTypeAdvert {
	return []device.SecretTypeAdvert{
		{
			Type:        "basic-auth",
			TypeVersion: "1.0",
			FieldsSchema: json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["username", "password"],
  "properties": {
    "username": {"type": "string"},
    "password": {"type": "string", "format": "password"}
  }
}`),
			DisplayName: map[string]string{"en": "Basic auth", "zh": "基础认证"},
		},
		{
			Type:        "mqtt-basic",
			TypeVersion: "1.0",
			FieldsSchema: json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["username", "password"],
  "properties": {
    "username": {"type": "string"},
    "password": {"type": "string", "format": "password"},
    "clientid": {"type": "string"}
  }
}`),
			DisplayName: map[string]string{"en": "MQTT broker credentials", "zh": "MQTT 凭据"},
		},
		{
			Type:        "coap-psk",
			TypeVersion: "1.0",
			FieldsSchema: json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["psk_key"],
  "properties": {
    "psk_key": {"type": "string", "format": "password", "description": "base64 PSK"},
    "identity": {"type": "string"}
  }
}`),
			DisplayName: map[string]string{"en": "CoAP PSK", "zh": "CoAP 预共享密钥"},
		},
		{
			Type:        "opcua-user",
			TypeVersion: "1.0",
			FieldsSchema: json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["username", "password"],
  "properties": {
    "username": {"type": "string"},
    "password": {"type": "string", "format": "password"},
    "security_policy": {"type": "string", "enum": ["None", "Basic256Sha256", "Aes128Sha256RsaOaep", "Aes256Sha256RsaPss"]},
    "security_mode": {"type": "string", "enum": ["None", "Sign", "SignAndEncrypt"]}
  }
}`),
			DisplayName: map[string]string{"en": "OPC-UA user credentials", "zh": "OPC-UA 用户凭据"},
		},
		{
			Type:        "snmp-v3",
			TypeVersion: "1.0",
			FieldsSchema: json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["security_name", "auth_passphrase"],
  "properties": {
    "security_name": {"type": "string"},
    "auth_protocol": {"type": "string", "enum": ["MD5", "SHA"]},
    "auth_passphrase": {"type": "string", "format": "password"},
    "priv_protocol": {"type": "string", "enum": ["DES", "AES"]},
    "priv_passphrase": {"type": "string", "format": "password"}
  }
}`),
			DisplayName: map[string]string{"en": "SNMP v3 credentials", "zh": "SNMP v3 凭据"},
		},
	}
}
