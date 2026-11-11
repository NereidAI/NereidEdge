<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# EdgeX Device Service Connection-Credential Inventory + NereidEdge `secret_types` Proposal

- Date: 2026-10-10
- Scope: officially maintained device services in EdgeX Foundry v4.x
- Sources: source code of the edgexfoundry DS repositories (`device-onvif-camera`, `device-usb-camera`, `device-mqtt-go`, `device-coap-c`, `device-opc-ua`, `device-snmp-go`, `device-s7`, `device-modbus-go`, `device-rest-go`, `device-virtual-go`, `device-gpio`, `device-can`, `device-uart`, `device-rfid-llrp-go`, `device-bluetooth`, `device-bacnet`, `device-grove-c`) + `go-mod-bootstrap/bootstrap/secret`
- Purpose: determine the `secret_types[]` that NereidEdge registers with NereidLink in `hello` (ADR-0034 / section 12 of `docs/protocols/nereid-edge/v1.md`). This document is an input to NL, used to write the credential types into the SDK and the tenant secret-type catalog.
- Related: [edgex-device-services-compat.md](edgex-device-services-compat.md) (protocol matrix and ports; its "secret form" column complements this document)

## 1. Inventory: Which Device Services Require Secret-Based Connection Information

| Device service | Protocol | Credentials required | Fields | Storage location / secret name | Granularity |
|---|---|---|---|---|---|
| **device-onvif-camera** | ONVIF camera | Yes | `username`, `password`, `mode` | keeper `AppCustom.CredentialsMap` (MAC↔secretName) + `AppCustom.DefaultSecretName` | **Device-level** (credential ↔ MAC) |
| **device-usb-camera** | UVC/USB camera RTSP | Yes | `username`, `password` | fixed secret name **`rtspauth`** | Service-level |
| **device-mqtt-go** | external MQTT broker | Yes | `username`, `password` (`authMode=usernamepassword`) | secret named by the `CredentialsName` configuration option | Service-level |
| **device-coap-c** | CoAP (PSK) | Yes | `PskKey` (base64) | fixed secret name **`psk`** | Service-level |
| device-opc-ua | OPC-UA | No | — (endpoint only, with SecurityMode=**None**; no user/certificate) | — | — |
| device-snmp-go | SNMP | No | — (community string hard-coded to `"private"`; v1/v2c only, no v3) | — | — |
| device-s7 | Siemens S7 | No | rack/slot | — | — |
| device-modbus-go | Modbus | No | — | — | — |
| device-rest-go | REST | No | — | — | — |
| device-virtual-go / device-gpio / device-can / device-uart / device-rfid-llrp-go / device-bluetooth / device-bacnet / device-grove-c | their respective protocols | No | — | — | — |

**Conclusions:**
- In stock EdgeX, only **four** device services actually read secrets: ONVIF, USB-camera, MQTT, and CoAP.
- **OPC-UA (stock `device-opc-ua`) currently consumes no credentials** — only `SecurityMode=None`. Real OPC-UA servers typically require "username/password + SecurityPolicy/Mode + certificate", but that is a capability of other/vendor OPC-UA device services, not of the one shipped with EdgeX. **If NE is to support OPC-UA, the device service to use must be decided first.**
- Field names follow EdgeX conventions: `username` / `password` (`secret.UsernameKey` / `secret.PasswordKey` in `go-mod-bootstrap`); ONVIF adds `mode`; CoAP uses `PskKey`.

## 2. `secret_types[]` Proposed for Registration by NereidEdge

Entries marked **Implemented** are currently in place and verified; the rest are proposed for inclusion (NE has not yet implemented the corresponding binder).

### 1. `onvif-basic` (implemented)

```json
{
  "type": "onvif-basic",
  "type_version": "1.0",
  "fields_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["username", "password"],
    "properties": {
      "username": {"type": "string"},
      "password": {"type": "string", "format": "password"},
      "mode": {"type": "string", "enum": ["usernametoken", "digest", "both", "none"]}
    }
  },
  "display_name": {"en": "ONVIF credentials"}
}
```

- `mode` is optional and defaults to `usernametoken` (in device-onvif-camera, `tryGetCredentialsInternal` silently falls back to NoAuth when mode is absent, so it must be set explicitly).
- Binding semantics: the credential is bound to the ONVIF device with the corresponding MAC.

### 2. `basic-auth` (proposed)

Generic HTTP/RTSP Basic authentication; the RTSP authentication in device-usb-camera takes this form (secret name `rtspauth`).

```json
{
  "type": "basic-auth",
  "type_version": "1.0",
  "fields_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["username", "password"],
    "properties": {
      "username": {"type": "string"},
      "password": {"type": "string", "format": "password"}
    }
  },
  "display_name": {"en": "Basic auth"}
}
```

### 3. `mqtt-basic` (proposed)

device-mqtt-go connecting to an external broker.

```json
{
  "type": "mqtt-basic",
  "type_version": "1.0",
  "fields_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["username", "password"],
    "properties": {
      "username": {"type": "string"},
      "password": {"type": "string", "format": "password"},
      "clientid": {"type": "string"}
    }
  },
  "display_name": {"en": "MQTT broker credentials"}
}
```

### 4. `coap-psk` (proposed)

PSK security mode of device-coap-c.

```json
{
  "type": "coap-psk",
  "type_version": "1.0",
  "fields_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["psk_key"],
    "properties": {
      "psk_key": {"type": "string", "format": "password", "description": "base64 PSK"},
      "identity": {"type": "string"}
    }
  },
  "display_name": {"en": "CoAP PSK"}
}
```

### 5. `opcua-user` (proposed; device service must be decided first)

```json
{
  "type": "opcua-user",
  "type_version": "1.0",
  "fields_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["username", "password"],
    "properties": {
      "username": {"type": "string"},
      "password": {"type": "string", "format": "password"},
      "security_policy": {"type": "string", "enum": ["None", "Basic256Sha256", "Aes128Sha256RsaOaep", "Aes256Sha256RsaPss"]},
      "security_mode": {"type": "string", "enum": ["None", "Sign", "SignAndEncrypt"]}
    }
  },
  "display_name": {"en": "OPC-UA user credentials"}
}
```

### 6. `snmp-v3` (proposed; stock device-snmp currently supports only v1/v2c)

```json
{
  "type": "snmp-v3",
  "type_version": "1.0",
  "fields_schema": {
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
  },
  "display_name": {"en": "SNMP v3 credentials"}
}
```

## 3. Open Decisions

> Decided on 2026-10-10: all six types are registered into the `hello` `secret_types`; NL already parses/registers them server-side (verdict: **announce all six types together**; `opcua-user`/`snmp-v3` are registered ahead of need; the five types other than `onvif-basic` are currently **copy-only**).

1. **Granularity** (not yet implemented; to be handled later): only `onvif-basic` is **device-level (credential ↔ MAC, via selector `kind=mac`)**; USB/MQTT/CoAP are all **service-level (credential ↔ a fixed service)**. The phase-one `selectors` currently defined in `v1.md` support only `kind=mac`.
   - Option A: extend selector kinds (e.g. `service`/`name`) so that everything uniformly follows the "credential ↔ target" model.
   - Option B: leave selectors empty for service-level types; NE writes directly to the fixed secret name of the corresponding service.
   This directly affects the NL protocol and console presentation.
2. **Ahead-of-need types**: `opcua-user` / `snmp-v3` are not consumed by stock EdgeX today. Should they be registered in the SDK catalog now to pave the way, or added only once a corresponding device service exists?
   - Decided: **register now** (announced in full via `hello` `secret_types`).
3. **Naming**: standardize on `<protocol>-<authkind>` (`onvif-basic` / `mqtt-basic` / `coap-psk` / `snmp-v3` / `opcua-user`); keep `basic-auth` as a generic name. NL may harmonize the overall style.
   - Decided: adopt the naming proposed above; keep `basic-auth` as a generic name.

## Appendix: Basis for Field Names

- Standard EdgeX keys: `username` = `secret.UsernameKey`, `password` = `secret.PasswordKey` (`go-mod-bootstrap/v4/bootstrap/secret/helper.go`).
- ONVIF: `username` / `password` / `mode` (`mode` ∈ `digest|usernametoken|both|none`, default `usernametoken`).
- CoAP-C: `PskKey` (base64, `PSK_KEY_KEY` in `device-coap.c`).
- MQTT: `username` / `password` (`authMode=usernamepassword`).
- USB-camera RTSP: `username` / `password` (secret name `rtspauth`).
