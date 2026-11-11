<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# EdgeX 4.0 Official Device Services Compatibility Matrix

- Date: 2026-10-05
- Scope: all device services officially maintained in EdgeX Foundry v4.0.2 (Odessa)
- Sources: source code and `res/` examples from the `v4.0.2` tags / `odessa` branches of the official repositories, the edgex-compose odessa compose-builder, and docs.edgexfoundry.org/4-0
- Purpose: guide NereidEdge's profile uplink, device-manifest downlink (edgex-device connector spec), and the generic SecretInjector design

## 1. Quick-reference table (15 maintained DSes)

| DS (serviceKey) | Port | protocols key | Required protocolProperties | Secret form | Discovery | Container privileges/deployment |
|---|---|---|---|---|---|---|
| device-virtual | - | `other` | None (decorative) | None | None | None |
| device-mqtt | 59982 | `mqtt` | `CommandTopic` only (for bidirectional commands) | C global broker auth | None | None |
| device-modbus | 59901 | `modbus-tcp` / `-rtu` / `-ascii` | TCP: `Address,Port,UnitID`; serial: `Address,BaudRate,DataBits,StopBits,Parity,UnitID` | None | None | RTU/ASCII require a serial port |
| device-rest | 59986 | `REST` / `other` | `Host,Port,Path?` (bidirectional); `other:{}` (async-only) | None | None | None |
| device-snmp | 59993 | `TCP` (literal key) | `Address,Port` (both strings) | None (community hardcoded to `private`; no v3/trap support) | None | None |
| device-gpio | 59910 | `{}` (ignored) | None; the point table lives in profile attributes `line` | None | None | Linux, `/sys/class/gpio` |
| device-onvif-camera | 59984 | `Onvif` (+`CustomMetadata`) | `Address,Port,FriendlyName,MACAddress` | B MAC→CredentialsMap | WS-Discovery / netscan | Host network recommended for multicast discovery |
| device-usb-camera | 59983 (RTSP 8554) | `USB` | `Paths[]` (`Path` deprecated), `AutoStreaming` | D fixed name `rtspauth` | V4L2 auto-discovery | root, `/dev` mount, `device_cgroup_rules`, kernel ≥ 5.10 |
| device-opc-ua | 59997 | `opcua` | `Endpoint` | E file certificates (no secret store; anonymous login only) | None | cert/key file mounts |
| device-bacnet-ip | 59980 | `BACnet-IP` | `DeviceInstance,Port?` | None | Who-Is | UDP 47808 |
| device-bacnet-mstp | 59981 | `BACnet-MSTP` | `DeviceInstance,Path(/dev/ttyUSBx)` | None | Who-Is | Serial port |
| device-coap (C) | 59988 | `COAP` / `other` | `ED_ADDR,ED_SecurityMode(NoSec/PSK),ED_PskKey` | D global `CoAP/PskKey` | None | UDP/DTLS |
| device-uart | 59995 | `UART` | `deviceLocation,baudRate` | None | None | Serial port `/dev/tty*` |
| device-can (C) | 59999 | `CAN` | `DevInterface,FilterMsgId,FilterMask,TimeOut` | None | None | Host network + NET_RAW, pre-provisioned can0/vcan0 |
| device-rfid-llrp | 59989 | `tcp` | `host,port` (5084) | None | Yes (subnet scan + provision watcher) | Host network recommended |
| device-s7 | 59994 | `s7` | `Host,Port,Rack,Slot` (+`Timeout,IdleTimeout`) | None | None | None (outbound TCP/102) |

Archived/unavailable: device-ble (repository does not exist), device-bluetooth, device-grove-c, device-camera-go (superseded by onvif/usb), device-random, the legacy device-bacnet (pre-C version), device-fischertechnik.

## 2. Per-DS details

### 2.1 device-virtual (`device-virtual`, device-virtual-go)

- protocols (purely decorative; the driver never reads them):
  ```yaml
  protocols:
    other:
      Address: device-virtual-int-01
      Protocol: 300
  ```
- Ships with 5 preloaded profiles: bool/int/uint/float/binary (object was added in v4.0). Each resource `X` is paired with a writable hidden Bool resource `EnableRandomization_X`; the random range is taken from the properties `minimum/maximum/defaultValue`.
- Makes heavy use of auto-events (every 15s in the example). Discover() is not implemented.
- Source: `github.com/edgexfoundry/device-virtual-go@v4.0.2`, `cmd/res/devices/devices.yaml`

### 2.2 device-mqtt (`device-mqtt`)

- The device level has only one optional property (not needed for async-only uplink devices):
  ```yaml
  protocols:
    mqtt:
      CommandTopic: "command/MQTT-test-device"
  ```
  The broker connection is **global configuration** `MQTTBrokerInfo` (Schema/Host/Port/Qos/IncomingTopic/ResponseTopic), not device-level:
  ```yaml
  MQTTBrokerInfo:
    Schema: "tcp"
    Host: "localhost"
    Port: 1883
    AuthMode: "usernamepassword"   # or none
    CredentialsName: "credentials"
  Writable:
    InsecureSecrets:
      MQTT:
        SecretName: "credentials"
        SecretData: {username: "", password: ""}
  ```
- Secrets: `GetSecret(CredentialsName, "username","password")`; only these two keys are supported, and there is no TLS client-certificate code.
- Secrets are read when the MQTT client is created (with startup retries); **no hot-reload callback is registered**, so rotation requires a reconnect/restart.
- Source: `device-mqtt-go@v4.0.2`, `internal/driver/config.go:110-138`, `cmd/res/configuration.yaml`

### 2.3 device-modbus (`device-modbus`)

```yaml
# Modbus TCP
protocols:
  modbus-tcp:
    Address: 192.0.2.1
    Port: 1502
    UnitID: 1
    Timeout: 5
    IdleTimeout: 5
# Serial RTU/ASCII
protocols:
  modbus-rtu:
    Address: /tmp/slave
    UnitID: 1
    BaudRate: 19200
    DataBits: 8
    StopBits: 1
    Parity: N      # N/E/O
```

- Required profile resource attributes: `primaryTable` (COILS/DISCRETE_INPUTS/INPUT_REGISTERS/HOLDING_REGISTERS) and `startingAddress` (sub-register addresses such as 10.10 are supported); optional: `rawType,isByteSwap,isWordSwap,stringRegisterSize`.
- Caveat: the sample configuration leaves `ProfilesDir/DevicesDir` empty, so the res examples are not loaded automatically.
- Source: `device-modbus-go@v4.0.2`, `internal/driver/protocolpropertykey.go`, `res/devices/modbus.test.devices.yaml`

### 2.4 device-rest (`device-rest`)

```yaml
# Bidirectional
protocols:
  REST: {Host: 127.0.0.1, Port: "5000", Path: api}
# Unidirectional (third-party POST push)
protocols:
  other: {}
```

- URL = `http://Host:Port/Path/<resource>?urlRawQuery`; GET maps to HTTP GET, SET maps to HTTP PUT.
- Optional resource-level attribute `urlRawQuery`; **composite deviceCommands are not supported**; Binary resources require a mediaType (image/jpeg|png).
- Async pushes go to the service route `api/v3/resource/:deviceName/:resourceName`.
- Source: `device-rest-go@v4.0.2`, `driver/resthandler.go:40`, `res/devices/sample-devices.yaml`

### 2.5 device-snmp (`device-snmp`)

```yaml
protocols:
  TCP:                      # the key is literally TCP (even though SNMP runs over UDP)
    Address: "198.51.100.200"
    Port: "161"
```

- Required profile resource attribute: `oid`; the example carries a `community`, but the v4.0 driver actually **hardcodes community=`private`** (`const.go:10`). SNMPv3 is not supported and traps are not supported.
- Source: `device-snmp-go@v4.0.2`, `internal/driver/snmpdriver.go:64`, `snmpclient.go:64`

### 2.6 device-gpio (`device-gpio`; the repository name has no -go suffix)

- protocols is ignored; the example simply uses `protocols: {}`. The hardware backend is selected by the global config `Driver.Interface` (sysfs by default / chardev experimental) and `ChipSelected`.
- Profile resource attributes:
  ```yaml
  deviceResources:
    - name: "Power"
      properties: {valueType: "Bool", readWrite: "RW"}
      attributes: {line: 17}
    - name: "LED"
      properties: {valueType: "Bool", readWrite: "W"}
      attributes: {line: 27, defaultDirection: "out"}
  ```
  `defaultDirection: in` must be paired with `readWrite: R`.
- Source: `edgexfoundry/device-gpio@v4.0.2` (note the repository name)

### 2.7 device-onvif-camera (`device-onvif-camera`, the most complex)

```yaml
protocols:
  Onvif:
    Address: 198.51.100.123
    Port: "80"
    FriendlyName: Home camera
    MACAddress: "aa:bb:cc:dd:ee:ff"
  CustomMetadata:
    Location: Front door
```

- The default profile name is `onvif-camera` (a single generic profile); FriendlyName/MACAddress are themselves readable/writable resources.
- **Secrets are associated by MAC, not by a device-level SecretName**:
  - The secret pack always has three fixed keys: `username`, `password`, `mode` (`usernametoken|digest|both|none`)
  - The association lives in global custom config: `AppCustom.DefaultSecretName` and `AppCustom.CredentialsMap` (`secretName → comma-separated MAC list`); the magic name `NoAuth` means no authentication
  - non-secure: configured under `Writable.InsecureSecrets.<name>`; secure: `POST :59984/api/v3/secret` (the official docs say /api/v4/secret, but the route constant in code is still /api/v3/secret; **both versions must be verified**)
  - The driver registers a wildcard secret-updated callback, so secret rotation needs no restart; after a successful connection the driver writes the device MACAddress back.
- Discovery: WS-Discovery multicast (UDP 239.255.255.250:3702, essentially unusable under Docker NAT) or netscan unicast scans of `DiscoverySubnets`; trigger it with `POST /api/v4/discovery`; the provision watcher matches Address by regex and attaches the profile automatically.
- Deployment: multicast mode effectively requires host networking plus an explicit `DiscoveryEthernetInterface`; netscan works on a bridge network.
- Source: `device-onvif-camera@odessa`, `internal/driver/credentials.go`, `macmapper.go`, `docs/.../credentials.md`, `cmd/res/devices/camera.yaml.example`

### 2.8 device-usb-camera (`device-usb-camera`)

```yaml
protocols:
  USB:
    Paths: ["/dev/video6"]      # new format, multiple nodes
    AutoStreaming: "false"
    CardName: "..."             # optional
    SerialNumber: "..."         # optional
```

- Discovery: V4L2 auto-scan; the device name is `<CardName>-<SerialNumber>`; the default provision watcher matches on Paths, profile `USB-Camera-General`.
- Secrets: a single fixed-name secret **`rtspauth`** with keys `username/password` (no mode), used for the built-in MediaMTX RTSP authentication; leaving it empty is not the same as disabling authentication.
- Deployment: the container runs as **root**, mounts `/dev:/dev` and `/run/udev:/run/udev:ro`, with `device_cgroup_rules: ['c 81:* rw']`; ports 59983 and RTSP 8554; kernel ≥ 5.10; FFmpeg + MediaMTX are bundled.
- Source: `device-usb-camera@odessa`, `add-device-usb-camera.yml`, `cmd/res/configuration.yaml`, `supplementary-info/USB-protocol.md`

### 2.9 device-opc-ua (`device-opc-ua`)

```yaml
protocols:
  opcua:
    Endpoint: "opc.tcp://198.51.100.21:53530/OPCUA/SimulationServer"
```

- Security is global configuration under `OPCUAServer`: `Policy` (None/Basic256Sha256/…), `Mode` (None/Sign/SignAndEncrypt), `CertFile`/`KeyFile` (**local file paths**); **anonymous login only** — no username/password and no secret-store integration. Certificates can only be delivered via mounted volumes/file distribution.
- Profile attributes: `nodeId` (e.g. `ns=3;i=1001`); method resources use `{methodId,objectId,inputMap}`; subscriptions are the global `OPCUAServer.Writable.Resources`.
- Source: `device-opc-ua@v4.0.2`, `internal/driver/config.go`, `subscriptionlistener.go`

### 2.10 device-bacnet-ip / device-bacnet-mstp (device-bacnet-c; one C codebase, two services)

```json
// IP
"protocols": {"BACnet-IP":  {"DeviceInstance": "52", "Port": "0xBAC0"}}
// MSTP
"protocols": {"BACnet-MSTP": {"DeviceInstance": "53", "Path": "/dev/ttyUSB0"}}
```

- DeviceInstance accepts a BACnet instance number or the device IP; Port defaults to 47808; the MSTP serial path is also affected by the global `Driver.DefaultDevicePath`.
- No secrets; Who-Is discovery is supported; types are limited to Bool/String/Int/Float (no Enum/OctetString/Date/Time).
- Profiles are in JSON format (KMC examples). Compose defines two independent services; the container entrypoint uses `--confdir=/res`.
- Source: `device-bacnet-c@v4.0.2`, `src/c/main.c`, `res/devices/*.json.example`

### 2.11 device-coap (device-coap-c)

```json
// client device
"COAP": {"ED_ADDR": "127.0.0.1", "ED_SecurityMode": "PSK", "ED_PskKey": "hello123"}
// server device (device reports proactively to /a1r/{device}/{resource})
"other": {}
```

- DTLS-PSK, certificate-free mode. The PSK is a global secret:
  ```yaml
  Writable:
    InsecureSecrets:
      CoAP:
        SecretName: psk
        SecretData: {PskKey: "<base64, up to 16 bytes>"}
  ```
- Source: `device-coap-c@v4.0.2`, `res/configuration.yaml`, `res/devices/devices.json`

### 2.12 device-uart (`device-uart`)

```yaml
protocols:
  UART:
    deviceLocation: "/dev/ttyAMA2"
    baudRate: 115200      # int
```

- Profile resource attributes: `{type:"generic", maxbytes:160, timeout:1}`; the driver only sets Name/Baud/ReadTimeout — there is no parity/stop-bit configuration.
- Deployment: map the host serial port into the container. Discover() is not implemented.
- Source: `device-uart@v4.0.2`, `internal/driver/drv_uart_generic.go`

### 2.13 device-can (device-can-c)

```json
"protocols": {
  "CAN": {"DevInterface": "can0", "FilterMsgId": "123", "FilterMask": "2047", "TimeOut": "5"}
}
```

- Native SocketCAN (`SOCK_RAW` + CAN_RAW_FILTER); requires host networking + the NET_RAW capability and a pre-provisioned host `can0/vcan0` (`ip link add dev vcan0 type vcan && ip link set up`).
- Profile attributes are effectively unused; readings are decoded as a UInt32Array `[msgId,dlc,d1..d8]`.
- Note: the EXPOSE 59980 in `scripts/Dockerfile.alpine` is stale; the actual port is 59999.
- Source: `device-can@v4.0.2` (there is no v4.0 branch; use the tag), `src/c/device-can.c`

### 2.14 device-rfid-llrp (`device-rfid-llrp`)

```yaml
protocols:
  tcp:
    host: "203.0.113.10"
    port: "5084"
```

- No secrets. Enable discovery with `Discovery.Enabled:true, Interval:"1h"` plus `AppCustom.DiscoverySubnets` CIDRs; it scans TCP 5084 and attaches the profile automatically via the provision watcher's `vendorPEN` (Impinj=25882). The container typically uses host networking.
- The profile has no attributes; resources are LLRP operations directly (ReaderCapabilities/ROSpec/AccessSpec etc., typed Object/UInt32).
- Source: `device-rfid-llrp-go@v4.0.2`, `internal/driver/discover.go`, `res/provision_watchers/`

### 2.15 device-s7 (`device-s7`)

```yaml
protocols:
  s7:
    Host: 198.51.100.199
    Port: 102
    Rack: 0
    Slot: 1
    Timeout: 30
    IdleTimeout: 30
```

- The single required profile resource attribute is `NodeName`: `DB4.DBX0.0` (bool)/`DB4.DBB1`/`DB4.DBW2`/`DB4.DBD4`.
- Plain outbound TCP/102, no privileges. The sample ProfilesDir/DevicesDir default to empty.
- Source: `device-s7@v4.0.2`, `internal/driver/driver.go:443`

## 3. Secret model deep dive

### 3.1 Five forms

| Form | Representative DS | Association mechanism | Hot reload |
|---|---|---|---|
| A device-level SecretName | Common in future third-party DSes | protocolProperties.SecretName → GetSecret | Depends on whether the DS registers a callback |
| B MAC mapping | onvif | secret-store secret pack + global `AppCustom.CredentialsMap` (secretName→MAC list) | Yes (onvif registers a wildcard callback) |
| C global broker auth | mqtt | global `MQTTBrokerInfo.AuthMode/CredentialsName` | No; reconnect/restart required |
| D global fixed name | usb (`rtspauth`), coap (`CoAP/psk`) | Fixed SecretName | Implemented separately by usb/coap |
| E file certificates | opc-ua | CertFile/KeyFile local paths | No; delivered via files |

Of the 15 DSes, only 4 (onvif/mqtt/usb/coap) consume secrets.

### 3.2 Secret write paths (go-mod-bootstrap v4)

- **Secure mode**: OpenBao (Vault-compatible) isolates paths at `secret/edgex/<serviceKey>/<secretName>`; a service token can only write its own partition, so an app service **cannot write across partitions directly**. The official path is to call the target DS's `POST http://<ds>:<port>/api/v3/secret` (the DS persists it with its own token; an EdgeX JWT is required):
  ```json
  {"apiVersion":"v3","secretName":"credentials001",
   "secretData":[{"key":"username","value":"..."},{"key":"password","value":"..."},{"key":"mode","value":"usernametoken"}]}
  ```
- **Non-secure mode**: write the core-keeper leaf paths `Writable/InsecureSecrets/<name>/SecretName` and `.../SecretData/<key>`; the DS config watcher then triggers the update callback. With pure file configuration (no keeper), StoreSecret returns an error and the only option is to edit the file and restart.
- The secret update notification mechanism is callback-based (`RegisterSecretUpdatedCallback`), not an OpenBao watch; editing OpenBao directly does not notify the DS.
- opc-ua certificates and SecretsFile bootstrapping (`SECRETSTORE_SECRETSFILE`) are file-path solutions outside the scope of the secret API.
- Source: go-mod-bootstrap v4 `bootstrap/secret/{secure,insecure}.go`, `controller/commonapi.go`; go-mod-core-contracts `common/constants.go` (ApiSecretRoute=/api/v3/secret)

## 4. Architectural constraints for NereidEdge

1. **The SecretInjector must be a table of DS capability adapters**; pure pass-through is not enough:
   - Common action: deliver the secret pack (in secure mode call the target DS `/api/v3/secret`; in non-secure mode write keeper InsecureSecrets)
   - DS-specific actions: onvif appends `CredentialsMap` config, mqtt changes `MQTTBrokerInfo`, coap changes SecurityMode (all via keeper config updates)
   - Suggested spec config: `secrets[]{target_ds, secret_name, data, bind?}`; bind declares the association mechanism (e.g. `{type:"onvif-mac", mac}` or `{type:"broker-auth"}`)
   - opc-ua file certificates go through a separate file-delivery channel (volume), not the SecretInjector
2. **Profile uplink must pass deviceResource.attributes through verbatim** (modbus primaryTable/startingAddress, s7 NodeName, opcua nodeId, snmp oid, gpio line, uart type/maxbytes/timeout); the NL parser extracts only Measurement metadata and stores the attributes body as opaque JSON.
3. **Upfront validation of hard profile constraints**: rest does not support composite deviceCommands; modbus/uart/s7 require attributes; the snmp community is not configurable — these must produce an explicit FAILED reason during NL parsing / edge apply.
4. **Tiered Horizon deployment contracts**:
   - Plain containers: virtual/mqtt/modbus-tcp/rest/snmp/opc-ua/s7/llrp (onvif netscan)
   - Serial-port mapping: modbus-rtu/ascii, bacnet-mstp, uart
   - Privileged: can (host network + NET_RAW + pre-provisioned interface), usb (root + /dev + cgroup + kernel version), gpio (`/sys/class/gpio`)
   - Host-network scenarios: onvif multicast, llrp subnet scan
5. **M9 acceptance order**: virtual (zero dependencies) → modbus/s7 (typical protocolProperties) → mqtt (form C) → onvif (form B + discovery, most complex) → usb/coap (D) → opc-ua (E file certificates).

## 5. Source link inventory

- General
  - go-mod-bootstrap v4: `github.com/edgexfoundry/go-mod-bootstrap` (the main branch is v4), `bootstrap/secret/`, `bootstrap/config/types.go`, `bootstrap/environment/variables.go`
  - Secret route: go-mod-core-contracts v4 `common/constants.go`, `dtos/common/secret.go`
  - Documentation: docs.edgexfoundry.org/4-0/security/ (Ch-SecretStore, Ch-SecretProviderApi, SeedingServiceSecrets)
- DS repositories (tags/branches are all v4.0.2 / odessa):
  - github.com/edgexfoundry/device-virtual-go
  - github.com/edgexfoundry/device-mqtt-go
  - github.com/edgexfoundry/device-modbus-go
  - github.com/edgexfoundry/device-rest-go
  - github.com/edgexfoundry/device-snmp-go
  - github.com/edgexfoundry/device-gpio (no -go)
  - github.com/edgexfoundry/device-onvif-camera
  - github.com/edgexfoundry/device-usb-camera
  - github.com/edgexfoundry/device-opc-ua
  - github.com/edgexfoundry/device-bacnet-c (dual services)
  - github.com/edgexfoundry/device-coap-c
  - github.com/edgexfoundry/device-uart
  - github.com/edgexfoundry/device-can
  - github.com/edgexfoundry/device-rfid-llrp-go
  - github.com/edgexfoundry/device-s7
- Compose topology: edgex-compose odessa `compose-builder/add-device-*.yml`
