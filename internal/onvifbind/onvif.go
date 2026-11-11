// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package onvifbind binds NereidEdge-delivered ONVIF credentials to EdgeX
// device-onvif-camera cameras (ADR-0034). The device service keeps credentials
// in its secret store by name and maps a secret name to cameras through its
// custom AppCustom config held in core-keeper: CredentialsMap[secretName] is a
// comma-separated list of camera MAC addresses, joined on each device's
// Protocols.Onvif.MACAddress; AppCustom.DefaultSecretName is the fallback for
// cameras without a mapping (see internal/driver/macmapper.go and
// credentials.go in device-onvif-camera). The EdgeX REST secret API has no
// device mapping, so NereidEdge performs the mapping itself:
//
//  1. POST /api/v3/secret stores the credential data under the secret name;
//  2. a core-keeper write records the MAC -> secret-name mapping; and
//  3. removal deletes both shapes from core-keeper so missing credentials fall
//     back to NoAuth inside the device service.
package onvifbind

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	"github.com/nereidai/nereidedge/internal/secrets"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// onvifSecretType is the secret type descriptor this binder owns. Entries of
// any other type exist only as the local encrypted copy.
const onvifSecretType = "onvif-basic"

// onvifSecretTypeVersion is the descriptor version advertised in hello.
const onvifSecretTypeVersion = "1.0"

// onvifBasicSchema is the self-contained draft 2020-12 JSON Schema for an
// onvif-basic document. NL registers it in the tenant secret catalog and
// validates delivered documents against it; password is marked format=password
// so consoles mask it, and mode is optional because the device service falls
// back to usernametoken when it is absent (credentials.go).
var onvifBasicSchema = json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["username", "password"],
  "properties": {
    "username": {"type": "string"},
    "password": {"type": "string", "format": "password"},
    "mode": {"type": "string", "enum": ["usernametoken", "digest", "both", "none"]}
  }
}`)

// SecretTypeAdverts returns the credential type descriptors this binder
// implements, for the hello negotiation (ADR-0034). Only types with a real
// device-service binding are advertised, so the platform catalog stays honest;
// add a new advert here when a binder lands for another type.
func SecretTypeAdverts() []device.SecretTypeAdvert {
	return []device.SecretTypeAdvert{{
		Type:         onvifSecretType,
		TypeVersion:  onvifSecretTypeVersion,
		FieldsSchema: onvifBasicSchema,
		DisplayName: map[string]string{
			"en": "ONVIF credentials",
			"zh": "ONVIF 凭据",
		},
	}}
}

// EdgeX topology constants for the device-onvif-camera service. The keeper
// config base is edgex/v4/<service>, mirroring how the deployed edge holds its
// configuration.
const (
	// defaultDeviceServiceURL is the docker-network address of
	// device-onvif-camera's common REST API.
	defaultDeviceServiceURL = "http://edgex-device-onvif-camera:59984"
	// defaultKeeperURL is the docker-network address of core-keeper.
	defaultKeeperURL = "http://edgex-core-keeper:59890"
	// defaultKVSBase is the core-keeper config stem of the onvif device
	// service (its 4.0 bootstrapper uses edgex/v4/<service>).
	defaultKVSBase = "edgex/v4/device-onvif-camera"
	// KVSAPIPrefix is the core-keeper REST prefix; the key path follows.
	KVSAPIPrefix = "/api/v3/kvs/key/"
)

// Key names accepted in an onvif-basic document and the EdgeX secret data keys
// they map to. getCredentialsForDevice reads exactly these three keys, so a
// missing mode would make the device service treat the secret as absent and
// silently fall back to NoAuth (credentials.go:tryGetCredentialsInternal).
const (
	usernameKey = "username"
	passwordKey = "password"
	modeKey     = "mode"

	// onvifProtocol and onvifMACAddress locate the camera MAC inside an EdgeX
	// device's Protocols map.
	onvifProtocol   = "Onvif"
	onvifMACAddress = "MACAddress"

	// defaultAuthMode is applied when the document omits mode; it matches the
	// invalid-mode fallback inside the device service.
	defaultAuthMode = "usernametoken"

	// appCustom* are the core-keeper key suffixes joined onto the KVS base.
	appCustomCredentialsMap    = "AppCustom/CredentialsMap"
	appCustomDefaultSecretName = "AppCustom/DefaultSecretName"
	writableInsecureSecrets    = "Writable/InsecureSecrets"

	// defaultTimeout bounds one HTTP call to the device service / keeper.
	defaultTimeout = 10 * time.Second

	// apiVersion is the EdgeX common API version on all request/response DTOs.
	apiVersion = "v3"

	// maxRespBytes caps an upstream error body read.
	maxRespBytes = 4096
)

// DeviceReader resolves an EdgeX device by name from core-metadata. The app
// service adapter fulfills it; tests substitute a fake.
type DeviceReader interface {
	DeviceByName(ctx context.Context, name string) (dtos.Device, error)
}

// NameLookup resolves a platform-facing device reference to an EdgeX device
// name (for example the NereidLink sub-device UUID). It is optional.
type NameLookup func(ref string) (edgeXName string, ok bool)

// Config carries the dependencies for the onvif binder.
type Config struct {
	// DeviceServiceURL is the base URL of device-onvif-camera's REST API.
	// Empty selects the docker-network default.
	DeviceServiceURL string
	// KeeperURL is the base URL of core-keeper. Empty selects the default.
	KeeperURL string
	// KVSBase is the core-keeper config stem of the onvif device service.
	// Empty selects "edgex/v4/device-onvif-camera".
	KVSBase string
	// Devices reads EdgeX devices from core-metadata.
	Devices DeviceReader
	// LookupName optionally maps a platform device reference to an EdgeX
	// device name before the metadata lookup.
	LookupName NameLookup
	// Logger is the application logging client.
	Logger logger.LoggingClient
	// Timeout bounds one HTTP call. Zero selects 10s.
	Timeout time.Duration
}

// Onvif binds onvif-basic secrets into device-onvif-camera.
type Onvif struct {
	deviceServiceURL string
	keeperURL        string
	kvsBase          string
	devices          DeviceReader
	lookupName       NameLookup
	lc               logger.LoggingClient
	http             *http.Client
}

// New creates an onvif binder. A nil Devices reader makes every binding fail
// with ErrNoTarget; an empty URL selects the docker-network default.
func New(cfg Config) *Onvif {
	if cfg.DeviceServiceURL == "" {
		cfg.DeviceServiceURL = defaultDeviceServiceURL
	}
	if cfg.KeeperURL == "" {
		cfg.KeeperURL = defaultKeeperURL
	}
	if cfg.KVSBase == "" {
		cfg.KVSBase = defaultKVSBase
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewMockClient()
	}
	return &Onvif{
		deviceServiceURL: strings.TrimSuffix(cfg.DeviceServiceURL, "/"),
		keeperURL:        strings.TrimSuffix(cfg.KeeperURL, "/"),
		kvsBase:          strings.Trim(cfg.KVSBase, "/"),
		devices:          cfg.Devices,
		lookupName:       cfg.LookupName,
		lc:               cfg.Logger,
		http:             &http.Client{Timeout: cfg.Timeout},
	}
}

// Apply binds one secret. It returns nil for non-onvif types so generic
// secrets are unaffected, ErrNoTarget when no camera resolves from the
// selectors, and a wrapped error for any failed EdgeX call.
func (b *Onvif) Apply(ctx context.Context, entry device.SecretEntry) error {
	if entry.Type != onvifSecretType {
		return nil
	}
	if entry.Name == "" {
		return errors.New("onvif binding with an empty secret name")
	}
	macs, err := b.resolveMACs(ctx, entry.Selectors)
	if err != nil {
		return err
	}
	if err := b.storeSecret(ctx, entry.Name, entry.Document); err != nil {
		return err
	}

	if len(macs) == 0 {
		// No camera selected: the secret becomes the default credential group
		// for every unmapped camera of the service.
		return b.putKV(ctx, appCustomDefaultSecretName, entry.Name)
	}
	return b.putKV(ctx, appCustomCredentialsMap+"/"+entry.Name, strings.Join(macs, ","))
}

// Remove undoes an onvif binding: the MAC->secret mapping and the credential
// data are deleted from core-keeper; the device service config watcher then
// drops the secret and its cameras fall back to NoAuth. Deleting an
// already-missing key is treated as success so removal is idempotent.
func (b *Onvif) Remove(ctx context.Context, name string) error {
	if name == "" {
		return nil
	}
	if err := b.deleteKV(ctx, appCustomCredentialsMap+"/"+name, false); err != nil {
		return err
	}
	// The secret data lives under Writable/InsecureSecrets/<name>/... with an
	// unknown set of child keys, so it must be prefix-deleted.
	return b.deleteKV(ctx, writableInsecureSecrets+"/"+name, true)
}

// resolveMACs turns selectors into a normalized (lowercase, colon-separated)
// MAC address list. A selector value that is one MAC, or a comma-separated
// list of MACs, is used verbatim; otherwise it names a device in core-metadata
// (possibly after a platform reference lookup). An absent selector list means
// whole-host default; any present-but-unresolvable selector aborts the apply
// with ErrNoTarget so a credential is never half-bound.
func (b *Onvif) resolveMACs(ctx context.Context, selectors []device.SecretSelector) ([]string, error) {
	if len(selectors) == 0 {
		return nil, nil
	}
	var macs []string
	seen := make(map[string]struct{})
	add := func(mac string) {
		if _, dup := seen[mac]; dup {
			return
		}
		seen[mac] = struct{}{}
		macs = append(macs, mac)
	}
	for _, sel := range selectors {
		// One selector may carry several MACs at once (the same comma-separated
		// shape the device service stores in CredentialsMap); accept it and the
		// single-MAC form through the same parse.
		if list, ok := parseMACList(sel.Value); ok {
			for _, mac := range list {
				add(mac)
			}
			continue
		}
		edgeXName := sel.Value
		if b.lookupName != nil {
			if name, ok := b.lookupName(sel.Value); ok {
				edgeXName = name
			}
		}
		mac, err := b.deviceMAC(ctx, edgeXName)
		if err != nil {
			return nil, err
		}
		add(mac)
	}
	if len(macs) == 0 {
		return nil, fmt.Errorf("%w: no selector resolved to an onvif camera", secrets.ErrNoTarget)
	}
	return macs, nil
}

// deviceMAC reads a camera's MAC from its onvif protocol properties in
// core-metadata and normalizes it. A device without an onvif MAC is a failed
// resolution.
func (b *Onvif) deviceMAC(ctx context.Context, edgeXName string) (string, error) {
	if b.devices == nil || edgeXName == "" {
		return "", fmt.Errorf("%w: missing metadata reader or empty device reference", secrets.ErrNoTarget)
	}
	dev, err := b.devices.DeviceByName(ctx, edgeXName)
	if err != nil {
		return "", fmt.Errorf("%w: resolve device %q", secrets.ErrNoTarget, edgeXName)
	}
	mac, ok := protocolString(dev.Protocols, onvifProtocol, onvifMACAddress)
	if !ok || mac == "" {
		return "", fmt.Errorf("%w: device %q has no onvif MAC", secrets.ErrNoTarget, edgeXName)
	}
	normalized, ok := parseMAC(mac)
	if !ok {
		return "", fmt.Errorf("%w: device %q has an invalid MAC", secrets.ErrNoTarget, edgeXName)
	}
	return normalized, nil
}

// storeSecret POSTs the credential data to device-onvif-camera. mode defaults
// to usernametoken so the device service can always read all three keys.
func (b *Onvif) storeSecret(ctx context.Context, name string, document map[string]any) error {
	username, _ := docString(document, usernameKey)
	password, _ := docString(document, passwordKey)
	mode, _ := docString(document, modeKey)
	if mode == "" {
		mode = defaultAuthMode
	}
	if username == "" || password == "" {
		return errors.New("onvif-basic document must carry username and password")
	}

	body, err := json.Marshal(map[string]any{
		"apiVersion": apiVersion,
		"secretName": name,
		"secretData": []map[string]string{
			{"key": usernameKey, "value": username},
			{"key": passwordKey, "value": password},
			{"key": modeKey, "value": mode},
		},
	})
	if err != nil {
		return fmt.Errorf("encode secret request: %w", err)
	}
	status, resp, err := b.do(ctx, http.MethodPost, b.deviceServiceURL+"/api/v3/secret", body)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("device service rejected secret store (%d): %s", status, safeResp(resp))
	}
	b.lc.Infof("onvif credential %q stored", name)
	return nil
}

// putKV writes one AppCustom configuration leaf into core-keeper. The device
// service watches AppCustom and rebuilds its MAC map on change.
func (b *Onvif) putKV(ctx context.Context, keySuffix, value string) error {
	body, err := json.Marshal(map[string]any{"apiVersion": apiVersion, "value": value})
	if err != nil {
		return fmt.Errorf("encode kvs request: %w", err)
	}
	path := b.kvsBase + "/" + keySuffix
	status, resp, err := b.do(ctx, http.MethodPut, b.keeperURL+KVSAPIPrefix+path+"?flatten=false", body)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("keeper rejected config write %q (%d): %s", keySuffix, status, safeResp(resp))
	}
	b.lc.Infof("onvif binding map updated: %s = %q", keySuffix, value)
	return nil
}

// deleteKV removes a core-keeper key, optionally by prefix. A 404 is treated
// as success (already gone); every other failure surfaces so removal is
// reported honestly on the status frame.
func (b *Onvif) deleteKV(ctx context.Context, keySuffix string, prefix bool) error {
	path := b.kvsBase + "/" + keySuffix
	if prefix {
		path += "?prefixMatch=true"
	}
	status, resp, err := b.do(ctx, http.MethodDelete, b.keeperURL+KVSAPIPrefix+path, nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("keeper rejected config delete %q (%d): %s", keySuffix, status, safeResp(resp))
	}
	return nil
}

// do performs one HTTP call and returns the status, a bounded response body
// and any transport error.
func (b *Onvif) do(ctx context.Context, method, rawURL string, body []byte) (int, string, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return 0, "", fmt.Errorf("build %s request: %w", method, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("%s: %w", rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	if err != nil {
		return 0, "", fmt.Errorf("read %s response: %w", method, err)
	}
	return resp.StatusCode, string(payload), nil
}

// parseMAC normalizes any IEEE EUI-48/64 MAC presentation
// (aa:bb:cc:dd:ee:ff, AA-BB-.., or bare hex) to the lowercase colon form the
// device service's SanitizeMACAddress produces.
func parseMAC(s string) (string, bool) {
	mac, err := net.ParseMAC(strings.TrimSpace(s))
	if err != nil {
		return "", false
	}
	return strings.ToLower(mac.String()), true
}

// parseMACList parses a selector value that holds either a single MAC or a
// comma-separated list of MACs, matching the shape device-onvif-camera keeps
// in AppCustom.CredentialsMap. It returns the normalized (lowercase,
// colon-separated) MACs and true only when every non-empty part is a valid
// MAC; an empty or partially invalid value reports false so the caller falls
// back to interpreting the value as a device reference.
func parseMACList(s string) ([]string, bool) {
	var macs []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		mac, ok := parseMAC(part)
		if !ok {
			return nil, false
		}
		macs = append(macs, mac)
	}
	if len(macs) == 0 {
		return nil, false
	}
	return macs, true
}

// protocolString reads a string value from an EdgeX device Protocols map under
// protocol.ProtocolProperties[protocol][key], tolerating non-string JSON
// values.
func protocolString(props map[string]dtos.ProtocolProperties, protocol, key string) (string, bool) {
	nested, ok := props[protocol]
	if !ok {
		return "", false
	}
	return valueString(nested[key])
}

// docString reads a string value from a secret document field.
func docString(document map[string]any, key string) (string, bool) {
	v, ok := document[key]
	if !ok {
		return "", false
	}
	return fmt.Sprint(v), true
}

// valueString renders any JSON scalar as its string form.
func valueString(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	return fmt.Sprint(v), true
}

// safeResp bounds an upstream error body so a misbehaving service cannot flood
// the logs or the status reason with arbitrary content.
func safeResp(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxRespBytes {
		s = s[:maxRespBytes]
	}
	return s
}
