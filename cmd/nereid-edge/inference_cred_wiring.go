// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"

	"github.com/edgexfoundry/app-functions-sdk-go/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"

	device "github.com/nereidai/nereidlink/sdk/device/go"

	"github.com/nereidai/nereidedge/internal/connector"
	"github.com/nereidai/nereidedge/internal/inferencecred"
	"github.com/nereidai/nereidedge/internal/secrets"
)

// inferenceCredPageSize bounds one core-metadata page when protecting the
// secrets referenced by NI Job devices during replica pruning.
const inferenceCredPageSize = 100

// newInferenceCredentialManager wires the stream credential bridge between
// device-onvif-camera (source of the onvif-basic secret) and
// device-inference (where NI reads the ffmpeg credential by name).
func newInferenceCredentialManager(app *myApp, lc logger.LoggingClient) connector.InferenceCredentialManager {
	ic := app.svcConfig.NereidEdge.Inference
	sc := app.svcConfig.NereidEdge.Secrets
	mgr := inferencecred.New(inferencecred.Config{
		Metadata:            metadataDeviceReader{service: app.service},
		Devices:             metadataServiceDeviceLister{service: app.service},
		KeeperURL:           sc.KeeperURL,
		OnvifKVSBase:        sc.OnvifKVSBase,
		InferenceKVSBase:    ic.KVSBase,
		InferenceServiceURL: ic.ServiceURL,
		InferenceDSName:     ic.DeviceService,
		Logger:              lc,
	})
	return applierCredentialAdapter{mgr: mgr}
}

// applierCredentialAdapter converts connector-package binding DTOs to the
// inferencecred package types so neither package depends on the other.
type applierCredentialAdapter struct {
	mgr *inferencecred.Manager
}

func (a applierCredentialAdapter) ResolveSecretName(ctx context.Context, cameraEdgeXName string) (string, error) {
	return a.mgr.ResolveSecretName(ctx, cameraEdgeXName)
}

func (a applierCredentialAdapter) Ensure(ctx context.Context, names []string) error {
	return a.mgr.Ensure(ctx, names)
}

func (a applierCredentialAdapter) Reconcile(ctx context.Context, live []connector.InferenceSecretBinding) error {
	bindings := make([]inferencecred.Binding, 0, len(live))
	for _, b := range live {
		bindings = append(bindings, inferencecred.Binding{
			CameraEdgeXName: b.CameraEdgeXName,
			SecretName:      b.SecretName,
		})
	}
	return a.mgr.Reconcile(ctx, bindings)
}

// metadataServiceDeviceLister lists every EdgeX device registered against
// one device service via paged core-metadata calls.
type metadataServiceDeviceLister struct {
	service interfaces.ApplicationService
}

func (l metadataServiceDeviceLister) ListServiceDevices(ctx context.Context, serviceName string) ([]dtos.Device, error) {
	var out []dtos.Device
	for offset := 0; ; offset += inferenceCredPageSize {
		resp, err := l.service.DeviceClient().AllDevices(ctx, nil, offset, inferenceCredPageSize)
		if err != nil {
			return nil, err
		}
		for i := range resp.Devices {
			if resp.Devices[i].ServiceName == serviceName {
				out = append(out, resp.Devices[i])
			}
		}
		if len(resp.Devices) < inferenceCredPageSize {
			break
		}
	}
	return out, nil
}

// notifyingSecretBinder fans onvif secret delivery events into the inference
// credential reconciler. The inner binder's error is preserved; the
// notification fires only on success, so a failed delivery never prunes.
type notifyingSecretBinder struct {
	inner  secrets.Binder
	notify func()
}

func (n notifyingSecretBinder) Apply(ctx context.Context, entry device.SecretEntry) error {
	if err := n.inner.Apply(ctx, entry); err != nil {
		return err
	}
	n.notify()
	return nil
}

func (n notifyingSecretBinder) Remove(ctx context.Context, name string) error {
	if err := n.inner.Remove(ctx, name); err != nil {
		return err
	}
	n.notify()
	return nil
}
