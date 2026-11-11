// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"encoding/json"

	"github.com/nereidai/nereidlink/sdk/device/go"
)

// SubscribeConnectors registers one handler for both desired spec pushes
// and removals (the server writes to topics under one filter:
// v1/gateway/connectors/#). De-duplicate application by (SpecID, Revision);
// the server re-sends the full desired set and every unacknowledged explicit
// delete after every (re)hello. Transport delivery is not a delete ACK.
func (c *Client) SubscribeConnectors(ctx context.Context, qos byte,
	onPush func(device.ConnectorPush)) error {
	return c.mqtt.SubscribeDecoded(ctx, device.FilterGatewayConnectors, qos,
		func(_ string, raw []byte) error {
			var push device.ConnectorPush
			if err := json.Unmarshal(raw, &push); err != nil {
				return err
			}
			onPush(push)
			return nil
		})
}

// ReportConnectorStatus publishes the edge-reported lifecycle state for one
// desired spec on v1/gateway/connectors/status.
func (c *Client) ReportConnectorStatus(ctx context.Context, status device.ConnectorStatus, qos byte) error {
	return c.publish(ctx, device.TopicGatewayConnectorStatus, status, qos)
}

// SubscribeSecrets registers the handler for the full desired secret set
// on v1/gateway/secrets/sync (ADR-0034). De-duplicate each entry by
// revision; the server re-sends the full set after every (re)hello.
func (c *Client) SubscribeSecrets(ctx context.Context, qos byte,
	onSync func(device.SecretSync)) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewaySecretsSync, qos,
		func(_ string, raw []byte) error {
			var sync device.SecretSync
			if err := json.Unmarshal(raw, &sync); err != nil {
				return err
			}
			onSync(sync)
			return nil
		})
}

// SubscribeLinkEpisodeAcks registers the handler for link-episode responses
// on v1/gateway/linkhealth/ack. Register before ReportLinkEpisode; a
// "duplicate" or "rejected" status means the corresponding report must not
// be retried.
func (c *Client) SubscribeLinkEpisodeAcks(ctx context.Context, qos byte,
	onAck func(device.LinkEpisodeAck)) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayLinkHealthAck, qos,
		func(_ string, raw []byte) error {
			var ack device.LinkEpisodeAck
			if err := json.Unmarshal(raw, &ack); err != nil {
				return err
			}
			onAck(ack)
			return nil
		})
}

// ReportSecretStatus publishes one or more edge-reported binding states on
// v1/gateway/secrets/status.
func (c *Client) ReportSecretStatus(ctx context.Context, frame device.SecretStatusFrame, qos byte) error {
	return c.publish(ctx, device.TopicGatewaySecretsStatus, frame, qos)
}

// SubscribeProfiles registers the handler for the full desired library-profile
// set on v1/gateway/profiles/spec (ADR-0038). The platform only publishes
// after negotiating the "profiles" feature and re-sends the full set after
// every (re)hello. De-duplicate each entry by (name, revision); never delete a
// local profile merely because it is missing from the frame, only explicit
// removing entries delete one.
func (c *Client) SubscribeProfiles(ctx context.Context, qos byte,
	onSpec func(device.ProfileSpecFrame)) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayProfilesSpec, qos,
		func(_ string, raw []byte) error {
			var spec device.ProfileSpecFrame
			if err := json.Unmarshal(raw, &spec); err != nil {
				return err
			}
			onSpec(spec)
			return nil
		})
}

// ReportProfileStatus publishes one or more edge-reported profile delivery
// states on v1/gateway/profiles/status. The topic is fire-and-forget: the
// platform never answers, so do not wait for an acknowledgement. QoS1 is the
// recommended qos.
func (c *Client) ReportProfileStatus(ctx context.Context, frame device.ProfileStatusFrame, qos byte) error {
	return c.publish(ctx, device.TopicGatewayProfilesStatus, frame, qos)
}

// SubscribeOperations registers the handler for whole-gateway operation
// requests on v1/gateway/operations/invoke (ADR-0035). Answer with
// RespondOperation within the frame's timeout; execute only operations the
// gateway advertised.
func (c *Client) SubscribeOperations(ctx context.Context, qos byte,
	onInvoke func(device.OperationInvoke)) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayOperationsInvoke, qos,
		func(_ string, raw []byte) error {
			var invoke device.OperationInvoke
			if err := json.Unmarshal(raw, &invoke); err != nil {
				return err
			}
			onInvoke(invoke)
			return nil
		})
}

// RespondOperation publishes the result of a gateway operation on
// v1/gateway/operations/result. By protocol agreement the result must not
// contain passwords: redact them before calling (the platform does not
// inspect the content).
func (c *Client) RespondOperation(ctx context.Context, frame device.OperationResult, qos byte) error {
	return c.publish(ctx, device.TopicGatewayOperationsResult, frame, qos)
}

// SubscribeDiscoveryResults registers the handler for the local_id -> UUID
// mapping returned after ReportDiscovery; under approval-mode review a
// "pending" result may later be followed by a final result on the same
// topic.
func (c *Client) SubscribeDiscoveryResults(ctx context.Context, qos byte,
	onResult func(device.DiscoveryResult)) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayDiscoveryResult, qos,
		func(_ string, raw []byte) error {
			var result device.DiscoveryResult
			if err := json.Unmarshal(raw, &result); err != nil {
				return err
			}
			onResult(result)
			return nil
		})
}

// ReportDiscovery publishes local-device scan candidates on
// v1/gateway/discovery. Tenant and gateway are derived server-side from
// the authenticated, negotiated connection, never from the frame.
func (c *Client) ReportDiscovery(ctx context.Context, frame device.DiscoveryFrame, qos byte) error {
	return c.publish(ctx, device.TopicGatewayDiscovery, frame, qos)
}

// SubscribeProbes registers the aggregated on-demand probe request
// handler. The gateway answers per device with ProbeResult/ProbeError.
func (c *Client) SubscribeProbes(ctx context.Context, qos byte,
	onRequest func(device.ProbeRequest)) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayProbe, qos,
		func(_ string, raw []byte) error {
			var req device.ProbeRequest
			if err := json.Unmarshal(raw, &req); err != nil {
				return err
			}
			onRequest(req)
			return nil
		})
}

// ProbeResult reports one successful per-device probe. Result is opaque
// JSON passed through to the requesting application.
func (c *Client) ProbeResult(ctx context.Context, probeID, deviceID string, result any, qos byte) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	frame := device.ProbeResponse{ProbeID: probeID, Device: deviceID, Result: raw}
	return c.publish(ctx, device.TopicGatewayProbeResponse, frame, qos)
}

// ProbeError reports one failed per-device probe.
func (c *Client) ProbeError(ctx context.Context, probeID, deviceID, reason string, qos byte) error {
	frame := device.ProbeResponse{ProbeID: probeID, Device: deviceID, Error: reason}
	return c.publish(ctx, device.TopicGatewayProbeResponse, frame, qos)
}

// SubscribeOTATasks registers the firmware task descriptor handler. The
// gateway downloads the external URL itself (with Headers when present),
// must verify SHA256 before applying, and reports per-device progress.
func (c *Client) SubscribeOTATasks(ctx context.Context, qos byte,
	onTask func(device.GatewayOTATask)) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayOTATask, qos,
		func(_ string, raw []byte) error {
			var task device.GatewayOTATask
			if err := json.Unmarshal(raw, &task); err != nil {
				return err
			}
			onTask(task)
			return nil
		})
}

// ReportOTAProgress publishes one per-device firmware progress frame on
// v1/gateway/ota/progress. Progress states are forward-only and
// progress_pct must not move backwards.
func (c *Client) ReportOTAProgress(ctx context.Context, progress device.GatewayOTAProgress, qos byte) error {
	return c.publish(ctx, device.TopicGatewayOTAProgress, progress, qos)
}

// PublishAttributes reports CLIENT-scope attributes for attached
// sub-devices on v1/gateway/attributes: {device: {key: value}}. Entries
// for devices not attached to this connection are skipped server-side
// without failing the batch.
func (c *Client) PublishAttributes(ctx context.Context, update device.GatewayAttributeUpdate, qos byte) error {
	return c.publish(ctx, device.TopicGatewayAttributes, update, qos)
}

// GatewayCommandHandler processes one command addressed to an attached
// sub-device. Answer one-way commands with no call; answer
// request-response commands with RespondCommand.
type GatewayCommandHandler func(device.GatewayCommand)

// SubscribeCommands registers the handler for commands relayed to
// attached sub-devices on v1/gateway/command. One subscription serves
// every sub-device; GatewayCommand.Device selects the target.
func (c *Client) SubscribeCommands(ctx context.Context, qos byte,
	onCommand GatewayCommandHandler) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayCommand, qos,
		func(_ string, raw []byte) error {
			var cmd device.GatewayCommand
			if err := json.Unmarshal(raw, &cmd); err != nil {
				return err
			}
			onCommand(cmd)
			return nil
		})
}

// RespondCommand publishes the arbitrary JSON result of a sub-device
// request-response command on v1/gateway/command/response. The frame is
// accepted only for a device attached to this negotiated connection.
func (c *Client) RespondCommand(ctx context.Context, commandID, subDeviceID string,
	result any, qos byte) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	frame := device.GatewayCommandResponse{
		CommandID: commandID,
		Device:    subDeviceID,
		Data:      raw,
	}
	return c.publish(ctx, device.TopicGatewayCommandResponse, frame, qos)
}

// GatewayAttributePushHandler processes one SERVER/SHARED attribute
// change pushed to an attached sub-device.
type GatewayAttributePushHandler func(device.GatewayAttributesPush)

// SubscribeAttributePushes registers the handler for
// v1/gateway/attributes/push.
func (c *Client) SubscribeAttributePushes(ctx context.Context, qos byte,
	onPush GatewayAttributePushHandler) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayAttributesPush, qos,
		func(_ string, raw []byte) error {
			var push device.GatewayAttributesPush
			if err := json.Unmarshal(raw, &push); err != nil {
				return err
			}
			onPush(push)
			return nil
		})
}

// GatewayAttributeAckHandler processes the on-demand fetch reply on
// v1/gateway/attributes/get/ack.
type GatewayAttributeAckHandler func(device.GatewayAttributesGetAck)

// SubscribeAttributeAcks registers the handler for
// v1/gateway/attributes/get/ack. Subscribe before RequestAttributes.
func (c *Client) SubscribeAttributeAcks(ctx context.Context, qos byte,
	onAck GatewayAttributeAckHandler) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayAttributesGetAck, qos,
		func(_ string, raw []byte) error {
			var ack device.GatewayAttributesGetAck
			if err := json.Unmarshal(raw, &ack); err != nil {
				return err
			}
			onAck(ack)
			return nil
		})
}

// RequestAttributes fetches one attached sub-device's platform-managed
// attributes on v1/gateway/attributes/get. Empty scopes means both
// SERVER_SCOPE and SHARED_SCOPE. Call it after each reconnect for a
// sub-device to recover pushes missed while the gateway was offline.
func (c *Client) RequestAttributes(ctx context.Context, subDeviceID string, scopes []string, qos byte) error {
	return c.publish(ctx, device.TopicGatewayAttributesGet,
		device.GatewayAttributesGetRequest{Device: subDeviceID, Scopes: scopes}, qos)
}

// ReportSubdeviceShadow publishes one or more sub-device reported-state
// patches on v1/gateway/shadow/report. A single sub-device is a length-one
// batch. Entries for devices not attached to this connection are skipped
// server-side without failing the rest.
func (c *Client) ReportSubdeviceShadow(ctx context.Context, reports []device.GatewayShadowReport, qos byte) error {
	return c.publish(ctx, device.TopicGatewayShadowReport,
		device.GatewayShadowReportFrame{Reports: reports}, qos)
}

// GatewayShadowDesiredHandler processes a batch of full desired-state
// pushes for attached sub-devices; demux each entry by its Device field.
type GatewayShadowDesiredHandler func(device.GatewayShadowDesiredFrame)

// SubscribeSubdeviceShadowDesired registers the handler for desired-state
// pushes on v1/gateway/shadow/desired. One subscription serves every
// sub-device; each frame entry names its target and carries complete state.
func (c *Client) SubscribeSubdeviceShadowDesired(ctx context.Context, qos byte,
	onDesired GatewayShadowDesiredHandler) error {
	return c.mqtt.SubscribeDecoded(ctx, device.TopicGatewayShadowDesired, qos,
		func(_ string, raw []byte) error {
			var frame device.GatewayShadowDesiredFrame
			if err := json.Unmarshal(raw, &frame); err != nil {
				return err
			}
			onDesired(frame)
			return nil
		})
}
