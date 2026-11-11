// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package device

// MQTT topic contract for the device ingress. These identifiers are a
// frozen wire contract; they mirror the server-side mqttapi constants and
// must change only when the platform protocol changes.
const (
	// TopicTelemetry accepts a device's flat JSON telemetry frames.
	TopicTelemetry = "v1/me/telemetry"

	// TopicAttributes accepts client-scope attribute reports.
	TopicAttributes = "v1/me/attributes"

	// TopicEvents accepts single objects or arrays of domain events.
	TopicEvents = "v1/me/events"

	// TopicAttributesDown carries SERVER/SHARED attribute pushes.
	TopicAttributesDown = "v1/me/attributes/down"

	// TopicAttributesGet carries a device's on-demand pull of
	// platform-managed attributes after reconnect.
	TopicAttributesGet = "v1/me/attributes/get"

	// TopicAttributesGetAck carries the per-scope key/value reply.
	TopicAttributesGetAck = "v1/me/attributes/get/ack"

	// TopicShadowReport accepts device shadow reports.
	TopicShadowReport = "v1/me/shadow/report"

	// FilterCommands is the downlink filter a device subscribes to; the
	// concrete topic is "v1/me/cmd/<command-name>".
	FilterCommands = "v1/me/cmd/#"

	// PrefixCommands is the downlink command prefix.
	PrefixCommands = "v1/me/cmd/"

	// PrefixCommandResponse is the uplink prefix for a request-response
	// reply; the suffix is the command ID. It deliberately sits outside
	// FilterCommands so a device never receives its own responses.
	PrefixCommandResponse = "v1/me/cmdres/"

	// TopicShadowDesired carries complete desired-state pushes.
	TopicShadowDesired = "v1/me/shadow/desired"

	// Gateway topics (gateway capability, hello negotiation required).

	// TopicGatewayHello is the permanent negotiation bootstrap topic.
	TopicGatewayHello = "v1/gateway/hello"

	// TopicGatewayHelloAck is the default ack topic.
	TopicGatewayHelloAck = "v1/gateway/hello/ack"

	// TopicGatewayHeartbeat carries application-level liveness pings.
	TopicGatewayHeartbeat = "v1/gateway/heartbeat"

	// TopicGatewayGoodbye carries graceful shutdown and is also the LWT.
	TopicGatewayGoodbye = "v1/gateway/goodbye"

	// TopicGatewayLinkHealth accepts edge-local link watchdog episode
	// reports (unplanned down/up intervals); QoS1, published only after a
	// negotiated hello.
	TopicGatewayLinkHealth = "v1/gateway/linkhealth"

	// TopicGatewayLinkHealthAck carries the per-episode accept/duplicate/
	// reject response unless the report names a response_topic.
	TopicGatewayLinkHealthAck = "v1/gateway/linkhealth/ack"

	// TopicGatewayConnect announces attached sub-devices coming online.
	TopicGatewayConnect = "v1/gateway/connect"

	// TopicGatewayDisconnect announces attached sub-devices going offline.
	TopicGatewayDisconnect = "v1/gateway/disconnect"

	// TopicGatewayManifest carries the periodic full sub-device list.
	TopicGatewayManifest = "v1/gateway/manifest"

	// TopicGatewayProfiles accepts edge-reported EdgeX device profiles
	// (name + deviceResources) that NL caches and can adopt as native
	// profiles. Fire-and-forget, QoS1, replayed in full after hello. This
	// uplink topic is unrelated to the ADR-0038 downlink pair below.
	TopicGatewayProfiles = "v1/gateway/profiles"

	// TopicGatewayProfilesSpec carries the full desired library-profile set
	// for one gateway (ADR-0038). The edge subscribes; frames are full-state,
	// idempotent and re-sent after every (re)hello. Each entry names the
	// library profile, revision, render format and the rendered document.
	TopicGatewayProfilesSpec = "v1/gateway/profiles/spec"

	// TopicGatewayProfilesStatus accepts edge-reported profile delivery
	// states (APPLIED/FAILED/REJECTED/REMOVED) keyed by profile name.
	// Fire-and-forget, QoS1; there is no ack channel.
	TopicGatewayProfilesStatus = "v1/gateway/profiles/status"

	// TopicGatewayTelemetry accepts batched sub-device telemetry.
	TopicGatewayTelemetry = "v1/gateway/telemetry"

	// TopicGatewayAttributes accepts sub-device CLIENT-scope attribute
	// reports: {device: {key: value}}.
	TopicGatewayAttributes = "v1/gateway/attributes"

	// TopicGatewayAttributesPush carries SERVER/SHARED attribute pushes
	// to one sub-device via the gateway.
	TopicGatewayAttributesPush = "v1/gateway/attributes/push"

	// TopicGatewayAttributesGet carries the gateway's on-demand fetch of
	// one sub-device's platform-managed attributes.
	TopicGatewayAttributesGet = "v1/gateway/attributes/get"

	// TopicGatewayAttributesGetAck carries the per-scope reply, naming
	// the sub-device.
	TopicGatewayAttributesGetAck = "v1/gateway/attributes/get/ack"

	// TopicGatewayCommand carries commands addressed to a sub-device and
	// relayed through its owning gateway.
	TopicGatewayCommand = "v1/gateway/command"

	// TopicGatewayCommandResponse accepts the gateway's sub-device
	// command results.
	TopicGatewayCommandResponse = "v1/gateway/command/response"

	// TopicGatewayShadowReport accepts batched sub-device reported-state
	// patches published by a gateway: {reports:[{device,state,version?}]}.
	TopicGatewayShadowReport = "v1/gateway/shadow/report"

	// TopicGatewayShadowDesired carries batched full desired-state pushes
	// to sub-devices through a gateway: {desired:[{device,version,desired}]}.
	// The gateway subscribes once and demuxes each entry by device.
	TopicGatewayShadowDesired = "v1/gateway/shadow/desired"

	// TopicGatewayDiscovery accepts local-device scan candidates.
	TopicGatewayDiscovery = "v1/gateway/discovery"

	// TopicGatewayDiscoveryResult carries the local_id -> UUID mapping.
	TopicGatewayDiscoveryResult = "v1/gateway/discovery/result"

	// TopicGatewayProbe carries aggregated on-demand probe requests.
	TopicGatewayProbe = "v1/gateway/probe"

	// TopicGatewayProbeResponse accepts per-device probe results.
	TopicGatewayProbeResponse = "v1/gateway/probe/response"

	// TopicGatewayOTATask carries firmware task descriptors.
	TopicGatewayOTATask = "v1/gateway/ota/task"

	// TopicGatewayOTAProgress accepts per-device firmware progress.
	TopicGatewayOTAProgress = "v1/gateway/ota/progress"

	// TopicGatewayConnectorStatus accepts connector instance states.
	TopicGatewayConnectorStatus = "v1/gateway/connectors/status"

	// FilterGatewayConnectors is the single filter for spec and delete.
	FilterGatewayConnectors = "v1/gateway/connectors/#"

	// TopicGatewayConnectorSpec carries desired connector specs.
	TopicGatewayConnectorSpec = "v1/gateway/connectors/spec"

	// TopicGatewayConnectorDelete carries desired connector removals.
	TopicGatewayConnectorDelete = "v1/gateway/connectors/delete"

	// TopicGatewaySecretsSync carries the full desired secret delivery set.
	TopicGatewaySecretsSync = "v1/gateway/secrets/sync"

	// TopicGatewaySecretsStatus accepts edge secret binding states.
	TopicGatewaySecretsStatus = "v1/gateway/secrets/status"

	// FilterGatewaySecrets is the single downlink filter for secret pushes.
	FilterGatewaySecrets = "v1/gateway/secrets/#"

	// TopicGatewayOperationsInvoke carries whole-gateway operation requests.
	TopicGatewayOperationsInvoke = "v1/gateway/operations/invoke"

	// TopicGatewayOperationsResult accepts edge-reported operation results.
	TopicGatewayOperationsResult = "v1/gateway/operations/result"

	// FilterGatewayOperations is the single downlink filter for invokes.
	FilterGatewayOperations = "v1/gateway/operations/#"
)

// CommandResponseTopic builds the response topic for commandID.
func CommandResponseTopic(commandID string) string {
	return PrefixCommandResponse + commandID
}
