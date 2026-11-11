// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

// Package gatewayoperation answers NereidLink whole-gateway operations
// (ADR-0035). NereidLink has no built-in operation vocabulary: the firmware
// advertises the operations it supports in hello and, after negotiation, NL
// invokes them on v1/gateway/operations/invoke. The handler executes only the
// advertised descriptors and replies on v1/gateway/operations/result within
// each frame's deadline.
//
// By protocol agreement a result must never carry passwords or key material;
// NereidLink does not inspect result content, so redaction is the edge's job.
package gatewayoperation

import (
	"encoding/json"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// Operation keys understood by this firmware.
const (
	// OperationDiagPing sends ICMP echo probes to a host.
	OperationDiagPing = "diag.ping"
	// OperationDiagDNSResolve resolves a host name with the local resolver.
	OperationDiagDNSResolve = "diag.dns_resolve"
	// OperationDiagTCPConnect probes TCP reachability without sending data.
	OperationDiagTCPConnect = "diag.tcp_connect"
	// OperationDiagTLSInspect completes a TLS handshake and reports the leaf
	// certificate.
	OperationDiagTLSInspect = "diag.tls_inspect"
	// OperationSysInfo reports read-only host identity facts.
	OperationSysInfo = "sys.info"
	// OperationSysResources samples CPU, load, memory, swap and mounts.
	OperationSysResources = "sys.resources"
	// OperationSysNetwork enumerates interfaces, route and resolver state.
	OperationSysNetwork = "sys.network"
	// OperationSysReboot restarts the gateway (dangerous, one-way).
	OperationSysReboot = "sys.reboot"
	// OperationSysProcessesTop lists the top processes by CPU or memory.
	OperationSysProcessesTop = "sys.processes_top"
	// OperationSysNTPStatus reports clock synchronisation state.
	OperationSysNTPStatus = "sys.ntp_status"
	// OperationSysNTPSync triggers an immediate clock step.
	OperationSysNTPSync = "sys.ntp_sync"
	// OperationDiagHTTPCheck issues a GET and reports the final status.
	OperationDiagHTTPCheck = "diag.http_check"
	// OperationSysLogLevel reports the agent's effective log level. The level
	// itself is set through the gateway's log.level SHARED_SCOPE attribute, so
	// this operation is read-only.
	OperationSysLogLevel = "sys.log_level"
	// OperationSysAgentRestart restarts the agent process (dangerous, one-way).
	OperationSysAgentRestart = "sys.agent_restart"
	// OperationSysLogsTail tails the agent log with secret redaction.
	OperationSysLogsTail = "sys.logs_tail"
	// OperationSysCertInfo reports the northbound identity certificate.
	OperationSysCertInfo = "sys.cert_info"
	// OperationServiceRestart restarts an allowlisted managed service.
	OperationServiceRestart = "service.restart"
	// OperationNetworkReconnect reconnects the uplink (dangerous, one-way).
	OperationNetworkReconnect = "network.reconnect"
	// OperationDiagUplinkStatus reports the active uplink state.
	OperationDiagUplinkStatus = "diag.uplink_status"
	// OperationInferenceDiag distinguishes runtime/model/source failures for
	// an edge-inference binding without adding a new operations frame.
	OperationInferenceDiag = "inference.diag"
	// OperationCameraProfiles enumerates a camera's ONVIF media profiles
	// (main/sub/third streams) for stream-mode binding profile selection.
	OperationCameraProfiles = "camera.profiles"
)

// schemaPreamble is the shared draft 2020-12 header; schemas are inline and
// never use external $ref.
const schemaPreamble = `"$schema": "https://json-schema.org/draft/2020-12/schema",`

// Adverts is the firmware operation catalog advertised in hello. Every schema
// is self-contained and inline (no external $ref), and timeout_ms stays within
// the protocol range 100..60000. service.restart is offered only when a
// managed-service allowlist is configured.
func Adverts(opts Options) []device.OperationAdvert {
	adverts := []device.OperationAdvert{
		{
			Operation:   OperationDiagPing,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Ping host", "zh": "Ping 探测"},
			Description: map[string]string{
				"en": "Send ICMP echo probes and report RTT/loss.",
				"zh": "发送 ICMP 探测并返回时延与丢包率。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   10000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["host"],
				"properties": {
					"host":  {"type": "string", "description": "target host or IP"},
					"count": {"type": "integer", "description": "probe count"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"ok":     {"type": "boolean"},
					"host":   {"type": "string"},
					"rtt_ms": {"type": "array", "items": {"type": "number"}},
					"loss":   {"type": "number"}
				}
			}`),
		},
		{
			Operation:   OperationDiagDNSResolve,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Resolve name", "zh": "域名解析"},
			Description: map[string]string{
				"en": "Resolve a host name with the local resolver (A/AAAA).",
				"zh": "用本机 resolver 解析主机名（A/AAAA）。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   5000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["name"],
				"properties": {
					"name":        {"type": "string", "description": "host name to resolve"},
					"record_type": {"type": "string", "enum": ["A", "AAAA"], "description": "record type, default A"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"answers":  {"type": "array", "items": {"type": "object", "properties": {"address": {"type": "string"}, "type": {"type": "string"}}}},
					"resolver": {"type": "string"},
					"rtt_ms":   {"type": "number"}
				}
			}`),
		},
		{
			Operation:   OperationDiagTCPConnect,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "TCP connect", "zh": "TCP 连通性"},
			Description: map[string]string{
				"en": "Open a TCP connection then close it; no application data is sent.",
				"zh": "建立 TCP 连接后立即关闭，不发送任何应用数据。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   5000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["host", "port"],
				"properties": {
					"host":       {"type": "string"},
					"port":       {"type": "integer", "minimum": 1, "maximum": 65535},
					"timeout_ms": {"type": "integer", "minimum": 100, "maximum": 5000, "description": "dial timeout, default 2000"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"reachable": {"type": "boolean"},
					"rtt_ms":    {"type": "number"},
					"error":     {"type": "string"}
				}
			}`),
		},
		{
			Operation:   OperationDiagTLSInspect,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Inspect TLS", "zh": "TLS 证书检查"},
			Description: map[string]string{
				"en": "Complete a TLS handshake and report the leaf certificate; no HTTP request is sent.",
				"zh": "完成 TLS 握手并返回叶证书信息；不发送 HTTP 请求。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   8000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["host"],
				"properties": {
					"host":        {"type": "string"},
					"port":        {"type": "integer", "minimum": 1, "maximum": 65535, "description": "default 443"},
					"server_name": {"type": "string", "description": "SNI / verification name, default host"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"version":   {"type": "string"},
					"cipher":    {"type": "string"},
					"not_after": {"type": "string"},
					"chain_ok":  {"type": "boolean"},
					"error":     {"type": "string"}
				}
			}`),
		},
		{
			Operation:   OperationSysInfo,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "System info", "zh": "系统信息"},
			Description: map[string]string{
				"en": "Read-only host identity and uptime.",
				"zh": "只读的系统标识与运行时长。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"hostname":      {"type": "string"},
					"os":            {"type": "string"},
					"kernel":        {"type": "string"},
					"arch":          {"type": "string"},
					"agent_version": {"type": "string"},
					"proto":         {"type": "string"},
					"uptime_s":      {"type": "number"},
					"time":          {"type": "string"},
					"timezone":      {"type": "string"}
				}
			}`),
		},
		{
			Operation:   OperationSysResources,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "System resources", "zh": "系统资源"},
			Description: map[string]string{
				"en": "Sample CPU, load, memory, swap and mount usage.",
				"zh": "采样 CPU、负载、内存、交换分区与挂载点用量。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"sample_ms": {"type": "integer", "minimum": 100, "maximum": 3000, "description": "CPU sample window, default 1000"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"cpu_percent": {"type": "number"},
					"load":        {"type": "object", "properties": {"1": {"type": "number"}, "5": {"type": "number"}, "15": {"type": "number"}}},
					"mem":         {"type": "object", "properties": {"total": {"type": "number"}, "used": {"type": "number"}, "available": {"type": "number"}}},
					"swap":        {"type": "object", "properties": {"total": {"type": "number"}, "used": {"type": "number"}}},
					"mounts":      {"type": "array", "items": {"type": "object", "properties": {"path": {"type": "string"}, "total": {"type": "number"}, "used": {"type": "number"}, "available": {"type": "number"}}}}
				}
			}`),
		},
		{
			Operation:   OperationSysNetwork,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "System network", "zh": "系统网络"},
			Description: map[string]string{
				"en": "Enumerate interfaces, default route and resolver; read-only.",
				"zh": "枚举网卡、默认路由与 DNS；只读。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"interfaces":    {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "mac": {"type": "string"}, "ipv4": {"type": "array", "items": {"type": "string"}}, "ipv6": {"type": "array", "items": {"type": "string"}}, "up": {"type": "boolean"}}}},
					"default_route": {"type": "string"},
					"dns":           {"type": "array", "items": {"type": "string"}},
					"ntp_synced":    {"type": "boolean"}
				}
			}`),
		},
		{
			Operation:   OperationSysReboot,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Reboot gateway", "zh": "重启网关"},
			Description: map[string]string{
				"en": "Restart the gateway. Dangerous; no result is returned.",
				"zh": "重启网关。危险操作；不回报结果。",
			},
			Interaction: device.OperationInteractionOneWay,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskDangerous,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
		},
		{
			Operation:   OperationSysProcessesTop,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Top processes", "zh": "进程排行"},
			Description: map[string]string{
				"en": "List the top processes by CPU or memory usage; read-only.",
				"zh": "按 CPU 或内存占用列出前 N 个进程；只读。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"limit": {"type": "integer", "minimum": 1, "maximum": 20, "description": "default 10"},
					"sort":  {"type": "string", "enum": ["cpu", "mem"], "description": "default cpu"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"processes": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "pid": {"type": "integer"}, "cpu_percent": {"type": "number"}, "mem_percent": {"type": "number"}}}}
				}
			}`),
		},
		{
			Operation:   OperationSysNTPStatus,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "NTP status", "zh": "NTP 状态"},
			Description: map[string]string{
				"en": "Report NTP synchronisation state and current offset; never changes the clock.",
				"zh": "返回 NTP 同步状态与当前偏差；不修改时钟。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"synced":    {"type": "boolean"},
					"offset_ms": {"type": "number"},
					"server":    {"type": "string"}
				}
			}`),
		},
		{
			Operation:   OperationSysNTPSync,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "NTP sync now", "zh": "立即对时"},
			Description: map[string]string{
				"en": "Trigger one immediate NTP step and report before/after offset.",
				"zh": "触发一次立即对时并返回前后偏差。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   10000,
			Risk:        device.OperationRiskNormal,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"server": {"type": "string", "description": "optional NTP server"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"before_offset_ms": {"type": "number"},
					"after_offset_ms":  {"type": "number"},
					"synced":           {"type": "boolean"}
				}
			}`),
		},
		{
			Operation:   OperationDiagHTTPCheck,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "HTTP check", "zh": "HTTP 检查"},
			Description: map[string]string{
				"en": "Issue a GET (http/https only), follow at most 5 redirects, and report the final status.",
				"zh": "发送 GET（仅 http/https），最多跟随 5 次重定向，返回最终状态。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   10000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["url"],
				"properties": {
					"url":        {"type": "string"},
					"timeout_ms": {"type": "integer", "minimum": 100, "maximum": 10000, "description": "default 5000"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"status":      {"type": "integer"},
					"ok":          {"type": "boolean"},
					"redirects":   {"type": "array", "items": {"type": "string"}},
					"tls_version": {"type": "string"},
					"rtt_ms":      {"type": "number"},
					"error":       {"type": "string"}
				}
			}`),
		},
		{
			Operation:   OperationSysLogLevel,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Get log level", "zh": "查看日志级别"},
			Description: map[string]string{
				"en": "Report the agent's effective log level; read-only. Set it via the log.level attribute.",
				"zh": "返回代理当前日志级别；只读。通过 log.level 属性修改。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"level": {"type": "string", "enum": ["debug", "info", "warn", "error"]}
				}
			}`),
		},
		{
			Operation:   OperationSysAgentRestart,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Restart agent", "zh": "重启代理"},
			Description: map[string]string{
				"en": "Restart only the agent process. Dangerous; no result is returned.",
				"zh": "仅重启代理进程。危险操作；不回报结果。",
			},
			Interaction: device.OperationInteractionOneWay,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskDangerous,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"delay_s": {"type": "integer", "minimum": 0, "maximum": 60, "description": "default 1"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
		},
		{
			Operation:   OperationSysLogsTail,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Tail logs", "zh": "查看日志"},
			Description: map[string]string{
				"en": "Tail the agent log with secrets redacted; read-only.",
				"zh": "读取代理日志尾部并脱敏；只读。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   5000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"lines":   {"type": "integer", "minimum": 1, "maximum": 200, "description": "default 100"},
					"service": {"type": "string", "description": "default the agent itself"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"entries": {"type": "array", "items": {"type": "string"}}
				}
			}`),
		},
		{
			Operation:   OperationSysCertInfo,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Identity certificate", "zh": "身份证书"},
			Description: map[string]string{
				"en": "Report the northbound mTLS identity certificate metadata; never the key.",
				"zh": "返回北向 mTLS 身份证书元信息；不返回私钥。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"not_after":       {"type": "string"},
					"not_before":      {"type": "string"},
					"issuer":          {"type": "string"},
					"subject":         {"type": "string"},
					"days_remaining": {"type": "integer"}
				}
			}`),
		},
		{
			Operation:   OperationNetworkReconnect,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Reconnect uplink", "zh": "重连上行"},
			Description: map[string]string{
				"en": "Reconnect the uplink, briefly interrupting traffic. Dangerous; no result is returned.",
				"zh": "重连上行链路，会短暂中断业务。危险操作；不回报结果。",
			},
			Interaction: device.OperationInteractionOneWay,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskDangerous,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"uplink":  {"type": "string", "enum": ["ethernet", "wifi", "cellular"], "description": "default current uplink"},
					"delay_s": {"type": "integer", "minimum": 0, "maximum": 30, "description": "default 1"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
		},
		{
			Operation:   OperationDiagUplinkStatus,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Uplink status", "zh": "上行状态"},
			Description: map[string]string{
				"en": "Report the active uplink kind, connectivity and counters; read-only.",
				"zh": "返回当前上行类型、连通状态与累计流量；只读。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   3000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object"
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"kind":      {"type": "string", "enum": ["ethernet", "wifi", "cellular", "other"]},
					"connected": {"type": "boolean"},
					"rssi_dbm":  {"type": "integer"},
					"rx_bytes":  {"type": "number"},
					"tx_bytes":  {"type": "number"},
					"ssid":      {"type": "string"}
				}
			}`),
		},
	}
	if opts.InferenceDiag != nil {
		adverts = append(adverts, device.OperationAdvert{
			Operation:   OperationInferenceDiag,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Inference diagnostics", "zh": "推理诊断"},
			Description: map[string]string{
				"en": "Check runtime reachability, model readiness and one bound inference path.",
				"zh": "检查推理运行时可达性、模型就绪状态和一条绑定推理路径。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   30000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["model_ref"],
				"additionalProperties": false,
				"properties": {
					"model_ref": {
						"type": "object",
						"required": ["id", "version"],
						"properties": {"id": {"type": "string"}, "version": {"type": "string"}}
					},
					"source": {"type": "string"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["reachable", "model_ready", "infer_ok"],
				"properties": {
					"reachable": {"type": "boolean"},
					"model_ready": {"type": "boolean"},
					"infer_ok": {"type": "boolean"},
					"latency_ms": {"type": "number"},
					"frame_source": {"type": "string"},
					"reason": {"type": "string"}
				}
			}`),
		})
	}

	if opts.CameraProfiles != nil {
		adverts = append(adverts, device.OperationAdvert{
			Operation:   OperationCameraProfiles,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Camera stream profiles", "zh": "摄像头码流配置"},
			Description: map[string]string{
				"en": "List the ONVIF media profiles (main/sub/third stream tokens, resolution and encoding) of one managed camera.",
				"zh": "列出一台受管摄像头的 ONVIF 媒体配置（主/子/第三码流的 token、分辨率与编码）。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   15000,
			Risk:        device.OperationRiskRead,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["source"],
				"additionalProperties": false,
				"properties": {
					"source": {"type": "string", "description": "Managed camera sub-device UUID"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["profiles"],
				"properties": {
					"profiles": {
						"type": "array",
						"items": {
							"type": "object",
							"required": ["token"],
							"properties": {
								"token": {"type": "string"},
								"name": {"type": "string"},
								"encoding": {"type": "string"},
								"width": {"type": "integer"},
								"height": {"type": "integer"},
								"fps": {"type": "number"}
							}
						}
					}
				}
			}`),
		})
	}

	// service.restart is only offered when a managed-service allowlist is
	// configured; without one there is nothing safe to restart.
	if len(opts.Services) > 0 {
		return append(adverts, device.OperationAdvert{
			Operation:   OperationServiceRestart,
			TypeVersion: "1",
			DisplayName: map[string]string{"en": "Restart service", "zh": "重启服务"},
			Description: map[string]string{
				"en": "Restart one allowlisted managed service. Dangerous; RBAC and step-up required.",
				"zh": "重启白名单内的受管服务。危险操作，需要 RBAC 与二次验证。",
			},
			Interaction: device.OperationInteractionRequestResponse,
			TimeoutMs:   15000,
			Risk:        device.OperationRiskDangerous,
			ParamSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"required": ["name"],
				"properties": {
					"name": {"type": "string", "description": "allowlisted service name"}
				}
			}`),
			ResultSchema: json.RawMessage(`{
				` + schemaPreamble + `
				"type": "object",
				"properties": {
					"ok": {"type": "boolean"}
				}
			}`),
		})
	}
	return adverts
}
