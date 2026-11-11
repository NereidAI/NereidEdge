// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gatewayoperation

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

// agentServiceName is the agent's own service name; sys.logs_tail accepts only
// this value (or none) so it can never be pointed at another service's files.
const agentServiceName = "nereid-edge"

// sysLogLevelResult is the sys.log_level result document.
type sysLogLevelResult struct {
	Level string `json:"level"`
}

// runLogLevelGet reports the agent's effective log level. The level is set
// through the gateway's log.level SHARED_SCOPE attribute, so this operation
// only reads it back.
func (h *Handler) runLogLevelGet(_ device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	level := strings.ToLower(strings.TrimSpace(h.lc.LogLevel()))
	if level == "" {
		level = "info"
	}
	return marshalResult(sysLogLevelResult{Level: level})
}

// runAgentRestart schedules a supervised restart of the agent process alone. It
// is one-way: NL does not wait for a result and confirms via re-hello/link
// health, since the process dies before it could reply.
func (h *Handler) runAgentRestart(inv device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	if h.opts.Restart == nil {
		return nil, errors.New("agent restart is not available")
	}
	delay, err := optionalInt(inv.Params, "delay_s", 0, 60, 1)
	if err != nil {
		return nil, err
	}
	go func() {
		time.Sleep(time.Duration(delay) * time.Second)
		h.opts.Restart()
	}()
	return nil, nil
}

var (
	privateKeyRE = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----`)
	secretRefRE  = regexp.MustCompile(`secret://[^\s"',]+`)
	kvSecretRE   = regexp.MustCompile(`(?i)((?:password|passwd|token|secret|api[-_]?key|access[-_]?key|authorization)["']?\s*[:=]\s*)["']?[^\s"',}]+`)
)

// redactLog removes secrets a log line might carry before it leaves the edge:
// PEM private keys, secret:// references and password/token key-values.
func redactLog(s string) string {
	s = privateKeyRE.ReplaceAllString(s, "[redacted-private-key]")
	s = secretRefRE.ReplaceAllString(s, "secret://[redacted]")
	s = kvSecretRE.ReplaceAllString(s, "${1}[redacted]")
	return s
}

// sysLogsTailResult is the sys.logs_tail result document. Entries is an array
// of lines, not one newline-joined string: the platform pretty-prints the
// result as JSON, and a string's newlines would render as escaped "\n" on a
// single row.
type sysLogsTailResult struct {
	Entries []string `json:"entries"`
}

// runLogsTail returns the last N lines of the agent log with secrets redacted.
// It only ever reads the configured log file, never a caller-supplied path.
func (h *Handler) runLogsTail(inv device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	if h.opts.LogFile == "" {
		return nil, errors.New("log file is not configured")
	}
	if name, ok := stringParam(inv.Params, "service"); ok {
		if name = strings.TrimSpace(name); name != "" && name != agentServiceName {
			return nil, errors.New("unknown service")
		}
	}
	lines, err := optionalInt(inv.Params, "lines", 1, 200, 100)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(h.opts.LogFile)
	if err != nil {
		return nil, errors.New("could not read log file")
	}
	entries := tailLines(string(data), lines)
	for i := range entries {
		entries[i] = redactLog(entries[i])
	}
	// Keep the most recent lines that fit the result budget.
	for len(entries) > 0 {
		buf, err := json.Marshal(sysLogsTailResult{Entries: entries})
		if err != nil {
			return nil, errors.New("encode result")
		}
		if len(buf) <= maxResultBytes {
			return buf, nil
		}
		entries = entries[1:]
	}
	return marshalResult(sysLogsTailResult{Entries: []string{}})
}

// tailLines returns at most the final n newline-separated lines of s.
func tailLines(s string, n int) []string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

// sysCertInfoResult is the sys.cert_info result document.
type sysCertInfoResult struct {
	NotAfter      string `json:"not_after"`
	NotBefore     string `json:"not_before"`
	Issuer        string `json:"issuer"`
	Subject       string `json:"subject"`
	DaysRemaining int    `json:"days_remaining"`
}

// runCertInfo reports the northbound mTLS identity certificate metadata. It
// never returns the certificate body or the private key.
func (h *Handler) runCertInfo(_ device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	if h.opts.IdentityCertFile == "" {
		return nil, errors.New("identity certificate is not configured")
	}
	data, err := os.ReadFile(h.opts.IdentityCertFile)
	if err != nil {
		return nil, errors.New("could not read identity certificate")
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("identity certificate is not valid PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.New("could not parse identity certificate")
	}
	return marshalResult(sysCertInfoResult{
		NotAfter:      cert.NotAfter.UTC().Format(time.RFC3339),
		NotBefore:     cert.NotBefore.UTC().Format(time.RFC3339),
		Issuer:        cert.Issuer.String(),
		Subject:       cert.Subject.String(),
		DaysRemaining: int(time.Until(cert.NotAfter).Hours() / 24),
	})
}

// serviceRestartResult is the service.restart result document.
type serviceRestartResult struct {
	OK bool `json:"ok"`
}

// runServiceRestart restarts one allowlisted managed service. The name is
// resolved through the allowlist to a fixed unit, so no caller string ever
// reaches the command line.
func (h *Handler) runServiceRestart(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	name, ok := stringParam(inv.Params, "name")
	name = strings.TrimSpace(name)
	if !ok || name == "" {
		return nil, errors.New("missing required param: name")
	}
	unit, ok := h.opts.Services[name]
	if !ok {
		return nil, errors.New("service is not allowlisted")
	}

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout(advert, inv.TimeoutMs))
	defer cancel()
	if err := exec.CommandContext(ctx, "systemctl", "restart", unit).Run(); err != nil {
		if ctx.Err() != nil {
			return nil, errors.New("service restart timed out")
		}
		return nil, errors.New("service restart failed")
	}
	return marshalResult(serviceRestartResult{OK: true})
}
