// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gatewayoperation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

var (
	pingRTTRe  = regexp.MustCompile(`time[=<]([0-9.]+)`)
	pingLossRe = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)% packet loss`)
)

// pingResult is the diag.ping result document.
type pingResult struct {
	OK    bool      `json:"ok"`
	Host  string    `json:"host"`
	RTTMs []float64 `json:"rtt_ms"`
	Loss  float64   `json:"loss"`
}

// runPing sends ICMP echo probes and reports per-probe RTT and packet loss.
func runPing(inv device.OperationInvoke, advert device.OperationAdvert) (resJSON json.RawMessage, err error) {
	host, _ := stringParam(inv.Params, "host")
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, errors.New("missing required param: host")
	}
	if strings.HasPrefix(host, "-") || strings.HasPrefix(host, "secret://") {
		return nil, errors.New("invalid host")
	}
	count, err := optionalInt(inv.Params, "count", 1, maxPingCount, defaultPingCount)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout(advert, inv.TimeoutMs))
	defer cancel()

	out, runErr := exec.CommandContext(ctx, "ping", "-c", strconv.Itoa(count), "-W", "2", host).CombinedOutput()
	if ctx.Err() != nil {
		return nil, errors.New("ping timed out")
	}

	rtts := parseRTT(string(out))
	loss, haveLoss := parseLoss(string(out))
	if !haveLoss {
		if len(rtts) == 0 {
			loss = 100
		} else {
			loss = 100 * float64(count-len(rtts)) / float64(count)
		}
	}
	if len(rtts) == 0 && !haveLoss && runErr != nil {
		return nil, errors.New("ping failed")
	}

	return marshalResult(pingResult{OK: true, Host: host, RTTMs: rtts, Loss: round2(loss)})
}

// parseRTT extracts per-probe RTT values from ping output.
func parseRTT(out string) []float64 {
	matches := pingRTTRe.FindAllStringSubmatch(out, -1)
	rtts := make([]float64, 0, len(matches))
	for _, m := range matches {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil {
			rtts = append(rtts, v)
		}
	}
	return rtts
}

// parseLoss extracts the packet-loss percentage from ping output.
func parseLoss(out string) (float64, bool) {
	m := pingLossRe.FindStringSubmatch(out)
	if m == nil {
		return 0, false
	}
	v, err := strconv.ParseFloat(m[1], 64)
	return v, err == nil
}

// dnsAnswer is one resolved address.
type dnsAnswer struct {
	Address string `json:"address"`
	Type    string `json:"type"`
}

// dnsResolveResult is the diag.dns_resolve result document.
type dnsResolveResult struct {
	Answers  []dnsAnswer `json:"answers"`
	Resolver string      `json:"resolver"`
	RTTMs    float64     `json:"rtt_ms"`
}

// runDNSResolve resolves a host name with the system resolver.
func runDNSResolve(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	name, ok := stringParam(inv.Params, "name")
	name = strings.TrimSpace(name)
	if !ok || name == "" {
		return nil, errors.New("missing required param: name")
	}
	rtype := "A"
	network := "ip4"
	if s, ok := stringParam(inv.Params, "record_type"); ok {
		switch strings.ToUpper(strings.TrimSpace(s)) {
		case "", "A":
			rtype, network = "A", "ip4"
		case "AAAA":
			rtype, network = "AAAA", "ip6"
		default:
			return nil, errors.New("param record_type must be A or AAAA")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout(advert, inv.TimeoutMs))
	defer cancel()

	start := time.Now()
	ips, err := net.DefaultResolver.LookupIP(ctx, network, name)
	if err != nil {
		return nil, fmt.Errorf("dns lookup failed: %s", dnsReason(ctx, err))
	}
	answers := make([]dnsAnswer, 0, len(ips))
	for _, ip := range ips {
		answers = append(answers, dnsAnswer{Address: ip.String(), Type: rtype})
	}

	return marshalResult(dnsResolveResult{
		Answers:  answers,
		Resolver: firstResolver(),
		RTTMs:    round2(float64(time.Since(start).Microseconds()) / 1000),
	})
}

// dnsReason maps a lookup failure to a stable, redacted message.
func dnsReason(ctx context.Context, err error) string {
	if ctx.Err() != nil {
		return "timeout"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsNotFound {
			return "name not found"
		}
		if dnsErr.IsTimeout {
			return "timeout"
		}
	}
	return "lookup error"
}

// tcpConnectResult is the diag.tcp_connect result document.
type tcpConnectResult struct {
	Reachable bool    `json:"reachable"`
	RTTMs     float64 `json:"rtt_ms"`
	Error     string  `json:"error,omitempty"`
}

// runTCPConnect probes TCP reachability without sending application data.
func runTCPConnect(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	host, ok := stringParam(inv.Params, "host")
	host = strings.TrimSpace(host)
	if !ok || host == "" {
		return nil, errors.New("missing required param: host")
	}
	port, err := requiredInt(inv.Params, "port", 1, 65535)
	if err != nil {
		return nil, err
	}
	dialMs, err := optionalInt(inv.Params, "timeout_ms", 100, 5000, 2000)
	if err != nil {
		return nil, err
	}

	timeout := time.Duration(dialMs) * time.Millisecond
	if deadline := opTimeout(advert, inv.TimeoutMs); timeout > deadline {
		timeout = deadline
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return marshalResult(tcpConnectResult{Reachable: false, Error: dialReason(ctx)})
	}
	_ = conn.Close()
	return marshalResult(tcpConnectResult{
		Reachable: true,
		RTTMs:     round2(float64(time.Since(start).Microseconds()) / 1000),
	})
}

// dialReason maps a dial failure to a stable, redacted message.
func dialReason(ctx context.Context) string {
	if ctx.Err() != nil {
		return "connect timed out"
	}
	return "connect failed"
}

// tlsInspectResult is the diag.tls_inspect result document.
type tlsInspectResult struct {
	Version  string `json:"version"`
	Cipher   string `json:"cipher"`
	NotAfter string `json:"not_after"`
	ChainOK  bool   `json:"chain_ok"`
	Error    string `json:"error,omitempty"`
}

// runTLSInspect completes a TLS handshake and reports the leaf certificate.
// The handshake skips verification so cert details are still returned when the
// chain is untrusted; validity is checked explicitly and reported in chain_ok.
func runTLSInspect(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	host, ok := stringParam(inv.Params, "host")
	host = strings.TrimSpace(host)
	if !ok || host == "" {
		return nil, errors.New("missing required param: host")
	}
	port, err := optionalInt(inv.Params, "port", 1, 65535, 443)
	if err != nil {
		return nil, err
	}
	serverName := host
	if s, ok := stringParam(inv.Params, "server_name"); ok && strings.TrimSpace(s) != "" {
		serverName = strings.TrimSpace(s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout(advert, inv.TimeoutMs))
	defer cancel()

	rawConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("tls inspect failed: %s", dialReason(ctx))
	}
	defer func() { _ = rawConn.Close() }()

	tlsConn := tls.Client(rawConn, &tls.Config{ServerName: serverName, InsecureSkipVerify: true})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		if ctx.Err() != nil {
			return nil, errors.New("tls inspect failed: timeout")
		}
		return nil, errors.New("tls inspect failed: handshake error")
	}
	defer func() { _ = tlsConn.Close() }()

	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, errors.New("tls inspect failed: no certificate")
	}
	leaf := state.PeerCertificates[0]
	res := tlsInspectResult{
		Version:  tlsVersionName(state.Version),
		Cipher:   tls.CipherSuiteName(state.CipherSuite),
		NotAfter: leaf.NotAfter.UTC().Format(time.RFC3339),
	}

	inter := x509.NewCertPool()
	for _, cert := range state.PeerCertificates[1:] {
		inter.AddCert(cert)
	}
	roots, _ := x509.SystemCertPool()
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: inter,
		DNSName:       serverName,
	}); err != nil {
		res.ChainOK = false
		res.Error = "certificate verification failed"
	} else {
		res.ChainOK = true
	}
	return marshalResult(res)
}

// tlsVersionName renders a TLS version constant.
func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "TLS 1.3"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS11:
		return "TLS 1.1"
	default:
		return fmt.Sprintf("0x%04x", v)
	}
}

// httpCheckResult is the diag.http_check result document.
type httpCheckResult struct {
	Status     int      `json:"status"`
	OK         bool     `json:"ok"`
	Redirects  []string `json:"redirects"`
	TLSVersion string   `json:"tls_version,omitempty"`
	RTTMs      float64  `json:"rtt_ms"`
	Error      string   `json:"error,omitempty"`
}

// runHTTPCheck issues a plain GET (no body, auth headers or cookies) to an
// http/https URL, follows at most five redirects, and reports the final status.
func runHTTPCheck(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	raw, ok := stringParam(inv.Params, "url")
	raw = strings.TrimSpace(raw)
	if !ok || raw == "" {
		return nil, errors.New("missing required param: url")
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, errors.New("url must be an absolute http or https URL")
	}
	timeoutMs, err := optionalInt(inv.Params, "timeout_ms", 100, 10000, 5000)
	if err != nil {
		return nil, err
	}
	if deadline := opTimeout(advert, inv.TimeoutMs); time.Duration(timeoutMs)*time.Millisecond > deadline {
		timeoutMs = int(deadline / time.Millisecond)
	}

	var redirects []string
	client := &http.Client{
		Timeout: time.Duration(timeoutMs) * time.Millisecond,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return http.ErrUseLastResponse
			}
			redirects = append(redirects, req.URL.String())
			return nil
		},
	}

	start := time.Now()
	resp, err := client.Get(parsed.String())
	rtt := round2(float64(time.Since(start).Microseconds()) / 1000)
	if err != nil {
		return marshalResult(httpCheckResult{Redirects: redirects, RTTMs: rtt, Error: "request failed"})
	}
	defer func() { _ = resp.Body.Close() }()

	res := httpCheckResult{
		Status:    resp.StatusCode,
		OK:        resp.StatusCode >= 200 && resp.StatusCode < 400,
		Redirects: redirects,
		RTTMs:     rtt,
	}
	if resp.TLS != nil {
		res.TLSVersion = tlsVersionName(resp.TLS.Version)
	}
	if res.Redirects == nil {
		res.Redirects = []string{}
	}
	return marshalResult(res)
}
