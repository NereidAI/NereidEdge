// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gatewayoperation

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	device "github.com/nereidai/nereidlink/sdk/device/go"

	"github.com/nereidai/nereidedge/internal/gateway"
)

const (
	// maxMounts bounds the sys.resources mount list.
	maxMounts = 20
	// maxInterfaces bounds the sys.network interface list.
	maxInterfaces = 16
	// staUnsync is the kernel STA_UNSYNC flag: when set the clock is not
	// NTP-synchronised.
	staUnsync = 0x0040
)

// sysInfoResult is the sys.info result document.
type sysInfoResult struct {
	Hostname     string  `json:"hostname"`
	OS           string  `json:"os"`
	Kernel       string  `json:"kernel"`
	Arch         string  `json:"arch"`
	AgentVersion string  `json:"agent_version"`
	Proto        string  `json:"proto"`
	UptimeS      float64 `json:"uptime_s"`
	Time         string  `json:"time"`
	Timezone     string  `json:"timezone"`
}

// runSysInfo reports static build/host identity. It reads no config files.
func runSysInfo(_ device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	now := time.Now()
	tz := now.Location().String()
	if tz == "" || tz == "Local" {
		tz = now.Format("MST")
	}
	return marshalResult(sysInfoResult{
		Hostname:     hostname(),
		OS:           runtime.GOOS,
		Kernel:       readFileTrim("/proc/sys/kernel/osrelease"),
		Arch:         runtime.GOARCH,
		AgentVersion: gateway.AgentVersion,
		Proto:        gateway.ProtoMin + "-" + gateway.ProtoMax,
		UptimeS:      uptimeSeconds(),
		Time:         now.UTC().Format(time.RFC3339),
		Timezone:     tz,
	})
}

// memInfo is a memory/swap usage document.
type memInfo struct {
	Total     uint64 `json:"total"`
	Used      uint64 `json:"used"`
	Available uint64 `json:"available"`
}

// loadAvg is the sys.resources load document.
type loadAvg struct {
	One     float64 `json:"1"`
	Five    float64 `json:"5"`
	Fifteen float64 `json:"15"`
}

// mountInfo is one sys.resources mount entry.
type mountInfo struct {
	Path      string `json:"path"`
	Total     uint64 `json:"total"`
	Used      uint64 `json:"used"`
	Available uint64 `json:"available"`
}

// sysResourcesResult is the sys.resources result document.
type sysResourcesResult struct {
	CPUPercent float64     `json:"cpu_percent"`
	Load       loadAvg     `json:"load"`
	Mem        memInfo     `json:"mem"`
	Swap       memInfo     `json:"swap"`
	Mounts     []mountInfo `json:"mounts"`
}

// runSysResources samples CPU over a window and reports load, memory, swap and
// mount usage.
func runSysResources(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	deadline := opTimeout(advert, inv.TimeoutMs)
	sample, err := optionalInt(inv.Params, "sample_ms", 100, 3000, 1000)
	if err != nil {
		return nil, err
	}
	maxSample := int(deadline/time.Millisecond) - 500
	if maxSample < 100 {
		maxSample = 100
	}
	if sample > maxSample {
		sample = maxSample
	}

	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	cpu, err := sampleCPU(ctx, time.Duration(sample)*time.Millisecond)
	if err != nil {
		return nil, errors.New("cpu sampling failed")
	}

	mem, swap := readMemSwap()
	return marshalResult(sysResourcesResult{
		CPUPercent: cpu,
		Load:       readLoadAvg(),
		Mem:        mem,
		Swap:       swap,
		Mounts:     readMounts(),
	})
}

// sampleCPU measures busy CPU across a fixed window.
func sampleCPU(ctx context.Context, window time.Duration) (float64, error) {
	idle0, total0, err := readCPUTime()
	if err != nil {
		return 0, err
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(window):
	}
	idle1, total1, err := readCPUTime()
	if err != nil {
		return 0, err
	}

	total := total1 - total0
	idle := idle1 - idle0
	if total == 0 {
		return 0, nil
	}
	return round2(100 * float64(total-idle) / float64(total)), nil
}

// readCPUTime returns cumulative idle and total jiffies from /proc/stat.
func readCPUTime() (idle, total uint64, err error) {
	fields, err := readProcLineFields("/proc/stat", "cpu ")
	if err != nil {
		return 0, 0, err
	}
	for i, f := range fields {
		v, perr := strconv.ParseUint(f, 10, 64)
		if perr != nil {
			return 0, 0, perr
		}
		total += v
		if i == 3 || i == 4 { // idle + iowait
			idle += v
		}
	}
	return idle, total, nil
}

// readLoadAvg reads the 1/5/15 minute load averages from /proc/loadavg.
func readLoadAvg() loadAvg {
	fields := strings.Fields(readFileTrim("/proc/loadavg"))
	var out loadAvg
	if len(fields) >= 3 {
		out.One, _ = strconv.ParseFloat(fields[0], 64)
		out.Five, _ = strconv.ParseFloat(fields[1], 64)
		out.Fifteen, _ = strconv.ParseFloat(fields[2], 64)
	}
	return out
}

// readMemSwap reads memory and swap totals from /proc/meminfo.
func readMemSwap() (memInfo, memInfo) {
	values := map[string]uint64{}
	for _, line := range strings.Split(readFileTrim("/proc/meminfo"), "\n") {
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		field := strings.Fields(val)
		if len(field) == 0 {
			continue
		}
		n, err := strconv.ParseUint(field[0], 10, 64)
		if err != nil {
			continue
		}
		values[strings.TrimSpace(key)] = n * 1024 // kB -> bytes
	}

	total := values["MemTotal"]
	avail := values["MemAvailable"]
	used := uint64(0)
	if total > avail {
		used = total - avail
	}
	swapTotal := values["SwapTotal"]
	swapFree := values["SwapFree"]
	swapUsed := uint64(0)
	if swapTotal > swapFree {
		swapUsed = swapTotal - swapFree
	}
	return memInfo{Total: total, Used: used, Available: avail},
		memInfo{Total: swapTotal, Used: swapUsed}
}

// readMounts returns real-filesystem usage from /proc/mounts, capped.
func readMounts() []mountInfo {
	content := readFileTrim("/proc/mounts")
	out := make([]mountInfo, 0, maxMounts)
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		path := unescapeMount(fields[1])
		var st syscall.Statfs_t
		if err := syscall.Statfs(path, &st); err != nil {
			continue
		}
		bsize := uint64(st.Bsize)
		total := st.Blocks * bsize
		if total == 0 {
			continue
		}
		out = append(out, mountInfo{
			Path:      path,
			Total:     total,
			Used:      (st.Blocks - st.Bfree) * bsize,
			Available: st.Bavail * bsize,
		})
		if len(out) >= maxMounts {
			break
		}
	}
	return out
}

// unescapeMount decodes octal escapes (e.g. \040 for space) used by /proc/mounts.
func unescapeMount(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// ifaceInfo is one sys.network interface entry.
type ifaceInfo struct {
	Name string   `json:"name"`
	MAC  string   `json:"mac"`
	IPv4 []string `json:"ipv4"`
	IPv6 []string `json:"ipv6"`
	Up   bool     `json:"up"`
}

// sysNetworkResult is the sys.network result document.
type sysNetworkResult struct {
	Interfaces   []ifaceInfo `json:"interfaces"`
	DefaultRoute string      `json:"default_route"`
	DNS          []string    `json:"dns"`
	NTPSynced    bool        `json:"ntp_synced"`
}

// runSysNetwork enumerates interfaces, default route, resolver and NTP state.
func runSysNetwork(_ device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	return marshalResult(sysNetworkResult{
		Interfaces:   collectInterfaces(),
		DefaultRoute: defaultRoute(),
		DNS:          resolvConfNameservers(),
		NTPSynced:    ntpSynced(),
	})
}

// collectInterfaces gathers link addresses, capped at maxInterfaces.
func collectInterfaces() []ifaceInfo {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	out := make([]ifaceInfo, 0, maxInterfaces)
	for _, iface := range ifaces {
		info := ifaceInfo{
			Name: iface.Name,
			MAC:  iface.HardwareAddr.String(),
			Up:   iface.Flags&net.FlagUp != 0,
			IPv4: []string{},
			IPv6: []string{},
		}
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			if ipNet.IP.To4() != nil {
				info.IPv4 = append(info.IPv4, ipNet.IP.String())
			} else {
				info.IPv6 = append(info.IPv6, ipNet.IP.String())
			}
		}
		out = append(out, info)
		if len(out) >= maxInterfaces {
			break
		}
	}
	return out
}

// defaultRoute reads the IPv4 default gateway from /proc/net/route.
func defaultRoute() string {
	content := readFileTrim("/proc/net/route")
	for _, line := range strings.Split(content, "\n")[1:] {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[1] != "00000000" {
			continue
		}
		return ipv4FromProcHex(fields[2])
	}
	return ""
}

// ipv4FromProcHex converts the little-endian hex address used by /proc/net/route.
func ipv4FromProcHex(s string) string {
	if len(s) != 8 {
		return ""
	}
	var b [4]byte
	for i := 0; i < 4; i++ {
		n, err := strconv.ParseUint(s[i*2:i*2+2], 16, 8)
		if err != nil {
			return ""
		}
		b[i] = byte(n)
	}
	return net.IPv4(b[3], b[2], b[1], b[0]).String()
}

// ntpSynced reports whether the kernel clock is NTP-synchronised.
func ntpSynced() bool {
	var tx syscall.Timex
	state, err := syscall.Adjtimex(&tx)
	if err != nil {
		return false
	}
	return state&staUnsync == 0
}

// runSysReboot triggers a best-effort restart. It is one-way: NL does not wait
// for a result and infers success from re-hello/link health.
func runSysReboot(_ device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	go triggerReboot()
	return nil, nil
}

// triggerReboot tries the common restart entry points in turn.
func triggerReboot() {
	for _, argv := range [][]string{
		{"reboot"},
		{"/sbin/reboot"},
		{"systemctl", "reboot"},
		{"shutdown", "-r", "now"},
	} {
		if err := exec.Command(argv[0], argv[1:]...).Run(); err == nil {
			return
		}
	}
}

// hostname returns the system host name.
func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return ""
	}
	return name
}

// uptimeSeconds returns the system uptime from /proc/uptime.
func uptimeSeconds() float64 {
	fields := strings.Fields(readFileTrim("/proc/uptime"))
	if len(fields) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(fields[0], 64)
	return v
}

// firstResolver returns the first configured nameserver, if any.
func firstResolver() string {
	if servers := resolvConfNameservers(); len(servers) > 0 {
		return servers[0]
	}
	return ""
}

// resolvConfNameservers parses nameservers from /etc/resolv.conf.
func resolvConfNameservers() []string {
	var out []string
	for _, line := range strings.Split(readFileTrim("/etc/resolv.conf"), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "nameserver" {
			out = append(out, fields[1])
		}
	}
	return out
}

// readFileTrim reads a file and trims surrounding whitespace.
func readFileTrim(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// readProcLineFields returns the whitespace fields of the first line in path
// that starts with prefix, with the prefix removed.
func readProcLineFields(path, prefix string) ([]string, error) {
	for _, line := range strings.Split(readFileTrim(path), "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.Fields(strings.TrimPrefix(line, prefix)), nil
		}
	}
	return nil, errors.New("no matching line")
}

// sysNTPStatusResult is the sys.ntp_status result document.
type sysNTPStatusResult struct {
	Synced   bool    `json:"synced"`
	OffsetMs float64 `json:"offset_ms"`
	Server   string  `json:"server"`
}

// runSysNTPStatus reports the kernel clock sync state and current offset. It is
// read-only: Adjtimex is called with zero modes, so the clock is never changed.
func runSysNTPStatus(_ device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	offsetMs, synced, err := readNTPOffset()
	if err != nil {
		return marshalResult(sysNTPStatusResult{Synced: false, Server: ntpServer()})
	}
	return marshalResult(sysNTPStatusResult{Synced: synced, OffsetMs: offsetMs, Server: ntpServer()})
}

// sysNTPSyncResult is the sys.ntp_sync result document.
type sysNTPSyncResult struct {
	BeforeOffsetMs float64 `json:"before_offset_ms"`
	AfterOffsetMs  float64 `json:"after_offset_ms"`
	Synced         bool    `json:"synced"`
}

// runSysNTPSync triggers one immediate clock step and reports the offset before
// and after. On failure it returns an error and leaves the clock untouched.
func runSysNTPSync(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	server, _ := stringParam(inv.Params, "server")
	server = strings.TrimSpace(server)

	before, _, err := readNTPOffset()
	if err != nil {
		return nil, errors.New("ntp state unavailable")
	}
	if err := triggerNTPSync(opTimeout(advert, inv.TimeoutMs), server); err != nil {
		return nil, err
	}
	after, synced, err := readNTPOffset()
	if err != nil {
		return nil, errors.New("ntp state unavailable")
	}
	return marshalResult(sysNTPSyncResult{BeforeOffsetMs: before, AfterOffsetMs: after, Synced: synced})
}

// readNTPOffset returns the current clock offset in milliseconds and whether
// the kernel considers the clock synchronised.
func readNTPOffset() (float64, bool, error) {
	var tx syscall.Timex
	state, err := syscall.Adjtimex(&tx)
	if err != nil {
		return 0, false, err
	}
	return round2(float64(tx.Offset) / 1000), state&staUnsync == 0, nil
}

// ntpServer returns the first configured NTP server, or "" when unknown.
func ntpServer() string {
	for _, path := range []string{"/etc/chrony/chrony.conf", "/etc/ntp.conf"} {
		for _, line := range strings.Split(readFileTrim(path), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && (fields[0] == "server" || fields[0] == "pool" || fields[0] == "peer") {
				return fields[1]
			}
		}
	}
	return ""
}

// triggerNTPSync runs one immediate clock step with the first available NTP
// client. It never alters configuration; a missing client is an error.
func triggerNTPSync(deadline time.Duration, server string) error {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	var attempts [][]string
	if server != "" {
		attempts = append(attempts,
			[]string{"sntp", "-S", server},
			[]string{"ntpdate", server},
			[]string{"chronyc", "makestep"},
		)
	}
	attempts = append(attempts,
		[]string{"chronyc", "makestep"},
		[]string{"ntpd", "-gq"},
	)
	for _, argv := range attempts {
		if _, err := exec.LookPath(argv[0]); err != nil {
			continue
		}
		if err := exec.CommandContext(ctx, argv[0], argv[1:]...).Run(); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return errors.New("ntp sync timed out")
		}
	}
	return errors.New("no ntp client available")
}

// procInfo is one sys.processes_top entry.
type procInfo struct {
	Name       string  `json:"name"`
	PID        int     `json:"pid"`
	CPUPercent float64 `json:"cpu_percent"`
	MemPercent float64 `json:"mem_percent"`
}

// sysProcessesTopResult is the sys.processes_top result document.
type sysProcessesTopResult struct {
	Processes []procInfo `json:"processes"`
}

// procSample is a per-pid CPU/memory snapshot.
type procSample struct {
	name  string
	ticks uint64
	rss   uint64
}

// runSysProcessesTop lists the top processes by CPU or memory over a short
// sampling window. It is read-only.
func runSysProcessesTop(inv device.OperationInvoke, advert device.OperationAdvert) (json.RawMessage, error) {
	limit, err := optionalInt(inv.Params, "limit", 1, 20, 10)
	if err != nil {
		return nil, err
	}
	sortBy := "cpu"
	raw, ok := stringParam(inv.Params, "sort")
	if ok {
		sortBy = raw
	}
	if sortBy != "cpu" && sortBy != "mem" {
		return nil, errors.New(`param sort must be "cpu" or "mem"`)
	}

	window := 200 * time.Millisecond
	if deadline := opTimeout(advert, inv.TimeoutMs); window > deadline-500*time.Millisecond {
		window = deadline / 4
	}

	_, total0, err := readCPUTime()
	if err != nil {
		return nil, errors.New("process sampling failed")
	}
	first := readProcSamples()
	time.Sleep(window)
	_, total1, err := readCPUTime()
	if err != nil {
		return nil, errors.New("process sampling failed")
	}
	second := readProcSamples()

	pageSize := float64(os.Getpagesize())
	mem, _ := readMemSwap()
	memTotal := float64(mem.Total)

	out := make([]procInfo, 0, len(second))
	for pid, cur := range second {
		var cpu float64
		if prev, seen := first[pid]; seen && total1 != total0 {
			cpu = 100 * float64(cur.ticks-prev.ticks) / float64(total1-total0)
		}
		memPct := 0.0
		if memTotal > 0 {
			memPct = 100 * float64(cur.rss) * pageSize / float64(memTotal)
		}
		out = append(out, procInfo{
			Name:       cur.name,
			PID:        pid,
			CPUPercent: round2(cpu),
			MemPercent: round2(memPct),
		})
	}

	if sortBy == "mem" {
		sort.Slice(out, func(i, j int) bool { return out[i].MemPercent > out[j].MemPercent })
	} else {
		sort.Slice(out, func(i, j int) bool { return out[i].CPUPercent > out[j].CPUPercent })
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return marshalResult(sysProcessesTopResult{Processes: out})
}

// readProcSamples snapshots CPU ticks and RSS for every readable process.
func readProcSamples() map[int]procSample {
	out := make(map[int]procSample)
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return out
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if sample, ok := readProcStat(pid); ok {
			out[pid] = sample
		}
	}
	return out
}

// readProcStat parses one /proc/<pid>/stat. The process name is in parentheses
// and may contain spaces, so field offsets are measured from the last ')'.
func readProcStat(pid int) (procSample, bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return procSample{}, false
	}
	s := string(data)
	open := strings.IndexByte(s, '(')
	close := strings.LastIndexByte(s, ')')
	if open < 0 || close <= open {
		return procSample{}, false
	}
	name := strings.TrimSpace(s[open+1 : close])
	if len(name) > 64 {
		name = name[:64]
	}
	rest := strings.Fields(s[close+1:])
	// After the state field (rest[0]) utime is field 14, stime field 15 and rss
	// (pages) field 24 in proc(5), i.e. indices 11, 12 and 21 here.
	if len(rest) < 22 {
		return procSample{}, false
	}
	utime, err := strconv.ParseUint(rest[11], 10, 64)
	if err != nil {
		return procSample{}, false
	}
	stime, _ := strconv.ParseUint(rest[12], 10, 64)
	rss, _ := strconv.ParseUint(rest[21], 10, 64)
	return procSample{name: name, ticks: utime + stime, rss: rss}, true
}

// uplinkStatusResult is the diag.uplink_status result document.
type uplinkStatusResult struct {
	Kind      string  `json:"kind"`
	Connected bool    `json:"connected"`
	RSSIDBm   *int    `json:"rssi_dbm,omitempty"`
	RxBytes   float64 `json:"rx_bytes"`
	TxBytes   float64 `json:"tx_bytes"`
	SSID      string  `json:"ssid,omitempty"`
}

// runDiagUplinkStatus reports the active uplink kind, connectivity and
// cumulative byte counters. It is read-only.
func runDiagUplinkStatus(_ device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	name, kind := activeUplink()
	res := uplinkStatusResult{Kind: kind, RxBytes: readIfaceStat(name, "rx_bytes"), TxBytes: readIfaceStat(name, "tx_bytes")}
	if name != "" {
		res.Connected = ifaceOperUp(name)
	}
	if kind == "wifi" {
		if rssi, ok := wifiSignal(name); ok {
			res.RSSIDBm = &rssi
		}
		res.SSID = wifiSSID(name)
	}
	return marshalResult(res)
}

// classifyIface maps an interface name to an uplink kind by common prefixes.
func classifyIface(name string) string {
	switch {
	case strings.HasPrefix(name, "wl") || strings.Contains(name, "wlan") || strings.Contains(name, "wifi"):
		return "wifi"
	case strings.HasPrefix(name, "ww") || strings.HasPrefix(name, "rmnet") ||
		strings.HasPrefix(name, "ppp") || strings.Contains(name, "wwan") || strings.HasPrefix(name, "usb"):
		return "cellular"
	case strings.HasPrefix(name, "eth") || strings.HasPrefix(name, "en") || strings.HasPrefix(name, "bond"):
		return "ethernet"
	default:
		return "other"
	}
}

// activeUplink returns the name and kind of the first up, non-loopback
// interface, preferring a wireless or cellular link when several are up.
func activeUplink() (string, string) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", "other"
	}
	bestName, bestKind := "", "other"
	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}
		kind := classifyIface(iface.Name)
		if kind == "other" {
			continue
		}
		if bestName == "" || kind == "wifi" || kind == "cellular" {
			bestName, bestKind = iface.Name, kind
		}
	}
	return bestName, bestKind
}

// ifaceOperUp reports whether the interface's operstate is up.
func ifaceOperUp(name string) bool {
	return readFileTrim("/sys/class/net/"+name+"/operstate") == "up"
}

// readIfaceStat reads a cumulative interface counter from sysfs.
func readIfaceStat(name, stat string) float64 {
	if name == "" {
		return 0
	}
	v, _ := strconv.ParseFloat(readFileTrim("/sys/class/net/"+name+"/statistics/"+stat), 64)
	return v
}

// wifiSignal reads the signal level (dBm) for a wireless interface from
// /proc/net/wireless, where a data line is "name: status quality signal ...".
func wifiSignal(name string) (int, bool) {
	for _, line := range strings.Split(readFileTrim("/proc/net/wireless"), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || strings.TrimSuffix(fields[0], ":") != name {
			continue
		}
		if v, err := strconv.Atoi(strings.TrimSuffix(fields[3], ".")); err == nil {
			return v, true
		}
	}
	return 0, false
}

// wifiSSID returns the current SSID for a wireless interface, or "".
func wifiSSID(name string) string {
	out, err := exec.Command("iw", "dev", name, "link").Output()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "SSID:"); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// runNetworkReconnect triggers a best-effort uplink reconnect after an optional
// delay. It is one-way and never touches persistent network configuration.
func runNetworkReconnect(inv device.OperationInvoke, _ device.OperationAdvert) (json.RawMessage, error) {
	delay, err := optionalInt(inv.Params, "delay_s", 0, 30, 1)
	if err != nil {
		return nil, err
	}
	want, _ := stringParam(inv.Params, "uplink")
	want = strings.TrimSpace(want)
	if want != "" && want != "ethernet" && want != "wifi" && want != "cellular" {
		return nil, errors.New(`param uplink must be ethernet, wifi or cellular`)
	}
	go func() {
		time.Sleep(time.Duration(delay) * time.Second)
		reconnectUplink(want)
	}()
	return nil, nil
}

// reconnectUplink bounces the selected uplink: nmcli when the interface is
// managed by NetworkManager, otherwise a transient link down/up. Persistent
// configuration is never modified.
func reconnectUplink(want string) {
	name, kind := activeUplink()
	if want != "" && want != kind {
		if candidate, _ := uplinkOfKind(want); candidate != "" {
			name = candidate
		}
	}
	if name == "" {
		return
	}
	if _, err := exec.LookPath("nmcli"); err == nil {
		_ = exec.Command("nmcli", "device", "disconnect", name).Run()
		_ = exec.Command("nmcli", "device", "connect", name).Run()
		return
	}
	_ = exec.Command("ip", "link", "set", name, "down").Run()
	time.Sleep(time.Second)
	_ = exec.Command("ip", "link", "set", name, "up").Run()
}

// uplinkOfKind returns the first interface matching the requested kind.
func uplinkOfKind(kind string) (string, string) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", ""
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if classifyIface(iface.Name) == kind {
			return iface.Name, kind
		}
	}
	return "", ""
}
