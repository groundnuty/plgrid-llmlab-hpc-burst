package main

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// proc is a process the harness started. The harness only ever signals PIDs it
// started itself or found as their descendants; it never matches by name.
type proc struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
	log  *os.File
}

func startProc(name, logPath string, env []string, argv ...string) (*proc, error) {
	lf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	// "nice -n 10" execs into the program, so the PID is the program's PID.
	cmd := exec.Command("nice", append([]string{"-n", "10"}, argv...)...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = lf, lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	p := &proc{name: name, cmd: cmd, done: make(chan struct{}), log: lf}
	go func() { _ = cmd.Wait(); close(p.done); lf.Close() }()
	return p, nil
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

func (p *proc) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// stop sends SIGTERM to the process and waits; after the grace period it sends SIGKILL
// to the same PID. Descendants found before the signal are checked afterwards.
func (p *proc) stop(grace time.Duration) (time.Duration, error) {
	start := time.Now()
	if !p.alive() {
		return 0, nil
	}
	kids := descendants([]int{p.pid()})
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(grace):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	took := time.Since(start)
	for _, k := range kids {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", k)); err == nil {
			time.Sleep(500 * time.Millisecond)
			if _, err := os.Stat(fmt.Sprintf("/proc/%d", k)); err == nil {
				return took, fmt.Errorf("%s: descendant pid %d still alive after stop", p.name, k)
			}
		}
	}
	return took, nil
}

func ppidOf(pid int) (int, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	f := strings.Fields(s[i+1:])
	if len(f) < 2 {
		return 0, false
	}
	pp, err := strconv.Atoi(f[1])
	return pp, err == nil
}

// descendants returns the given PIDs plus every process below them.
func descendants(roots []int) []int {
	ents, _ := os.ReadDir("/proc")
	parent := map[int]int{}
	for _, e := range ents {
		if pid, err := strconv.Atoi(e.Name()); err == nil {
			if pp, ok := ppidOf(pid); ok {
				parent[pid] = pp
			}
		}
	}
	set := map[int]bool{}
	for _, r := range roots {
		set[r] = true
	}
	for changed := true; changed; {
		changed = false
		for pid, pp := range parent {
			if !set[pid] && set[pp] {
				set[pid] = true
				changed = true
			}
		}
	}
	var out []int
	for pid := range set {
		out = append(out, pid)
	}
	sort.Ints(out)
	return out
}

func statusKB(pid int, key string) (int, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, false
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, key+":") {
			f := strings.Fields(l)
			if len(f) >= 2 {
				v, err := strconv.Atoi(f[1])
				return v, err == nil
			}
		}
	}
	return 0, false
}

func comm(pid int) string {
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	return strings.TrimSpace(string(b))
}

// rss sums VmRSS (MiB) over the processes and their descendants, with a per-process-name breakdown.
func rss(roots []int) (totalMiB float64, byName map[string]float64) {
	byName = map[string]float64{}
	for _, pid := range descendants(roots) {
		if kb, ok := statusKB(pid, "VmRSS"); ok {
			m := float64(kb) / 1024
			totalMiB += m
			byName[comm(pid)] += m
		}
	}
	return
}

type listener struct {
	Addr string
	Port int
}

// listeners returns the TCP LISTEN sockets owned by the processes (and descendants).
func listeners(roots []int) []listener {
	inodes := map[string]bool{}
	for _, pid := range descendants(roots) {
		fds, _ := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		for _, fd := range fds {
			l, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, fd.Name()))
			if err == nil && strings.HasPrefix(l, "socket:[") {
				inodes[strings.TrimSuffix(strings.TrimPrefix(l, "socket:["), "]")] = true
			}
		}
	}
	var out []listener
	for _, f := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		b, _ := os.ReadFile(f)
		for _, l := range strings.Split(string(b), "\n")[1:] {
			p := strings.Fields(l)
			if len(p) < 10 || p[3] != "0A" || !inodes[p[9]] {
				continue
			}
			hp := strings.Split(p[1], ":")
			port, _ := strconv.ParseInt(hp[1], 16, 32)
			out = append(out, listener{Addr: hexAddrToString(hp[0]), Port: int(port)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Port < out[j].Port })
	return out
}

func hexAddrToString(h string) string {
	switch h {
	case "0100007F":
		return "127.0.0.1"
	case "00000000":
		return "0.0.0.0"
	case "00000000000000000000000001000000":
		return "::1"
	case "00000000000000000000000000000000":
		return "::"
	}
	return "other:" + h
}

func loopback(l listener) bool { return l.Addr == "127.0.0.1" || l.Addr == "::1" }

// load1 and memAvailGiB read the shared machine's state for the capacity gate.
func load1() float64 {
	b, _ := os.ReadFile("/proc/loadavg")
	f := strings.Fields(string(b))
	v, _ := strconv.ParseFloat(f[0], 64)
	return v
}

func memAvailGiB() float64 {
	b, _ := os.ReadFile("/proc/meminfo")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "MemAvailable:") {
			kb, _ := strconv.ParseFloat(strings.Fields(l)[1], 64)
			return kb / 1024 / 1024
		}
	}
	return 0
}
