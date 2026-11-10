package main

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/recorder"
)

type router struct {
	cmd     *exec.Cmd
	log     *os.File
	done    chan error
	stopped bool
}

// startRouter starts the launcher in its own process group, with a clean environment so that no
// proxy variable can send the router's worker traffic elsewhere.
func startRouter(path, policy string, base int, out string) (*router, error) {
	log, err := os.Create(filepath.Join(out, "router.log"))
	if err != nil {
		return nil, err
	}
	home := filepath.Join(out, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, err
	}
	args := []string{
		"--host", "127.0.0.1", "--port", strconv.Itoa(base + 2),
		"--worker-urls", "http://127.0.0.1:" + strconv.Itoa(base), "http://127.0.0.1:" + strconv.Itoa(base+1),
		"--policy", policy,
		"--request-timeout-secs", "10800", // HBEP-5: maxRequestDuration + 1 h
		"--prometheus-host", "127.0.0.1", "--prometheus-port", strconv.Itoa(base + 3),
	}
	cmd := exec.Command(path, args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + home, "LANG=C.UTF-8", "NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost"}
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	fmt.Fprintf(log, "# %s %s\n", path, strings.Join(args, " "))
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	r := &router{cmd: cmd, log: log, done: make(chan error, 1)}
	go func() { r.done <- cmd.Wait() }()
	return r, nil
}

// waitReady waits until the router answers /health and has forwarded one request to a recorder.
func (r *router) waitReady(target string, rec *recorder.Recorder) error {
	deadline := time.Now().Add(180 * time.Second)
	client := &http.Client{Timeout: 5 * time.Second}
	for time.Now().Before(deadline) {
		select {
		case err := <-r.done:
			r.done <- err
			return fmt.Errorf("exited before it was ready: %v", err)
		default:
		}
		req, _ := http.NewRequest(http.MethodPost, target+"/v1/chat/completions",
			bytes.NewReader([]byte(`{"model":"`+recorder.Model+`","messages":[{"role":"user","content":"warm-up"}]}`)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(recorder.CaseHeader, "warm-up")
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && len(rec.ByCase("warm-up")) > 0 {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errors.New("not ready after 180 s")
}

var ssUser = regexp.MustCompile(`pid=(\d+),`)

// listeners lists the TCP listeners of the router's process group and refuses any that is not on
// 127.0.0.1 inside [lo, hi]. A router that shows no listener at all is refused too: what cannot be
// seen is not a pass.
func (r *router) listeners(lo, hi int) ([]string, error) {
	pgid := r.cmd.Process.Pid
	out, err := exec.Command("ss", "-H", "-ltnp").Output()
	if err != nil {
		return nil, fmt.Errorf("ss: %v", err)
	}
	var found []string
	var bad []string
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 6 {
			continue
		}
		mine := false
		for _, m := range ssUser.FindAllStringSubmatch(line, -1) {
			pid, _ := strconv.Atoi(m[1])
			if g, err := syscall.Getpgid(pid); err == nil && g == pgid {
				mine = true
			}
		}
		if !mine {
			continue
		}
		addr := f[3]
		found = append(found, addr)
		host, port, _ := strings.Cut(addr, "]:")
		if !strings.HasPrefix(addr, "[") {
			i := strings.LastIndex(addr, ":")
			host, port = addr[:i], addr[i+1:]
		}
		p, _ := strconv.Atoi(port)
		if host != "127.0.0.1" || p < lo || p > hi {
			bad = append(bad, addr)
		}
	}
	if len(found) == 0 {
		return nil, errors.New("no listener of the router's process group is visible")
	}
	if len(bad) > 0 {
		return found, fmt.Errorf("listeners outside 127.0.0.1:%d-%d: %v", lo, hi, bad)
	}
	return found, nil
}

// stop sends TERM to the process group, then KILL after 15 s, and records how it ended and
// whether anything of the group is left.
func (r *router) stop(run *Run) {
	if r.stopped {
		return
	}
	r.stopped = true
	pgid := r.cmd.Process.Pid
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	select {
	case err := <-r.done:
		run.RouterExit = fmt.Sprintf("exited after TERM: %v", err)
	case <-time.After(15 * time.Second):
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		run.RouterExit = fmt.Sprintf("killed after 15 s: %v", <-r.done)
	}
	if err := syscall.Kill(-pgid, 0); !errors.Is(err, syscall.ESRCH) {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		run.RouterExit += "; a process of the group outlived the router and was killed"
	}
	r.log.Close()
}
