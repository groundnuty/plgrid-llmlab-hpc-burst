// Command fakecontroller stands in for hpcburst-controller in the launcher's tests. It accepts
// exactly the flags of the controller's command line (--state-dir, --port-base and --health-addr,
// all required, and optionally --front-mode and --front-addr), so a launcher that passes another
// one, such as the earlier --kubeconfig, --credentials-dir or --agent-builds-dir, fails its start; records its argv and environment; serves GET /healthz on --health-addr; and exits on
// SIGTERM. Its behaviour is steered through files in the directory $FAKECTRL_DIR (default: the
// directory of this binary), so a test changes it while it runs:
//
//	exit          its content is an exit code: the process removes the file and exits with it
//	always-exit   the process exits with the code in the file at once, at every start
//	exit-once-recorded  like always-exit, but the process first waits (at most 10 s) until
//	              <state-dir>/controller/supervisor.json names its PID as a running entry: the
//	              supervisor writes that only after it has recorded the process's identity, so
//	              the exit cannot overtake that record (issue #130)
//	ignore-term   SIGTERM is ignored
//	slow-term     its content is milliseconds: SIGTERM is answered with exit 0 after that long
//	unhealthy     /healthz answers 503
//	caches-failing   the readiness check "caches" fails (the lifecycle controller's caches have
//	              not synced)
//	leading-failing  the readiness check "leading" fails (it does not hold its Lease)
//	readyz-before-split  the controller registers the one check "serving" of controllers before
//	              the split instead of "caches" and "leading": /readyz/caches and /readyz/leading
//	              answer 404, and "serving" fails while the file serving-failing exists
//	              Without these files every readiness check passes. /readyz is served by
//	              controller-runtime's own handler, mounted as its manager mounts it, so the
//	              statuses (200, 500, and 404 for an unknown check) and bodies are its.
//	no-health     the process serves nothing
//	listen-tcp    one host:port per line: the process also listens there (TCP), and serves
//	              /healthz there too
//	listen-udp    one host:port per line: the process also binds a UDP socket there; after
//	              binding every extra socket it writes extra-bound-<pid>, holding their addresses
//
// It records argv-<pid>.json and env-<pid>.json, and appends "<pid> <unix nanoseconds>" to the file
// starts, before it serves anything.
//
// Installed under the name hpcburst-front it stands in for the front instead (issue #64), and then
// accepts exactly the front's command line as the launcher writes it: --config, --state-dir,
// --port-base and --health-addr, all required, and optionally --mode. In that role a file usage in
// $FAKECTRL_DIR holds the usage state its /healthz reports in the front's usage member (HBEP-8:
// "ok" or "failing"); without the file it reports "ok". A file bind-gate makes it bind its health
// port late, as the real front does after its first lists: it writes front-waiting (its PID) and
// waits, at most 30 s, for a file bind-now before it binds; a port taken by then makes it exit 1.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func main() {
	// controller-runtime's healthz handler logs each failing check, and without a sink it prints a
	// notice and a goroutine dump to stderr.
	log.SetLogger(logr.Discard())
	fs := flag.NewFlagSet("fakecontroller", flag.ContinueOnError)
	asFront := filepath.Base(os.Args[0]) == "hpcburst-front"
	var config *string
	if asFront {
		config = fs.String("config", "", "")
		fs.String("mode", "", "")
	} else {
		for _, name := range []string{"front-mode", "front-addr"} {
			fs.String(name, "", "")
		}
	}
	stateDir := fs.String("state-dir", "", "")
	portBase := fs.Int("port-base", 0, "")
	health := fs.String("health-addr", "", "")
	if err := fs.Parse(os.Args[1:]); err != nil || fs.NArg() > 0 || *stateDir == "" || *portBase <= 0 || *health == "" || (asFront && *config == "") {
		fmt.Fprintln(os.Stderr, "fakecontroller: bad command line:", os.Args[1:])
		os.Exit(2)
	}
	dir := os.Getenv("FAKECTRL_DIR")
	if dir == "" {
		exe, _ := os.Executable()
		dir = filepath.Dir(exe)
	}
	pid := os.Getpid()
	record(filepath.Join(dir, fmt.Sprintf("argv-%d.json", pid)), os.Args[1:])
	record(filepath.Join(dir, fmt.Sprintf("env-%d.json", pid)), os.Environ())
	if f, err := os.OpenFile(filepath.Join(dir, "starts"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		fmt.Fprintf(f, "%d %d\n", pid, time.Now().UnixNano())
		f.Close()
	}
	ctl := func(name string) (string, bool) {
		b, err := os.ReadFile(filepath.Join(dir, name))
		return strings.TrimSpace(string(b)), err == nil
	}
	if code, ok := ctl("always-exit"); ok {
		n, _ := strconv.Atoi(code)
		os.Exit(n)
	}
	if code, ok := ctl("exit-once-recorded"); ok {
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && !recordedRunning(*stateDir, pid); {
			time.Sleep(5 * time.Millisecond)
		}
		n, _ := strconv.Atoi(code)
		os.Exit(n)
	}

	term := make(chan os.Signal, 1)
	if _, ignore := ctl("ignore-term"); ignore {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(term, syscall.SIGTERM)
	}
	if _, gated := ctl("bind-gate"); asFront && gated {
		_ = os.WriteFile(filepath.Join(dir, "front-waiting"), []byte(strconv.Itoa(pid)), 0o644)
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if _, open := ctl("bind-now"); open {
				break
			}
		}
	}
	readyz := readyzHandler(ctl)
	if _, off := ctl("no-health"); !off {
		l, err := net.Listen("tcp", *health)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fakecontroller:", err)
			os.Exit(1)
		}
		go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !asFront && (r.URL.Path == "/readyz" || strings.HasPrefix(r.URL.Path, "/readyz/")) {
				readyz.ServeHTTP(w, r)
				return
			}
			if _, bad := ctl("unhealthy"); bad || r.URL.Path != "/healthz" {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if !asFront {
				w.WriteHeader(http.StatusOK)
				return
			}
			state, ok := ctl("usage")
			if !ok {
				state = "ok"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"synced": true, "usage": map[string]any{"state": state, "lostRecords": 0}})
		}))
	}
	bindExtra(dir, pid, ctl)
	tick := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-term:
			if ms, ok := ctl("slow-term"); ok {
				n, _ := strconv.Atoi(ms)
				time.Sleep(time.Duration(n) * time.Millisecond)
			}
			os.Exit(0)
		case <-tick.C:
			if code, ok := ctl("exit"); ok {
				_ = os.Remove(filepath.Join(dir, "exit"))
				n, _ := strconv.Atoi(code)
				os.Exit(n)
			}
		}
	}
}

// readyzHandler is the lifecycle controller's /readyz: controller-runtime's own healthz.Handler
// with the checks the controller registers, mounted the way the manager mounts it (pkg/manager/
// internal.go:308-310: the prefix is stripped, for /readyz and /readyz/). Each check reads its
// steering file when it is asked, so a test changes the answer while the process runs.
func readyzHandler(ctl func(string) (string, bool)) http.Handler {
	failing := func(file, why string) healthz.Checker {
		return func(*http.Request) error {
			if _, bad := ctl(file); bad {
				return errors.New(why)
			}
			return nil
		}
	}
	split := http.StripPrefix("/readyz", &healthz.Handler{Checks: map[string]healthz.Checker{
		"caches":  failing("caches-failing", "caches not synced"),
		"leading": failing("leading-failing", "not leading its Lease"),
	}})
	beforeSplit := http.StripPrefix("/readyz", &healthz.Handler{Checks: map[string]healthz.Checker{
		"serving": failing("serving-failing", "not leading, or caches not synced"),
	}})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, old := ctl("readyz-before-split"); old {
			beforeSplit.ServeHTTP(w, r)
			return
		}
		split.ServeHTTP(w, r)
	})
}

// recordedRunning reports whether the supervisor's record in stateDir names pid as a running
// entry's process.
func recordedRunning(stateDir string, pid int) bool {
	b, err := os.ReadFile(filepath.Join(stateDir, "controller", "supervisor.json"))
	if err != nil {
		return false
	}
	var f struct {
		Entries []struct {
			State string `json:"state"`
			Proc  *struct {
				PID int `json:"pid"`
			} `json:"proc"`
		} `json:"entries"`
	}
	if json.Unmarshal(b, &f) != nil {
		return false
	}
	for _, e := range f.Entries {
		if e.State == "running" && e.Proc != nil && e.Proc.PID == pid {
			return true
		}
	}
	return false
}

// held keeps the extra UDP sockets referenced for the life of the process.
var held []net.PacketConn

// bindExtra opens the extra sockets that listen-tcp and listen-udp name and keeps them open for
// the life of the process; a socket that cannot be bound makes it exit 1.
func bindExtra(dir string, pid int, ctl func(string) (string, bool)) {
	var bound []string
	tcp, okT := ctl("listen-tcp")
	udp, okU := ctl("listen-udp")
	if !okT && !okU {
		return
	}
	for _, a := range strings.Fields(tcp) {
		l, err := net.Listen("tcp", a)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fakecontroller:", err)
			os.Exit(1)
		}
		go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
		bound = append(bound, "tcp "+l.Addr().String())
	}
	for _, a := range strings.Fields(udp) {
		c, err := net.ListenPacket("udp", a)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fakecontroller:", err)
			os.Exit(1)
		}
		held = append(held, c) // a socket nothing refers to is closed by its finalizer
		bound = append(bound, "udp "+c.LocalAddr().String())
	}
	record(filepath.Join(dir, fmt.Sprintf("extra-bound-%d", pid)), bound)
}

func record(path string, v any) {
	b, _ := json.Marshal(v)
	tmp := path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = os.Rename(tmp, path)
	}
}
