// Command fakecontroller stands in for hpcburst-controller in the launcher's tests. It accepts
// exactly the flags of the controller's command line (--state-dir, --port-base and --health-addr,
// all required, and optionally --front-mode and --front-addr), so a launcher that passes another
// one, such as the earlier --kubeconfig, --credentials-dir or --agent-builds-dir, fails its start; records its argv and environment; serves GET /healthz on --health-addr; and exits on
// SIGTERM. Its behaviour is steered through files in the directory $FAKECTRL_DIR (default: the
// directory of this binary), so a test changes it while it runs:
//
//	exit          its content is an exit code: the process removes the file and exits with it
//	always-exit   the process exits with the code in the file at once, at every start
//	ignore-term   SIGTERM is ignored
//	slow-term     its content is milliseconds: SIGTERM is answered with exit 0 after that long
//	unhealthy     /healthz answers 503
//	no-health     the process serves nothing
//
// It records argv-<pid>.json and env-<pid>.json, and appends "<pid> <unix nanoseconds>" to the file
// starts, before it serves anything.
//
// Installed under the name hpcburst-front it stands in for the front instead (issue #64), and then
// accepts exactly the front's command line as the launcher writes it: --config, --state-dir,
// --port-base and --health-addr, all required, and optionally --mode. In that role a file usage in
// $FAKECTRL_DIR holds the usage state its /healthz reports in the front's usage member (HBEP-8:
// "ok" or "failing"); without the file it reports "ok".
package main

import (
	"encoding/json"
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
)

func main() {
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

	term := make(chan os.Signal, 1)
	if _, ignore := ctl("ignore-term"); ignore {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(term, syscall.SIGTERM)
	}
	if _, off := ctl("no-health"); !off {
		l, err := net.Listen("tcp", *health)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fakecontroller:", err)
			os.Exit(1)
		}
		go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

func record(path string, v any) {
	b, _ := json.Marshal(v)
	tmp := path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = os.Rename(tmp, path)
	}
}
