package conformance

// The reach-host-dialed rows over a fake Slurm in run mode (issue #305), the way a dry TestReachHostDialed runs them for
// hack/livechecks. Every part of a live run is the real one but the cluster: the Provider's controller and its relay
// owner, the agent API server, the agent binary (built for this machine) as the job, and each relay a real ssh client
// to a user-level sshd login node (internal/rehearsal) whose forced command hands the line to the fake, whose srun
// --overlap step runs the agent's own "relay <channel>" as a host process. So both relay ends run in separate
// processes, with the srun step's standard input and output between them, and the bytes the checks read cross all of it.
//
// The faults stand in for what a cluster could do wrong, each where it would happen: in the relays' ssh (the wrapper's
// fault snippet: a line without -u, a login banner before the preamble, another epoch, a channel whose ssh fails) and in
// the node's tools that the serving probe uses (a byte changed each way, a socket directory of another mode, a digest
// that cannot be read). slot-held keeps the job pending past its start wait: NOT RUN.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// dryRelayFaults are the faults of the relays' ssh: a bash snippet of the wrapper (relayWrapperTemplate), run with the
// remote line in $last before it is written down and ssh is run.
var dryRelayFaults = map[string]string{
	"no-u":         `last=${last/#srun -u /srun }`,
	"banner":       `case $last in "srun "*) echo "Welcome to login.example" ;; esac`,
	"epoch":        fmt.Sprintf(`last=${last/ --epoch %d / --epoch %d }`, hostDialedEpoch, hostDialedEpoch-2),
	"control-down": `case $last in *" relay control "*) exit 255 ;; esac`,
	"serving-down": `case $last in *" relay serving "*) exit 255 ;; esac`,
}

// dryStaleFaults are the faults of the stale relays' own ssh, a snippet of their wrapper as above. epoch-blind gives the
// agent the stale relays with the kept relays' epoch, which an agent that compares only sequences sees too: the epoch check
// must then fail. stale-stall holds the stale relay's ssh as if it hung connecting, its process named after the wrapper
// (in the test's own directory), for TestAStaleRelayDiesWithItsTest.
var dryStaleFaults = map[string]string{
	"epoch-blind": fmt.Sprintf(`last=${last/ --epoch %d / --epoch %d }`, hostDialedEpoch-1, hostDialedEpoch),
	"stale-stall": `case $last in "srun "*) : > "$0.stalled"; exec -a "$0.stale" sleep 600 ;; esac`,
}

// The faults of the job and the run around it: the agent ends its job FAILED just before it is told to stop, as a node
// failure would (crash-at-stop); the job's TMPDIR is too long for a socket, so the agent puts it under /dev/shm, and the
// node's grep does not find the agent's warning of it, as a changed wording would not (shm-fallback-quiet); the node has no
// Apptainer, so the agent stops at its start (no-apptainer).
var dryJobFaults = map[string]bool{"crash-at-stop": true, "shm-fallback-quiet": true, "no-apptainer": true}

// dryNodeFaults are the faults of the node's tools: commands put first on the steps' PATH, so that the serving probe,
// and nothing else, runs them. Each takes the real tool's arguments.
var dryNodeFaults = map[string]map[string]string{
	// The bytes the node sends cross with their line feed turned into a carriage return, as a pty with OPOST would.
	"bytes-to-host": {"cat": "#!/bin/sh\nexec tr '\\012' '\\015' < \"$1\"\n"},
	// The bytes the host sends arrive with their zero byte changed.
	"bytes-to-node": {"head": "#!/bin/sh\n@HEAD@ \"$@\" | tr '\\000' '\\001'\n"},
	"socket-mode":   {"stat": "#!/bin/sh\necho 755\n"},
	"unreadable":    {"sha256sum": "#!/bin/sh\necho not-a-digest\n"},
	// The node's grep finds no fallback warning (with shm-fallback-quiet's long TMPDIR).
	"shm-fallback-quiet": {"grep": "#!/bin/sh\ncase \"$*\" in *\"is under the fallback\"*) echo 0; exit 1 ;; esac\nexec @GREP@ \"$@\"\n"},
}

// dryHostDialedFault reports whether fault is one of the rows' dry faults ("" is the healthy run).
func dryHostDialedFault(fault string) bool {
	_, relay := dryRelayFaults[fault]
	_, node := dryNodeFaults[fault]
	_, stale := dryStaleFaults[fault]
	return fault == "" || fault == "slot-held" || relay || node || stale || dryJobFaults[fault]
}

// dryAgent is the agent binary of the dry runs: HPCBURST_ALPHA_AGENT_BIN when set (hack/livechecks builds it, since its
// go is a stand-in), else ./cmd/hb-agent built once for this machine into this process's own directory, which TestMain
// removes.
var dryAgent struct {
	once sync.Once
	dir  string
	path string
	err  error
}

func dryAgentBinary(t testing.TB) []byte {
	t.Helper()
	if p := os.Getenv("HPCBURST_ALPHA_AGENT_BIN"); p != "" {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("HPCBURST_ALPHA_AGENT_BIN: %v", err)
		}
		return b
	}
	dryAgent.once.Do(func() {
		dryAgent.dir, dryAgent.err = os.MkdirTemp("", "hda")
		if dryAgent.err != nil {
			return
		}
		dryAgent.path = filepath.Join(dryAgent.dir, "hb-agent")
		cmd := exec.Command("go", "build", "-o", dryAgent.path, "./cmd/hb-agent")
		cmd.Dir = filepath.Join("..", "..", "..")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			dryAgent.err = fmt.Errorf("go build ./cmd/hb-agent: %v\n%s", err, out)
		}
	})
	if dryAgent.err != nil {
		t.Fatal(dryAgent.err)
	}
	b, err := os.ReadFile(dryAgent.path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// dryReachHostDialed is the dry run of TestReachHostDialed.
func dryReachHostDialed(t *testing.T) {
	t.Helper()
	// Before anything else, as dryWorld does again: a dry pass must never stand for a live one.
	for _, k := range os.Environ() {
		if strings.HasPrefix(k, "HPCBURST_CONF") {
			t.Fatalf("a dry run refuses to start with live settings present (%s)", strings.SplitN(k, "=", 2)[0])
		}
	}
	row := os.Getenv("HPCBURST_ALPHA_ROW")
	fault := os.Getenv("HPCBURST_ALPHA_DRY_FAULT")
	w, g := dryHostDialedWorld(t, row, fault, os.Getenv("HPCBURST_ALPHA_OUT"))
	hostDialedBody(t, w, g, row)
}

// dryHostDialedWorld is the world and the rig of a dry run of a reach-host-dialed row with the fault.
func dryHostDialedWorld(t *testing.T, row, fault, out string) (*world, *hostDialedRig) {
	t.Helper()
	r, ok := alpha.RowByName(row)
	if !ok || !strings.HasPrefix(row, "reach-host-dialed-") {
		t.Fatalf("HPCBURST_ALPHA_ROW is %q, not a reach-host-dialed row", row)
	}
	if !dryHostDialedFault(fault) {
		t.Fatalf("the dry fault %q is not one of the row's", fault)
	}
	rehearsal.RequireTools(t)
	agent := dryAgentBinary(t)
	// Short paths, each a directory of its own right under TMPDIR: the job's TMPDIR holds the agent's socket (at most 107
	// bytes), the login node's directory its own sockets, and the relays' control directory a socket path sshrun holds to
	// 90 bytes. Through the script TMPDIR is a short one of the caller's (dryWrapper).
	base, err := os.MkdirTemp("", "d")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	ctlDir, err := os.MkdirTemp("", "c")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(ctlDir) })
	stepPath := os.Getenv("PATH")
	if tools, ok := dryNodeFaults[fault]; ok {
		stubs := filepath.Join(base, "stubs")
		if err := os.Mkdir(stubs, 0o700); err != nil {
			t.Fatal(err)
		}
		head, err := exec.LookPath("head")
		if err != nil {
			t.Fatal(err)
		}
		grep, err := exec.LookPath("grep")
		if err != nil {
			t.Fatal(err)
		}
		for name, body := range tools {
			if err := os.WriteFile(filepath.Join(stubs, name), []byte(strings.NewReplacer("@HEAD@", head, "@GREP@", grep).Replace(body)), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		stepPath = stubs + ":" + stepPath
	}
	g := newHostDialedRig(t, ctlDir, dryRelayFaults[fault], dryStaleFaults[fault], func(s *providerctl.RelaySettings) {
		// A register and slots of the world's own, so that parallel worlds on one login host do not refuse each other.
		s.Hosts, s.Slots = hostdial.NewHosts(), hostdial.NewSlots(0)
		s.MinBackoff, s.MaxBackoff, s.StopGrace = 50*time.Millisecond, 500*time.Millisecond, 2*time.Second
	})
	jobEnv := map[string]string{
		// The agent needs no Apptainer with no Instance, but stops at its start without one on the node; the path is
		// never run.
		"HPCBURST_APPTAINER": "/nonexistent/apptainer",
		"PATH":               os.Getenv("PATH"),
	}
	if fault == "no-apptainer" {
		// No setting and none on the job's PATH, whatever this machine has.
		delete(jobEnv, "HPCBURST_APPTAINER")
		var keep []string
		for _, d := range filepath.SplitList(os.Getenv("PATH")) {
			if _, err := os.Stat(filepath.Join(d, "apptainer")); err != nil {
				keep = append(keep, d)
			}
		}
		jobEnv["PATH"] = strings.Join(keep, string(filepath.ListSeparator))
	}
	jobDir := filepath.Join(base, "j")
	if fault == "shm-fallback-quiet" {
		// A TMPDIR of 77 bytes, <JobDir>/1001/tmp (the fake's first job): the relay socket's path under it would pass 107
		// bytes (hostdial's socketDir), so the agent falls back to /dev/shm, this machine's; its run directory's longest
		// socket still fits (internal/agent, MakeRunDir), so it runs on. The directory under /dev/shm is the agent's own and
		// goes when it stops; the clean-up below removes one a killed agent left.
		pad := 68 - len(base) - 1
		if pad < 1 {
			t.Fatalf("TMPDIR is too long for this fault's job directory (%d bytes)", len(base))
		}
		jobDir = filepath.Join(base, strings.Repeat("l", pad))
		if n := len(jobDir + "/1001/tmp"); n != 77 {
			t.Fatalf("the job's TMPDIR would be %d bytes, not 77", n)
		}
		t.Cleanup(func() { removeShmSocketDir(t, out) })
	}
	login := func(plat *fakeslurm.Platform, creds, home string) {
		dir := filepath.Join(base, "n")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		node, err := rehearsal.StartLoginNode(rehearsal.LoginConfig{Dir: dir, Platform: plat,
			Login: fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": home, "PATH": os.Getenv("PATH")}}})
		if err != nil {
			t.Fatalf("the login node: %v", err)
		}
		t.Cleanup(func() {
			if err := node.Close(); err != nil {
				t.Errorf("the login node did not close cleanly: %v", err)
			}
		})
		// The relays reach the fake through it, with the Provider's credentials; the controller's own calls stay on the
		// in-process login. The file is the run's, mode 600; it is written over the dry world's example credentials.
		if err := node.WriteCredentials(creds); err != nil {
			t.Fatal(err)
		}
	}
	w, plat := dryWorld(t, r, fault, out, dryShape{cpus: "1", memory: "1Gi"}, agent, jobEnv, dryExtra{
		run: func(m *fakeslurm.RunMode) {
			m.JobDir = jobDir
			m.KillWait = 2 * time.Second
			m.StepEnv = map[string]string{"PATH": stepPath}
		},
		login: login,
		kit:   func(k *worldKit) { k.hostDialed, k.poll = g, time.Second },
	})
	// Short bounds, so a fault whose session never comes up ends in seconds: a fault's run waits out its bounds by design
	// (what it breaks never comes), and a healthy run ends each wait at once. The probe keeps its own bound, which it never
	// reaches: it ends by itself once a connection with no session has waited out the agent's wait for one
	// (hostdial.DefaultWait) and its read of 5 s.
	w.pace, w.probeRead = 200*time.Millisecond, 5
	limit := 20 * time.Second
	if fault != "" {
		// A fault fails the check it stands for whether or not the rest comes up in time, so its bounds can be short.
		limit = 5 * time.Second
	}
	w.shorten = func(d time.Duration) time.Duration { return min(d, limit) }
	if fault == "crash-at-stop" {
		w.beforeStop = func(jobID string) {
			id, err := strconv.Atoi(jobID)
			if err != nil {
				t.Fatalf("job ID %q", jobID)
			}
			if err := plat.End(id, string(fakeslurm.Failed)); err != nil {
				t.Errorf("the fake did not end the job: %v", err)
			}
		}
	}
	g.startHostSide(t, w)
	return w, g
}

// removeShmSocketDir removes the agent's socket directory under /dev/shm that the run's record names, when an agent that
// was killed left it: a directory of this run's own, named in its record, never one found by a pattern.
func removeShmSocketDir(t *testing.T, out string) {
	b, err := os.ReadFile(filepath.Join(out, alpha.RecordFile))
	if err != nil {
		return
	}
	m := regexp.MustCompile(`"socket": "(/dev/shm/hb-relay-[0-9]+)/relay\.sock"`).FindSubmatch(b)
	if m == nil {
		return
	}
	if _, err := os.Stat(string(m[1])); err == nil {
		t.Logf("the agent left its socket's directory under /dev/shm; removing it")
		_ = os.RemoveAll(string(m[1]))
	}
}
