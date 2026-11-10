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

// dryNodeFaults are the faults of the node's tools: commands put first on the steps' PATH, so that the serving probe,
// and nothing else, runs them. Each takes the real tool's arguments.
var dryNodeFaults = map[string]map[string]string{
	// The bytes the node sends cross with their line feed turned into a carriage return, as a pty with OPOST would.
	"bytes-to-host": {"cat": "#!/bin/sh\nexec tr '\\012' '\\015' < \"$1\"\n"},
	// The bytes the host sends arrive with their zero byte changed.
	"bytes-to-node": {"head": "#!/bin/sh\n@HEAD@ \"$@\" | tr '\\000' '\\001'\n"},
	"socket-mode":   {"stat": "#!/bin/sh\necho 755\n"},
	"unreadable":    {"sha256sum": "#!/bin/sh\necho not-a-digest\n"},
}

// dryHostDialedFault reports whether fault is one of the rows' dry faults ("" is the healthy run).
func dryHostDialedFault(fault string) bool {
	_, relay := dryRelayFaults[fault]
	_, node := dryNodeFaults[fault]
	return fault == "" || fault == "slot-held" || relay || node
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
	// Short paths: the job's TMPDIR holds the agent's socket (at most 107 bytes) and the login node's directory its own.
	base, err := os.MkdirTemp("", "hdd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	ctlDir := filepath.Join(base, "c")
	if err := os.Mkdir(ctlDir, 0o700); err != nil {
		t.Fatal(err)
	}
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
		for name, body := range tools {
			if err := os.WriteFile(filepath.Join(stubs, name), []byte(strings.ReplaceAll(body, "@HEAD@", head)), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		stepPath = stubs + ":" + stepPath
	}
	g := newHostDialedRig(t, ctlDir, dryRelayFaults[fault], func(s *providerctl.RelaySettings) {
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
	w, _ := dryWorld(t, r, fault, out, dryShape{cpus: "1", memory: "1Gi"}, agent, jobEnv, dryExtra{
		run: func(m *fakeslurm.RunMode) {
			m.JobDir = filepath.Join(base, "j")
			m.KillWait = 2 * time.Second
			m.StepEnv = map[string]string{"PATH": stepPath}
		},
		login: login,
		kit:   func(k *worldKit) { k.hostDialed, k.poll = g, time.Second },
	})
	// Short bounds, so a fault whose session never comes up ends in seconds; the probe's still lets a connection with no
	// session wait out the agent's own wait for one (hostdial.DefaultWait) and then read.
	w.pace, w.probeRead = 200*time.Millisecond, 5
	w.shorten = func(d time.Duration) time.Duration {
		if d == hostDialedProbeWait {
			return hostdial.DefaultWait + time.Duration(w.probeRead)*time.Second + 10*time.Second
		}
		return min(d, 20*time.Second)
	}
	g.startHostSide(t, w)
	return w, g
}
