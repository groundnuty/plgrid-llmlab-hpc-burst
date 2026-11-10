package conformance

// The stale relay's lifecycle (PR 306's review, B3): it is a relay like the kept ones, so its ssh must die with the
// process that started it, as the relay owner's do (hostdial's locked starter thread with Pdeathsig), whatever ends that
// process. The test runs this test binary as a dry TestReachHostDialed whose stale relay's ssh stands still (the dry fault
// stale-stall), stops it with TERM while it does, and looks for the relay's process by its argv[0], a path in the child's
// own directory: never by a name another run could have.

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
)

func TestAStaleRelayDiesWithItsTest(t *testing.T) {
	rehearsal.RequireTools(t)
	dryAgentBinary(t)
	agentBin := os.Getenv("HPCBURST_ALPHA_AGENT_BIN")
	if agentBin == "" {
		agentBin = dryAgent.path
	}
	dir, err := os.MkdirTemp("", "k")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if n := killUnder(dir); n > 0 {
			t.Logf("stopped %d process(es) the killed child left in its directory", n)
		}
		_ = os.RemoveAll(dir)
	})
	out := filepath.Join(dir, "out")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self, "-test.run", "^TestReachHostDialed$", "-test.count=1", "-test.v")
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "HPCBURST_CONF") && !strings.HasPrefix(kv, "HPCBURST_ALPHA_") && !strings.HasPrefix(kv, "TMPDIR=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "HPCBURST_ALPHA_DRY=1", "HPCBURST_ALPHA_ROW=reach-host-dialed-athena", "HPCBURST_ALPHA_DRY_FAULT=stale-stall",
		"HPCBURST_ALPHA_OUT="+out, "HPCBURST_ALPHA_AGENT_BIN="+agentBin, "TMPDIR="+dir)
	var log bytes.Buffer
	cmd.Stdout, cmd.Stderr = &log, &log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	// The stale relay's wrapper writes <itself>.stalled, then stands still under the argv[0] <itself>.stale.
	var stalled string
	for end := time.Now().Add(3 * time.Minute); stalled == ""; time.Sleep(50 * time.Millisecond) {
		if m, _ := filepath.Glob(filepath.Join(dir, "hdw*", "ssh.stalled")); len(m) > 0 {
			stalled = m[0]
			break
		}
		select {
		case <-exited:
			t.Fatalf("the child ended before its stale relay stood still:\n%s", log.String())
		default:
		}
		if time.Now().After(end) {
			_ = cmd.Process.Kill()
			<-exited
			t.Fatalf("the stale relay never stood still within 3 min:\n%s", log.String())
		}
	}
	marker := strings.TrimSuffix(stalled, ".stalled") + ".stale"
	waitFor(t, 30*time.Second, func() bool { return len(procsWithArg0(marker)) > 0 })
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		<-exited
		t.Error("the child did not end within 30 s of its TERM")
	}
	// The relay's process ends with its parent's thread (a parent-death signal), at once; the wait is on state.
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		left := procsWithArg0(marker)
		if len(left) == 0 {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the stale relay's ssh outlived its test: %d process(es) 10 s after the test process ended", len(left))
		}
	}
}

// procsWithArg0 is the processes whose argv[0] is arg0.
func procsWithArg0(arg0 string) []int {
	var pids []int
	ents, _ := os.ReadDir("/proc")
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if first, _, _ := bytes.Cut(b, []byte{0}); string(first) == arg0 {
			pids = append(pids, pid)
		}
	}
	return pids
}

// killUnder kills the processes whose command line names a path under dir, a directory of this test's own, and says how
// many there were.
func killUnder(dir string) int {
	n := 0
	ents, _ := os.ReadDir("/proc")
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if bytes.Contains(b, []byte(dir+"/")) {
			if syscall.Kill(pid, syscall.SIGKILL) == nil {
				n++
			}
		}
	}
	return n
}
