package livechecks

// The dry runs of the host-dialed relay rows through the script (issue #305). The row's own test is compiled and run for
// real by the script, as it runs it live: the script reads the settings, builds, takes the slot, runs the binary, reads
// its verbose log for the verdict and cleans up. What differs from a live run is what the dry wrapper (alpha_dry_test.go)
// takes: the cluster. The binary then runs the real Provider controller and its relay owner, the real agent API server and
// the real agent (built here for this machine, since the stub go builds nothing real) over a fake Slurm behind a
// user-level sshd login node, with each relay a real ssh to it whose srun step carries the session over its standard
// streams. The checks' names are written out here from the issue, never read from the package.

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
)

// The agent of the dry runs: HPCBURST_LIVECHECKS_AGENT_BIN when set (a developer builds it under the shared build lock),
// else ./cmd/hb-agent built once for this machine into the first test's own directory.
var dryAgentBuild struct {
	once sync.Once
	path string
	err  error
}

func realAgentBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("HPCBURST_LIVECHECKS_AGENT_BIN"); p != "" {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("HPCBURST_LIVECHECKS_AGENT_BIN names no file: %v", err)
		}
		return p
	}
	dryAgentBuild.once.Do(func() {
		dir, err := os.MkdirTemp("", "lca")
		if err != nil {
			dryAgentBuild.err = err
			return
		}
		dryAgentBuild.path = filepath.Join(dir, "hb-agent")
		cmd := exec.Command("go", "build", "-o", dryAgentBuild.path, "./cmd/hb-agent")
		cmd.Dir = repoRoot(t)
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if b, err := cmd.CombinedOutput(); err != nil {
			dryAgentBuild.err = err
			t.Logf("go build ./cmd/hb-agent: %s", b)
		}
	})
	if dryAgentBuild.err != nil {
		t.Fatalf("the agent for the dry runs: %v", dryAgentBuild.err)
	}
	return dryAgentBuild.path
}

// hostDialedDryWorld is a world whose test binary is the dry conformance one, with the agent and a short directory for
// the run's sockets in the stub's environment (dryWrapper's host-dialed case). The fake login node needs OpenSSH, so a
// machine without it skips here, before the script runs: a skip inside the binary would be the script's failure.
func hostDialedDryWorld(t *testing.T) (*world, []string) {
	t.Helper()
	rehearsal.RequireTools(t)
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_CONF_TEST", confPkg)
	agent := realAgentBinary(t)
	short, err := os.MkdirTemp("", "h")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(short) })
	w := hostDialedWorld(t)
	w.installDry(dryWrapper, real, filepath.Join(repoRoot(t), confPkg))
	return w, []string{"STUB_AGENT_BIN=" + agent, "STUB_SHORT_TMP=" + short}
}

// The checks of the text to post, by the fault that stands for each (the issue's list: both sessions, srun -u, the
// rendezvous, the preamble, the epoch and the 256-byte check; and a state that cannot be read).
var hostDialedFaultLines = map[string]string{
	"no-u":          "- srun -u: FAIL: ",
	"banner":        "- the preamble: FAIL: ",
	"epoch":         "- the epoch: FAIL: ",
	"control-down":  "- the control session: FAIL: ",
	"serving-down":  "- the serving session: FAIL: ",
	"bytes-to-host": "- the 256-byte check (HBEP-4, open question 16): FAIL: node to host",
	"bytes-to-node": "- the 256-byte check (HBEP-4, open question 16): FAIL: host to node",
	"socket-mode":   "- the rendezvous: FAIL: the socket's directory is mode 755, not 700",
	"unreadable":    "- the run's state: FAIL: the serving probe's report could not be read",
}

var hostDialedOKLines = []string{
	"- the control session: ok: up: the agent registered over the control relay",
	"- the serving session: ok: up: a stream the job opened crossed it to the serving port",
	"- srun -u: ok: every relay ran srun -u --overlap --jobid=<the job> --ntasks=1 <the staged agent> relay <channel>",
	"- the rendezvous: ok: the agent's socket was in a mode-700 hb-relay-<random> directory under the job's $TMPDIR",
	"- the preamble: ok: each relay's first line was hpcburst-relay/1 <the Allocation's UID>",
	"- the epoch: ok: both relays carried epoch 7; a control relay of epoch 6 was refused as superseded",
	"- the 256-byte check (HBEP-4, open question 16): ok: all 256 byte values crossed the serving session intact both ways",
	"- frps and frpc: not run: ",
}

func TestEveryHostDialedRowPassesThroughTheScriptOverTheFake(t *testing.T) {
	for _, s := range hostDialedShapes {
		t.Run(s.row, func(t *testing.T) {
			t.Parallel()
			w, env := hostDialedDryWorld(t)
			res := w.run(env, "--run", s.row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d, want 0:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": tests: 1 passed, 0 failed, 0 skipped", s.row+": PASS", "cleanup: left behind 0")
			posts := w.postsOf(s.row)
			for _, want := range append([]string{s.row + ": PASS (posts to #305 and #145)"}, hostDialedOKLines...) {
				if !strings.Contains(posts, want) {
					t.Errorf("the text to post lacks %q:\n%s", want, posts)
				}
			}
			postsAreHostDialedClean(t, posts)
			log, _ := os.ReadFile(filepath.Join(w.out(), s.row+".log"))
			if !strings.Contains(string(log), "DRY RUN") {
				t.Errorf("the test's log does not say it was a dry run:\n%s", log)
			}
		})
	}
}

// Each fault fails the row through the script, exit 1, and the text to post says FAIL on the check it stands for.
func TestEachHostDialedFaultFailsTheRowThroughTheScript(t *testing.T) {
	for fault, line := range hostDialedFaultLines {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			row := "reach-host-dialed-athena"
			w, env := hostDialedDryWorld(t)
			res := w.run(append(env, "STUB_DRY_FAULT="+fault), "--run", row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			w.contains(res, row+": tests: 0 passed, 1 failed, 0 skipped", row+": FAIL", "cleanup: left behind 0")
			posts := w.postsOf(row)
			for _, want := range []string{row + ": FAIL (posts to #305 and #145)", line} {
				if !strings.Contains(posts, want) {
					t.Errorf("the text to post lacks %q:\n%s", want, posts)
				}
			}
			postsAreHostDialedClean(t, posts)
		})
	}
}

// The owner's other job holds the slot, so this run's job pends past its start wait: NOT RUN, exit 3, the line, the job
// cancelled by its tag, and no text to post. Under QOS now on Helios, under the partition's own QOS on Athena.
func TestAHostDialedRowWhoseSlotIsHeldIsNotRunThroughTheScript(t *testing.T) {
	for _, s := range hostDialedShapes {
		t.Run(s.row, func(t *testing.T) {
			t.Parallel()
			w, env := hostDialedDryWorld(t)
			res := w.run(append(env, "STUB_DRY_FAULT=slot-held"), "--run", s.row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": tests: 0 passed, 0 failed, 1 skipped", s.row+": NOT RUN (a job did not start within 10 min of its acceptance and was cancelled by its tag; not a failure)", "cleanup: left behind 0")
			if m, _ := filepath.Glob(filepath.Join(w.out(), s.row+"-*", "posts.md")); len(m) != 0 {
				t.Errorf("a NOT RUN run left a text to post: %v", m)
			}
		})
	}
}

// A dry binary refuses live settings, so a dry pass can never stand for a live one.
func TestADryHostDialedBinaryRefusesLiveSettings(t *testing.T) {
	w, env := hostDialedDryWorld(t)
	w.installDry(dryWrapperKeepsLive, realTestBinary(t, "HPCBURST_LIVECHECKS_CONF_TEST", confPkg), filepath.Join(repoRoot(t), confPkg))
	res := w.run(env, "--run", "reach-host-dialed-athena", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	log, _ := os.ReadFile(filepath.Join(w.out(), "reach-host-dialed-athena.log"))
	if !strings.Contains(string(log), "a dry run refuses to start with live settings present") || strings.Contains(string(log), "DRY RUN:") {
		t.Errorf("the refusal is not in the log, or the run went on:\n%s", log)
	}
}

// postsAreHostDialedClean fails when the text to post holds anything but printable ASCII and line ends, or a path or a job ID.
func postsAreHostDialedClean(t *testing.T, posts string) {
	t.Helper()
	for i := 0; i < len(posts); i++ {
		if c := posts[i]; c != '\n' && (c < 0x20 || c > 0x7e) {
			t.Errorf("the text to post holds the byte %#x at %d", c, i)
			break
		}
	}
	for _, bad := range []string{os.TempDir() + "/", "conf-", "/stage/"} {
		if strings.Contains(posts, bad) {
			t.Errorf("the text to post holds %q:\n%s", bad, posts)
		}
	}
	if m := regexp.MustCompile(`hb-relay-[0-9]|jobid=[0-9]|job [0-9]`).FindString(posts); m != "" {
		t.Errorf("the text to post holds %q, a socket's directory or a job ID:\n%s", m, posts)
	}
}
