package alpha

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// The bulk row's node readings (betabulk.go): the agent pick and the Apptainer above the engine, off a listing; and the digest step on a real
// process whose command line names one file while its executable is another, as an exec'd agent's does.
func TestPickAgentReadsTheJobsOneAgent(t *testing.T) {
	agent := NodeProc{PID: 100, PPID: 1, InJob: true, Argv: []string{"/staged/agent/abc/hb-agent", "/staged/alloc/t-1"}}
	relay := NodeProc{PID: 101, PPID: 1, InJob: true, Argv: []string{"/staged/agent/abc/hb-agent", "relay", "control", "--dir", "/x"}}
	child := NodeProc{PID: 102, PPID: 100, Argv: []string{"/staged/agent/abc/hb-agent", "/x"}}
	other := NodeProc{PID: 200, PPID: 1, Argv: []string{"/elsewhere/hb-agent", "/y"}}
	notAgent := NodeProc{PID: 103, PPID: 100, Argv: []string{"/bin/sleep", "hb-agent"}}
	for _, c := range []struct {
		name  string
		procs []NodeProc
		want  int
	}{
		{"the agent beside a relay", []NodeProc{agent, relay}, 100},
		{"an hb-agent under the agent is not another", []NodeProc{agent, child}, 100},
		{"another user's hb-agent", []NodeProc{agent, other}, 100},
		{"a program that only mentions hb-agent", []NodeProc{agent, notAgent}, 100},
		{"none", []NodeProc{relay, notAgent}, 0},
		{"two", []NodeProc{agent, {PID: 300, PPID: 1, InJob: true, Argv: []string{"hb-agent", "/z"}}}, 0},
	} {
		got, ok := PickAgent(c.procs)
		if c.want == 0 && ok || c.want != 0 && (!ok || got.PID != c.want) {
			t.Errorf("%s: PickAgent = %d, %t; want %d", c.name, got.PID, ok, c.want)
		}
	}
}

func TestApptainerAboveTheEngine(t *testing.T) {
	agent := NodeProc{PID: 100, PPID: 1, InJob: true, Argv: []string{"/staged/hb-agent", "/a"}}
	app := NodeProc{PID: 110, PPID: 100, Argv: []string{"/staged/test-agent/x/shim-y/apptainer", "exec", "image.sif", "vllm", "serve"}}
	engine := NodeProc{PID: 120, PPID: 110, Argv: []string{"vllm", "serve"}}
	bare := NodeProc{PID: 130, PPID: 100, Argv: []string{"vllm", "serve"}}
	if got, ok := ApptainerAbove([]NodeProc{agent, app, engine}, engine); !ok || got.PID != 110 {
		t.Errorf("ApptainerAbove(engine) = %d, %t; want 110", got.PID, ok)
	}
	if _, ok := ApptainerAbove([]NodeProc{agent, bare}, bare); ok {
		t.Error("ApptainerAbove found one above a bare engine")
	}
}

// The digest step reads the file the kernel runs, not the one the command line names: a process exec'd from a copy of sleep under another
// name, whose argv[0] is a path of its own, reads as the copy's digest. That is the exec'd agent's case: its command line is the staged
// shim's, its executable the shipped agent's.
func TestTheDigestStepReadsTheExecutableNotTheCommandLine(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep on PATH")
	}
	dir := t.TempDir()
	real := filepath.Join(dir, "the-shipped-agent")
	b, err := os.ReadFile(sleep)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(real, b, 0o700); err != nil {
		t.Fatal(err)
	}
	const job = "4242002"
	pid := startNamed(t, filepath.Join(dir, "agent", "abc", "hb-agent"), real, job, "60") // the command line names the staged shim
	var agent NodeProc
	deadline := time.Now().Add(10 * time.Second)
	for {
		procs, err := ParseListing(runScriptHere(t, ListScript(job)))
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := PickAgent(procs); ok && got.PID == pid {
			agent = got
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the listing never showed the process %d as the job's one agent", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
	script, err := ExeDigestScript(agent)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := ParseExeDigest(runScriptHere(t, script))
	if want := fileDigest(t, real); !ok || got != want {
		t.Fatalf("the step read %q (ok %t), want the executable's own digest %s, not the named file's", got, ok, want)
	}
}
