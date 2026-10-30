package fakeslurm

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestMain lets the test binary act as the fake: a child started with
// FAKESLURM_TEST_AS=<command> runs that command with the remaining arguments,
// so tests can start and kill real processes.
func TestMain(m *testing.M) {
	if cmd := os.Getenv("FAKESLURM_TEST_AS"); cmd != "" {
		os.Exit(Main(Env{}, append([]string{cmd}, os.Args[1:]...)))
	}
	os.Exit(m.Run())
}

// child starts the fake as a real process with the rig's environment.
func (r *rig) child(command string, args ...string) (*exec.Cmd, *bytes.Buffer) {
	r.t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	// Only the rig's settings reach the child: drop any FAKE_SLURM_* the
	// caller's shell exports.
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "FAKE_SLURM_") && !strings.HasPrefix(kv, "FAKESLURM_TEST_AS=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "FAKESLURM_TEST_AS="+command)
	for k, v := range r.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	return cmd, &out
}

// waitForEvents polls the log until it holds n events of the given name.
func (r *rig) waitForEvents(name string, n int) {
	r.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for r.countEvents(name) < n {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %d %s events; log: %v", n, name, r.logEvents())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 7. A real sbatch process waits at the gate, registers nothing while it is
// closed, and registers exactly one job after it opens. A second process
// killed at the gate registers nothing, ever.
func TestSbatchGateRealProcess(t *testing.T) {
	r := newRig(t)
	gate := r.marker("gate")
	r.env["FAKE_SLURM_SBATCH_GATE"] = gate

	// opens after a wait
	cmd, out := r.child("sbatch", "--parsable", "x.sh")
	r.waitForEvents("sbatch_gate_wait", 1)
	time.Sleep(300 * time.Millisecond) // several 50 ms polls
	if r.countEvents("sbatch_submitted") != 0 || len(ids(t, r.squeue())) != 0 {
		t.Errorf("a job was registered while the gate was closed")
	}
	if !lockFree(t, r.env["FAKE_SLURM_STATE"]) {
		t.Errorf("the waiting process holds the lock")
	}
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("sbatch after the gate opened: %v", err)
	}
	if got := out.String(); got != "1001\n" {
		t.Errorf("stdout %q, want 1001", got)
	}
	if got := ids(t, r.squeue()); !sameInts(got, []int{1001}) {
		t.Errorf("squeue lists %v, want exactly [1001]", got)
	}

	// killed while waiting
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	cmd, out = r.child("sbatch", "--parsable", "y.sh")
	r.waitForEvents("sbatch_gate_wait", 2)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait() // "signal: killed"
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := ids(t, r.squeue()); !sameInts(got, []int{1001}) {
		t.Errorf("after the kill squeue lists %v, want still [1001]", got)
	}
	if c := r.countEvents("sbatch_submitted"); c != 1 {
		t.Errorf("%d sbatch_submitted events, want 1", c)
	}
	if out.Len() != 0 || !lockFree(t, r.env["FAKE_SLURM_STATE"]) {
		t.Errorf("killed process printed %q or left the lock held", out.String())
	}
	if strings.Contains(r.ctl("dump"), `"y.sh"`) {
		t.Errorf("the killed sbatch left a job in the state")
	}
}
