package alpha

// Issue #274: the dry runs left fake-Slurm job shells (/bin/sh .../slurm_script)
// looping on the dev VM for days, re-parented to init, after a builder's run was
// killed. Two verifications prove nothing here: a pass that only looks at the
// cases that end normally (Close ends the jobs there, with or without the
// fix), and a search by the name slurm_script alone (this VM is shared, and
// other agents' runs show up). The kill test below ends the test binary the way
// a builder's timeout did, and looks only under its own temp dir.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// jobShellsUnder lists the PIDs of the fake Slurm's batch shells whose script is
// under dir: their command line is "/bin/sh <dir>/.../slurm_script". It reads
// /proc, so a shell that is gone, or a zombie (it has no command line), is not
// listed.
func jobShellsUnder(dir string) []int {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var pids []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue
		}
		for _, arg := range strings.Split(string(b), "\x00") {
			if strings.HasPrefix(arg, dir+"/") && strings.HasSuffix(arg, "/slurm_script") {
				pids = append(pids, pid)
				break
			}
		}
	}
	return pids
}

// noJobShellsLeft fails the test if a batch shell of the run under dir is
// still alive. The caller has closed the fake cluster.
func noJobShellsLeft(t testing.TB, dir string) {
	t.Helper()
	if left := jobShellsUnder(dir); len(left) > 0 {
		t.Errorf("job shells %v of the run under %s are still alive after the fake cluster closed", left, dir)
	}
}

// A dry run whose test binary is killed while its jobs run leaves no job shell:
// the cleanups never run on SIGKILL, so the kernel must end the shells. The
// child is this test binary running one dry case, with the parent's temp dir as
// its TMPDIR and GOTMPDIR, so every shell it starts is found by that directory. It holds
// right after its n-th submission (holdForKill), and the parent, on seeing the
// marker file, checks that a shell of the run is alive, kills the child, and
// waits on the shells' state, not on a time.
func TestADryRunKilledMidJobLeavesNoJobShell(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("job shells are read from /proc")
	}
	cases := []struct {
		row, fault string
		holdAt     int // the submission to hold at: the second job of a row that has two is the successor
	}{
		// The two cases the shells of 8 Nov 2026 were found under (issue #274).
		{"alpha-athena-handover", "default-lease", 2},
		{"alpha-athena-handover", "unattributed", 2},
		// One of each other family: a successor submitted late, and the hour-long tunnel row.
		{"alpha-helios-nooverlap", "late-submit", 2},
		{"tunnel-hour-helios", "short", 1},
	}
	for _, c := range cases {
		t.Run(c.row+"/"+c.fault, func(t *testing.T) {
			dir := t.TempDir()
			t.Cleanup(func() { // a shell found below must not outlive this test either
				for _, pid := range jobShellsUnder(dir) {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			})
			run := fmt.Sprintf("^TestDryRunsOfEveryRowAndEveryFault$/^%s$/^%s$", regexp.QuoteMeta(c.row), regexp.QuoteMeta(c.fault))
			cmd := exec.Command(os.Args[0], "-test.run", run, "-test.count=1", "-test.timeout=5m")
			cmd.Env = append(os.Environ(), "TMPDIR="+dir, "GOTMPDIR="+dir, holdEnv+"="+strconv.Itoa(c.holdAt)) // t.TempDir reads GOTMPDIR first
			var log strings.Builder
			cmd.Stdout, cmd.Stderr = &log, &log
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			held := filepath.Join(dir, "held-at-job")
			deadline := time.Now().Add(30 * time.Second)
			for {
				if _, err := os.Stat(held); err == nil {
					break
				}
				select {
				case err := <-exited:
					t.Fatalf("the child dry run ended (%v) before submission %d; the test proves nothing:\n%s", err, c.holdAt, log.String())
				default:
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					t.Fatalf("the child dry run did not reach submission %d in 30 s:\n%s", c.holdAt, log.String())
				}
				time.Sleep(time.Millisecond)
			}
			shells := jobShellsUnder(dir)
			if len(shells) == 0 {
				_ = cmd.Process.Kill()
				t.Fatalf("no job shell under %s at the hold, so the kill would leave nothing to see:\n%s", dir, log.String())
			}
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			<-exited
			// The kernel ends the shells when the child's last thread does: milliseconds. The deadline is only generous.
			deadline = time.Now().Add(20 * time.Second)
			for left := jobShellsUnder(dir); len(left) > 0; left = jobShellsUnder(dir) {
				if time.Now().After(deadline) {
					t.Fatalf("job shells %v of the killed dry run (alive at the kill: %v) are still alive under %s", left, shells, dir)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
}
