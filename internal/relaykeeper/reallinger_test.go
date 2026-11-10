package relaykeeper

// New for issue #343 (PR 3): the lingering relay processes of the Runner's
// tests (realworld_test.go): an ssh client that stands in for a relay process
// still there after its stop began.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

// lingeringSSH is an ssh client for the Runner (OwnerRunnerOptions.SSHBinary)
// that stands in for a relay process that is still there after its stop
// began. Every other relay, and every other call (ssh -G), is the system
// ssh, exec'd as it is.
//   - A relay whose N is listed in the world's trap-seqs file (lingerSeq)
//     sets its TERM to be ignored, then execs the system ssh, which keeps it
//     so: the Runner's stop ends it only when its session ends, or by the
//     KILL after the stop's grace, so it can still connect after its stop
//     began.
//   - A relay started while the world's trap-all file exists (lingerAll)
//     ignores TERM and runs the system ssh as its child; once the ssh has
//     ended, it waits for the world's release file before it exits, so the
//     Runner's process of the relay outlives its ssh connection until the
//     test lets it go, or the KILL after the stop's grace ends it.
func (w *rkWorld) lingeringSSH() string {
	w.t.Helper()
	ssh, err := exec.LookPath("ssh")
	if err != nil {
		w.t.Fatal(err)
	}
	dir := filepath.Join(w.base, "b")
	script := filepath.Join(dir, "ssh")
	body := "#!/bin/sh\n" +
		// Only a relay lingers: ssh -G, the login host's read, never does.
		"case \" $* \" in *\" srun -u --overlap \"*) ;; *) exec " + ssh + " \"$@\";; esac\n" +
		"if [ -e " + dir + "/trap-all ]; then\n" +
		"  trap '' TERM\n" +
		"  " + ssh + " \"$@\"\n" +
		"  rc=$?\n" +
		"  while [ ! -e " + dir + "/release ]; do sleep 0.02; done\n" +
		"  exit $rc\n" +
		"fi\n" +
		"if [ -e " + dir + "/trap-seqs ]; then\n" +
		"  for n in $(cat " + dir + "/trap-seqs); do\n" +
		"    case \" $* \" in *\" --seq $n \"*) trap '' TERM; : > " + dir + "/trapped-$n;; esac\n" +
		"  done\n" +
		"fi\n" +
		"exec " + ssh + " \"$@\"\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		w.t.Fatal(err)
	}
	return script
}

// lingerSeq makes the relay of N n linger past its stop's TERM
// (lingeringSSH). It must be called before that relay's process starts. Once
// the relay's ssh has set its TERM to be ignored, the world's trapped-<n>
// file exists (trapped).
func (w *rkWorld) lingerSeq(n int64) {
	w.t.Helper()
	f, err := os.OpenFile(filepath.Join(w.base, "b", "trap-seqs"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		w.t.Fatal(err)
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, n); err != nil {
		w.t.Fatal(err)
	}
}

// trapped reports whether the relay of N n has set its TERM to be ignored
// (lingerSeq).
func (w *rkWorld) trapped(n int64) bool {
	_, err := os.Stat(filepath.Join(w.base, "b", "trapped-"+strconv.FormatInt(n, 10)))
	return err == nil
}

// lingerAll makes every relay that starts from now on linger once its ssh
// has ended (lingeringSSH), until release, or, with on false, none that
// starts from now on.
func (w *rkWorld) lingerAll(on bool) {
	w.t.Helper()
	if on {
		w.t.Cleanup(w.release)
	}
	p := filepath.Join(w.base, "b", "trap-all")
	if !on {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			w.t.Fatal(err)
		}
		return
	}
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		w.t.Fatal(err)
	}
}

// release lets every lingering relay process end (lingerAll).
func (w *rkWorld) release() {
	if err := os.WriteFile(filepath.Join(w.base, "b", "release"), nil, 0o600); err != nil {
		w.t.Error(err)
	}
}
