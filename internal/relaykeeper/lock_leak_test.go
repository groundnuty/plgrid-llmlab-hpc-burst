package relaykeeper

// Tests for issue 402: the double ssh of TestSshGRunsOutsideTheKeepersLock
// must not outlive a test that failed. A failed test never reaches the line
// that writes the double's release file, so a shell loop that waits for it ran
// on, parentless, until someone found it 22 h 40 min later.
//
// testing runs a test's cleanups after it ends, last in, first out, and then
// the directory's removal (registered by TempDir, so first, so last). A test
// cannot fail on purpose and then look at what its own failure left, so
// handCleaner is a test that is run by hand: it takes hangingSSHG's TempDir
// and Cleanup calls, in the order testing would run them, and lets the test
// say which of them run. Each way the double is ended is then shown alone:
// the release file written by the cleanup with the directory kept, and the
// directory removed with the cleanup never run. Each is killed by dropping
// the one thing it tests. The real test uses the same helper. A crash of the
// test binary (a go test -timeout, a SIGKILL, an out-of-memory kill) runs no
// cleanup at all; two more tests show the double ends then, when its parent is
// gone and at a bound of its own (the review of PR 410).

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// handCleaner is a cleaner whose cleanups run when finish says so.
type handCleaner struct {
	t        *testing.T
	cleanups []handCleanup
	// atRemove, when set, is told of the directory just before the removal.
	atRemove func(dir string)
}

type handCleanup struct {
	removal bool // the removal of TempDir's directory
	f       func()
}

func (h *handCleaner) Helper() {}

func (h *handCleaner) Fatalf(format string, args ...any) {
	h.t.Helper()
	h.t.Fatalf(format, args...)
}

// TempDir makes the directory under the real test's own, and registers its
// removal first, as testing does.
func (h *handCleaner) TempDir() string {
	dir := filepath.Join(h.t.TempDir(), "hb")
	if err := os.Mkdir(dir, 0o700); err != nil {
		h.t.Fatal(err)
	}
	h.cleanups = append(h.cleanups, handCleanup{removal: true, f: func() {
		if h.atRemove != nil {
			h.atRemove(dir)
		}
		_ = os.RemoveAll(dir)
	}})
	return dir
}

func (h *handCleaner) Cleanup(f func()) { h.cleanups = append(h.cleanups, handCleanup{f: f}) }

// finish runs the cleanups that run selects, last in, first out.
func (h *handCleaner) finish(run func(handCleanup) bool) {
	for i := len(h.cleanups) - 1; i >= 0; i-- {
		if run(h.cleanups[i]) {
			h.cleanups[i].f()
		}
	}
}

// startFake runs the double's ssh -G as a child of the test, and waits until
// it is waiting (it writes "running" first). The channel gets its exit. A
// double that a failing test leaves is ended through os/exec's own handle on
// the child by the context's cancel.
func startFake(t *testing.T, ssh, dir string) (exit <-chan error, stdout *strings.Builder) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var out strings.Builder
	cmd := exec.CommandContext(ctx, ssh, "-G", "-F", "x", "hpcburst-login")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("the double ssh did not start: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	eventuallyQ(t, 30*time.Second, "the double's ssh -G waiting", func() bool {
		_, err := os.Stat(filepath.Join(dir, "running"))
		return err == nil
	})
	return done, &out
}

// requireExit waits, with a generous deadline, for the double to end. A
// double that does not is the leak.
func requireExit(t *testing.T, exit <-chan error, why string) {
	t.Helper()
	select {
	case err := <-exit:
		if err != nil {
			t.Errorf("the double ssh ended with %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("the double ssh is still running 30 s after %s: it would outlive its test", why)
	}
}

// The control: the double waits for release, and answers when it is written.
// Without it the other tests could pass on a double that never waits.
func TestTheDoubleSshGWaitsForTheReleaseFile(t *testing.T) {
	h := &handCleaner{t: t}
	_, ssh, release := hangingSSHG(h)
	exit, out := startFake(t, ssh, filepath.Dir(release))
	select {
	case err := <-exit:
		t.Fatalf("the double ended by itself with %v, before anything released it", err)
	case <-time.After(300 * time.Millisecond): // one-sided: a double that waits always passes
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	requireExit(t, exit, "the release file was written")
	if got := strings.TrimSpace(out.String()); got != "hostname login.example" {
		t.Errorf("the double answered %q, want hostname login.example", got)
	}
	h.finish(func(handCleanup) bool { return true })
}

// A failed test: its cleanups run, and the directory is still there (the
// removal is held back, to show the cleanup alone). The double ends because
// the cleanup wrote release.
func TestAFailedTestsCleanupReleasesTheDoubleSshG(t *testing.T) {
	h := &handCleaner{t: t}
	_, ssh, release := hangingSSHG(h)
	exit, _ := startFake(t, ssh, filepath.Dir(release))
	h.finish(func(c handCleanup) bool { return !c.removal })
	if _, err := os.Stat(release); err != nil {
		t.Errorf("the cleanup did not write the release file: %v", err)
	}
	requireExit(t, exit, "the cleanup ran with the directory still there")
}

// A failed test whose cleanup never ran, or could not write: only the
// directory's removal runs, and the double ends because its directory is gone.
func TestTheDoubleSshGEndsWhenItsDirectoryIsGone(t *testing.T) {
	h := &handCleaner{t: t}
	dir, ssh, _ := hangingSSHG(h)
	exit, _ := startFake(t, ssh, dir)
	h.finish(func(c handCleanup) bool { return c.removal }) // the cleanup that writes release does not run
	requireExit(t, exit, "its directory was removed")
}

// The order testing runs them in: the cleanup that writes release runs before
// the directory is removed, so it is the cleanup that frees the double on
// every exit.
func TestTheReleaseIsWrittenBeforeTheDirectoryIsRemoved(t *testing.T) {
	h := &handCleaner{t: t}
	_, ssh, release := hangingSSHG(h)
	dir := filepath.Dir(release)
	exit, _ := startFake(t, ssh, dir)
	released := false
	h.atRemove = func(string) {
		_, err := os.Stat(release)
		released = err == nil
	}
	h.finish(func(handCleanup) bool { return true })
	if !released {
		t.Error("the directory was removed before the release file was written: the cleanup runs after TempDir's removal")
	}
	requireExit(t, exit, "all the cleanups ran")
}

// A crash of the test binary runs no cleanup (a go test -timeout panics from
// the alarm goroutine; a SIGKILL or an out-of-memory kill gives it no chance),
// so neither release nor the directory's removal comes. The double then ends
// because its parent is gone. Here the parent is a short-lived intermediate
// shell that starts the double in the background, waits until the double is
// waiting (so the double started under the intermediate's PID), and ends. The
// double's answer goes to a file, and it is written only when the loop ends:
// that file is the observation, never a signal or a PID. Nothing else can end
// the double: no release, its directory stays, its own bound is five minutes.
func TestTheDoubleSshGEndsWhenItsParentIsGone(t *testing.T) {
	h := &handCleaner{t: t}
	dir, ssh, _ := hangingSSHG(h)
	// A double this test leaves (its parent check broken) is ended by the
	// cleanups of a test that failed: release written, directory removed.
	t.Cleanup(func() { h.finish(func(handCleanup) bool { return true }) })
	answer := filepath.Join(dir, "answer")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c",
		`"$1" -G >"$2" 2>&1 & while [ ! -e "$3" ]; do sleep 0.02; done`, "sh", ssh, answer, filepath.Join(dir, "running"))
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	if err := cmd.Run(); err != nil {
		t.Fatalf("the intermediate shell: %v", err)
	}
	eventuallyQ(t, 30*time.Second, "the orphaned double ending on its own (its answer written)", func() bool {
		b, _ := os.ReadFile(answer)
		return strings.Contains(string(b), "hostname login.example")
	})
}

// The double's own bound: with its parent alive, its directory there and
// nothing to release it, it ends after its polls. The bound is shortened to 20
// polls here (the default is 6,000, about five minutes).
func TestTheDoubleSshGEndsAtItsOwnBound(t *testing.T) {
	h := &handCleaner{t: t}
	dir, ssh, _ := hangingSSHGWith(h, 20)
	t.Cleanup(func() { h.finish(func(handCleanup) bool { return true }) })
	exit, _ := startFake(t, ssh, dir)
	requireExit(t, exit, "its own bound of 20 polls")
}
