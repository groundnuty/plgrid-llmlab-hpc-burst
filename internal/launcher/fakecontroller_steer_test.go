package launcher

// The fake controller's numeric steering files are read as a number only once they hold one (issue
// #491). A writer that creates the file empty and fills it later, as os.WriteFile does, once made the
// fake read "" as 0 and exit 0 where the test asked for 3. These tests make that window as wide as they
// like: they leave the file empty until the fake shows it has read it so (the steer-wait-<name> file it
// writes), and only then fill it, so each run takes the path the race took, every time.

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// steerFake is the fake controller started directly, with the channel closed once it has exited.
type steerFake struct {
	stderr strings.Builder
	exited chan struct{}
	err    error // cmd.Wait's, once exited is closed
}

// startFake starts the fake controller directly, with the controller's command line, steered from
// dir, serving nothing (no-health). Its stderr is read only after it has exited.
func startFake(t *testing.T, dir string) *steerFake {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "no-health"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	f := &steerFake{exited: make(chan struct{})}
	cmd := exec.Command(fakeControllerBin(t), "--state-dir", t.TempDir(), "--port-base", "29220", "--health-addr", "127.0.0.1:29226")
	cmd.Env = []string{"FAKECTRL_DIR=" + dir}
	cmd.Stderr = &f.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { f.err = cmd.Wait(); close(f.exited) }()
	t.Cleanup(func() {
		select {
		case <-f.exited:
		default:
			_ = cmd.Process.Kill() // the child's own process, never a group
			<-f.exited
		}
		if t.Failed() {
			t.Logf("the fake's stderr:\n%s", f.stderr.String())
		}
	})
	return f
}

// waitForMarker waits until the fake has written steer-wait-<name>, which it does the first time it
// reads the file and finds no number in it, and fails if the fake exits first: it acted on the file.
func waitForMarker(t *testing.T, dir, name string, f *steerFake) string {
	t.Helper()
	marker := filepath.Join(dir, "steer-wait-"+name)
	deadline := time.After(30 * time.Second)
	for {
		if b, err := os.ReadFile(marker); err == nil {
			return string(b)
		}
		select {
		case <-f.exited:
			t.Fatalf("the fake exited (%v) on the steering file %s while it was still empty, instead of waiting for a number", f.err, name)
		case <-deadline:
			t.Fatalf("the fake never wrote %s within 30 s", marker)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// exitCode waits for the fake to exit and returns its exit code.
func exitCode(t *testing.T, f *steerFake) int {
	t.Helper()
	select {
	case <-f.exited:
	case <-time.After(30 * time.Second):
		t.Fatal("the fake did not exit within 30 s of being given its exit code")
	}
	var ee *exec.ExitError
	switch {
	case f.err == nil:
		return 0
	case errors.As(f.err, &ee):
		return ee.ExitCode()
	}
	t.Fatalf("waiting for the fake: %v", f.err)
	return -1
}

// TestFakeControllerWaitsForANumberInTheExitFile: the exit file, created empty while the fake runs,
// is read as empty on a tick (the marker shows it) without making the fake exit; filled with 3 by a
// plain write after that, it makes the fake exit 3.
func TestFakeControllerWaitsForANumberInTheExitFile(t *testing.T) {
	dir := t.TempDir()
	f := startFake(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "exit"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := waitForMarker(t, dir, "exit", f); got != `""` {
		t.Errorf("steer-wait-exit holds %s, want what the fake read, \"\"", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "exit"), []byte("3"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := exitCode(t, f); code != 3 {
		t.Errorf("the fake exited %d, want 3", code)
	}
}

// TestFakeControllerWaitsForANumberInAStartFile: always-exit, read once as the fake starts, is waited
// on the same way: empty at the start, it holds the fake until a number is written, and then the fake
// exits with it.
func TestFakeControllerWaitsForANumberInAStartFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "always-exit"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	f := startFake(t, dir)
	waitForMarker(t, dir, "always-exit", f)
	if err := os.WriteFile(filepath.Join(dir, "always-exit"), []byte("5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := exitCode(t, f); code != 5 {
		t.Errorf("the fake exited %d, want 5", code)
	}
}

// TestFakeControllerRefusesASteeringFileThatNeverHoldsANumber: a start file that never becomes a
// number is not read as 0: after its 10 s the fake exits 125 and names the file.
func TestFakeControllerRefusesASteeringFileThatNeverHoldsANumber(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "always-exit"), []byte("three"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := startFake(t, dir)
	if got := waitForMarker(t, dir, "always-exit", f); got != `"three"` {
		t.Errorf("steer-wait-always-exit holds %s, want \"three\"", got)
	}
	if code := exitCode(t, f); code != 125 {
		t.Errorf("the fake exited %d, want 125", code)
	}
	if want := `the steering file always-exit has held "three", not a number`; !strings.Contains(f.stderr.String(), want) {
		t.Errorf("the fake's stderr lacks %q:\n%s", want, f.stderr.String())
	}
}
