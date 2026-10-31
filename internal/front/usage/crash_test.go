//go:build unix

package usage_test

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A crash is a real SIGKILL of another process. The test re-executes its own
// binary as a child (os.Args[0] with -test.run on TestCrashChild, gated by an
// environment variable), so the child's Sink is a different process whose
// death the parent causes; nothing here "crashes" inside one process.

const (
	childDirEnv  = "USAGE_TEST_CHILD_DIR"
	childModeEnv = "USAGE_TEST_CHILD_MODE"
	// firstRun is how many records the first child writes.
	firstRun = 5
)

// TestCrashChild is the child's program. In a normal run of the suite the
// environment variable is not set and it does nothing.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv(childDirEnv)
	if dir == "" {
		return
	}
	s := sink(t, dir)
	switch mode := os.Getenv(childModeEnv); mode {
	case "first":
		for i := 1; i <= firstRun; i++ {
			write(t, s, rec(fmt.Sprint("first-", i)))
		}
		waitLines(t, dir, firstRun)
		fmt.Println("written")
		time.Sleep(time.Minute) // the parent kills this process; it is never closed
	case "second":
		c := write(t, s, rec("second-1"))
		waitLines(t, dir, firstRun+1)
		fmt.Printf("written counter=%d\n", c)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown child mode %q", mode)
	}
}

// waitLines waits until the log holds n lines: the sink's writer has handed
// them to the file.
func waitLines(t *testing.T, dir string, n int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for logLines(dir) < n {
		if time.Now().After(deadline) {
			t.Fatalf("the log never reached %d lines", n)
		}
		time.Sleep(time.Millisecond)
	}
}

// child is a running child process and its stdout lines.
type child struct {
	cmd   *exec.Cmd
	lines chan string
	errs  *bytes.Buffer
}

func startChild(t *testing.T, dir, mode string) *child {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), childDirEnv+"="+dir, childModeEnv+"="+mode)
	c := &child{cmd: cmd, lines: make(chan string, 16), errs: &bytes.Buffer{}}
	cmd.Stderr = c.errs
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() }) // a no-op once it has exited
	go func() {
		defer close(c.lines)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			c.lines <- sc.Text()
		}
	}()
	return c
}

// waitLine returns the first line the child prints that starts with prefix.
func (c *child) waitLine(t *testing.T, prefix string) string {
	t.Helper()
	timeout := time.After(30 * time.Second)
	for {
		select {
		case l, ok := <-c.lines:
			if !ok {
				t.Fatalf("the child exited before printing %q; stderr: %s", prefix, c.errs.String())
			}
			if strings.HasPrefix(l, prefix) {
				return l
			}
		case <-timeout:
			t.Fatalf("the child printed no %q within 30 s; stderr: %s", prefix, c.errs.String())
		}
	}
}

// TestCrashLeavesAGapInTheCounter: a child writes records and is killed with
// SIGKILL, with its Sink still open; a second child that opens the same
// directory starts at 1001, so the counters show a gap.
func TestCrashLeavesAGapInTheCounter(t *testing.T) {
	dir := t.TempDir()

	first := startChild(t, dir, "first")
	first.waitLine(t, "written")
	if err := first.cmd.Process.Kill(); err != nil { // SIGKILL
		t.Fatal(err)
	}
	err := first.cmd.Wait()
	ws, ok := first.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if err == nil || !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("the first child did not die of SIGKILL: err %v, state %v", err, first.cmd.ProcessState)
	}

	recs := readLog(t, dir)
	if want := []uint64{1, 2, 3, 4, 5}; !reflect.DeepEqual(counters(recs), want) {
		t.Errorf("after the kill the log holds counters %v, want %v: the records it had written must survive", counters(recs), want)
	}
	if got := counterFile(t, dir); got != "1000" {
		t.Errorf("after the kill the counter file = %q, want 1000", got)
	}

	second := startChild(t, dir, "second")
	line := second.waitLine(t, "written counter=")
	if err := second.cmd.Wait(); err != nil {
		t.Fatalf("the second child: %v; stderr: %s", err, second.errs.String())
	}
	if line != "written counter=1001" {
		t.Errorf("the second child printed %q, want its first counter to be 1001", line)
	}
	recs = readLog(t, dir)
	if want := []uint64{1, 2, 3, 4, 5, 1001}; !reflect.DeepEqual(counters(recs), want) {
		t.Errorf("counters in the log = %v, want %v: the gap 6 to 1000 must show", counters(recs), want)
	}
	if got := counterFile(t, dir); got != "1001" {
		t.Errorf("counter file = %q, want 1001: the second child closed cleanly, which persists its last counter", got)
	}
}
