package tests

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/harness"
)

// drain reads Lines until it is closed.
func drain(c *harness.Child) []string {
	var out []string
	for l := range c.Lines {
		out = append(out, l)
	}
	return out
}

func waitExited(t *testing.T, c *harness.Child) {
	t.Helper()
	waitFor(t, 10*time.Second, "the child to exit", c.Exited)
}

// The harness reads a child's output through its own pipe, so reaping the child can
// never discard lines the parent has not consumed yet. Here the consumer is slow on
// purpose: the Lines channel holds one line and the scanner buffer 4 KiB, so most of
// the roughly 14 KiB the child writes sits in the pipe when the child exits. With
// os/exec's StdoutPipe the reaper closes that pipe and the unread part is gone.
func TestHarness_NoOutputLostWhenTheReaderIsSlow(t *testing.T) {
	const n = 2000
	c, err := harness.SpawnWith(harness.SpawnOpts{LinesCap: 1, MaxLine: 4096}, "spew", map[string]string{"S8_LINES": strconv.Itoa(n)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Kill() })
	waitExited(t, c)
	time.Sleep(300 * time.Millisecond) // the reaper has run; a pipe owned by os/exec is closed by now
	got := drain(c)
	if err := c.Err(); err != nil {
		t.Fatalf("reading the child's output: %v", err)
	}
	if len(got) != n {
		t.Fatalf("read %d of the %d lines the child wrote: the capture lost output", len(got), n)
	}
	for i, l := range got {
		if want := fmt.Sprintf("L%d", i); l != want {
			t.Fatalf("line %d is %q, want %q", i, l, want)
		}
	}
}

// A read error must reach the caller. A line longer than the scanner accepts used to
// end the reading silently, so acknowledgements after it would have been dropped.
func TestHarness_ScanErrorIsReported(t *testing.T) {
	c, err := harness.SpawnWith(harness.SpawnOpts{MaxLine: 4096}, "longline", map[string]string{"S8_BYTES": "10000"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Kill() })
	got := drain(c)
	if len(got) != 0 {
		t.Fatalf("an over-long line was delivered as %d line(s)", len(got))
	}
	if err := c.Err(); !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("scan error = %v, want bufio.ErrTooLong", err)
	}
}

// The crash tests claim a SIGKILL. KillStrict must prove it from the wait status, and
// must refuse a child that was already gone (it died of something else first).
func TestHarness_KillStrictRequiresASigkill(t *testing.T) {
	t.Run("killed_child", func(t *testing.T) {
		c, err := harness.Spawn("sleep", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Kill() })
		if _, err := c.WaitLine("READY", 10*time.Second); err != nil {
			t.Fatal(err)
		}
		if err := c.KillStrict(); err != nil {
			t.Fatal(err)
		}
		if sig, err := c.TerminatedBy(); err != nil || sig != syscall.SIGKILL {
			t.Fatalf("TerminatedBy = %v, %v, want SIGKILL", sig, err)
		}
	})
	for _, role := range []string{"exit3", "spew"} { // exit status 3, exit status 0
		t.Run("already_exited_"+role, func(t *testing.T) {
			c, err := harness.Spawn(role, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Kill() })
			waitExited(t, c)
			err = c.KillStrict()
			if err == nil {
				t.Fatal("KillStrict accepted a child that had exited by itself")
			}
			if !errors.Is(err, os.ErrProcessDone) {
				t.Fatalf("KillStrict error = %v, want it to wrap os.ErrProcessDone", err)
			}
		})
	}
}
