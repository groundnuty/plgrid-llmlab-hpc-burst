package hostdial

// Unit tests of the parts' own rules. They are not the Test plan's evidence,
// which goes through srun's standard streams (testplan_test.go).

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestSlotsCapEachLoginNodeApart(t *testing.T) {
	s := NewSlots(2)
	ctx := context.Background()
	r1, _ := s.Acquire(ctx, "login-a:22")
	_, _ = s.Acquire(ctx, "login-a:22")
	if _, err := s.Acquire(ctx, "login-b:22"); err != nil {
		t.Fatalf("another login node's slot: %v", err)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := s.Acquire(short, "login-a:22"); err == nil {
		t.Fatal("a third slot on a node capped at 2 was given")
	}
	got := make(chan struct{})
	go func() {
		if _, err := s.Acquire(ctx, "login-a:22"); err == nil {
			close(got)
		}
	}()
	r1()
	r1() // a second release is a no-op
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("a waiting relay did not get the freed slot")
	}
	if n := s.InUse("login-a:22"); n != 2 {
		t.Errorf("InUse = %d, want 2", n)
	}
}

func TestJitterStaysWithinTheBackoff(t *testing.T) {
	for _, d := range []time.Duration{time.Second, 30 * time.Second} {
		for range 1000 {
			if j := jitter(d); j > d || j < d/2 {
				t.Fatalf("jitter(%s) = %s, want within [%s, %s]", d, j, d/2, d)
			}
		}
	}
}

// A connection that finds no session is closed once the wait passes.
func TestAConnectionWithNoSessionIsClosedAfterTheWait(t *testing.T) {
	base, err := os.MkdirTemp("", "hj")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	dir := filepath.Join(base, "r")
	j, err := StartJob(JobOptions{Dir: dir, Wait: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("the relay directory: %v, %v; want mode 700", fi, err)
	}
	c, err := net.Dial("tcp4", j.Addr(agentapi.RelayControl))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
	start := time.Now()
	if _, err := io.ReadAll(c); err != nil {
		t.Fatalf("the connection was not closed by the job side: %v", err)
	}
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Errorf("the connection was closed after %s, before the wait", d)
	}
}

func TestStartJobRefusesALinkedOrMissingDirectory(t *testing.T) {
	base, err := os.MkdirTemp("", "hj")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	link := filepath.Join(base, "link")
	if err := os.Symlink(base, link); err != nil {
		t.Fatal(err)
	}
	if j, err := StartJob(JobOptions{Dir: link}); err == nil {
		j.Close()
		t.Error("a relay directory that is a link was accepted")
	}
	if err := Pipe(filepath.Join(base, "missing"), agentapi.RelayControl, nil, io.Discard); err == nil {
		t.Error("Pipe accepted a relay directory that does not exist")
	}
}
