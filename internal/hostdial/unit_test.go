package hostdial

// Unit tests of the parts' own rules. They are not the Test plan's evidence,
// which goes through srun's standard streams (testplan_test.go).

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestSlotsCapEachLoginNodeApart(t *testing.T) {
	s := NewSlots(4)
	ctx := context.Background()
	r1, _ := s.Acquire(ctx, "login-a:22", 2)
	_, _ = s.Acquire(ctx, "login-a:22", 2)
	if _, err := s.Acquire(ctx, "login-b:22", 2); err != nil {
		t.Fatalf("another login node's pair: %v", err)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := s.Acquire(short, "login-a:22", 2); err == nil {
		t.Fatal("a third pair on a node capped at 4 was given")
	}
	if _, err := s.Acquire(short, "login-a:22", 1); err == nil {
		t.Fatal("a fifth slot on a node capped at 4 was given")
	}
	got := make(chan struct{})
	go func() {
		if _, err := s.Acquire(ctx, "login-a:22", 2); err == nil {
			close(got)
		}
	}()
	r1()
	r1() // a second release is a no-op
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("a waiting Allocation did not get the freed pair")
	}
	if n := s.InUse("login-a:22"); n != 4 {
		t.Errorf("InUse = %d, want 4", n)
	}
	if _, err := s.Acquire(ctx, "login-a:22", 5); err == nil {
		t.Error("more slots than the cap were asked and not refused")
	}
}

// The preamble: bytes before it are dropped, a relay with none within the
// bound or naming the other channel is refused by name.
func TestPreamble(t *testing.T) {
	read := func(in string, ch agentapi.RelayChannel) (int, string, error) {
		br := bufio.NewReader(strings.NewReader(in))
		n, err := readPreamble(br, ch)
		rest, _ := io.ReadAll(br)
		return n, string(rest), err
	}
	if n, rest, err := read("Welcome to login.example\n"+Preamble(agentapi.RelayServing)+"SESSION", agentapi.RelayServing); err != nil || n != 25 || rest != "SESSION" {
		t.Errorf("a banner before the preamble: skipped %d, rest %q, %v", n, rest, err)
	}
	if _, _, err := read(Preamble(agentapi.RelayControl)+"x", agentapi.RelayServing); !errors.Is(err, ErrWrongChannel) {
		t.Errorf("the other channel's preamble: %v, want ErrWrongChannel", err)
	}
	if _, _, err := read(strings.Repeat("x", MaxPreambleScan+10)+Preamble(agentapi.RelayServing), agentapi.RelayServing); !errors.Is(err, ErrNoPreamble) {
		t.Errorf("a preamble past the bound: %v, want ErrNoPreamble", err)
	}
	if _, _, err := read("no preamble here", agentapi.RelayServing); !errors.Is(err, ErrNoPreamble) {
		t.Errorf("an output with none: %v, want ErrNoPreamble", err)
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

// Serve returns once its session ends, even while a joined connection's
// target is silent and never closes (a long poll, an idle tunnel): the relay
// owner waits for Serve before it starts the relay again.
func TestServeReturnsWhenItsSessionEndsUnderASilentTarget(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	held := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			held <- c // never read, never written, never closed until the test ends
		}
	}()
	jobEnd, hostEnd := net.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- Serve(context.Background(), hostEnd, ln.Addr().String(), agentapi.RelayControl, nil, nil)
	}()
	if _, err := io.WriteString(jobEnd, Preamble(agentapi.RelayControl)); err != nil {
		t.Fatal(err)
	}
	sess, err := yamux.Client(jobEnd, sessionConfig(nil, "job"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := sess.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var c net.Conn
	select {
	case c = <-held:
		defer c.Close()
	case <-time.After(30 * time.Second):
		t.Fatal("the stream was not joined to the target")
	}
	_ = sess.Close()
	select {
	case <-served:
	case <-time.After(30 * time.Second):
		t.Fatal("Serve did not return after its session ended, with a silent target joined")
	}
}
