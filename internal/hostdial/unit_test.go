package hostdial

// Unit tests of the parts' own rules. They are not the Test plan's evidence,
// which goes through srun's standard streams (testplan_test.go,
// review_test.go), except where a case cannot be made there: another user's
// peer on the agent's socket needs a second account, so its check is shown
// here with an injected peer.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fatedier/yamux"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func shortDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "hj")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

func TestSlotsCapEachLoginNodeApart(t *testing.T) {
	s := NewSlots(4)
	ctx := context.Background()
	r1, _ := s.Acquire(ctx, "login-a", 2)
	_, _ = s.Acquire(ctx, "login-a", 2)
	if _, err := s.Acquire(ctx, "login-b", 2); err != nil {
		t.Fatalf("another login node's pair: %v", err)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := s.Acquire(short, "login-a", 2); err == nil {
		t.Fatal("a third pair on a node capped at 4 was given")
	}
	if _, err := s.Acquire(short, "login-a", 1); err == nil {
		t.Fatal("a fifth slot on a node capped at 4 was given")
	}
	got := make(chan struct{})
	go func() {
		if _, err := s.Acquire(ctx, "login-a", 2); err == nil {
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
	if n := s.InUse("login-a"); n != 4 {
		t.Errorf("InUse = %d, want 4", n)
	}
	if _, err := s.Acquire(ctx, "login-a", 5); err == nil {
		t.Error("more slots than the cap were asked and not refused")
	}
}

// One host-dialed Provider per login host (HBEP-6, Capacity), the oldest by
// creationTimestamp, then by name (the delta review of PR 301, D4): whoever
// claims first, the older keeps the host; an older Provider that has enrolled
// but not said which host it uses holds back a younger one; a Provider that
// uses no host, or that leaves, holds back none; the host's case does not
// matter.
func TestHostsTheOldestProviderKeepsTheLoginHost(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	h := NewHosts()
	if _, _, ok := h.Claim("login.example", "helios-a"); ok {
		t.Fatal("an unenrolled Provider was admitted")
	}
	h.Enroll("helios-a", t0)
	h.Enroll("helios-b", t0.Add(time.Hour))
	// After a restart B asks first: A has enrolled but not said, so B waits.
	if holder, pending, ok := h.Claim("login.example", "helios-b"); ok || holder != "helios-a" || !pending {
		t.Errorf("B before A has said: %q, pending %v, ok %v; want held back by helios-a, pending", holder, pending, ok)
	}
	if _, _, ok := h.Claim("LOGIN.Example", "helios-a"); !ok {
		t.Fatal("the older Provider was not admitted")
	}
	if holder, pending, ok := h.Claim("login.example", "helios-b"); ok || holder != "helios-a" || pending {
		t.Errorf("B after A: %q, pending %v, ok %v; want refused, held by helios-a", holder, pending, ok)
	}
	// Another host for B: admitted; the same age breaks by name.
	if _, _, ok := h.Claim("login2.example", "helios-b"); !ok {
		t.Error("B on another host was refused")
	}
	h.Enroll("helios-c", t0)
	if holder, _, ok := h.Claim("login.example", "helios-c"); ok || holder != "helios-a" {
		t.Errorf("C, as old as A, on A's host: %q, %v; want refused by name, helios-a", holder, ok)
	}
	// A broken older Provider says it uses no host and holds back none.
	h.Enroll("helios-0", t0.Add(-time.Hour))
	if _, _, ok := h.Claim("login2.example", "helios-b"); ok {
		t.Error("B was admitted while an older Provider had not said which host it uses")
	}
	h.Claim("", "helios-0")
	if _, _, ok := h.Claim("login2.example", "helios-b"); !ok {
		t.Error("a Provider that uses no host still held B back")
	}
	h.Release("helios-a")
	if _, _, ok := h.Claim("login.example", "helios-c"); !ok {
		t.Error("the host of a Provider that left is still held")
	}
}

// The preamble (HBEP-6, The wire): exactly "hpcburst-relay/1 <UID>" first;
// anything else first is unexpected output, and an output that ends before a
// line is left to the process's end.
func TestPreamble(t *testing.T) {
	const uid = "0a1b2c3d-0000-4000-8000-000000000001"
	read := func(in string) (string, error) {
		br := bufio.NewReader(strings.NewReader(in))
		err := readPreamble(br, uid)
		rest, _ := io.ReadAll(br)
		return string(rest), err
	}
	if rest, err := read("hpcburst-relay/1 " + uid + "\nSESSION"); err != nil || rest != "SESSION" {
		t.Errorf("the preamble: rest %q, %v", rest, err)
	}
	for name, in := range map[string]string{
		"a banner first":        "Welcome to login.example\nhpcburst-relay/1 " + uid + "\n",
		"another Allocation's":  "hpcburst-relay/1 0a1b2c3d-0000-4000-8000-000000000002\n",
		"an older version":      "hpcburst-relay/0 " + uid + "\n",
		"a line too long":       strings.Repeat("x", 1000) + "\n",
		"a part line, then EOF": "hpcburst-rel",
	} {
		if _, err := read(in); !errors.Is(err, ErrUnexpectedOutput) {
			t.Errorf("%s: %v, want ErrUnexpectedOutput", name, err)
		}
	}
	if _, err := read(""); !errors.Is(err, io.EOF) {
		t.Errorf("no output at all: %v, want io.EOF, for the process to tell", err)
	}
	_, err := read("Welcome to login.example\n")
	if err == nil || strings.Contains(err.Error(), "login.example") {
		t.Errorf("unexpected output's error %v repeats what the login node printed", err)
	}
}

// What ends a relay (HBEP-6): each line the relay or srun writes is sorted
// into retried or final.
func TestClassifyEnd(t *testing.T) {
	for stderr, want := range map[string]EndKind{
		"hb-relay: superseded: the agent of Allocation u keeps a newer serving relay\n":          EndSuperseded,
		"hb-relay: agent not ready: Allocation u's agent has published no socket\n":              EndRetried,
		"srun: error: Unable to create step for job 7: Step limit reached for this job":          EndFailed,
		"srun: error: Unable to confirm allocation for job 7: Invalid job id specified":          EndJobGone,
		"srun: error: Unable to create step for job 7: Job/step already completing or completed": EndJobGone,
		"srun: error: Unable to confirm allocation for job 7: job is not running":                EndJobGone,
		"example@login.example: Permission denied (publickey).":                                  EndRetried,
		"": EndRetried,
	} {
		if got, cause := ClassifyEnd(stderr); got != want || strings.Contains(cause, "login.example") {
			t.Errorf("ClassifyEnd(%q) = %s, %q; want %s and a cause that repeats nothing", stderr, got, cause, want)
		}
	}
	if !EndSuperseded.Final() || !EndJobGone.Final() || !EndFailed.Final() || EndRetried.Final() {
		t.Error("Final")
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

// startTestJob is a job side with a socket under a short TMPDIR.
func startTestJob(t *testing.T, wait time.Duration) (*Job, string) {
	t.Helper()
	return startTestJobWith(t, wait, nil)
}

// startTestJobWith is startTestJob with a change to the job's options, made
// before the server serves.
func startTestJobWith(t *testing.T, wait time.Duration, mod func(*JobOptions)) (*Job, string) {
	t.Helper()
	base := shortDir(t)
	alloc := filepath.Join(base, "a")
	if err := os.Mkdir(alloc, 0o700); err != nil {
		t.Fatal(err)
	}
	o := JobOptions{ShmDir: base, TmpDir: base, AllocDir: alloc, Allocation: "u-1", Wait: wait}
	if mod != nil {
		mod(&o)
	}
	j, err := StartJob(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j, alloc
}

// The rendezvous file names the socket, mode 600; the socket's directory has
// a random name, mode 700; Close removes both.
func TestTheRendezvousFile(t *testing.T) {
	j, alloc := startTestJob(t, time.Second)
	fi, err := os.Stat(agentapi.RelaySocketFile(alloc))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("the rendezvous file: %v, %v; want mode 600", fi, err)
	}
	if got, err := readRendezvous(agentapi.RelaySocketFile(alloc)); err != nil || got != j.Socket() {
		t.Fatalf("the rendezvous file names %q, %v; want %q", got, err, j.Socket())
	}
	dir := filepath.Dir(j.Socket())
	if di, err := os.Stat(dir); err != nil || di.Mode().Perm() != 0o700 || !strings.HasPrefix(filepath.Base(dir), "hb-relay-") || filepath.Base(dir) == "hb-relay-" {
		t.Fatalf("the socket's directory %s: %v, %v; want a random name, mode 700", dir, di, err)
	}
	_ = j.Close()
	if _, err := os.Stat(agentapi.RelaySocketFile(alloc)); !os.IsNotExist(err) {
		t.Errorf("the rendezvous file is left after Close: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the socket's directory is left after Close: %v", err)
	}
}

// The socket's directory goes under the job's TMPDIR (science-agent ruling on
// #301), and under /dev/shm only when the socket's path under TMPDIR would
// pass 107 bytes, which is logged; an unset TMPDIR, an unusable one, or one
// too long with /dev/shm too long too fails by name.
// SocketPathUnder is the root and 31 bytes: "/hb-relay-" with 10 digits, "/relay.sock" (worked out by hand). A TMPDIR of
// 76 bytes keeps the socket at 107, the limit; one of 77 passes it, and the socket falls back.
func TestSocketPathUnderIsTheLongestPathTheSocketCanHave(t *testing.T) {
	for root, want := range map[string]int{"/tmp": 35, "/dev/shm": 39, "/" + strings.Repeat("t", 75): 107, "/" + strings.Repeat("t", 76): 108} {
		if got := SocketPathUnder(root); got != want {
			t.Errorf("SocketPathUnder(%d bytes) = %d, want %d", len(root), got, want)
		}
	}
	alloc := filepath.Join(shortDir(t), "a")
	if err := os.Mkdir(alloc, 0o700); err != nil {
		t.Fatal(err)
	}
	shm, base := shortDir(t), shortDir(t)
	for _, n := range []int{76, 77} {
		tmp := filepath.Join(base, strings.Repeat("p", n-len(base)-1))
		if err := os.Mkdir(tmp, 0o700); err != nil {
			t.Fatal(err)
		}
		j, err := StartJob(JobOptions{ShmDir: shm, TmpDir: tmp, AllocDir: alloc, Allocation: "u-1"})
		if err != nil {
			t.Fatal(err)
		}
		under := strings.HasPrefix(j.Socket(), tmp+"/")
		_ = j.Close()
		if under != (n == 76) {
			t.Errorf("a TMPDIR of %d bytes: the socket is under it %v, want %v", n, under, n == 76)
		}
	}
}

func TestTheSocketDirectoryIsTMPDIRThenDevShm(t *testing.T) {
	alloc := filepath.Join(shortDir(t), "a")
	if err := os.Mkdir(alloc, 0o700); err != nil {
		t.Fatal(err)
	}
	shm, tmp := shortDir(t), shortDir(t)
	var logs bytes.Buffer
	start := func(o JobOptions) (*Job, error) {
		logs.Reset()
		o.AllocDir, o.Allocation, o.Logger = alloc, "u-1", slog.New(slog.NewTextHandler(&logs, nil))
		return StartJob(o)
	}
	j, err := start(JobOptions{ShmDir: shm, TmpDir: tmp})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(j.Socket(), tmp+"/hb-relay-") || strings.Contains(logs.String(), "fallback") {
		t.Errorf("with both: the socket is %s, want under TMPDIR %s, and no fallback logged: %s", j.Socket(), tmp, logs.String())
	}
	_ = j.Close()
	long := filepath.Join(tmp, strings.Repeat("d", 90))
	if err := os.Mkdir(long, 0o700); err != nil {
		t.Fatal(err)
	}
	j, err = start(JobOptions{ShmDir: shm, TmpDir: long})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(j.Socket(), shm+"/hb-relay-") || !strings.Contains(logs.String(), "would pass 107 bytes") {
		t.Errorf("with a TMPDIR too long: the socket is %s, want under %s, with the fallback logged: %s", j.Socket(), shm, logs.String())
	}
	_ = j.Close()
	for name, o := range map[string]JobOptions{
		"no TMPDIR":                 {ShmDir: shm, TmpDir: ""},
		"a TMPDIR that is missing":  {ShmDir: shm, TmpDir: filepath.Join(tmp, "missing")},
		"a TMPDIR and shm too long": {ShmDir: long, TmpDir: long},
	} {
		if j, err := start(o); !errors.Is(err, ErrNoSocketDir) {
			if j != nil {
				_ = j.Close()
			}
			t.Errorf("%s: %v, want ErrNoSocketDir, not a fallback", name, err)
		}
	}
}

// A connection that finds no session is closed once the wait passes.
func TestAConnectionWithNoSessionIsClosedAfterTheWait(t *testing.T) {
	j, _ := startTestJob(t, 300*time.Millisecond)
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

// Another user's peer on the agent's socket is refused (SO_PEERCRED): it
// gets no greeting. A second account is needed to make one for real, so the
// peer's user is injected.
func TestAnotherUsersPeerIsRefused(t *testing.T) {
	// The peer's user is injected into the job's options, before the server
	// serves, and flipped through an atomic: the server's goroutines read it
	// while the test changes what it answers (issue #379: swapping a package
	// variable raced with them under -race).
	var another atomic.Bool
	j, _ := startTestJobWith(t, time.Second, func(o *JobOptions) {
		o.peerOf = func(c net.Conn) (int, error) {
			if another.Load() {
				return os.Geteuid() + 1, nil
			}
			return peerUID(c)
		}
	})
	read := func() (string, error) {
		c, err := net.Dial("unix", j.Socket())
		if err != nil {
			return "", err
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		return readLine(c)
	}
	if g, err := read(); err != nil || g != agentapi.RelayGreeting("u-1") {
		t.Fatalf("this user's peer: %q, %v; want the greeting", g, err)
	}
	another.Store(true)
	if g, err := read(); err == nil {
		t.Errorf("another user's peer got %q", g)
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
	go func() { served <- Serve(context.Background(), hostEnd, ln.Addr().String(), "u-1", nil, nil) }()
	if _, err := io.WriteString(jobEnd, agentapi.RelayPreamble("u-1")+"\n"); err != nil {
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

// The relay's exit code decides superseded and agent not ready before its
// standard error, which the ssh client keeps only in part (the delta review
// of PR 301, N2).
func TestAnEndIsReadFromTheRelaysExitCodeFirst(t *testing.T) {
	noise := strings.Repeat("a login shell's line\n", 300) // past the 4 KiB kept
	if k, _ := classifyEnd(ExitSuperseded, noise); k != EndSuperseded {
		t.Errorf("exit %d with noise: %s, want superseded", ExitSuperseded, k)
	}
	if k, c := classifyEnd(ExitNotReady, noise); k != EndRetried || c != "agent not ready (exit 4)" {
		t.Errorf("exit %d with noise: %s %q, want retried, agent not ready (exit 4)", ExitNotReady, k, c)
	}
	if _, c := classifyEnd(ExitSuperseded, noise); !strings.Contains(c, "(exit 5)") {
		t.Errorf("exit %d: cause %q does not say it was read from the exit code", ExitSuperseded, c)
	}
	if k, _ := classifyEnd(1, noise); k != EndRetried {
		t.Errorf("exit 1 with noise: %s, want retried", k)
	}
}

// A greeting for another Allocation and a socket of another user each have
// their own fixed line (N4, N1), and both are agent not ready.
func TestTheRelaysRefusalsHaveTheirOwnLines(t *testing.T) {
	a := agentapi.RelayArgs{Channel: agentapi.RelayControl, Allocation: "u-2"}
	wrong := RelayMessage(a, fmt.Errorf("%w: %w", ErrNotReady, errWrongAllocation))
	other := RelayMessage(a, fmt.Errorf("%w: %w", ErrNotReady, errNotOurs))
	plain := RelayMessage(a, ErrNotReady)
	if wrong == plain || other == plain || wrong == other || !strings.Contains(wrong, "another Allocation") || !strings.Contains(other, "another user") {
		t.Errorf("the lines are not their own:\n%s\n%s\n%s", wrong, other, plain)
	}
	for _, l := range []string{wrong, other} {
		if k, _ := ClassifyEnd(l); k != EndRetried || !strings.HasPrefix(l, "hb-relay: agent not ready") {
			t.Errorf("%q is not read as agent not ready", l)
		}
	}
}

// The relay refuses a socket whose peer is another user, and a socket
// directory that is not this user's own mode 700, before it trusts any
// greeting (N1). Another user's socket needs a second account, so the peer's
// user is injected on the relay's side only; the agent's side is untouched.
func TestTheRelayRefusesAnotherUsersSocket(t *testing.T) {
	j, alloc := startTestJob(t, time.Second)
	a := agentapi.RelayArgs{Channel: agentapi.RelayControl, Dir: alloc, Allocation: "u-1", Epoch: 1, Seq: 1}
	old := relayPeerUID
	relayPeerUID = func(net.Conn) (int, error) { return os.Geteuid() + 1, nil }
	err := Relay(a, strings.NewReader(""), io.Discard)
	relayPeerUID = old
	if !errors.Is(err, errNotOurs) || !errors.Is(err, ErrNotReady) {
		t.Errorf("another user's peer: %v, want agent not ready, not ours", err)
	}
	if err := os.Chmod(filepath.Dir(j.Socket()), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Relay(a, strings.NewReader(""), io.Discard); !errors.Is(err, errNotOurs) {
		t.Errorf("a socket directory of mode 755: %v, want not ours", err)
	}
}
