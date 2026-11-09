package hostdial

// Unit tests of the parts' own rules. They are not the Test plan's evidence,
// which goes through srun's standard streams (testplan_test.go,
// review_test.go), except where a case cannot be made there: another user's
// peer on the agent's socket needs a second account, so its check is shown
// here with an injected peer.

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

// One host-dialed Provider per login host (HBEP-6, Capacity): a second is
// refused and named; a Provider whose credentials move to another host gives
// up the first.
func TestHostsOneProviderPerLoginHost(t *testing.T) {
	h := NewHosts()
	if _, ok := h.Claim("login.example", "helios-a"); !ok {
		t.Fatal("the first claim was refused")
	}
	if holder, ok := h.Claim("login.example", "helios-b"); ok || holder != "helios-a" {
		t.Errorf("a second Provider's claim: %q, %v; want a refusal naming helios-a", holder, ok)
	}
	if _, ok := h.Claim("login.example", "helios-a"); !ok {
		t.Error("the holder's own claim again was refused")
	}
	if _, ok := h.Claim("login2.example", "helios-a"); !ok {
		t.Fatal("a move to another host was refused")
	}
	if _, ok := h.Claim("login.example", "helios-b"); !ok {
		t.Error("the host a Provider moved away from is still held")
	}
	h.Release("helios-b")
	if _, ok := h.Claim("login.example", "helios-c"); !ok {
		t.Error("a released host is still held")
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
	base := shortDir(t)
	alloc := filepath.Join(base, "a")
	if err := os.Mkdir(alloc, 0o700); err != nil {
		t.Fatal(err)
	}
	j, err := StartJob(JobOptions{ShmDir: base, TmpDir: base, AllocDir: alloc, Allocation: "u-1", Wait: wait})
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

// The socket's directory goes under /dev/shm first, then under TMPDIR when
// /dev/shm is missing or too long for a socket path, and a job side with
// neither fails by name.
func TestTheSocketDirectoryIsDevShmThenTMPDIR(t *testing.T) {
	alloc := filepath.Join(shortDir(t), "a")
	if err := os.Mkdir(alloc, 0o700); err != nil {
		t.Fatal(err)
	}
	shm, tmp := shortDir(t), shortDir(t)
	start := func(o JobOptions) (*Job, error) {
		o.AllocDir, o.Allocation = alloc, "u-1"
		return StartJob(o)
	}
	j, err := start(JobOptions{ShmDir: shm, TmpDir: tmp})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(j.Socket(), shm+"/hb-relay-") {
		t.Errorf("with both: the socket is %s, want under the shm directory %s", j.Socket(), shm)
	}
	_ = j.Close()
	for name, o := range map[string]JobOptions{
		"no shm":     {ShmDir: filepath.Join(shm, "missing"), TmpDir: tmp},
		"a long shm": {ShmDir: filepath.Join(shm, strings.Repeat("d", 90)), TmpDir: tmp},
	} {
		if strings.Contains(name, "long") {
			if err := os.Mkdir(o.ShmDir, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		j, err := start(o)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !strings.HasPrefix(j.Socket(), tmp+"/hb-relay-") {
			t.Errorf("%s: the socket is %s, want under TMPDIR %s", name, j.Socket(), tmp)
		}
		_ = j.Close()
	}
	if _, err := start(JobOptions{ShmDir: filepath.Join(shm, "missing"), TmpDir: ""}); !errors.Is(err, ErrNoSocketDir) {
		t.Errorf("neither: %v, want ErrNoSocketDir", err)
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
	j, _ := startTestJob(t, time.Second)
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
	old := peerUID
	peerUID = func(net.Conn) (int, error) { return os.Geteuid() + 1, nil }
	defer func() { peerUID = old }()
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
