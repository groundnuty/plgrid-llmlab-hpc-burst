package agent_test

// Frpc's supervision against a stand-in frpc, a shell script, with no frp
// binaries: which Instance it serves, the stop at the Instance's end, the
// restart after an exit with backoff, a start that follows what changed
// during a stop, the files it cannot read yet, and the files that change
// after they were checked.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

const (
	uidA = "aaaaaaaa-0000-0000-0000-000000000001"
	uidB = "bbbbbbbb-0000-0000-0000-000000000002"
	uidC = "cccccccc-0000-0000-0000-000000000003"
)

// testServer is a target the test moves, as a forward's state moves.
type testServer struct {
	mu sync.Mutex
	t  agent.FrpcTarget
	ch chan struct{}
}

func newTestServer(port int) *testServer {
	return &testServer{t: agent.FrpcTarget{Host: "127.0.0.1", Port: port, Ready: true}, ch: make(chan struct{})}
}

func (s *testServer) get() (agent.FrpcTarget, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.t, s.ch
}

func (s *testServer) set(t agent.FrpcTarget) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.t = t
	close(s.ch)
	s.ch = make(chan struct{})
}

// supStart is one frpc start as the test saw it in OnStart: the process,
// when, and the configuration it was started with.
type supStart struct {
	id     agent.ProcessID
	at     time.Time
	uid    string
	port   string
	others []agent.ProcessID // the earlier starts still alive at this start
}

// supRig runs Frpc with a stand-in binary that writes "<version> <pid>" to a
// log at each start.
type supRig struct {
	t      *testing.T
	dir    string
	bin    string
	server *testServer
	f      *agent.Frpc
	logs   *logBuf
	mu     sync.Mutex
	starts []supStart
	done   chan error
	cancel context.CancelFunc
}

// stubScript is a stand-in frpc that records its version and PID, then runs
// until it is stopped.
func stubScript(version, log string) []byte {
	return []byte("#!/bin/sh\necho \"" + version + " $$\" >> " + log + "\nwhile :; do sleep 1; done\n")
}

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

var (
	nameLine = regexp.MustCompile(`(?m)^name = "([^"]*)"$`)
	portLine = regexp.MustCompile(`(?m)^serverPort = ([0-9]+)$`)
)

func newSupRig(t *testing.T, grace time.Duration, setup func(r *supRig)) *supRig {
	t.Helper()
	dir := shortTemp(t)
	r := &supRig{t: t, dir: dir, server: newTestServer(41001), logs: &logBuf{}, done: make(chan error, 1)}
	script := stubScript("v1", filepath.Join(dir, "versions.log"))
	r.bin = writeFile(t, filepath.Join(dir, "frpc"), script)
	if err := os.Chmod(r.bin, 0o700); err != nil {
		t.Fatal(err)
	}
	c := makeCert(t, launcherName)
	cert := writeFile(t, filepath.Join(dir, "staged.crt"), c.pem)
	run := filepath.Join(dir, "run")
	if err := os.Mkdir(run, 0o700); err != nil {
		t.Fatal(err)
	}
	r.f = &agent.Frpc{
		Binary: r.bin, FrpcDigest: digestOf(script), CertFile: cert, CertSPKISHA256: c.pin,
		Credential: hCredential, RunDir: run, Socket: filepath.Join(run, "serve.sock"),
		Server: r.server.get, MinBackoff: 300 * time.Millisecond, MaxBackoff: 5 * time.Second,
		StopGrace: grace, Logger: slogTo(r.logs),
	}
	r.f.OnStart = func(id agent.ProcessID) {
		b, _ := os.ReadFile(r.f.ConfigPath())
		s := supStart{id: id, at: time.Now()}
		if m := nameLine.FindSubmatch(b); m != nil {
			s.uid = string(m[1])
		}
		if m := portLine.FindSubmatch(b); m != nil {
			s.port = string(m[1])
		}
		r.mu.Lock()
		for _, p := range r.starts {
			if p.id.Alive() {
				s.others = append(s.others, p.id)
			}
		}
		r.starts = append(r.starts, s)
		r.mu.Unlock()
	}
	if setup != nil {
		setup(r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() { r.done <- r.f.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(15 * time.Second):
			t.Error("Run did not return within 15 s of its end")
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, s := range r.starts {
			if s.id.Alive() {
				_ = killID(s.id)
				t.Errorf("frpc %d outlived Run", s.id.PID)
			}
		}
		if t.Failed() {
			t.Logf("Frpc's log:\n%s", r.logs.String())
		}
	})
	return r
}

func (r *supRig) all() []supStart {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]supStart(nil), r.starts...)
}

// waitStarts waits until there are n starts and returns them.
func (r *supRig) waitStarts(n int, d time.Duration) []supStart {
	r.t.Helper()
	eventually(r.t, d, fmt.Sprintf("%d frpc starts", n), func() bool { return len(r.all()) >= n })
	return r.all()
}

// noStartFor checks that no start beyond n comes within d.
func (r *supRig) noStartFor(n int, d time.Duration) {
	r.t.Helper()
	time.Sleep(d)
	if got := len(r.all()); got != n {
		r.t.Fatalf("frpc started %d times, want %d", got, n)
	}
}

func (r *supRig) versions() string {
	b, _ := os.ReadFile(filepath.Join(r.dir, "versions.log"))
	return string(b)
}

// waitRecorded waits until a start's own process has written its line to
// versions.log, and returns that line's version. The stand-in writes the
// line after it starts, so a start that the supervisor stops at once, at the
// next Serve, may never write it: a test that reads which file a start ran
// waits for the start's line before it changes what frpc serves (issue
// #141: a frpc stopped 11 ms after its start had not written its line yet).
func (r *supRig) waitRecorded(s supStart, d time.Duration) string {
	r.t.Helper()
	tail := fmt.Sprintf(" %d\n", s.id.PID)
	eventually(r.t, d, fmt.Sprintf("frpc %d to record itself", s.id.PID), func() bool {
		return strings.Contains(r.versions(), tail)
	})
	for _, line := range strings.SplitAfter(r.versions(), "\n") {
		if strings.HasSuffix(line, tail) {
			return strings.TrimSuffix(line, tail)
		}
	}
	r.t.Fatalf("no line of frpc %d in:\n%s", s.id.PID, r.versions())
	return ""
}

func waitGoneID(t *testing.T, id agent.ProcessID, d time.Duration) {
	t.Helper()
	eventually(t, d, fmt.Sprintf("frpc %d to be gone", id.PID), func() bool { return !id.Alive() })
}

// Serve names the Instance frpc serves (HBEP-6, frpc in the job; Paths): A
// starts A; B stops A before B starts, and B's configuration names B; ""
// stops frpc, and nothing starts again.
func TestFrpcFollowsTheServedInstance(t *testing.T) {
	r := newSupRig(t, time.Second, nil)
	r.f.Serve(uidA)
	s := r.waitStarts(1, 5*time.Second)
	if s[0].uid != uidA {
		t.Fatalf("the first frpc serves %q, want %q", s[0].uid, uidA)
	}
	r.f.Serve(uidB)
	s = r.waitStarts(2, 5*time.Second)
	if s[1].uid != uidB {
		t.Fatalf("the second frpc serves %q, want %q", s[1].uid, uidB)
	}
	if len(s[1].others) != 0 {
		t.Fatalf("B's frpc started while %v still ran", s[1].others)
	}
	r.f.Serve("")
	waitGoneID(t, s[1].id, 5*time.Second)
	r.noStartFor(2, 4*r.f.MinBackoff)
}

// frpc that exits by itself is started again after a backoff that doubles
// from MinBackoff, without asking the controller (HBEP-6, frpc in the job);
// once the Instance has ended, an exit starts nothing.
func TestFrpcRestartsAfterAnExitWithBackoff(t *testing.T) {
	r := newSupRig(t, time.Second, nil)
	r.f.Serve(uidA)
	s := r.waitStarts(1, 5*time.Second)
	var killedAt []time.Time
	for i := 1; i <= 2; i++ {
		killedAt = append(killedAt, time.Now())
		if err := syscall.Kill(-s[i-1].id.PID, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		s = r.waitStarts(i+1, 10*time.Second)
	}
	for i, want := range []time.Duration{r.f.MinBackoff, 2 * r.f.MinBackoff} {
		if gap := s[i+1].at.Sub(killedAt[i]); gap < want {
			t.Fatalf("restart %d came %s after the kill, sooner than the %s backoff", i+1, gap, want)
		}
		if s[i+1].uid != uidA {
			t.Fatalf("restart %d serves %q", i+1, s[i+1].uid)
		}
	}
	// A kill, then the Instance ends during the backoff: nothing restarts.
	if err := syscall.Kill(-s[2].id.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitGoneID(t, s[2].id, 5*time.Second)
	r.f.Serve("")
	r.noStartFor(3, 3*4*r.f.MinBackoff)
}

// A stop can take the whole grace. What changed meanwhile decides the next
// start, not what was read before the stop: B's frpc starts on connection 2's
// new port, once, and not before the target is ready (Astra B1). The old
// frpc ignores TERM and is frozen with SIGSTOP, so it lasts the grace. (A
// frozen process that does not ignore TERM dies of it: a fatal signal is
// delivered to a stopped process.)
func TestFrpcStartsFromWhatChangedDuringAStop(t *testing.T) {
	for _, tc := range []struct {
		name     string
		duringOn func(*testServer)
		after    func(*testServer)
	}{
		{"the port changes", func(s *testServer) {
			s.set(agent.FrpcTarget{Host: "127.0.0.1", Port: 41002, Ready: true})
		}, nil},
		{"the forward goes down", func(s *testServer) {
			s.set(agent.FrpcTarget{Host: "127.0.0.1", Port: 41001, Ready: false})
		}, func(s *testServer) {
			s.set(agent.FrpcTarget{Host: "127.0.0.1", Port: 41002, Ready: true})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const grace = 1500 * time.Millisecond
			r := newSupRig(t, grace, func(r *supRig) {
				script := []byte("#!/bin/sh\ntrap '' TERM\necho \"v1 $$\" >> " + filepath.Join(r.dir, "versions.log") + "\nwhile :; do sleep 1; done\n")
				writeFile(t, r.bin, script)
				r.f.FrpcDigest = digestOf(script)
			})
			r.f.Serve(uidA)
			s := r.waitStarts(1, 5*time.Second)
			if err := syscall.Kill(s[0].id.PID, syscall.SIGSTOP); err != nil {
				t.Fatal(err)
			}
			r.f.Serve(uidB)
			time.Sleep(grace / 3)
			if !s[0].id.Alive() {
				t.Fatal("the frozen frpc is gone before its grace: the test changes nothing during a stop")
			}
			tc.duringOn(r.server)
			if tc.after != nil {
				waitGoneID(t, s[0].id, 3*grace)
				r.noStartFor(1, 4*r.f.MinBackoff)
				tc.after(r.server)
			}
			s = r.waitStarts(2, 4*grace)
			if s[1].uid != uidB || s[1].port != "41002" {
				t.Fatalf("B's first frpc serves %q on port %s, want %q on 41002", s[1].uid, s[1].port, uidB)
			}
			r.noStartFor(2, 4*r.f.MinBackoff)
		})
	}
}

// Files frpc cannot read yet are retried with backoff, never taken as a
// mismatch: with the staged certificate and frpc missing at the start, Run
// keeps going, and starts frpc once they are there (Opus B1).
func TestFrpcRetriesFilesItCannotReadYet(t *testing.T) {
	var cert, bin []byte
	r := newSupRig(t, time.Second, func(r *supRig) {
		var err error
		if cert, err = os.ReadFile(r.f.CertFile); err != nil {
			t.Fatal(err)
		}
		if bin, err = os.ReadFile(r.bin); err != nil {
			t.Fatal(err)
		}
		os.Remove(r.f.CertFile)
		os.Remove(r.bin)
	})
	r.f.Serve(uidA)
	time.Sleep(4 * r.f.MinBackoff)
	select {
	case err := <-r.done:
		r.done <- err
		t.Fatalf("Run ended on files it could not read: %v", err)
	default:
	}
	if n := len(r.all()); n != 0 {
		t.Fatalf("frpc started %d times without its files", n)
	}
	writeFile(t, r.f.CertFile, cert)
	writeFile(t, r.bin, bin)
	if err := os.Chmod(r.bin, 0o700); err != nil {
		t.Fatal(err)
	}
	s := r.waitStarts(1, 15*time.Second)
	if s[0].uid != uidA {
		t.Fatalf("frpc serves %q", s[0].uid)
	}
}

// What runs is the frpc that was hashed, and what frpc trusts is the
// certificate that was checked, whatever happens to the staged files after
// the check (Astra B2, Opus B1 and N1):
//   - the staged frpc is purged: the next Instance's frpc starts all the same;
//   - another frpc is put at its path: the hashed one still runs;
//   - another certificate is put at the staged path: frpc's trustedCaFile is
//     the checked copy in the run directory, unchanged;
//   - the hashed file itself is written to: it is hashed again before the
//     next start, and refused.
//
// Which file a start ran is read from the line its process writes, so each
// start's line is waited for before the next Serve stops it (waitRecorded).
func TestFrpcRunsWhatItCheckedAfterTheStagedFilesChange(t *testing.T) {
	const wait = 30 * time.Second
	r := newSupRig(t, time.Second, nil)
	staged, err := os.ReadFile(r.f.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	r.f.Serve(uidA)
	s := r.waitStarts(1, wait)
	ran := []string{r.waitRecorded(s[0], wait)}

	held := filepath.Join(r.dir, "held-frpc") // the hashed file, renamed away
	if err := os.Rename(r.bin, held); err != nil {
		t.Fatal(err)
	}
	r.f.Serve(uidB)
	s = r.waitStarts(2, wait)
	if s[1].uid != uidB {
		t.Fatalf("after the purge frpc serves %q", s[1].uid)
	}
	ran = append(ran, r.waitRecorded(s[1], wait))

	v2 := stubScript("v2", filepath.Join(r.dir, "versions.log"))
	writeFile(t, r.bin, v2)
	if err := os.Chmod(r.bin, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, r.f.CertFile, makeCert(t, launcherName).pem)
	r.f.Serve(uidC)
	s = r.waitStarts(3, wait)
	ran = append(ran, r.waitRecorded(s[2], wait))
	if strings.Contains(r.versions(), "v2") {
		t.Fatalf("the frpc put at the staged path ran:\n%s", r.versions())
	}
	for i, v := range ran {
		if v != "v1" {
			t.Fatalf("start %d ran %q, want the hashed v1:\n%s", i+1, v, r.versions())
		}
	}
	if n := strings.Count(r.versions(), "v1 "); n != 3 || len(r.all()) != 3 {
		t.Fatalf("the hashed frpc ran %d times in %d starts, want 3 and 3:\n%s", n, len(r.all()), r.versions())
	}
	cfg, err := os.ReadFile(r.f.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(cfg, []byte(`transport.tls.trustedCaFile = "`+r.f.TrustedCertPath()+`"`)) {
		t.Fatalf("frpc does not trust the checked copy:\n%s", cfg)
	}
	if b, err := os.ReadFile(r.f.TrustedCertPath()); err != nil || !bytes.Equal(b, staged) {
		t.Fatalf("the trusted copy is not the checked certificate: %v", err)
	}

	// A write into the hashed file itself.
	hf, err := os.OpenFile(held, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hf.WriteString("# changed\n"); err != nil {
		t.Fatal(err)
	}
	hf.Close()
	r.f.Serve(uidA)
	select {
	case err := <-r.done:
		r.done <- err
		if err == nil || !strings.Contains(err.Error(), "SHA-256") {
			t.Fatalf("Run returned %v; want the digest refusal", err)
		}
	case <-time.After(wait):
		t.Fatal("Run went on with a frpc that no longer has agent.json's digest")
	}
	if n := len(r.all()); n != 3 {
		t.Fatalf("frpc started %d times; the changed file must not run", n)
	}
}

func slogTo(w *logBuf) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }
