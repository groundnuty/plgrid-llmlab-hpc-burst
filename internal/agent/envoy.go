package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/envoypin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
)

// The router's launch (HBEP-5, Inside an Instance; #429, beta, opt-in). Envoy runs bare, as frpc
// does (the science agent's ruling on #429, Q11): the agent checks the binary against its embedded
// pin (envoypin) through the descriptor it then runs, writes the bootstrap the guard accepted, starts
// Envoy in a process group of its own, and waits for its two sockets. Only then is the router running,
// and only after the check of what Envoy listens on.

// envoyConcurrency is Envoy's worker threads: spike S13's setting. Its default, one per hardware
// thread, would be dozens on a GPU node for one Instance's traffic.
const envoyConcurrency = 2

// EnvoyStart is one start of the router.
type EnvoyStart struct {
	// Binary is the staged Envoy. Its sha256 must be the pin for this machine's architecture.
	Binary string
	// Config is what the bootstrap is rendered from; its RunDir is the job's private directory.
	Config EnvoyConfig
	// Ready bounds the wait for Envoy's two sockets after it starts.
	Ready time.Duration
}

// EnvoyProc is a running router.
type EnvoyProc struct {
	Group *procgroup.Group
	log   *os.File
}

// Close releases the log file once the group is done.
func (p *EnvoyProc) Close() { p.log.Close() }

// StartEnvoy renders and checks the bootstrap, verifies the binary, starts Envoy and waits for its
// sockets, then checks what it listens on. On any failure it leaves no Envoy running.
func StartEnvoy(s EnvoyStart, now time.Time) (*EnvoyProc, error) {
	c := s.Config
	raw, err := RenderEnvoyConfig(c)
	if err != nil {
		return nil, err
	}
	if err := CheckEnvoyConfig(raw, c); err != nil {
		return nil, err
	}
	bin, err := openVerifiedEnvoy(s.Binary)
	if err != nil {
		return nil, err
	}
	defer bin.Close()
	// Envoy runs bare, so the node's glibc must be new enough for it (#454).
	if err := checkEnvoyGlibc(bin); err != nil {
		return nil, err
	}
	cfgPath := filepath.Join(c.RunDir, EnvoyConfigFile)
	if err := writeFileAtomic(cfgPath, raw, 0o600); err != nil {
		return nil, fmt.Errorf("envoy: config: %w", err)
	}
	listener, admin := filepath.Join(c.RunDir, EnvoyListenerSocket), filepath.Join(c.RunDir, EnvoyAdminSocket)
	for _, p := range []string{listener, admin} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("envoy: a stale socket: %w", err)
		}
	}
	// Envoy is no Go binary, so it keeps the soft descriptor limit it inherits. The agent raises its
	// own to the hard limit, which every later child inherits (the ruling on #429, Q8): Envoy holds a
	// descriptor for each client stream and one for each worker connection.
	if err := raiseNoFile(); err != nil {
		return nil, fmt.Errorf("envoy: descriptor limit: %w", err)
	}
	logf, err := os.OpenFile(filepath.Join(c.RunDir, EnvoyLogFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("envoy: log: %w", err)
	}
	g, err := procgroup.Start(procgroup.Spec{
		Path: "/proc/self/fd/3", Argv0: "envoy", ExtraFiles: []*os.File{bin},
		Args: []string{"-c", cfgPath, "--disable-hot-restart", "--concurrency", fmt.Sprint(envoyConcurrency),
			"--log-path", filepath.Join(c.RunDir, EnvoyLogFile), "--log-level", "warn"},
		// No variable of the agent's reaches Envoy: it needs none.
		Env: []string{}, Dir: c.RunDir, Stdout: logf, Stderr: logf,
	}, now)
	if err != nil {
		logf.Close()
		return nil, fmt.Errorf("envoy: start: %w", err)
	}
	p := &EnvoyProc{Group: g, log: logf}
	fail := func(err error) (*EnvoyProc, error) {
		g.Kill()
		<-g.Done()
		logf.Close()
		return nil, err
	}
	if err := waitSockets(g, s.Ready, listener, admin); err != nil {
		return fail(err)
	}
	if err := CheckEnvoySockets(g.PID(), listener, admin); err != nil {
		return fail(err)
	}
	return p, nil
}

// openVerifiedEnvoy opens the binary and checks its sha256 against the pin, through the descriptor
// the caller then runs, so what runs is what was hashed whatever happens to the path.
func openVerifiedEnvoy(path string) (*os.File, error) {
	pin, err := envoypin.For(runtime.GOARCH)
	if err != nil {
		return nil, refuse("%v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("envoy: %w", err)
	}
	st, err := stampOf(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("envoy: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, st.size)); err != nil {
		f.Close()
		return nil, fmt.Errorf("envoy: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != pin.SHA256 {
		f.Close()
		return nil, refuse("%s has SHA-256 %s, not the pin of Envoy %s for %s, %s", path, got, pin.Version, pin.Arch, pin.SHA256)
	}
	return f, nil
}

// waitSockets waits until the process listens on each path, or exits, or the deadline passes.
// Listening is read from /proc/net/unix by the process's own socket inodes: a socket file exists from
// Envoy's bind, a moment before its listen, and a check or a connect in between finds no listener (seen
// twice in 100 starts under load), so the file's existence alone is not readiness.
func waitSockets(g *procgroup.Group, within time.Duration, paths ...string) error {
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		listening, _ := listeningUnixSockets(socketInodes(g.PID()))
		all := true
		for _, p := range paths {
			all = all && listening[p]
		}
		if all {
			return nil
		}
		select {
		case <-g.Exited():
			return fmt.Errorf("envoy exited with code %d before it listened", g.ExitCode())
		case <-deadline.C:
			return fmt.Errorf("envoy did not create %v within %v", paths, within)
		case <-tick.C:
		}
	}
}

// raiseNoFile raises this process's soft RLIMIT_NOFILE to its hard limit.
func raiseNoFile() error {
	var r syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r); err != nil {
		return err
	}
	// Set even when the soft limit already equals the hard one: Go's runtime keeps the limit it
	// started with and restores it for its children unless the program sets one itself, so this
	// Setrlimit is what makes Envoy inherit the hard limit (#442's review).
	r.Cur = r.Max
	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &r)
}

// writeFileAtomic writes b to path through a temporary file in the same directory, renamed into
// place, so a reader never sees half of it.
func writeFileAtomic(path string, b []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, werr := f.Write(b)
	if cerr := f.Chmod(mode); werr == nil {
		werr = cerr
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(tmp, path)
	}
	if werr != nil {
		os.Remove(tmp)
	}
	return werr
}
