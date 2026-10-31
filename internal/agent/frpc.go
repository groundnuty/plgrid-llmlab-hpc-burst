package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// FrpcTarget is where frpc dials at one moment.
type FrpcTarget struct {
	Host string
	Port int
	// Ready is true when frpc may start: in ssh mode, once connection 2's
	// forward listens (HBEP-6, Dial-home modes); in direct mode always.
	Ready bool
}

// FrpcServer reports frpc's target and a channel that is closed at its next
// change (nil when it never changes).
type FrpcServer func() (FrpcTarget, <-chan struct{})

// ForwardServer is frpc's target in ssh mode: connection 2's local end on
// 127.0.0.1, ready while its forward listens.
func ForwardServer(f *SSHForward) FrpcServer {
	return func() (FrpcTarget, <-chan struct{}) {
		st, ch := f.State()
		return FrpcTarget{Host: "127.0.0.1", Port: st.Port, Ready: st.Listening && st.Port != 0}, ch
	}
}

// DirectServer is frpc's target in direct mode: the front's dial-home
// listener, agent.json's endpoint, always ready.
func DirectServer(endpoint string) (FrpcServer, error) {
	host, ps, err := net.SplitHostPort(endpoint)
	if err != nil {
		return nil, fmt.Errorf("dial-home endpoint %q: %w", endpoint, err)
	}
	port, err := strconv.Atoi(ps)
	if err != nil {
		return nil, fmt.Errorf("dial-home endpoint %q: %w", endpoint, err)
	}
	if strings.Contains(host, ":") {
		// HBEP-6 names the front by a DNS name; frpc's serverAddr takes no
		// bracketed IPv6 literal.
		return nil, fmt.Errorf("dial-home endpoint %q: an IPv6 literal is not supported; name the front by its DNS name or an IPv4 address", endpoint)
	}
	return func() (FrpcTarget, <-chan struct{}) {
		return FrpcTarget{Host: host, Port: port, Ready: true}, nil
	}, nil
}

// Frpc runs frpc for the Instance the agent serves (HBEP-6, frpc in the
// job): one tcpmux proxy, named by the Instance's UID, that serves the
// agent's serving socket over wss to the dial-home listener.
//
// frpc is the agent's transport, not a part of the Instance: when it exits by
// itself, Frpc starts it again after a backoff, without asking the controller.
// It restarts frpc when the target's port changes, since frpc keeps retrying
// the port it was started with, and when the Instance changes. A target that
// is not ready only holds back a start: a running frpc rides out its
// forward's reconnects on the same port.
//
// At its start Run opens frpc once and checks its SHA-256 through that
// descriptor, which it keeps: every frpc it starts runs as /proc/self/fd/3
// from it, so what runs is what was hashed, even after the staged file is
// replaced or purged. It checks the staged certificate and its pin and copies
// the checked bytes into the run directory, which frpc then trusts. A
// mismatch (digest, pin, PEM, name, validity) ends Run with the reason; an
// I/O error is retried with backoff.
type Frpc struct {
	// Binary is the staged frpc (<stagingDir>/frpc/<FrpcDigest>/frpc).
	Binary string
	// FrpcDigest is agent.json's frpcDigest: frpc's SHA-256.
	FrpcDigest string
	// CertFile is the staged launcher certificate (dial-home.crt in the
	// Allocation's directory), frpc's only trusted certificate.
	CertFile string
	// CertSPKISHA256 is agent.json's pin, which CertFile must have.
	CertSPKISHA256 string
	// Credential is the Allocation's credential, frpc's login token. It is
	// only in frpc's configuration file, mode 600, never on a command line or
	// in a log.
	Credential string
	// RunDir is the job's private run directory: frpc's configuration and
	// log go there.
	RunDir string
	// Socket is the Unix socket the proxy serves: the agent's serving socket
	// (ServingSocket).
	Socket string
	// Server is where frpc dials.
	Server FrpcServer
	// Env is the environment frpc starts with, less the proxy variables
	// (os.Environ() when nil).
	Env []string
	// Logger reports starts, exits and refusals.
	Logger *slog.Logger
	// MinBackoff and MaxBackoff bound the wait between starts after frpc
	// exited by itself (defaults 1 s and 30 s).
	MinBackoff, MaxBackoff time.Duration
	// StopGrace is how long a stopped frpc has after TERM before KILL
	// (default 5 s).
	StopGrace time.Duration
	// OnStart, when set, is told each frpc process Frpc starts.
	OnStart func(ProcessID)

	mu   sync.Mutex
	uid  string
	kick chan struct{}

	// bin is the verified frpc, set by prepare; only Run's goroutine uses it.
	bin *verifiedBinary
}

// Serve sets the Instance whose proxy frpc serves; "" stops frpc. It never
// blocks.
func (f *Frpc) Serve(uid string) {
	f.mu.Lock()
	changed := f.uid != uid
	f.uid = uid
	k := f.kickLocked()
	f.mu.Unlock()
	if changed {
		select {
		case k <- struct{}{}:
		default:
		}
	}
}

func (f *Frpc) kickLocked() chan struct{} {
	if f.kick == nil {
		f.kick = make(chan struct{}, 1)
	}
	return f.kick
}

func (f *Frpc) desired() (string, <-chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.uid, f.kickLocked()
}

// ConfigPath is frpc's configuration file in the run directory.
func (f *Frpc) ConfigPath() string { return filepath.Join(f.RunDir, "frpc.toml") }

// LogPath is frpc's log file in the run directory.
func (f *Frpc) LogPath() string { return filepath.Join(f.RunDir, "frpc.log") }

// TrustedCertPath is the copy of the checked certificate in the run
// directory, frpc's trustedCaFile.
func (f *Frpc) TrustedCertPath() string { return filepath.Join(f.RunDir, "dial-home.crt") }

// running is the frpc process Frpc runs and what it was started for.
type running struct {
	g    *procgroup.Group
	uid  string
	host string
	port int
}

// Run serves the Instance set by Serve until ctx ends, and returns once its
// frpc is gone and reaped. It returns an error, starting nothing more, when a
// setting is missing or frpc or its certificate does not match agent.json;
// it retries what it cannot read yet.
func (f *Frpc) Run(ctx context.Context) error {
	if err := f.check(); err != nil {
		return err
	}
	logger := f.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	minB, maxB := f.MinBackoff, f.MaxBackoff
	if minB <= 0 {
		minB = time.Second
	}
	if maxB < minB {
		maxB = max(30*time.Second, minB)
	}
	grace := f.StopGrace
	if grace <= 0 {
		grace = 5 * time.Second
	}
	defer func() {
		if f.bin != nil {
			f.bin.f.Close()
			f.bin = nil
		}
		_ = os.Remove(f.ConfigPath())
	}()
	if err := f.prepare(ctx, logger, minB, maxB); err != nil || ctx.Err() != nil {
		return err
	}
	var cur *running
	stop := func(why string) {
		cur.g.Stop(grace)
		<-cur.g.Done()
		logger.Info("frpc stopped", "pid", cur.g.PID(), "why", why)
		cur = nil
	}
	defer func() {
		if cur != nil {
			stop("the agent's tunnel stopped")
		}
	}()
	backoff := minB
	var notBefore time.Time
	refusedUID := "" // an Instance UID frpc cannot serve, refused until Serve names another
	for {
		if ctx.Err() != nil {
			return nil
		}
		uid, kick := f.desired()
		target, changed := f.Server()
		if cur != nil {
			why := ""
			switch {
			case uid != cur.uid:
				why = "the Instance changed"
			case target.Host != cur.host || target.Port != cur.port:
				// frpc keeps retrying the port it was started with (HBEP-6,
				// A port that cannot be bound again).
				why = "the tunnel's port changed"
			}
			if why != "" {
				stop(why)
				// The stop can take the whole grace: read everything again.
				continue
			}
		}
		if uid != refusedUID {
			refusedUID = ""
		}
		startable := cur == nil && uid != "" && uid != refusedUID && target.Ready
		if startable && !time.Now().Before(notBefore) {
			r, err := f.start(ctx, uid, target, logger)
			switch {
			case errors.Is(err, errStale):
				continue
			case errors.Is(err, errBadUID):
				logger.Error("frpc not started for this Instance", "uid", uid, "err", err)
				refusedUID = uid
			case errors.Is(err, errRefused):
				return err
			case err != nil:
				logger.Warn("frpc did not start; trying again", "after", backoff, "err", err)
				notBefore = time.Now().Add(backoff)
				backoff = min(2*backoff, maxB)
			default:
				cur = r
			}
			continue
		}
		var exited <-chan struct{}
		if cur != nil {
			exited = cur.g.Exited()
		}
		var timer *time.Timer
		var wake <-chan time.Time
		if startable {
			timer = time.NewTimer(time.Until(notBefore))
			wake = timer.C
		}
		select {
		case <-ctx.Done():
		case <-kick:
		case <-changed:
		case <-wake:
		case <-exited:
			code := cur.g.ExitCode()
			started := cur.g.Start()
			cur.g.Stop(grace)
			<-cur.g.Done()
			cur = nil
			if time.Since(started) > time.Minute {
				backoff = minB
			}
			logger.Warn("frpc exited; starting it again", "code", code, "after", backoff)
			notBefore = time.Now().Add(backoff)
			backoff = min(2*backoff, maxB)
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

var (
	// errRefused marks frpc or its certificate not matching agent.json, or a
	// setting frpc cannot run with: Run ends, since a staged file does not
	// fix itself.
	errRefused = errors.New("frpc refused")
	// errBadUID marks an Instance UID frpc cannot name a proxy by.
	errBadUID = errors.New("the Instance's UID cannot name a proxy")
	// errStale marks a start abandoned because what it was for changed.
	errStale = errors.New("the start's Instance or target changed")
)

// check refuses settings that cannot run frpc, before anything starts.
func (f *Frpc) check() error {
	var errs []error
	if f.Binary == "" {
		errs = append(errs, errors.New("frpc: no binary"))
	}
	if f.RunDir == "" {
		errs = append(errs, errors.New("frpc: no run directory"))
	}
	if f.Server == nil {
		errs = append(errs, errors.New("frpc: no server"))
	}
	if f.CertFile == "" {
		errs = append(errs, errors.New("no dial-home certificate: frpc would verify nothing without one"))
	}
	if f.CertSPKISHA256 == "" {
		errs = append(errs, errors.New("no certificate pin (agent.json's certSPKISHA256)"))
	}
	if !digestForm.MatchString(f.FrpcDigest) {
		errs = append(errs, fmt.Errorf("frpc digest %q: want 64 lowercase hex characters", f.FrpcDigest))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", errRefused, err)
	}
	return nil
}

// prepare opens and checks frpc and the certificate, once, retrying what it
// cannot read with backoff until ctx ends. It returns a mismatch at once.
func (f *Frpc) prepare(ctx context.Context, logger *slog.Logger, minB, maxB time.Duration) error {
	backoff := minB
	for {
		err := f.prepareOnce()
		if err == nil || errors.Is(err, errRefused) {
			return err
		}
		logger.Warn("frpc's files cannot be read yet; trying again", "after", backoff, "err", err)
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
		backoff = min(2*backoff, maxB)
	}
}

func (f *Frpc) prepareOnce() error {
	b, err := os.ReadFile(f.CertFile)
	if err != nil {
		return fmt.Errorf("the dial-home certificate: %w", err)
	}
	if err := VerifyDialHomeCert(b, f.CertSPKISHA256, time.Now()); err != nil {
		return err
	}
	bin, err := openVerifiedBinary(f.Binary, f.FrpcDigest)
	if err != nil {
		return err
	}
	// frpc trusts the checked bytes, in the job's private directory, never
	// the staged file, which can change after the check (HBEP-6, Certificate
	// checks).
	if err := writePrivate(f.TrustedCertPath(), b); err != nil {
		bin.f.Close()
		return fmt.Errorf("the dial-home certificate's copy: %w", err)
	}
	f.bin = bin
	return nil
}

// start writes frpc's configuration for uid and target, mode 600, and starts
// frpc from its verified descriptor in a process group of its own, with the
// proxy variables dropped from its environment and its console output
// discarded: frpc logs to its log file, which stays in the job's private
// temporary directory (HBEP-6). Right before it starts frpc it reads the
// desired Instance and the target again, and abandons the start (errStale)
// if either changed or the target is no longer ready.
func (f *Frpc) start(ctx context.Context, uid string, target FrpcTarget, logger *slog.Logger) (*running, error) {
	if !uidForm.MatchString(uid) {
		return nil, fmt.Errorf("%w: %q is not a Kubernetes UID", errBadUID, uid)
	}
	if err := f.bin.recheck(); err != nil {
		return nil, err
	}
	cfg := FrpcConfig{
		ServerAddr: target.Host, ServerPort: target.Port,
		LogFile: f.LogPath(), Token: f.Credential, CAFile: f.TrustedCertPath(),
		ServerName: agentapi.DialHomeServerName, UID: uid, Socket: f.Socket,
	}
	b, err := cfg.Render()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errRefused, err)
	}
	if err := writePrivate(f.ConfigPath(), b); err != nil {
		return nil, fmt.Errorf("frpc's configuration: %w", err)
	}
	env := f.Env
	if env == nil {
		env = os.Environ()
	}
	if nowUID, _ := f.desired(); nowUID != uid || ctx.Err() != nil {
		return nil, errStale
	}
	if now, _ := f.Server(); now != target || !now.Ready {
		return nil, errStale
	}
	g, err := procgroup.Start(procgroup.Spec{
		Path: "/proc/self/fd/3", Argv0: f.Binary, ExtraFiles: []*os.File{f.bin.f},
		Args: []string{"-c", f.ConfigPath()}, Env: frpcEnv(env),
	}, time.Now())
	if err != nil {
		return nil, err
	}
	id := g.ID()
	logger.Info("frpc started", "pid", id.PID, "startTime", id.StartTime, "uid", uid, "server", net.JoinHostPort(target.Host, strconv.Itoa(target.Port)))
	if f.OnStart != nil {
		f.OnStart(id)
	}
	return &running{g: g, uid: uid, host: target.Host, port: target.Port}, nil
}

// writePrivate replaces the file at path with b, mode 600, by a rename, so
// frpc never reads a half-written file.
func writePrivate(path string, b []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".frpc-*.toml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
