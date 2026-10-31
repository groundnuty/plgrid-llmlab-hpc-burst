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
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
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
// Before every start Frpc checks frpc's SHA-256 and the staged certificate's
// pin, and refuses to run frpc when either fails (Run returns the reason).
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

// running is the frpc process Frpc runs and what it was started for.
type running struct {
	g    *procgroup.Group
	uid  string
	host string
	port int
}

// Run serves the Instance set by Serve until ctx ends, and returns once its
// frpc is gone and reaped. It returns an error at once, starting nothing,
// when a setting is missing or frpc or its certificate fails its check, and
// later when a check fails before a restart.
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
	var cur *running
	stop := func(why string) {
		if cur == nil {
			return
		}
		cur.g.Stop(grace)
		<-cur.g.Done()
		logger.Info("frpc stopped", "pid", cur.g.PID(), "why", why)
		cur = nil
	}
	defer func() {
		stop("the agent's tunnel stopped")
		_ = os.Remove(f.ConfigPath())
	}()
	backoff := minB
	var notBefore time.Time
	refusedUID := "" // an Instance UID frpc cannot serve, refused until the next
	for {
		uid, kick := f.desired()
		target, changed := f.Server()
		if cur != nil {
			switch {
			case uid != cur.uid:
				stop("the Instance changed")
			case target.Host != cur.host || target.Port != cur.port:
				// frpc keeps retrying the port it was started with (HBEP-6,
				// A port that cannot be bound again).
				stop("the tunnel's port changed")
			}
		}
		if uid != refusedUID {
			refusedUID = ""
		}
		startable := cur == nil && uid != "" && uid != refusedUID && target.Ready
		if startable && !time.Now().Before(notBefore) {
			r, err := f.start(uid, target, logger)
			switch {
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
		}
		var exited <-chan struct{}
		if cur != nil {
			exited = cur.g.Exited()
		}
		var timer *time.Timer
		var wake <-chan time.Time
		if cur == nil && uid != "" && uid != refusedUID && target.Ready {
			timer = time.NewTimer(time.Until(notBefore))
			wake = timer.C
		}
		select {
		case <-ctx.Done():
			return nil
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
	// errRefused marks a failed check of frpc or its certificate: Run ends,
	// since a staged file does not fix itself.
	errRefused = errors.New("frpc refused")
	// errBadUID marks an Instance UID frpc cannot name a proxy by.
	errBadUID = errors.New("the Instance's UID cannot name a proxy")
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
	if err := errors.Join(errs...); err != nil {
		return err
	}
	return f.preflight()
}

// preflight is the check before every start: frpc's digest, and the
// certificate and its pin (HBEP-6, frpc in the job).
func (f *Frpc) preflight() error {
	if err := CheckFrpcDigest(f.Binary, f.FrpcDigest); err != nil {
		return fmt.Errorf("%w: %w", errRefused, err)
	}
	if err := CheckDialHomeCert(f.CertFile, f.CertSPKISHA256); err != nil {
		return fmt.Errorf("%w: %w", errRefused, err)
	}
	return nil
}

// start writes frpc's configuration for uid and target, mode 600, and starts
// frpc in a process group of its own, with the proxy variables dropped from
// its environment and its console output discarded: frpc logs to its log
// file, which stays in the job's private temporary directory (HBEP-6).
func (f *Frpc) start(uid string, target FrpcTarget, logger *slog.Logger) (*running, error) {
	if err := f.preflight(); err != nil {
		return nil, err
	}
	cfg := FrpcConfig{
		ServerAddr: target.Host, ServerPort: target.Port,
		LogFile: f.LogPath(), Token: f.Credential, CAFile: f.CertFile,
		ServerName: dialHomeServerName, UID: uid, Socket: f.Socket,
	}
	if !uidForm.MatchString(uid) {
		return nil, fmt.Errorf("%w: %q is not a Kubernetes UID", errBadUID, uid)
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
	g, err := procgroup.Start(procgroup.Spec{Path: f.Binary, Args: []string{"-c", f.ConfigPath()}, Env: frpcEnv(env)}, time.Now())
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
