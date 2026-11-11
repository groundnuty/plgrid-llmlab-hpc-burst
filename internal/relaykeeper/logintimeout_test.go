package relaykeeper

// Issue 403, ruling (b) on PR 409 (`@hpc-burst-science-agent`, 11 Nov 2026): when
// the ssh -G that names a Provider's login host times out in the relay
// keeper, the keeper claims NOTHING for that set. A host it guessed (the
// configured HostName) could be another Provider's login host, and a refusal is
// the safe direction: the controller reads it as no decision, sends again on
// its next round, and the Provider waits. The tests use a real OwnerRunner
// and a double ssh whose -G hangs, with a short bound.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// syncLog is a log's destination that more than one goroutine writes to.
type syncLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// timeoutWorld is a keeper on a real OwnerRunner whose ssh -G hangs, after
// writing ssh's verbose Match exec line for a command with a distinctive word,
// until the file "answer" is written, and then answers login.example. The
// bound is short, so the first ssh -G of a Provider times out. The double's
// wait also ends with its directory and the cleanup that writes "answer" (the
// lesson of issue 402), though the Relay's bound ends it first.
type timeoutWorld struct {
	r      *rig
	k      *Keeper
	runner *OwnerRunner
	answer string
	log    *syncLog
}

const timeoutBound = 2 * time.Second

func newTimeoutWorld(t *testing.T) *timeoutWorld {
	t.Helper()
	dir := t.TempDir()
	if strings.Contains(dir, "'") {
		t.Fatalf("the test's directory holds a quote: %q", dir)
	}
	w := &timeoutWorld{r: newRig(t), answer: filepath.Join(dir, "answer"), log: &syncLog{}}
	t.Cleanup(func() { _ = os.WriteFile(w.answer, nil, 0o600) })
	ssh := filepath.Join(dir, "ssh")
	script := "#!/bin/sh\ncase \"$1\" in -G)\n" +
		"  if [ ! -e '" + w.answer + "' ]; then\n" +
		"    printf \"debug1: Executing command: 'hang-marker-cmd --flag'\\n\" >&2\n" +
		"    while [ ! -e '" + w.answer + "' ] && [ -d '" + dir + "' ]; do sleep 0.05; done\n" +
		"  fi\n  echo hostname login.example; exit 0;;\nesac\nexec sleep 300 </dev/null\n"
	if err := os.WriteFile(ssh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	w.k, _ = w.r.open(func(o *Options) {
		o.Runner = func(e int64) (Runner, error) {
			var err error
			w.runner, err = NewOwnerRunner(context.Background(), OwnerRunnerOptions{Epoch: e,
				Ports:  map[agentapi.RelayChannel]int{agentapi.RelayControl: 30002, agentapi.RelayServing: 30003},
				SSHDir: filepath.Join(dir, "d"), SSHBinary: ssh, SSHLoginTimeout: timeoutBound,
				Logger: slog.New(slog.NewTextHandler(w.log, nil))})
			return w.runner, err
		}
	})
	t.Cleanup(func() { w.runner.Stop() })
	return w
}

func (w *timeoutWorld) release(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(w.answer, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// claims are the keeper's claims, host to holder.
func (w *timeoutWorld) claims() map[string]string {
	w.k.mu.Lock()
	defer w.k.mu.Unlock()
	out := map[string]string{}
	for host, c := range w.k.st.Claims {
		out[host] = c.Provider
	}
	return out
}

// The Runner's LoginHost on a timed-out ssh -G is "" and an error that wraps
// sshrun.ErrLoginTimeout, never the configured HostName; the Match exec
// command is in the log; and the timeout is not kept, so the next call, once
// ssh -G answers, is the host.
func TestLoginHostOfATimedOutSshGIsAnErrorNotTheConfiguredHostName(t *testing.T) {
	w := newTimeoutWorld(t)
	ssh := set("example-a", 1, true, "LOGIN.Configured.example").SSH
	host, err := w.runner.LoginHost("example-a", ssh)
	if !errors.Is(err, sshrun.ErrLoginTimeout) || host != "" {
		t.Fatalf("LoginHost on a timed-out ssh -G = %q, %v, want \"\" and an error that wraps ErrLoginTimeout", host, err)
	}
	if log := w.log.String(); !strings.Contains(log, "hang-marker-cmd --flag") {
		t.Errorf("the log lacks the Match exec command:\n%s", log)
	}
	w.release(t)
	if host, err := w.runner.LoginHost("example-a", ssh); err != nil || host != "login.example" {
		t.Errorf("LoginHost once ssh -G answers = %q, %v, want login.example and nil: a timeout is not kept", host, err)
	}
}

// In the keeper, a set whose login host cannot be read is refused with
// RefusedState, naming the timeout and none of the Match exec command, and
// claims nothing: no claim is kept, no wish for the host is recorded (the
// oldest-first rule counts wishes, refused sets' included), no fence moves, and
// once ssh -G answers, a younger Provider's set for the host is accepted as if
// the first had never come.
func TestATimedOutSshGInKeeperModeClaimsNoHost(t *testing.T) {
	w := newTimeoutWorld(t)
	ref := refused(t, w.k, wire.KindSet, set("example-a", 1, true, "login.example"))
	if ref.Reason != wire.RefusedState || !strings.Contains(ref.Message, "ssh -G timed out") || strings.Contains(ref.Message, "hang-marker-cmd") {
		t.Errorf("the timed-out set was refused %+v, want RefusedState saying ssh -G timed out and nothing of the Match exec command", ref)
	}
	if c := w.claims(); len(c) != 0 {
		t.Errorf("the keeper holds claims %v after a set whose login host could not be read, want none", c)
	}
	w.k.mu.Lock()
	fences, wishes := len(w.k.st.Providers), len(w.k.wishes)
	w.k.mu.Unlock()
	if fences != 0 || wishes != 0 {
		t.Errorf("the refused set moved %d fences and recorded %d wishes for a login host, want none: the oldest-first rule would count a wish for a host it guessed", fences, wishes)
	}
	w.release(t)
	// The younger Provider's set is accepted: no wish of the older Provider's
	// timed-out set stands before it for the host.
	accepted(t, w.k, set("example-b", 1, true, "login.example"))
}
