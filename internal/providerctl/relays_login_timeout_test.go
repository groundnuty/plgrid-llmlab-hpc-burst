package providerctl

// Issue 403 and PR 409's ruling (`@hpc-burst-science-agent`, 11 Nov 2026): when
// the ssh -G that names a Provider's login host times out, the Provider's
// status says only that it timed out. The Match exec command ssh's standard
// error showed goes to the controller's log alone: an operator's ssh
// configuration can hold host names, paths and user names, and a status field
// is read by status, events and dumps.

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/go-logr/logr/funcr"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
)

// logSink is the controller's log, kept.
type logSink struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logSink) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.WriteString(s + "\n")
}

func (l *logSink) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// hangingSSHGStub is an ssh whose -G writes ssh's verbose Match exec line for
// a command with a distinctive word in it, and then waits, as a stuck Match
// exec does; any other call exits 1. Its wait ends with its directory, and
// with the release file the test's cleanup writes (the lesson of issue 402),
// though the Relay's bound ends it first.
func hangingSSHGStub(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if strings.Contains(dir, "'") {
		t.Fatalf("the test's directory holds a quote: %q", dir)
	}
	release := filepath.Join(dir, "release")
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })
	stub := filepath.Join(dir, "ssh")
	body := "#!/bin/sh\ncase \" $* \" in *' -G '*)\n" +
		"  printf \"debug1: Executing command: 'hang-marker-cmd --flag'\\n\" >&2\n" +
		"  while [ ! -e '" + release + "' ] && [ -d '" + dir + "' ]; do sleep 0.05; done\n  exit 1;;\nesac\nexit 1\n"
	if err := os.WriteFile(stub, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return stub
}

// A Provider whose login host's ssh -G times out goes Ready False with the
// reason LoginHostInUse (it claims nothing this pass) and a message that says
// ssh -G timed out and carries no part of the Match exec command; neither does
// any other part of the Provider's status, or any event. The controller's log
// has the command.
func TestAHungSshGShowsOnlyTheTimeoutInTheProvidersStatus(t *testing.T) {
	stub := hangingSSHGStub(t)
	var logs logSink
	e := newEnv(t, hostDialedOn(hostdial.NewHosts(), 1000), func(e *env) {
		e.opts.Relays.SSHBinary = stub
		e.opts.Relays.LoginTimeout = 2 * time.Second
		e.opts.Log = logging.NewLogrLogger(funcr.New(func(_, args string) { logs.add(args) }, funcr.Options{}))
	})
	runRelays(e)
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	var prov v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	c := apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionProviderReady)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonLoginHostInUse || !strings.Contains(c.Message, "ssh -G timed out") {
		t.Fatalf("Ready is %+v; want False, LoginHostInUse, saying that ssh -G timed out", c)
	}
	status, err := json.Marshal(prov.Status)
	if err != nil {
		t.Fatal(err)
	}
	e.rec.mu.Lock()
	var events []string
	for _, ev := range e.rec.events {
		events = append(events, string(ev.Reason)+": "+ev.Message)
	}
	e.rec.mu.Unlock()
	for _, where := range []struct{ name, text string }{{"the Provider's status", string(status)}, {"the events", strings.Join(events, "\n")}} {
		for _, leaked := range []string{"hang-marker-cmd", "Match exec", "--flag"} {
			if strings.Contains(where.text, leaked) {
				t.Errorf("%s carry %q, part of the Match exec command: %s", where.name, leaked, where.text)
			}
		}
	}
	if log := logs.String(); !strings.Contains(log, "hang-marker-cmd --flag") {
		t.Errorf("the controller's log lacks the Match exec command:\n%s", log)
	}
}
