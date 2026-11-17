package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Without --event-log-token-file there is no publisher: nothing publishes, and no recorder is built
// with a log (HBEP-8, The durable log, Who writes; #625).
func TestNoTokenFileMeansNoPublisher(t *testing.T) {
	p, err := newEventPublisher(flags{})
	if p != nil || err != nil {
		t.Fatalf("newEventPublisher with no token file = %v, %v, want nil, nil", p, err)
	}
	stopEventPublisher(nil) // a stop of none does nothing
}

// A token file that is not there, or does not hold a token, stops the command at its start, naming the
// flag and the file and never the file's content, rather than failing at the first event.
func TestATokenFileThatIsNotATokenStopsThePublisher(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "none")
	bad := filepath.Join(dir, "bad")
	const secret = "not-a-token-but-private"
	if err := os.WriteFile(bad, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, file := range map[string]string{"missing": missing, "not a token": bad} {
		_, err := newEventPublisher(flags{host: agentapi.Host{StateDir: dir, PortBase: 28000}, eventLogTokenFile: file})
		if err == nil || !strings.Contains(err.Error(), "--event-log-token-file") {
			t.Errorf("%s: error %v, want one naming the flag", name, err)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: the error %q holds the file's content", name, err)
		}
	}
}

// With a token file the process has a publisher, which stops at once when nothing is queued.
func TestATokenFileStartsAPublisher(t *testing.T) {
	dir := t.TempDir()
	tok, err := eventlog.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "token")
	if err := os.WriteFile(file, []byte(tok+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := newEventPublisher(flags{host: agentapi.Host{StateDir: dir, PortBase: 28000}, eventLogTokenFile: file})
	if err != nil || p == nil {
		t.Fatalf("newEventPublisher = %v, %v, want a publisher", p, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p.Shutdown(ctx)
}
