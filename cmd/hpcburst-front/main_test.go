package main

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	dynfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestOptionsSendThePathEventsToTheAPIServer (PR #129 review, B1): the
// command gives Run its API client and event recorder and leaves Transports
// and PathEvents unset. Run then builds its own Tunnel and probe and raises
// PathProbeFailing on that recorder, which internal/front's
// TestRunRaisesPathProbeFailingThroughItsRecorder shows; a PathEvents set
// here would bypass it.
func TestOptionsSendThePathEventsToTheAPIServer(t *testing.T) {
	cs := kubefake.NewSimpleClientset()
	rec := &front.MemoryRecorder{}
	o := options(front.Config{}, agentapi.Host{StateDir: t.TempDir(), PortBase: 29440}, front.ModeSSH,
		dynfake.NewSimpleDynamicClient(runtime.NewScheme()), cs, rec, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	if o.PathEvents != nil {
		t.Errorf("PathEvents = %T, want unset so Run raises PathProbeFailing on the recorder", o.PathEvents)
	}
	if o.Transports != nil {
		t.Errorf("Transports = %T, want unset so Run builds HBEP-6's Tunnel and its probe", o.Transports)
	}
	if o.Recorder != front.Recorder(rec) || o.Kube != cs {
		t.Errorf("Recorder %v, Kube %v: want the command's recorder and API client", o.Recorder, o.Kube)
	}
}

// TestModeFlagTakesTheThreeDialHomeModes: --mode direct, ssh and host-dialed (HBEP-6, Dial-home
// modes) pass the flag check, so run goes on to read the configuration, which is absent here and
// fails as an ordinary error, never as a usage error; another value is a usage error that names
// all three.
func TestModeFlagTakesTheThreeDialHomeModes(t *testing.T) {
	args := func(mode string) []string {
		return []string{"--config", filepath.Join(t.TempDir(), "absent.json"), "--state-dir", t.TempDir(), "--port-base", "29440", "--mode", mode}
	}
	for _, mode := range []string{"direct", "ssh", "host-dialed"} {
		err := run(args(mode))
		var ue usageError
		if err == nil || errors.As(err, &ue) {
			t.Errorf("--mode %s: run = %v, want an error from reading the absent configuration, not a usage error", mode, err)
		}
	}
	err := run(args("tunnel"))
	var ue usageError
	if !errors.As(err, &ue) {
		t.Fatalf("--mode tunnel: run = %v, want a usage error", err)
	}
	for _, mode := range []string{"direct", "ssh", "host-dialed"} {
		if !strings.Contains(err.Error(), mode) {
			t.Errorf("--mode tunnel: the error %q does not name %s", err, mode)
		}
	}
}
