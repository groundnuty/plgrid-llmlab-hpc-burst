package main

import (
	"io"
	"log/slog"
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
