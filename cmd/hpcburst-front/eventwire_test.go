package main

// The front's real recorder wiring with the durable log (HBEP-8, The durable log, Who writes; #625, PR E2, the
// fix round after the reviews of #697): newRecorder is what run calls, so a recorder built without the
// publisher, which does nothing visible in a process with no log, is caught here. The log is a real child
// process (internal/eventlog/node/nodetest) on the two ports after the launcher's base + 16, and what it holds
// is read through a fresh client.
//
// Ports: 28502 and 28503, this package's pair of #625's block (IMPL-COMMON), fixed: the log's start reads both,
// and a pair that the kernel gave and the test bound later was taken in between by another test's kernel port
// ("can't listen to the monitor port: ... address already in use", #737). The two tests start one log at a time
// and stop it at their end.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestMain(m *testing.M) {
	code := m.Run()
	nodetest.Cleanup()
	os.Exit(code)
}

// The log's client and monitoring ports, and the port base whose launcher addresses (base + 16 and + 17) they are.
const (
	logClientPort  = 28502
	logMonitorPort = 28503
	logBase        = logClientPort - agentapi.EventLogPortOffset
)

var logPorts = nodetest.Ports{Client: logClientPort, Monitor: logMonitorPort}

// With a token file the front's recorder publishes its typed events to the log, and its unlogged series
// exists at 0; the address is the launcher's base + 16.
func TestTheFrontsRecorderPublishesToTheDurableLog(t *testing.T) {
	d := nodetest.NewDir(t, logPorts, eventlog.Caps{Events: 1 << 20, Usage: 1 << 20})
	d.Start(t).Ready()
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: logBase}
	if host.EventLogAddr() != d.Ports.Addr() || host.EventLogMonitorPort() != d.Ports.Monitor {
		t.Fatalf("the launcher's address for the log is %s and its monitoring port %d, the rig's %s and %d", host.EventLogAddr(), host.EventLogMonitorPort(), d.Ports.Addr(), d.Ports.Monitor)
	}
	cs := kubefake.NewSimpleClientset(&coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseFront, UID: "front-lease-uid"}})
	rec, stop, err := newRecorder(cs, d.TokenFile(), host)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if v, exists := rec.Counter.Unlogged(eventcount.ControllerFront); !exists || v != 0 {
		t.Fatalf("hpcburst_events_unlogged_total{controller=front} is %v (exists %v) with a token file, want 0 and existing", v, exists)
	}
	md := &v1alpha1.ModelDeployment{}
	md.Namespace, md.Name, md.UID = "ns-a", "md-wiring", types.UID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")
	rec.Event(md, true, "degraded", "WiringProbe", "the front's recorder reaches the log")
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, m := range d.ReadAll(t, eventlog.StreamEvents) {
			_, data, err := eventcount.Decode(m.Payload)
			if err == nil && data.Reason == "WiringProbe" {
				if m.Subject != "hb.ev.modeldeployment.aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa" || data.ReportingController != "hpcburst-front" {
					t.Fatalf("the front's event is on %q reported by %q", m.Subject, data.ReportingController)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the front's typed event did not reach the log within 30 s: its recorder was built without the publisher")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Without a token file there is no publisher and no unlogged series: the recorder is what it was.
func TestTheFrontsRecorderWithoutATokenFileHasNoLog(t *testing.T) {
	rec, stop, err := newRecorder(kubefake.NewSimpleClientset(), "", agentapi.Host{})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if _, exists := rec.Counter.Unlogged(eventcount.ControllerFront); exists {
		t.Error("a front with no token file has an unlogged series")
	}
}

// run itself: with the token file on its command line, the recorder it hands front.Run is the one with the
// durable log's publisher. front.Run is replaced by a stub that reads that recorder, so a call in run that drops
// the token file, or the publisher it builds, is caught here and not only in newRecorder's own test.
func TestRunHandsTheServerARecorderWithTheDurableLog(t *testing.T) {
	d := nodetest.NewDir(t, logPorts, eventlog.Caps{Events: 1 << 20, Usage: 1 << 20})
	d.Start(t).Ready()
	state := t.TempDir()
	kubeconfig := filepath.Join(state, "kubeconfig")
	cfg := filepath.Join(state, "front.json")
	for path, body := range map[string]string{
		kubeconfig: "apiVersion: v1\nkind: Config\nclusters:\n- name: c\n  cluster: {server: \"http://127.0.0.1:1\"}\ncontexts:\n- name: x\n  context: {cluster: c, user: u}\ncurrent-context: x\nusers:\n- name: u\n  user: {}\n",
		cfg:        `{"clientKeysFile": "` + filepath.Join(state, "keys") + `", "usageDir": "` + filepath.Join(state, "usage") + `"}`,
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	saved := frontRun
	t.Cleanup(func() { frontRun = saved })
	var gotErr error
	called := false
	frontRun = func(_ context.Context, o front.Options) error {
		called = true
		rec, ok := o.Recorder.(front.KubeRecorder)
		if !ok {
			gotErr = errString("run handed the server a " + typeName(o.Recorder) + ", want the front's KubeRecorder")
			return nil
		}
		if v, exists := rec.Counter.Unlogged(eventcount.ControllerFront); !exists || v != 0 {
			gotErr = errString("hpcburst_events_unlogged_total{controller=front} is not 0 and existing in the recorder run built: its call dropped the log")
			return nil
		}
		md := &v1alpha1.ModelDeployment{}
		md.Namespace, md.Name, md.UID = "ns-a", "md-run", types.UID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb")
		rec.Event(md, true, "degraded", "RunWiringProbe", "the recorder run built reaches the log")
		for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
			for _, m := range d.ReadAll(t, eventlog.StreamEvents) {
				if _, data, err := eventcount.Decode(m.Payload); err == nil && data.Reason == "RunWiringProbe" {
					return nil
				}
			}
		}
		gotErr = errString("the typed event of the recorder run built did not reach the log within 30 s")
		return nil
	}
	err := run([]string{"--config", cfg, "--state-dir", state, "--port-base", strconv.Itoa(logBase), "--kubeconfig", kubeconfig, "--event-log-token-file", d.TokenFile()})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !called {
		t.Fatal("run did not reach front.Run")
	}
	if gotErr != nil {
		t.Error(gotErr)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func typeName(v any) string { return fmt.Sprintf("%T", v) }
