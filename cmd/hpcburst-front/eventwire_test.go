package main

// The front's real recorder wiring with the durable log (HBEP-8, The durable log, Who writes; #625, PR E2, the
// fix round after the reviews of #697): newRecorder is what run calls, so a recorder built without the
// publisher, which does nothing visible in a process with no log, is caught here. The log is a real child
// process (internal/eventlog/node/nodetest) on the two ports after the launcher's base + 16, taken from the
// kernel, and what it holds is read through a fresh client.

import (
	"net"
	"os"
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
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestMain(m *testing.M) {
	code := m.Run()
	nodetest.Cleanup()
	os.Exit(code)
}

// kernelPair returns two adjacent loopback ports the kernel gave, free at the call.
func kernelPair(t *testing.T) int {
	t.Helper()
	for range 50 {
		a, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := a.Addr().(*net.TCPAddr).Port
		b, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port+1)))
		a.Close()
		if err == nil {
			b.Close()
			return port
		}
	}
	t.Fatal("no two adjacent free ports")
	return 0
}

// With a token file the front's recorder publishes its typed events to the log, and its unlogged series
// exists at 0; the address is the launcher's base + 16.
func TestTheFrontsRecorderPublishesToTheDurableLog(t *testing.T) {
	port := kernelPair(t)
	d := nodetest.NewDir(t, nodetest.Ports{Client: port, Monitor: port + 1}, eventlog.Caps{Events: 1 << 20, Usage: 1 << 20})
	d.Start(t).Ready()
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: port - agentapi.EventLogPortOffset}
	if host.EventLogAddr() != d.Ports.Addr() {
		t.Fatalf("the launcher's address for the log is %s, the rig's %s", host.EventLogAddr(), d.Ports.Addr())
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
