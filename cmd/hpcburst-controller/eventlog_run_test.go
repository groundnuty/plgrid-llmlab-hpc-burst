package main

// The controller binary's own wiring with the durable log (HBEP-8, The durable log, Who writes; #625, PR E2, the
// fix round after the reviews of #697). run is the real code path, against a real API server with the committed
// CRDs (startAPIServer: etcd and kube-apiserver on kernel ports, so no block of internal/testenv is used) and a
// real log, a child process (internal/eventlog/node/nodetest) on the launcher's base + 16 and base + 17.
//
// The recorders are built with the log's option, which does nothing in a process with no log, so a plumbing line
// that drops the publisher (opts.EventLog, the wiring's eventLog, the publisher handed to the tunnel-state reader)
// leaves every other test green. Three observations tell them apart: how many recorders of each controller were
// built with the publisher (Publisher.Wrapped), the unlogged series that only a recorder with the log creates,
// and a typed Allocation event from a real Provider manager read from HB_EVENTS through a fresh client.

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// freeBlockWithLogPorts returns a port base whose block, and its base + 16 and base + 17, are free.
func freeBlockWithLogPorts(t *testing.T) int {
	t.Helper()
	for range 50 {
		base := freePortBase(t)
		ok := true
		for _, off := range []int{agentapi.EventLogPortOffset, agentapi.EventLogMonitorPortOffset} {
			l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(base+off)))
			if err != nil {
				ok = false
				break
			}
			l.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatal("no free port block with its two log ports")
	return 0
}

// unloggedSeries reads hpcburst_events_unlogged_total from the controller's /metrics: the value of each
// controller's series, for those that exist.
func unloggedSeries(t *testing.T, addr string) map[string]float64 {
	t.Helper()
	body, ok := metricsBody(addr)
	if !ok {
		return nil
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	if f := fams[eventcount.UnloggedName]; f != nil {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == eventcount.LabelController {
					out[l.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	return out
}

// With --event-log-token-file and a log, the lifecycle controller, its tunnel-state reader and each Provider
// manager, batch and managed, are built with the publisher; their unlogged series exist at 0; and a typed event
// of a real batch Provider manager is in HB_EVENTS.
func TestControllerBinaryPublishesToTheDurableLog(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	c := api.client
	base := freeBlockWithLogPorts(t)
	d := nodetest.NewDir(t, nodetest.Ports{Client: base + agentapi.EventLogPortOffset, Monitor: base + agentapi.EventLogMonitorPortOffset},
		eventlog.Caps{Events: 16 << 20, Usage: 1 << 20})
	d.Start(t).Ready()
	f, _, digest := prepareStateAt(t, api, base, "direct", exampleFront, "--event-log-token-file", d.TokenFile())
	if f.host.EventLogAddr() != d.Ports.Addr() {
		t.Fatalf("the launcher's address for the log is %s, the rig's %s", f.host.EventLogAddr(), d.Ports.Addr())
	}

	// A batch Provider and a managed one, as the wiring test of the whole binary makes them.
	sshCreds := sshCredentials(t)
	profile := batchProfile("evlog-slurm")
	batchProv := batchProvider("evlog-batch", profile.Name, partX86, sshCreds)
	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gwKey := "example-key-evlog-0001"
	gw := llmlabtest.Start(t, llmlabtest.Options{Listener: gwLn, Keys: []string{gwKey}})
	gw.SetModels(llmlabtest.Model{ID: "example-27b", Revision: "2026-09"})
	mcreds, err := provider.ManagedCredentials{Key: gwKey, BaseURL: gw.BaseURL()}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	mcredsPath := filepath.Join(t.TempDir(), "llmlab.json")
	if err := os.WriteFile(mcredsPath, mcreds, 0o600); err != nil {
		t.Fatal(err)
	}
	managedProfile := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "evlog-llmlab"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformManaged}}
	managedProv := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "evlog-managed", Labels: map[string]string{v1alpha1.LabelPlatform: "llmlab"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: managedProfile.Name},
			CredentialsRef: &v1alpha1.CredentialsReference{File: mcredsPath}},
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "evlog-allocs"}}
	for _, o := range []client.Object{profile, batchProv, managedProfile, managedProv, ns} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}

	_, session := newFakeSlurm()
	var mu sync.Mutex
	var pub *eventcount.Publisher
	b := startBinary(t, &f, deps{
		providerScheme: providerScheme,
		newPlatform: func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
			return slurm.NewBatch(session, v.Profile, v.Settings, slurm.Options{PollInterval: time.Second, BusyInterval: 500 * time.Millisecond}), io.NopCloser(nil), nil
		},
		eventPublisher: func(f flags) (*eventcount.Publisher, error) {
			p, err := newEventPublisher(f)
			mu.Lock()
			pub = p
			mu.Unlock()
			return p, err
		},
	})
	b.eventually(t, "the lifecycle Lease is held", 60*time.Second, func() bool { return held(ctx, c, v1alpha1.LeaseLifecycle) })

	// An Allocation of the batch Provider: its JobSubmitted is raised by the real manager's typed recorder.
	a := batchAllocation("alloc-evlog", ns.Name, batchProv.Name, partX86, digest)
	if err := c.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	writeCredential(t, f.host, a)
	b.eventually(t, "the Allocation's job is submitted", 90*time.Second, func() bool {
		if err := c.Get(ctx, client.ObjectKeyFromObject(a), a); err != nil {
			t.Fatal(err)
		}
		return a.Annotations[v1alpha1.AnnotationJobID] != ""
	})

	// How many recorders were built with the publisher, for each controller.
	mu.Lock()
	p := pub
	mu.Unlock()
	if p == nil {
		t.Fatal("run built no publisher although it was given a token file")
	}
	batch, mgd := eventcount.ControllerProvider(batchProv.Name), eventcount.ControllerProvider(managedProv.Name)
	b.eventually(t, "both Provider managers to be set up", 60*time.Second, func() bool {
		return p.Wrapped(batch) >= 1 && p.Wrapped(mgd) >= 1
	})
	if got := p.Wrapped(eventcount.ControllerLifecycle); got != 2 {
		t.Errorf("%d recorders of the lifecycle controller were built with the publisher, want 2: the manager's and the tunnel-state reader's", got)
	}
	if got := p.Wrapped(batch); got != 1 {
		t.Errorf("%d recorders of %s were built with the publisher, want 1: its typed recorder", got, batch)
	}
	if got := p.Wrapped(mgd); got != 1 {
		t.Errorf("%d recorders of %s were built with the publisher, want 1", got, mgd)
	}
	// The unlogged series, created at 0 only by a recorder with the log.
	b.eventually(t, "the unlogged series of the three controllers at 0 on /metrics", 60*time.Second, func() bool {
		got := unloggedSeries(t, f.host.ControllerMetricsAddr())
		for _, ctl := range []string{eventcount.ControllerLifecycle, batch, mgd} {
			if v, ok := got[ctl]; !ok || v != 0 {
				return false
			}
		}
		return true
	})
	// A typed event of the real batch Provider manager, read from the log through a fresh client.
	wantID := string(a.UID) + "/JobSubmitted/1"
	deadline := time.Now().Add(60 * time.Second)
	for {
		for _, m := range d.ReadAll(t, eventlog.StreamEvents) {
			if m.ID != wantID {
				continue
			}
			ev, _, err := eventcount.Decode(m.Payload)
			if err != nil {
				t.Fatal(err)
			}
			if m.Subject != "hb.ev.allocation."+string(a.UID) || ev.Source() != "/hpcburst/"+batch {
				t.Fatalf("%s is on %q from %q, want the Allocation's subject and the batch Provider's source", wantID, m.Subject, ev.Source())
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the log lacks %s after 60 s: the Provider manager's typed recorder was built without the publisher", wantID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
