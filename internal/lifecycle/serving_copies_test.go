package lifecycle

// Two real copies of the lifecycle controller on one API server (HBEP-8, Test
// plan, A standby; `@hpc-burst-science-agent`, 6 Nov 2026, for #249; PR #254
// review, B2), each read on its own /metrics. controller-runtime's metrics
// server serves its process's global registry (v0.23.1,
// pkg/metrics/server/server.go:221), so two managers in one process cannot
// each serve their own series: the second copy runs in a child process, this
// test binary re-executed on TestLifecycleCopyChild. Both copies are built by
// NewManager with the production wiring: leader election on, the collectors on
// controller-runtime's registry, served by its metrics server.
//
// The tests use #61's envtest block (managerPortBase) under envtest.lock. The
// copies' metrics and health servers bind by address through controller-runtime
// in this PR's block, 29720-29739: the rig hands each copy, in this process or
// the child, a pair of its own (portPairs), and no pair twice in a test. A copy
// whose bind still finds a port taken, a rare race with some other process,
// starts again on the next pair, at most maxBindRetries times, each logged, as
// cmd/hpcburst-controller's tests do (#219). A normal run needs none; each test
// logs its count, and TestMetricsCopiesStartAgainOnALostPort forces one.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock/leasefault"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

const (
	// servingPortBase is the first port of this PR's block, 29720-29739: ten
	// pairs, each a copy's metrics server (even) and health server (odd).
	servingPortBase  = 29720
	servingPortPairs = 10
	// maxBindRetries is how many times a copy whose bind found its port
	// taken is started again on the next pair (#219).
	maxBindRetries = 3
)

// copyPorts are a copy's metrics and health addresses.
type copyPorts struct{ metrics, health string }

// portPairs hands out a test's pairs of the block, in order from its base.
type portPairs struct{ next int }

func (p *portPairs) take(t *testing.T) copyPorts {
	t.Helper()
	c := p.peek(t)
	p.next++
	return c
}

// peek returns the pair take would hand out next, without taking it.
func (p *portPairs) peek(t *testing.T) copyPorts {
	t.Helper()
	if p.next == servingPortPairs {
		t.Fatalf("all %d port pairs of %d-%d are taken", servingPortPairs, servingPortBase, servingPortBase+2*servingPortPairs-1)
	}
	base := servingPortBase + 2*p.next
	return copyPorts{metrics: fmt.Sprintf("127.0.0.1:%d", base), health: fmt.Sprintf("127.0.0.1:%d", base+1)}
}

// addrInUse reports whether a bind's error text says its port was taken.
func addrInUse(s string) bool { return strings.Contains(s, "address already in use") }

// bound reports whether a copy answers on both its ports: /healthz 200, and /metrics 200 with
// controller-runtime's series (controlSeries), so the answer is a copy's, not a stranger's. It
// probes with bindProbe: a stranger that holds a port may accept and never answer.
func bound(p copyPorts) bool {
	get := func(url string) (int, string) {
		resp, err := bindProbe.Get(url)
		if err != nil {
			return 0, ""
		}
		defer resp.Body.Close()
		var b bytes.Buffer
		_, _ = b.ReadFrom(resp.Body)
		return resp.StatusCode, b.String()
	}
	health, _ := get("http://" + p.health + "/healthz")
	code, body := get("http://" + p.metrics + "/metrics")
	return health == http.StatusOK && code == http.StatusOK && strings.Contains(body, controlSeries)
}

// bindProbe is bound's client: no connection kept alive (probeClient's reason), and a short
// timeout, since bound is polled.
var bindProbe = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: time.Second}

// bindWait bounds the wait for a copy to answer on its ports.
const bindWait = time.Minute

// The child copy's settings, passed in its environment.
const (
	childKubeconfigEnv = "HPCBURST_LIFECYCLE_CHILD_KUBECONFIG"
	childCredsEnv      = "HPCBURST_LIFECYCLE_CHILD_CREDS"
	childBuildsEnv     = "HPCBURST_LIFECYCLE_CHILD_BUILDS"
	childMetricsEnv    = "HPCBURST_LIFECYCLE_CHILD_METRICS"
	childHealthEnv     = "HPCBURST_LIFECYCLE_CHILD_HEALTH"
)

// controlSeries is a series every running copy's /metrics has: controller-runtime's count of its
// API requests (pkg/metrics/client_go_adapter.go). A scrape that has it is a live copy's answer,
// so "no hpcburst_ series" in it is not a dead server or an empty body.
const controlSeries = "rest_client_requests_total{"

// TestLifecycleCopyChild is the child copy's program: a lifecycle manager built by NewManager with
// leader election, run until SIGTERM, which stops it cleanly. In a normal run of the suite its
// environment is unset and it does nothing.
func TestLifecycleCopyChild(t *testing.T) {
	kubeconfig := os.Getenv(childKubeconfigEnv)
	if kubeconfig == "" {
		return
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	mgr, _, err := NewManager(Options{
		Config: cfg, CredentialsDir: os.Getenv(childCredsEnv), AgentBuildsDir: os.Getenv(childBuildsEnv), FrontConfigured: true,
		MetricsAddr: os.Getenv(childMetricsEnv), HealthAddr: os.Getenv(childHealthEnv), LeaderElection: true, Checkpoints: &stubSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("the child copy's manager: %v", err)
	}
}

// lockedBuffer is a bytes.Buffer safe for a child's output and a reader.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// childCopy is a copy of the controller running in a child process of its own group.
type childCopy struct {
	cmd *exec.Cmd
	// exited is closed once the process has been waited for; waitErr is set before that.
	exited  chan struct{}
	waitErr error
	out     *lockedBuffer
	once    sync.Once
}

// startChildCopy starts the child copy against the rig's API server on the rig's next pair of
// ports, and waits until it answers on both. A child that exits having found a port taken is
// started again on the next pair, at most maxBindRetries times, each logged. It is stopped when the
// test ends if the test has not.
func (g *servingRig) startChildCopy(t *testing.T) (*childCopy, copyPorts) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		p := g.ports.take(t)
		c := g.launchChild(t, p)
		if c.awaitBound(t, p) {
			return c, p
		}
		if !addrInUse(c.out.String()) || attempt == maxBindRetries {
			t.Fatalf("the child copy exited (%v) before it answered on %s and %s; its output:\n%s", c.waitErr, p.metrics, p.health, c.out.String())
		}
		c.once.Do(func() {}) // it exited; the cleanup has nothing to stop
		g.retries++
		t.Logf("the child copy's start %d found a port of %s or %s taken; starting again on the next pair (retry %d of %d)",
			attempt+1, p.metrics, p.health, attempt+1, maxBindRetries)
	}
}

// awaitBound waits until the child answers on p (true) or exits first (false). A child that does
// neither within bindWait fails the test.
func (c *childCopy) awaitBound(t *testing.T, p copyPorts) bool {
	t.Helper()
	deadline := time.Now().Add(bindWait)
	for {
		if bound(p) {
			return true
		}
		select {
		case <-c.exited:
			return false
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("the child copy answered on neither %s nor %s within %s; its output:\n%s", p.metrics, p.health, bindWait, c.out.String())
		}
	}
}

// launchChild starts the child process on p.
func (g *servingRig) launchChild(t *testing.T, p copyPorts) *childCopy {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLifecycleCopyChild$", "-test.count=1", "-test.timeout=8m")
	cmd.Env = append(os.Environ(), childKubeconfigEnv+"="+g.kubeconfig, childCredsEnv+"="+g.creds, childBuildsEnv+"="+g.builds,
		childMetricsEnv+"="+p.metrics, childHealthEnv+"="+p.health)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c := &childCopy{cmd: cmd, exited: make(chan struct{}), out: &lockedBuffer{}}
	cmd.Stdout, cmd.Stderr = c.out, c.out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		c.waitErr = cmd.Wait()
		close(c.exited)
	}()
	t.Cleanup(func() { c.stop(t) })
	return c
}

// stop sends the child's group SIGTERM, a clean stop that releases its Lease, and waits for it,
// killing the group if it has not exited within managerStopLimit. It reports a child that needed
// the kill or exited with an error, and checks that nothing of its group is left.
func (c *childCopy) stop(t *testing.T) {
	t.Helper()
	c.once.Do(func() {
		pgid := c.cmd.Process.Pid
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		select {
		case <-c.exited:
			if c.waitErr != nil {
				t.Errorf("the child copy exited with %v; its output:\n%s", c.waitErr, c.out.String())
			}
		case <-time.After(managerStopLimit):
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			<-c.exited
			t.Errorf("the child copy did not stop within %s of SIGTERM and was killed; its output:\n%s", managerStopLimit, c.out.String())
		}
		if err := syscall.Kill(-pgid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("the child copy's process group %d is still there (%v)", pgid, err)
		}
	})
}

// servingRig is a control plane with the framework's CRDs and the controller's namespaces, a Ready
// Provider on its profile and one ModelDeployment, which the leading copy gives a pending Instance
// and its Allocation; the directories the copies' options need, and an admin kubeconfig for the
// child copy.
type servingRig struct {
	ctx                       context.Context
	cfg                       *rest.Config
	kubeconfig, creds, builds string
	cl                        client.Client
	ports                     portPairs
	// retries counts the copies' starts that found a port taken (startCopy, startChildCopy).
	retries int
}

func newServingRig(t *testing.T) *servingRig {
	t.Helper()
	if os.Getenv("HPCBURST_TESTENV_PORT_BASE") == "" {
		t.Setenv("HPCBURST_TESTENV_PORT_BASE", managerPortBase)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	cp, err := testenv.Start(ctx, t.TempDir(), testenv.Options{BeforeControllerManager: installTestCRDs})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	})
	s, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := client.New(cp.Config, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{v1alpha1.ControllerNamespace, rigNS} {
		if err := cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	prof := &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: rigProfile},
		Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, Partitions: []v1alpha1.PartitionProfile{{
			Name: rigPartition, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch},
			MaxTime: dur(48 * time.Hour), GPUsPerNode: ptrTo(int32(4)), CPUsPerGPU: ptrTo(int32(16)), MemPerGPU: ptrTo(resource.MustParse("120Gi")),
		}}},
	}
	if err := cl.Create(ctx, prof); err != nil {
		t.Fatal(err)
	}
	prov := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: rigProvider},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: rigProfile}, AllowedPartitions: []string{rigPartition}},
	}
	if err := cl.Create(ctx, prov); err != nil {
		t.Fatal(err)
	}
	apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPolled, LastTransitionTime: metav1.Now()})
	readAt := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
	prov.Status.LastSuccessfulStatusRead = &readAt
	if err := cl.Status().Update(ctx, prov, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
		t.Fatal(err)
	}
	md := mdSpec("md")
	md.Generation = 0
	if err := cl.Create(ctx, md); err != nil {
		t.Fatalf("create ModelDeployment: %v", err)
	}
	g := &servingRig{ctx: ctx, cfg: cp.Config, kubeconfig: cp.Kubeconfig, creds: t.TempDir(), builds: t.TempDir(), cl: cl}
	if err := os.WriteFile(filepath.Join(g.builds, "hb-agent-"+rigArch), []byte("a stand-in for the agent build"), 0o600); err != nil {
		t.Fatal(err)
	}
	return g
}

// inProcessCopy is a copy of the controller in this process: production wiring, its collectors on
// controller-runtime's registry, which its metrics server serves.
type inProcessCopy struct {
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	ok     bool
}

// startCopy starts an in-process copy built from o, completed with the rig's settings and its next
// pair of ports, on a context marked for leasefault, and waits until it answers on both ports. A
// copy whose bind finds a port taken, the health port in NewManager or the metrics port in Start,
// is started again on the next pair, at most maxBindRetries times, each logged. It is stopped when
// the test ends if the test has not.
func (g *servingRig) startCopy(t *testing.T, name string, o Options) (*inProcessCopy, copyPorts) {
	t.Helper()
	if o.Config == nil {
		o.Config = g.cfg
	}
	o.CredentialsDir, o.AgentBuildsDir, o.FrontConfigured, o.Checkpoints, o.LeaderElection = g.creds, g.builds, true, &stubSource{}, true
	for attempt := 0; ; attempt++ {
		p := g.ports.take(t)
		o.MetricsAddr, o.HealthAddr = p.metrics, p.health
		c, err := g.launchCopy(t, name, o, p)
		if err == nil {
			return c, p
		}
		if !addrInUse(err.Error()) || attempt == maxBindRetries {
			t.Fatalf("%s on %s and %s: %v", name, p.metrics, p.health, err)
		}
		g.retries++
		t.Logf("%s's start %d found a port of %s or %s taken (%v); starting again on the next pair (retry %d of %d)",
			name, attempt+1, p.metrics, p.health, err, attempt+1, maxBindRetries)
		awaitUnregistered(t) // a Start that failed may still be unregistering its collectors
	}
}

// launchCopy builds and starts one in-process copy on p, and returns once it answers on both ports,
// or with the error of a NewManager or a Start that failed first.
func (g *servingRig) launchCopy(t *testing.T, name string, o Options, p copyPorts) (*inProcessCopy, error) {
	t.Helper()
	mgr, _, err := NewManager(o)
	if err != nil {
		return nil, err
	}
	mctx, cancel := context.WithCancel(leasefault.Mark(g.ctx))
	c := &inProcessCopy{cancel: cancel, done: make(chan error, 1)}
	go func() { c.done <- mgr.Start(mctx) }()
	t.Cleanup(func() { c.stop(t, name) })
	deadline := time.Now().Add(bindWait)
	for !bound(p) {
		select {
		case err := <-c.done:
			c.once.Do(func() {}) // it has returned; the cleanup must not wait for it
			cancel()
			if err == nil {
				err = errors.New("the manager returned before it answered on its ports")
			}
			return nil, err
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s answered on neither %s nor %s within %s", name, p.metrics, p.health, bindWait)
		}
	}
	return c, nil
}

// stop cancels the copy and waits once for it (stopManager).
func (c *inProcessCopy) stop(t *testing.T, name string) bool {
	t.Helper()
	c.once.Do(func() { c.ok = stopManager(t, name, c.cancel, c.done) })
	return c.ok
}

// scrapeText reads a copy's /metrics: the status code and the body.
func scrapeText(addr string) (int, string) {
	resp, err := probeClient.Get("http://" + addr + "/metrics")
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	return resp.StatusCode, b.String()
}

// hpcburstLines returns the sample lines of a /metrics body whose name has the hpcburst_ prefix.
func hpcburstLines(body string) []string {
	var out []string
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, "hpcburst_") {
			out = append(out, l)
		}
	}
	return out
}

// exportsNothing fails unless the copy's /metrics answers 200 with controller-runtime's series and
// no hpcburst_ series, five times over: the state is held by the test, so a longer look can only
// find a fault.
func exportsNothing(t *testing.T, what, addr string) {
	t.Helper()
	for i := 0; i < 5; i++ {
		code, body := scrapeText(addr)
		if code != http.StatusOK || !strings.Contains(body, controlSeries) {
			t.Fatalf("%s: /metrics answered %d, with controller-runtime's %s present: %v; want 200 with it", what, code, controlSeries, strings.Contains(body, controlSeries))
		}
		if got := hpcburstLines(body); len(got) != 0 {
			t.Fatalf("%s: /metrics has %d hpcburst_ series, want none: %v", what, len(got), got)
		}
	}
}

// servesState reports whether a copy's /metrics has what the rig's state gives a serving copy: both
// success gauges at 1, the Instance's info series, every engine metric of its worker-0 unknown with
// reason stale, and the accounting gauges of its Allocation; and why not.
func servesState(addr string, in *v1alpha1.Instance, alloc string) (bool, string) {
	code, body := scrapeText(addr)
	if code != http.StatusOK {
		return false, fmt.Sprintf("/metrics answered %d", code)
	}
	lines := hpcburstLines(body)
	has := func(prefix string, parts ...string) int {
		n := 0
		for _, l := range lines {
			ok := strings.HasPrefix(l, prefix)
			for _, p := range parts {
				ok = ok && strings.Contains(l, p)
			}
			if ok {
				n++
			}
		}
		return n
	}
	uid := fmt.Sprintf(`instance_uid="%s"`, in.UID)
	missing := []string{}
	if has(`hpcburst_collector_success{collector="instances"} 1`) != 1 {
		missing = append(missing, "instances success 1")
	}
	if has(`hpcburst_collector_success{collector="accounting"} 1`) != 1 {
		missing = append(missing, "accounting success 1")
	}
	if has("hpcburst_instance_info{", uid) != 1 {
		missing = append(missing, "the Instance's info series")
	}
	if n := has("hpcburst_instance_metrics_unknown{", uid, `worker="worker-0"`, `reason="stale"`); n != len(engineNames) {
		missing = append(missing, fmt.Sprintf("%d of %d stale unknown samples", n, len(engineNames)))
	}
	if has("hpcburst_allocation_allocated_gpu_seconds{", `allocation="`+alloc+`"`) != 1 {
		missing = append(missing, "the Allocation's GPU-seconds")
	}
	if len(missing) != 0 {
		return false, fmt.Sprintf("missing %v; hpcburst_ lines %v", missing, lines)
	}
	return true, ""
}

// leaseOf reads the lifecycle Lease.
func (g *servingRig) leaseOf(t *testing.T) coordinationv1.Lease {
	t.Helper()
	var l coordinationv1.Lease
	if err := g.cl.Get(g.ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
		t.Fatal(err)
	}
	return l
}

func holderOf(l coordinationv1.Lease) string {
	if l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// awaitServing waits until the copy at metrics and health leads and serves the rig's state: its
// Instance and Allocation, the latter's accounting written by the test as the Provider's
// controller writes it. It returns the Instance and the Allocation's name.
func (g *servingRig) awaitServing(t *testing.T, what string, p copyPorts) (*v1alpha1.Instance, string) {
	t.Helper()
	var in v1alpha1.Instance
	eventually(t, 2*time.Minute, what+": the Instance and its Allocation exist", func() (bool, string) {
		var insts v1alpha1.InstanceList
		if err := g.cl.List(g.ctx, &insts, client.InNamespace(rigNS)); err != nil {
			return false, err.Error()
		}
		if len(insts.Items) != 1 || insts.Items[0].Spec.AllocationRef.Name == "" {
			return false, fmt.Sprintf("%d Instances", len(insts.Items))
		}
		in = insts.Items[0]
		var a v1alpha1.Allocation
		return g.cl.Get(g.ctx, client.ObjectKey{Namespace: rigNS, Name: in.Spec.AllocationRef.Name}, &a) == nil, "no Allocation yet"
	})
	alloc := in.Spec.AllocationRef.Name
	g.writeAccounting(t, alloc)
	eventually(t, 2*time.Minute, what+": leads and serves the Instance and accounting series", func() (bool, string) {
		if code := getStatus(t, "http://"+p.health+"/readyz"); code != http.StatusOK {
			return false, fmt.Sprintf("/readyz %d", code)
		}
		return servesState(p.metrics, &in, alloc)
	})
	return &in, alloc
}

// writeAccounting writes accounting on the Allocation's status, as the Provider's controller does
// for a started job: 4 GPUs for an hour. It writes once; a later write by the controller keeps it.
func (g *servingRig) writeAccounting(t *testing.T, name string) {
	t.Helper()
	eventually(t, time.Minute, "write the Allocation's accounting", func() (bool, string) {
		var a v1alpha1.Allocation
		if err := g.cl.Get(g.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &a); err != nil {
			return false, err.Error()
		}
		if a.Status.AtProvider.Accounting != nil {
			return true, ""
		}
		a.Status.AtProvider.Accounting = &v1alpha1.AllocationAccounting{AllocatedGPUs: i32(4), AllocatedCPUs: i32(64), ElapsedSeconds: i64(3600), ReadTime: metav1.Now()}
		if err := g.cl.Status().Update(g.ctx, &a, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
}

// TestMetricsStandbyOnTwoManagers is HBEP-8's standby case on two real managers on one API server,
// each read on its own /metrics. Copy A, in this process, leads and exports the Instance and
// accounting series and both success gauges. Copy B, in a child process, is a standby with synced
// caches: its /metrics answers with controller-runtime's own series and no hpcburst_ series. A
// stops: B takes the Lease and exports both gauges at 1, with the live Instance unknown, reason
// stale, as it has no report. Last, a copy that leads while one kind's informer is held unsynced
// exports no hpcburst_ series, and exports them once it is released.
func TestMetricsStandbyOnTwoManagers(t *testing.T) {
	g := newServingRig(t)
	defer g.logPorts(t)
	a, pa := g.startCopy(t, "copy A", Options{})
	g.awaitServing(t, "copy A", pa)
	holderA := holderOf(g.leaseOf(t))

	b, pb := g.startChildCopy(t)
	eventually(t, 2*time.Minute, "copy B is a standby with synced caches", func() (bool, string) {
		caches, leading := readyzCodes(t, pb.health)
		return caches == http.StatusOK && leading == http.StatusInternalServerError, fmt.Sprintf("/readyz/caches %d, /readyz/leading %d; output:\n%s", caches, leading, b.out.String())
	})
	if h := holderOf(g.leaseOf(t)); h != holderA {
		t.Fatalf("the Lease names %q, want copy A, %q, still", h, holderA)
	}
	exportsNothing(t, "copy B, a standby", pb.metrics)
	eventually(t, time.Minute, "copy A still serves", func() (bool, string) {
		in, alloc := g.currentInstance(t)
		return servesState(pa.metrics, in, alloc)
	})

	if !a.stop(t, "copy A") {
		t.FailNow()
	}
	in, alloc := g.currentInstance(t)
	eventually(t, 2*time.Minute, "copy B takes the Lease and serves", func() (bool, string) {
		if _, leading := readyzCodes(t, pb.health); leading != http.StatusOK {
			return false, fmt.Sprintf("/readyz/leading %d", leading)
		}
		return servesState(pb.metrics, in, alloc)
	})
	if h := holderOf(g.leaseOf(t)); h == holderA || h == "" {
		t.Fatalf("copy B serves while the Lease names %q", h)
	}
	b.stop(t)

	// A copy that leads with the Instance informer held unsynced.
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	held := &heldKindCache{kind: fmt.Sprintf("%T", &v1alpha1.Instance{}), gate: gate, asked: make(chan struct{})}
	_, pc := g.startCopy(t, "copy C", Options{
		newCache: func(c *rest.Config, o cache.Options) (cache.Cache, error) {
			inner, err := cache.New(c, o)
			if err != nil {
				return nil, err
			}
			held.Cache = inner
			return held, nil
		}})
	eventually(t, 2*time.Minute, "copy C leads with its caches not synced", func() (bool, string) {
		caches, leading := readyzCodes(t, pc.health)
		return caches == http.StatusInternalServerError && leading == http.StatusOK, fmt.Sprintf("/readyz/caches %d, /readyz/leading %d", caches, leading)
	})
	exportsNothing(t, "copy C, leading with its caches not synced", pc.metrics)
	release()
	eventually(t, 2*time.Minute, "copy C serves once its caches have synced", func() (bool, string) {
		return servesState(pc.metrics, in, alloc)
	})
}

// currentInstance reads the rig's one Instance and its Allocation's name.
func (g *servingRig) currentInstance(t *testing.T) (*v1alpha1.Instance, string) {
	t.Helper()
	var insts v1alpha1.InstanceList
	if err := g.cl.List(g.ctx, &insts, client.InNamespace(rigNS)); err != nil {
		t.Fatal(err)
	}
	if len(insts.Items) != 1 {
		t.Fatalf("%d Instances, want 1", len(insts.Items))
	}
	return &insts.Items[0], insts.Items[0].Spec.AllocationRef.Name
}

// copyScrape is one scrape of a copy's /metrics: when it was sent and answered, its status, and
// whether it had hpcburst_ series and controller-runtime's.
type copyScrape struct {
	start, end      time.Time
	code            int
	hpcburst, alive bool
}

// scrapeLog scrapes a copy's /metrics over and over, about every 50 ms, and keeps every scrape.
type scrapeLog struct {
	mu   sync.Mutex
	got  []copyScrape
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func startScrapeLog(addr string) *scrapeLog {
	l := &scrapeLog{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(l.done)
		for {
			select {
			case <-l.stop:
				return
			default:
			}
			s := copyScrape{start: time.Now()}
			code, body := scrapeText(addr)
			s.end, s.code = time.Now(), code
			s.hpcburst, s.alive = len(hpcburstLines(body)) != 0, code == http.StatusOK && strings.Contains(body, controlSeries)
			l.mu.Lock()
			l.got = append(l.got, s)
			l.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}()
	return l
}

// served reports whether a scrape so far had hpcburst_ series.
func (l *scrapeLog) served() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.got {
		if s.hpcburst {
			return true
		}
	}
	return false
}

// finish stops the scrapes and returns them all, in order.
func (l *scrapeLog) finish() []copyScrape {
	l.once.Do(func() { close(l.stop) })
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]copyScrape(nil), l.got...)
}

// awaitUnregistered waits until no collector on controller-runtime's registry describes
// hpcburst_collector_success: a copy that lost its Lease returns from Start without waiting for its
// runnables (controller-runtime zeroes its shutdown grace), and the next copy in this process
// registers its collectors on the same registry.
func awaitUnregistered(t *testing.T) {
	t.Helper()
	probe := &stateCollector{serving: func() bool { return false }, instances: &instanceCollector{}, accounting: &accountingCollector{}}
	eventually(t, time.Minute, "the lost copy's collectors leave controller-runtime's registry", func() (bool, string) {
		if err := ctrlmetrics.Registry.Register(probe); err != nil {
			return false, err.Error()
		}
		ctrlmetrics.Registry.Unregister(probe)
		return true, ""
	})
}

// TestMetricsLostLeaseStopsBeforeANewCopyLeads (PR #254 review, B2): a copy whose renews of the
// Lease fail stops exporting no later than another copy can lead, although its manager runs on.
// Copy A, in this process, leads and serves; copy B, a child process, is the standby. Then every
// renew of A fails, and A's elector's release read, which client-go makes before it calls
// OnStoppedLeading, is held by the test past B's take of the Lease (leasefault.
// RenewsFailReleaseHeld): A's Elected channel stays closed and its /metrics keeps answering
// throughout. A's first scrape without hpcburst_ series ends no later than B's acquireTime, every
// later scrape of A has none, and A answers after B leads, so the window was there.
func TestMetricsLostLeaseStopsBeforeANewCopyLeads(t *testing.T) {
	g := newServingRig(t)
	defer g.logPorts(t)
	f := leasefault.New(v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)
	defer f.OpenHeld()
	a, pa := g.startCopy(t, "copy A", Options{Config: f.Config(g.cfg)})
	in, alloc := g.awaitServing(t, "copy A", pa)
	holderA := holderOf(g.leaseOf(t))
	b, pb := g.startChildCopy(t)
	eventually(t, 2*time.Minute, "copy B is a standby with synced caches", func() (bool, string) {
		caches, leading := readyzCodes(t, pb.health)
		return caches == http.StatusOK && leading == http.StatusInternalServerError, fmt.Sprintf("/readyz/caches %d, /readyz/leading %d; output:\n%s", caches, leading, b.out.String())
	})

	scrapes := startScrapeLog(pa.metrics)
	defer scrapes.finish()
	eventually(t, time.Minute, "a scrape of copy A has hpcburst_ series", func() (bool, string) { return scrapes.served(), "none yet" })
	faultAt := time.Now()
	f.Set(leasefault.RenewsFailReleaseHeld)

	var bAcquire time.Time
	eventually(t, 2*time.Minute, "copy B takes the Lease", func() (bool, string) {
		l := g.leaseOf(t)
		h := holderOf(l)
		if h == "" || h == holderA || l.Spec.AcquireTime == nil {
			return false, "the Lease names " + h
		}
		bAcquire = l.Spec.AcquireTime.Time
		return true, ""
	})
	eventually(t, 2*time.Minute, "copy B serves", func() (bool, string) { return servesState(pb.metrics, in, alloc) })
	// A runs on while its release read is held: see it answer after B's take.
	eventually(t, time.Minute, "copy A answers after copy B's take", func() (bool, string) {
		code, body := scrapeText(pa.metrics)
		return code == http.StatusOK && strings.Contains(body, controlSeries) && time.Now().After(bAcquire.Add(time.Second)), fmt.Sprintf("/metrics %d", code)
	})
	got := scrapes.finish()
	f.OpenHeld()
	select {
	case err := <-a.done:
		if err == nil || !strings.Contains(err.Error(), "leader election lost") {
			t.Errorf("copy A returned %v, want the lost leadership", err)
		}
	case <-time.After(managerStopLimit):
		t.Fatalf("copy A did not return within %s of its release read's end; goroutines:\n%s", managerStopLimit, goroutines())
	}
	a.once.Do(func() {}) // it has returned; the cleanup must not wait for it again
	awaitUnregistered(t)

	served, firstEmpty, aliveAfter := false, -1, 0
	for i, s := range got {
		if s.end.Before(faultAt) && s.hpcburst {
			served = true
		}
		if firstEmpty < 0 && !s.start.Before(faultAt) && s.alive && !s.hpcburst {
			firstEmpty = i
		}
		if s.start.After(bAcquire) && s.alive {
			aliveAfter++
		}
	}
	if !served {
		t.Fatal("copy A exported nothing before the fault; the case proves nothing")
	}
	if firstEmpty < 0 {
		t.Fatalf("copy A never answered without hpcburst_ series after the fault (%d scrapes)", len(got))
	}
	if end := got[firstEmpty].end; end.After(bAcquire) {
		t.Errorf("copy A's first scrape without hpcburst_ series ended at %s, %s after copy B acquired the Lease at %s", end.Format(time.RFC3339Nano), end.Sub(bAcquire), bAcquire.Format(time.RFC3339Nano))
	}
	for _, s := range got[firstEmpty:] {
		if s.hpcburst {
			t.Errorf("copy A exported hpcburst_ series again in a scrape at %s (%s after copy B's take)", s.start.Format(time.RFC3339Nano), s.start.Sub(bAcquire))
		}
	}
	if aliveAfter == 0 {
		t.Error("copy A did not answer after copy B's take; the window the test needs was not there")
	}
	held := false
	for _, r := range f.Requests() {
		if r.Method == http.MethodGet && !r.Marked && r.At.Before(bAcquire) && r.Done.After(bAcquire) {
			held = true
		}
	}
	if !held {
		t.Errorf("no release read of copy A was held across copy B's take (requests %+v); the case proves nothing", f.Requests())
	}
	t.Logf("fault at %s; copy A's first empty scrape ended %s after it; copy B acquired %s after it; %d scrapes of copy A answered after the take",
		faultAt.Format(time.RFC3339Nano), got[firstEmpty].end.Sub(faultAt), bAcquire.Sub(faultAt), aliveAfter)
}

// logPorts logs the pairs the test's copies took and how many starts found a port taken.
func (g *servingRig) logPorts(t *testing.T) {
	t.Helper()
	t.Logf("the copies took %d port pairs from %d; %d starts found a port taken", g.ports.next, servingPortBase, g.retries)
}

// TestMetricsCopiesStartAgainOnALostPort covers the tests' own retry (#219): a copy whose bind
// finds a port taken starts again on the next pair, once, logged. A stranger holds the health port
// of the pair a copy is about to take, first for an in-process copy, whose NewManager finds it
// taken, then for the child, whose NewManager exits on it; each copy then serves on the next pair,
// and the count of retries is one per copy. The health port is the one held because its bind is in
// NewManager and fails at once; a metrics port found taken fails Start, which returns only after
// the manager's 30 s shutdown grace, the same path at a higher cost.
func TestMetricsCopiesStartAgainOnALostPort(t *testing.T) {
	g := newServingRig(t)
	defer g.logPorts(t)
	stranger := func(addr string) {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("the stranger's bind of %s: %v", addr, err)
		}
		t.Cleanup(func() { _ = l.Close() })
	}
	held := g.ports.peek(t)
	stranger(held.health)
	_, pa := g.startCopy(t, "copy A", Options{})
	if pa == held || g.retries != 1 {
		t.Fatalf("copy A serves on %+v after %d retries; want the pair after %+v, after one", pa, g.retries, held)
	}
	held = g.ports.peek(t)
	stranger(held.health)
	_, pb := g.startChildCopy(t)
	if pb == held || g.retries != 2 {
		t.Fatalf("the child copy serves on %+v after %d retries in all; want the pair after %+v, and one retry of its own", pb, g.retries, held)
	}
}
