package keeperproc

// Stage B's rig (#343's PR 4): PR 3's world (the rehearsal's login node, the
// fake Slurm in run mode, the echo targets), an envtest API server, a keeper
// process, and the tap between the keeper and the real controllers. The test
// process holds only these, the streams and the pins; the keeper, each
// controller, the job side and the login node's commands are processes of
// their own.
//
// The API server holds what the controllers read: the controller namespace
// for the Leases, an Allocation namespace, the Providers (their UID and
// creationTimestamp the API server's) and the Allocations, created before any
// controller starts. A running Allocation's job is submitted by the test under
// v1alpha1.JobTag(namespace, name, UID), the UID the API server gave, with the
// job side's --allocation set to that UID, so the controller ADOPTS it on its
// first reconcile (external.go, establish and adopt) and never submits it.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The example values of Stage B's API objects.
const (
	allocNS  = "example-ns"
	partName = "example-gpu"
	cpuArch  = "x86_64"
)

// stagedDigest is the world's staged agent's digest: its binary is linked at
// <staging>/agent/<digest>/hb-agent (newWorld).
var stagedDigest = strings.Repeat("e", 64)

// rig is Stage B's world.
type rig struct {
	t   *testing.T
	w   *world
	api *apiServer
	k   *keeper
	tap *tap

	kubeconfig          string
	credsDir, buildsDir string
	partition           v1alpha1.PartitionProfile
	certPEM             []byte
	certPin             string
	frpcDigests         map[string]string
	// timings, when set, are the controllers' timings in place of
	// stageBTimings.
	timings *ctlTimings
}

// ctlTimings are the timings of the rig's controllers.
func (g *rig) ctlTimings() ctlTimings {
	if g.timings != nil {
		return *g.timings
	}
	return stageBTimings
}

// newRig starts the world, the API server, a keeper on a new state directory
// and the tap toward it, listening.
func newRig(t *testing.T) *rig {
	t.Helper()
	w := newWorld(t)
	api := startAPIServer(t)
	ctx := context.Background()
	for _, ns := range []string{v1alpha1.ControllerNamespace, allocNS} {
		if err := api.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	dir, err := os.MkdirTemp("", "kr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	g := &rig{t: t, w: w, api: api, kubeconfig: filepath.Join(dir, "kubeconfig"),
		credsDir: filepath.Join(dir, "credentials"), buildsDir: filepath.Join(dir, "agent-builds")}
	if err := os.WriteFile(g.kubeconfig, api.kubeconfig, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{g.credsDir, g.buildsDir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	gpus, maxTime := int32(4), v1alpha1.Duration("48h")
	g.partition = v1alpha1.PartitionProfile{Name: partName, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: cpuArch},
		GPUsPerNode: &gpus, MaxTime: &maxTime}
	g.certPEM, g.certPin = ctltest.Cert(t)
	g.frpcDigests = map[string]string{cpuArch: ctltest.Frpc(t, g.buildsDir, cpuArch)}
	g.k = w.startKeeper("")
	g.tap = newTap(t, g.k.sock)
	g.tap.start()
	return g
}

// provider creates a batch Provider named name and returns it as its sets
// name it: its UID and creationTimestamp are the API server's.
func (g *rig) provider(name string) prov {
	g.t.Helper()
	p := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: "example-profile"}, Account: "example-account",
		AllowedPartitions: []string{partName}, CredentialsRef: &v1alpha1.CredentialsReference{File: filepath.Join(g.w.base, "creds")},
	}}
	if err := g.api.c.Create(context.Background(), p); err != nil {
		g.t.Fatalf("create Provider %s: %v", name, err)
	}
	if p.UID == "" || p.CreationTimestamp.IsZero() {
		g.t.Fatalf("Provider %s has no UID or creationTimestamp from the API server", name)
	}
	return prov{name: name, uid: string(p.UID), created: p.CreationTimestamp.Time}
}

// allocation creates an Allocation of Provider p, on the rig's partition,
// with the agent digest digest, and returns it with the UID the API server
// gave.
func (g *rig) allocation(p prov, name, digest string) *v1alpha1.Allocation {
	g.t.Helper()
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: allocNS},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: p.name},
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
				Partition: partName, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), AgentDigest: digest,
			}},
		},
	}
	if err := g.api.c.Create(context.Background(), a); err != nil {
		g.t.Fatalf("create Allocation %s: %v", name, err)
	}
	if a.UID == "" {
		g.t.Fatalf("Allocation %s has no UID from the API server", name)
	}
	return a
}

// tagOf is an Allocation's job tag.
func tagOf(a *v1alpha1.Allocation) string { return v1alpha1.JobTag(a.Namespace, a.Name, a.UID) }

// running creates an Allocation of p and submits its job, which runs the
// world's staged agent as the job side, and returns both.
func (g *rig) running(p prov, name string) (*v1alpha1.Allocation, job) {
	g.t.Helper()
	a := g.allocation(p, name, stagedDigest)
	return a, g.w.submitFor(tagOf(a), string(a.UID), 30*time.Second)
}

// submitFor is world.submit for an Allocation the API server made: the job
// is tagged tag, its job side's --allocation is uid, and its settings are the
// rig's Allocations' (partition, time limit, GPUs), so the controller's
// read-back of it matches. It returns once the job side's listeners are bound.
func (w *world) submitFor(tag, uid string, wait time.Duration) job {
	w.t.Helper()
	portsFile := filepath.Join(w.base, "ports-"+strconv.FormatInt(w.n.Add(1), 10))
	j := job{tag: tag, allocDir: filepath.Join(w.staging, agentapi.AllocationDir(tag)), uid: uid}
	if err := os.MkdirAll(j.allocDir, 0o700); err != nil {
		w.t.Fatal(err)
	}
	line, err := slurm.Line("sbatch", "--parsable", "--job-name="+tag, "--partition="+partName, "--time=30", "--gres=gpu:1",
		"--wrap=exec "+w.agent+" "+jobRole+" "+portsFile+" "+wait.String()+" "+j.allocDir+" "+j.uid)
	if err != nil {
		w.t.Fatal(err)
	}
	res, err := w.node.Session().Run(w.t.Context(), slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		w.t.Fatalf("sbatch: %v, %+v", err, res)
	}
	j.id = strings.TrimSpace(string(res.Stdout))
	waitFor(w.t, time.Minute, "job "+j.id+"'s listeners", func() bool {
		b, err := os.ReadFile(portsFile)
		return err == nil && json.Unmarshal(b, &j.ports) == nil && j.ports.Status != 0
	})
	return j
}

// sbatches counts the sbatch calls in the fake's log that name tag: the fake
// lives in the test process, so the count is independent of the controllers.
func (w *world) sbatches(tag string) int {
	n := 0
	for _, c := range w.plat.Calls() {
		if len(c.Words) == 0 || c.Words[0] != "sbatch" {
			continue
		}
		for _, word := range c.Words {
			if word == "--job-name="+tag {
				n++
			}
		}
	}
	return n
}

// allSbatches counts every sbatch call in the fake's log.
func (w *world) allSbatches() int {
	n := 0
	for _, c := range w.plat.Calls() {
		if len(c.Words) > 0 && c.Words[0] == "sbatch" {
			n++
		}
	}
	return n
}

// entryIn is the set's entry for tag, and whether it has one.
func entryIn(s *wire.DesiredSet, tag string) (wire.Entry, bool) {
	if s == nil {
		return wire.Entry{}, false
	}
	for _, e := range s.Allocations {
		if e.Tag == tag {
			return e, true
		}
	}
	return wire.Entry{}, false
}

// leads waits until a set of controller c, through the tap, is complete,
// names the job with the Allocation directory and agent the job side has
// (the paths the controller derives from its staging directory), and is
// answered with a report; and both of the job's relays run under the
// keeper, with the job side connected. It returns the set's request and
// the relays.
func (g *rig) leads(c *realCtl, p prov, j job) (tapReq, map[agentapi.RelayChannel]wire.RelayState) {
	g.t.Helper()
	r := g.tap.waitReq(g.t, 2*time.Minute, "controller "+c.name+"'s complete set naming job "+j.id, func(r *tapReq) bool {
		e, ok := entryIn(r.set, j.tag)
		return r.pid == c.pid() && r.report != nil && r.set.Complete && ok && e.JobID == j.id
	})
	got := g.tap.snapshot(r)
	e, _ := entryIn(got.set, j.tag)
	if e.AllocDir != j.allocDir || e.AgentPath != g.w.agent || e.UID != j.uid {
		g.t.Fatalf("controller %s's entry %+v: want the job side's directory %s, the staged agent %s and UID %s", c.name, e, j.allocDir, g.w.agent, j.uid)
	}
	return got, g.k.waitUp(p, j)
}

// adopted checks that a controller recorded the test's job as the
// Allocation's (its job-ID annotation, read from the API server): it adopted
// the job found by its tag.
func (g *rig) adopted(a *v1alpha1.Allocation, j job) {
	g.t.Helper()
	waitFor(g.t, time.Minute, "Allocation "+a.Name+" records job "+j.id, func() bool {
		al, ok := g.api.allocation(g.t, a.Namespace, a.Name)
		return ok && al.Annotations[v1alpha1.AnnotationJobID] == j.id
	})
}

// firstLease reads the Provider's Lease while its first leader, alone, holds
// it, and checks its anchor: the leader's elector created it, and client-go
// writes a created record with leaseTransitions 0, so the first leader's
// epoch is 0, and each later holder's is one up on its holder change.
func (g *rig) firstLease(provider string) leaseRecord {
	g.t.Helper()
	l := g.api.lease(g.t, provider)
	if !l.found || l.holder == "" {
		g.t.Fatalf("Provider %s's Lease %+v has no holder while its first leader leads", provider, l)
	}
	if l.transitions != 0 {
		g.t.Fatalf("Provider %s's Lease has leaseTransitions %d under its first leader, want 0: client-go creates the record with 0", provider, l.transitions)
	}
	return l
}

// ownSbatches checks that every sbatch call in the fake's log is the test's
// own, n of them: the controllers adopted the jobs and submitted none.
func (g *rig) ownSbatches(n int) {
	g.t.Helper()
	if got := g.w.allSbatches(); got != n {
		g.t.Errorf("%d sbatch calls reached the fake, want the test's own %d: a controller submitted a job", got, n)
	}
}

// setsFrom are the sets from the process pid that the keeper answered.
func (g *rig) setsFrom(pid int) []tapReq {
	return g.tap.requests(func(r *tapReq) bool { return r.pid == pid && r.set != nil && r.replyKind != "" })
}

// epochsOf checks that every set and acknowledgement from pid carries the
// Lease epoch want.
func (g *rig) epochsOf(name string, pid int, want int64) {
	g.t.Helper()
	for _, r := range g.tap.requests(from(pid)) {
		switch {
		case r.set != nil && r.set.LeaseEpoch != want:
			g.t.Errorf("controller %s's set %d carries Lease epoch %d, want %d", name, r.n, r.set.LeaseEpoch, want)
		case r.ack != nil && r.ack.LeaseEpoch != want:
			g.t.Errorf("controller %s's acknowledgement %d carries Lease epoch %d, want %d", name, r.n, r.ack.LeaseEpoch, want)
		}
	}
}

// describe is a request as a test's message shows it.
func (r tapReq) describe() string {
	return fmt.Sprintf("request %d (%s from %d at %s, %s)", r.n, r.kind, r.pid, r.at.Format(time.RFC3339Nano), r.replyKind)
}
