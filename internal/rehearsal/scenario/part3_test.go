package scenario_test

// Issue #193, the one-host rehearsal's third part: on #146's rig, over the
// real tunnel, the local half of the alpha criteria that #146 does not assert.
//  1. HBEP-5: the Test plan's 7,000-token stream completes through the front
//     and the tunnel; every response carries the attribution headers, and its
//     usage record holds the same facts.
//  2. HBEP-8: the events the scenario's code paths raise, in their form; the
//     engine gauges of the Instances, unknowns shown as unknown; the front's
//     metrics for both kinds of target, an Instance and a managed target
//     backed by the LLMLab double.
//  3. HBEP-3: a requeue of an Allocation's job fails its Instance, and each
//     Instance carries its deployment ID under its Allocation's tag.
//  4. HBEP-6: step (f)'s controller restart, while the 7,000-token stream is
//     in flight.
//  5. HBEP-7's no-overlap handover (science agent, issue #193, 1 Nov 2026), on
//     the queued path, Helios's (a partition QOS of 2,000 jobs, a Provider
//     QOS of one GPU per user), and on the one-slot path (a QOS of one job
//     and one GPU per user), side by side on one stack.
// Everything is asserted from the API, the watch, events, the usage files,
// /metrics, frps's list and the fake cluster's records, never from log text.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// ports193: issue #193's block is 29680-29699. hpcburst up takes launcher
// base 29680 (29680-29692); the front's client and dial-home listeners take
// 29693 and 29694. The LLMLab double binds the first free port of
// 29695-29699 (llmlabtest takes a range, not a kernel-chosen port).
// Everything else is kernel-chosen.
var ports193 = stack.Ports{Base: 29680, ClientAddr: "127.0.0.1:29693", DialHomeListen: "127.0.0.1:29694"}

const llmlabPortLo, llmlabPortHi = 29695, 29699

// longTokens is HBEP-5's Test plan stream: 7,000 tokens. At part3TokenDelay
// it lasts at least 49 s, under routing.maxRequestDuration (90 s), and longer
// than a controller's takeover of the Lease (18.7 s in CI on #161).
const longTokens = 7000

// part3TokenDelay is the served engines' token-delay mode in this test.
const part3TokenDelay = 7 * time.Millisecond

// part3MaxModelLen fits the 7,000-token reply and its prompt.
const part3MaxModelLen = "8192"

// The fake cluster's QOSes, by example names: partQOS is the queued
// partition's own QOS, 2,000 jobs per user as Helios's plgrid; each served
// deployment's Provider runs under its own QOS of one GPU per user, whose job
// limit of 1 is held by the partition QOS on the queued path (Helios's now)
// and binds on the one-slot path.
const partQOS = "example-part"

// dep is one served deployment of the test and its Provider, partition and
// QOS.
type dep struct {
	stack.Served
	partition, qos string
	oneSlot        bool
}

var (
	queuedDep = dep{Served: stack.Served{Name: "rehearsal-queued", Model: "example-org/queued-model", Provider: "rehearsal-queued",
		MaxModelLen: part3MaxModelLen}, partition: "rehearsal-queued", qos: "example-now"}
	oneSlotDep = dep{Served: stack.Served{Name: "rehearsal-oneslot", Model: "example-org/oneslot-model", Provider: "rehearsal-oneslot",
		MaxModelLen: part3MaxModelLen}, partition: "rehearsal-oneslot", qos: "example-one", oneSlot: true}
)

// The managed target: a Provider of a managed platform whose gateway is the
// LLMLab double, and a managed-source ModelDeployment under its own model
// name, so that a served model's requests never reach it.
const (
	managedProfileName = "rehearsal-llmlab"
	managedProvider    = "rehearsal-llmlab"
	managedPlatform    = "example-llmlab"
	managedMD          = "rehearsal-managed"
	managedModel       = "example-org/managed-model"
	managedEntry       = "example-org/managed-entry"
	managedKey         = "example-llmlab-key-0123456789abcdef"
)

func part3Shape() stack.ClusterShape {
	return stack.ClusterShape{
		Partitions: []fakeslurm.Partition{
			{Name: queuedDep.partition, GPUOnly: true, QOS: partQOS},
			{Name: oneSlotDep.partition, GPUOnly: true},
		},
		QOS: []fakeslurm.QOS{
			{Name: partQOS, MaxSubmitPerUser: fakeslurm.Cap(2000)},
			{Name: queuedDep.qos, MaxSubmitPerUser: fakeslurm.Cap(1), MaxGPUsPerUser: fakeslurm.Cap(1)},
			{Name: oneSlotDep.qos, MaxSubmitPerUser: fakeslurm.Cap(1), MaxGPUsPerUser: fakeslurm.Cap(1)},
		},
	}
}

// part3Profile is #146's batch profile with the two partitions and the QOS
// limits the plan reads, matching the fake cluster's.
func part3Profile(arch string) *v1alpha1.PlatformProfile {
	p := stack.BatchProfile(arch, rehearsalTiming)
	q, o := p.Spec.Partitions[0], p.Spec.Partitions[0]
	q.Name, q.QOS = queuedDep.partition, partQOS
	o.Name = oneSlotDep.partition
	p.Spec.Partitions = []v1alpha1.PartitionProfile{q, o}
	p.Spec.QOS = []v1alpha1.QOSProfile{
		{Name: partQOS, MaxSubmitPerUser: stack.Ptr(int32(2000))},
		{Name: queuedDep.qos, MaxSubmitPerUser: stack.Ptr(int32(1)), MaxGPUsPerUser: stack.Ptr(int32(1))},
		{Name: oneSlotDep.qos, MaxSubmitPerUser: stack.Ptr(int32(1)), MaxGPUsPerUser: stack.Ptr(int32(1))},
	}
	return p
}

func part3Provider(d dep, creds string) *v1alpha1.Provider {
	p := stack.BatchProvider(creds)
	p.Name = d.Provider
	p.Spec.AllowedPartitions = []string{d.partition}
	p.Spec.QOS = d.qos
	return p
}

// part3 is one run of the test: the rig, the LLMLab double and the two served
// deployments.
type part3 struct {
	*rig
	llm  *llmlabtest.Server
	deps []*depRun
	// replies is every reply the test got, each checked for its
	// attribution when it came (attributed).
	replies []attributed
	// managedID is the managed entry's deployment ID, from the Provider's
	// catalog.
	managedID v1alpha1.FullDeploymentID
	// requeued is the Allocation the requeue failed (HBEP-3).
	requeued requeueResult
	// base is the absence checks' first read of the raised-events counter,
	// taken as soon as the controller restarted in longStreamAcrossRestart
	// serves (part3_absence_test.go, orphans_test.go).
	base *absenceBase
	// s2rMiss, when set, is called by armFirstRead after each read that
	// found a class's start-to-ready time unrecorded (a test's hook; nil in
	// the rehearsal).
	s2rMiss func(missing []string)
}

// objects starts the LLMLab double, writes the managed Provider's
// credentials file, and returns the profiles and Providers.
func (p3 *part3) objects(r *rig, arch, creds string) []client.Object {
	r.t.Helper()
	p3.llm = llmlabtest.Start(r.t, llmlabtest.Options{PortLo: llmlabPortLo, PortHi: llmlabPortHi, Keys: []string{managedKey},
		Models: []llmlabtest.Model{{ID: managedEntry}}})
	b, err := provider.ManagedCredentials{Key: managedKey, BaseURL: p3.llm.BaseURL()}.Encode()
	if err != nil {
		r.t.Fatal(err)
	}
	mcreds := filepath.Join(r.dir, "llmlab.json")
	if err := os.WriteFile(mcreds, b, 0o600); err != nil {
		r.t.Fatal(err)
	}
	return []client.Object{
		part3Profile(arch), part3Provider(queuedDep, creds), part3Provider(oneSlotDep, creds),
		&v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: managedProfileName}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformManaged}},
		&v1alpha1.Provider{
			ObjectMeta: metav1.ObjectMeta{Name: managedProvider, Labels: map[string]string{v1alpha1.LabelPlatform: managedPlatform}},
			Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: managedProfileName},
				CredentialsRef: &v1alpha1.CredentialsReference{File: mcreds}},
		},
	}
}

func managedDeployment() *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: managedMD, Namespace: stack.Namespace},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model:     managedModel,
			Source:    v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: managedPlatform, CatalogName: managedEntry}},
			Instances: 1,
			Providers: v1alpha1.ProviderSelection{Names: []string{managedProvider}},
		},
	}
}

// part3EngineModes are the served engines' modes: the token delay, and no
// prefix-cache counter lines, so that two engine metrics are not reported
// (HBEP-8, Unknown samples: not_reported).
var part3EngineModes = "token-delay=" + part3TokenDelay.String() + ",no-prefix-cache-metrics"

// TestRehearsalPart3 is issue #193. The steps on the queued deployment's
// primary all run before its drain start; the no-overlap handovers of both
// deployments follow, then the requeue of the queued successor's job, the
// events, and the scale to 0 with hpcburst down.
//
//livecheck:rehearsal-part3
func TestRehearsalPart3(t *testing.T) {
	p3 := &part3{}
	// The engine gate: the no-overlap phase holds each successor at its engine's
	// start until its gap has been probed (waitSuccessors).
	r := startRigWith(t, rigSetup{mode: "direct", ports: ports193, shape: part3Shape(), engineGate: true, objects: p3.objects})
	p3.rig = r
	r.tokenDelay = part3TokenDelay
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: part3EngineModes}}

	done := r.stepTimer("start")
	p3.start()
	done()
	q := p3.queued()
	for _, d := range p3.deps {
		p3.checkIdentity(d, d.primary)
	}
	p3.checkQueuedSuccessor(q)
	done = r.stepTimer("attribution of errors and the managed target")
	p3.errorsAttributed()
	p3.frontMetricsBothKinds(q)
	done()
	p3.gauges(q, "before the restart")
	done = r.stepTimer("7,000-token stream across a controller restart")
	// The absence checks cover the restarted controller, from its first
	// moment of serving: no Instance has ended and no drain start has passed.
	// The first read is armed once each class's start-to-ready time is
	// recorded (issue #321).
	if err := p3.armFirstRead(); err != nil {
		r.t.Fatal(err)
	}
	p3.longStreamAcrossRestart(q)
	r.onLed = nil
	done()
	p3.gauges(q, "after the restart")
	done = r.stepTimer("no-overlap handovers")
	p3.noOverlap()
	done()
	done = r.stepTimer("requeue")
	p3.requeue(q)
	done()
	p3.checkEvents()
	p3.attributionSummary()
	done = r.stepTimer("scale to 0 and down")
	p3.scaleToZero()
	done()
}

// queued is the queued-path deployment's run.
func (p3 *part3) queued() *depRun {
	for _, d := range p3.deps {
		if !d.oneSlot {
			return d
		}
	}
	p3.t.Fatal("no queued-path deployment")
	return nil
}

// start waits for the batch Providers over the login node and for the
// managed deployment's catalog entry, creates the served deployments, and
// waits for each one's primary to be ready and registered at the front.
func (p3 *part3) start() {
	r := p3.rig
	r.t.Helper()
	for _, name := range []string{queuedDep.Provider, oneSlotDep.Provider} {
		var pv v1alpha1.Provider
		stack.WaitFor(r.t, stack.WaitDeadline, "Provider "+name+" to be Ready over the login node", func() bool {
			return r.get(&pv, name, "") && v1alpha1.ProviderReady(&pv, time.Now())
		})
	}
	r.assertRealSSH()
	stack.Create(r.t, r.c, managedDeployment())
	for _, d := range []dep{queuedDep, oneSlotDep} {
		stack.Create(r.t, r.c, stack.ServedDeploymentAs(d.Served, rehearsalTiming, r.engineEnv...))
		p3.deps = append(p3.deps, &depRun{dep: d})
	}
	p3.waitManaged()
	for _, d := range p3.deps {
		p3.waitPrimary(d)
	}
}

// waitManaged waits until the managed Provider's catalog lists the entry and
// the managed deployment is Ready, and reads the entry's deployment ID.
func (p3 *part3) waitManaged() {
	r := p3.rig
	r.t.Helper()
	stack.WaitFor(r.t, stack.WaitDeadline, "the managed deployment to be Ready on the LLMLab double's catalog", func() bool {
		var pv v1alpha1.Provider
		var md v1alpha1.ModelDeployment
		if !r.get(&pv, managedProvider, "") || !r.get(&md, managedMD, stack.Namespace) {
			return false
		}
		for _, e := range pv.Status.Catalog {
			if e.Name == managedEntry && e.Listed {
				p3.managedID = e.ManagedID
			}
		}
		return p3.managedID != "" && conditionTrue(md.Status.Conditions, v1alpha1.ConditionReady)
	})
}

// allocationsOf lists a deployment's Allocations, by its label.
func (p3 *part3) allocationsOf(md string) []v1alpha1.Allocation {
	var out []v1alpha1.Allocation
	for _, a := range p3.allocations() {
		if a.Labels[v1alpha1.LabelModelDeployment] == md {
			out = append(out, a)
		}
	}
	return out
}

// instancesOf lists a deployment's Instances, by its label.
func (p3 *part3) instancesOf(md string) []v1alpha1.Instance {
	var out []v1alpha1.Instance
	for _, in := range p3.instances() {
		if in.Labels[v1alpha1.LabelModelDeployment] == md {
			out = append(out, in)
		}
	}
	return out
}

// waitPrimary waits for the deployment's first Instance, the one with no
// predecessor, to be ready and registered at the front, and checks that its
// job was submitted held and released once.
func (p3 *part3) waitPrimary(d *depRun) {
	r := p3.rig
	r.t.Helper()
	stack.WaitState(r.t, stack.WaitDeadline, "the primary of "+d.Name+" to be ready and registered at the front", func() (bool, string) {
		for _, in := range p3.instancesOf(d.Name) {
			if in.Annotations[v1alpha1.AnnotationPredecessor] != "" || in.Status.Phase != v1alpha1.InstanceReady {
				continue
			}
			if reg, known := stack.Registered(r.ports, &in); known && reg {
				in := in
				d.primary = &in
				return true, ""
			}
		}
		return false, r.describe()
	})
	d.primaryJob = jobIDOf(r.t, d.primary)
	r.checkReleased(d.primaryJob)
	ds, ok := drainStartOf(d.primary)
	if !ok {
		r.t.Fatalf("the primary %s has no lease end", d.primary.Name)
	}
	d.ds = ds
	r.t.Logf("%s: primary %s ready on job %d; its drain start is %s, %s from now", d.Name, d.primary.Name, d.primaryJob,
		ds.UTC().Format(time.RFC3339), time.Until(ds).Round(time.Second))
}

// atLeastBefore fails unless the deployment's drain start is at least lead
// away: what follows must end before it (the brief's lease of 7 minutes). It
// logs the headroom left, so that a slow runner's failure here can be told
// from a product's.
func (p3 *part3) atLeastBefore(d *depRun, what string, lead time.Duration) {
	p3.t.Helper()
	left := time.Until(d.ds)
	if left < lead {
		p3.t.Fatalf("%s would start %s before %s's drain start %s, under the %s it needs; the run is too slow for the 7 min lease",
			what, left.Round(time.Second), d.Name, d.ds.UTC().Format(time.RFC3339), lead)
	}
	p3.t.Logf("HEADROOM %s: %s before %s's drain start, %s needed, %s to spare", what, left.Round(time.Second), d.Name, lead, (left - lead).Round(time.Second))
}

// longStreamAcrossRestart is items 1 and 4: the Test plan's 7,000-token
// stream through the front and the tunnel, in flight across step (f)'s
// controller restart, ends with its finish reason and [DONE], uncut.
func (p3 *part3) longStreamAcrossRestart(d *depRun) {
	p3.t.Helper()
	p3.atLeastBefore(d, "the 7,000-token stream", longStreamLead)
	sc := p3.startRestartScraper(string(d.primary.UID))
	s, row := p3.controllerRestart(d.primary, "rehearsal-part3-long", stack.ChatBody(d.Model, longTokens, longTokens, true, ""), longTokens)
	reads := sc.end()
	problems, stale, fresh, unserved := restartReadProblems(reads, string(d.primary.UID))
	for _, p := range problems {
		p3.t.Errorf("the primary's gauges across the controller restart: %s", p)
	}
	// No count of stale reads is required: a report can reach the new
	// controller before its first pass, and then no read shows them stale.
	// Every unknown sample a read holds must carry stale (or not_reported, on
	// a gauge the engine does not export): restartReadProblems.
	if fresh == 0 {
		p3.t.Errorf("the primary's gauges across the controller restart: none of %d reads showed them fresh", len(reads))
	}
	p3.t.Logf("the primary's gauges across the controller restart: %d reads, %d stale (no report yet since the restart), %d fresh, %d with no series of the Instance; none with a gauge neither known nor unknown, and every unknown sample stale",
		len(reads), stale, fresh, unserved)
	if unserved > 0 {
		p3.t.Logf("FINDING (#249): for %d reads (250 ms apart) while the new controller took the Lease, its /metrics had no series of the live primary at all, "+
			"neither its info series nor its gauges nor their unknown samples (internal/lifecycle/metrics.go:239, Collect over an inventory that only "+
			"the leader's passes fill): HBEP-8 gives every Instance not ended its info series, and after a controller restart each engine metric "+
			"unknown with reason stale until the agent's next report (Test plan); a share-of-unknown query reads 0/0 there", unserved)
	}
	p3.attribute("the 7,000-token stream", s.Reply, row, wantInstance(d, d.primary))
	p3.t.Logf("the 7,000-token stream: %d tokens in %d ms through the front, finish %v, across the controller restart", deref(row.CompletionTokens), row.DurationMs, derefS(row.Finish))
}

// jobIDOf is the Instance's job ID as a number.
func jobIDOf(t *testing.T, in *v1alpha1.Instance) int {
	t.Helper()
	var id int
	for _, c := range in.Status.JobID {
		if c < '0' || c > '9' {
			t.Fatalf("Instance %s's job ID %q is not a number", in.Name, in.Status.JobID)
		}
		id = id*10 + int(c-'0')
	}
	if id == 0 {
		t.Fatalf("Instance %s has no job ID", in.Name)
	}
	return id
}

func deref(p *int64) int64 {
	if p == nil {
		return -1
	}
	return *p
}

func derefS(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// hasWord reports whether s names the word w on its own (a reason in a
// condition's message), not as part of a longer word.
func hasWord(s, w string) bool {
	for i := strings.Index(s, w); i >= 0; {
		end := i + len(w)
		before := i == 0 || !isWordByte(s[i-1])
		after := end == len(s) || !isWordByte(s[end])
		if before && after {
			return true
		}
		j := strings.Index(s[i+1:], w)
		if j < 0 {
			break
		}
		i += 1 + j
	}
	return false
}

func isWordByte(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}
