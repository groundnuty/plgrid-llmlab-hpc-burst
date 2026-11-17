package e2e

// The rig of HBEP-8's beta rows for the Provider controller's typed Allocation
// events (#625): the controller over the real slurm.Batch and the in-process
// fake Slurm (internal/slurm/fakeslurm), one manual clock for both, and a real
// events.k8s.io recorder writing into the test control plane's API server. A
// test drives one reconcile at a time (Controller.Reconciler.Reconcile), so
// every pass is a barrier for its Allocation: the pass has finished emitting
// when Reconcile returns. What a test checks is what the API server holds
// (events.k8s.io/v1 Events, listed and filtered by regarding UID, reason,
// action and reportingController), never the recorder's in-memory double or what
// the emitter returned; absence is shown by hpcburst_events_raised_total with
// HBEP-8's four conditions (internal/eventcount/absence.go).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// The synthetic values of the rows: a job ID no other field holds, and the
// Provider's account and grant, which no typed note may name. They are the
// same shape as the real ones and none of them is real.
const (
	firstJobID = 1234567
	rowAccount = "example-account-a1"
	rowGrant   = "example-grant-g1"
	rowPart    = "example-gpu"
	// rowUser and rowHome are the login's user name and home on the synthetic
	// cluster, and rowCredsPath a path under that home: none of them may reach a
	// note the log keeps (HBEP-8, Allocation events, No job ID, path or name in a
	// note).
	rowUser      = "example-user-x1"
	rowHome      = "/home/" + rowUser
	rowCredsPath = rowHome + "/.hpcburst/creds-x1"
)

var rigSeq atomic.Int64

// rowDeploymentID and rowLaunch are the fields an Instance needs for the API
// server to admit it: the Draining Instance of the drain row.
const (
	rowDeploymentID = "0e1dd69efe8a57c3eadd5e0293fd8c0f0cc185629e79297b6530de361b887852"
	rowCommit       = "0123456789abcdef0123456789abcdef01234567"
)

func rowLaunch() v1alpha1.Launch {
	return v1alpha1.Launch{
		Image: "docker.io/vllm/vllm-openai", ImageDigest: "sha256:" + strings.Repeat("2", 64),
		Command:    []string{"vllm", "serve", "/hpcburst/model", "--uds", "/hpcburst/run/worker-0.sock", "--dtype=bfloat16", "--max-num-seqs=256"},
		Checkpoint: v1alpha1.LaunchCheckpoint{Repository: "example-org/example-1b", Revision: rowCommit},
		GPUs:       1,
		MaxNumSeqs: 256,
	}
}

// evRig is one Provider's controller over the fake Slurm, with its own
// namespace, PlatformProfile and Provider.
type evRig struct {
	t       *testing.T
	ctx     context.Context
	clock   *pfake.Clock
	sl      *fakeslurm.Platform
	session *fakeslurm.Session
	faults  *lineFaults
	ctl     *providerctl.Controller
	ns      string
	prov    string
	home    string
	credDir string
	builds  string
	digest  string
	counter *eventcount.Counter
	reg     *prometheus.Registry
	typed   eventcount.IDRecorder
	// the front's settings, which every Options of the rig carries
	certPEM       []byte
	certPin, frpc string

	// failStatus, when set, makes the next status write of an Allocation fail
	// once; failedStatus keeps the object that write carried.
	failStatus   atomic.Bool
	failedMu     sync.Mutex
	failedStatus *v1alpha1.Allocation
}

// rigOpts are the variations of a rig.
type rigOpts struct {
	// noHold makes the profile a fallback-mode one (no hold-release).
	noHold bool
	// badProfile makes the Provider's profile invalid in ways whose error text
	// reaches the Allocation's status: a credentials file with a line whose
	// keyword is a path under the synthetic home (the reader echoes a keyword it
	// refuses), and an account with a space in it.
	badProfile bool
}

// extraCredsLines is what the credentials file holds beyond the valid lines.
func (o rigOpts) extraCredsLines() string {
	if o.badProfile {
		return rowCredsPath + " yes\n"
	}
	return ""
}

// start is the rows' fake time: a UTC date, never the real clock.
var start = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// newEvRig creates the Provider, its profile and namespace and builds the
// controller. Everything it creates is removed at the end of the test.
func newEvRig(t *testing.T, o rigOpts) *evRig {
	t.Helper()
	n := rigSeq.Add(1)
	r := &evRig{t: t, ctx: context.Background(), clock: pfake.NewClock(start), prov: fmt.Sprintf("evrow-%d", n),
		home: t.TempDir(), counter: eventcount.New()}
	key, creds := filepath.Join(r.home, "key"), filepath.Join(r.home, "creds")
	for path, content := range map[string]string{
		key:   "example key\n",
		creds: "HostName login.example.org\nUser " + rowUser + "\nIdentityFile " + key + "\n" + o.extraCredsLines(),
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r.credDir, r.builds = filepath.Join(r.home, "credentials"), filepath.Join(r.home, "builds")
	for _, d := range []string{r.credDir, r.builds} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	agent := []byte("#!/bin/sh\n# an example agent build\n")
	sum := sha256.Sum256(agent)
	r.digest = hex.EncodeToString(sum[:])
	if err := os.WriteFile(filepath.Join(r.builds, agentapi.AgentBuildName("x86_64")), agent, 0o700); err != nil {
		t.Fatal(err)
	}
	r.certPEM, r.certPin = ctltest.Cert(t)
	r.frpc = ctltest.Frpc(t, r.builds, "x86_64")

	gpus := int32(4)
	maxTime, window := v1alpha1.Duration("48h"), v1alpha1.Duration("5m")
	pp := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: r.prov + "-profile"}, Spec: v1alpha1.PlatformProfileSpec{
		ProviderKind: v1alpha1.PlatformBatch, HoldRelease: !o.noHold, JSONDataParser: "v0.0.42", InflightWindow: &window,
		StagingDir: "$SCRATCH/stage",
		Partitions: []v1alpha1.PartitionProfile{{Name: rowPart, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: "x86_64"},
			GPUsPerNode: &gpus, MaxTime: &maxTime}},
	}}
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: r.prov}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: pp.Name}, Account: r.account(o), Grant: rowGrant,
		AllowedPartitions: []string{rowPart}, CredentialsRef: &v1alpha1.CredentialsReference{File: creds},
	}}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "evrows-"}}
	for _, obj := range []client.Object{pp, prov, ns} {
		if err := c.Create(r.ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	r.ns = ns.Name
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), prov)
		_ = c.Delete(context.Background(), pp)
		_ = c.Delete(context.Background(), ns)
	})

	r.sl = fakeslurm.New(r.clock, fakeslurm.Config{FirstJobID: firstJobID, Partitions: []fakeslurm.Partition{{Name: rowPart, GPUOnly: true}}})
	r.session = r.sl.Login(fakeslurm.Login{User: rowUser, Env: map[string]string{"HOME": rowHome, "SCRATCH": "/scratch/" + rowUser}})
	r.faults = &lineFaults{inner: r.session}
	r.typed = r.newTypedRecorder()
	r.reg = newRegistry(r.counter)
	// providerctl.Setup creates the typed series at 0 (Options.Counter), which the
	// rows that drive the controller with New do themselves, so an absence check
	// reads a real 0; TestTheTypedSeriesAreCreatedAtZero runs Setup itself.
	r.counter.Zero(eventcount.ControllerProvider(r.prov), providerctl.RaisedEvents())

	wc, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme()})
	if err != nil {
		t.Fatal(err)
	}
	kube := interceptor.NewClient(wc, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if a, ok := obj.(*v1alpha1.Allocation); ok && sub == "status" && r.failStatus.CompareAndSwap(true, false) {
				r.failedMu.Lock()
				r.failedStatus = a.DeepCopy()
				r.failedMu.Unlock()
				return errors.New("an injected failure of the status write")
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	ctl, err := providerctl.New(kube, c, scheme(), r.options())
	if err != nil {
		t.Fatal(err)
	}
	r.ctl = ctl
	return r
}

// account is the Provider's account: the synthetic one, with a space in it for a
// rig whose profile is invalid (the reader refuses it).
func (r *evRig) account(o rigOpts) string {
	if o.badProfile {
		return rowAccount + " x"
	}
	return rowAccount
}

// options are the controller's: the profile read from the API, the Batch over
// the fault-injecting runner, the manual clock, and the typed recorder.
func (r *evRig) options() providerctl.Options {
	return providerctl.Options{
		Provider: r.prov, Profiles: &slurm.ProfileReader{Reader: c, Home: r.home},
		NewPlatform: func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
			return slurm.NewBatch(r.faults, v.Profile, v.Settings, slurm.Options{Clock: r.clock, PollInterval: 100 * time.Millisecond, BusyInterval: 100 * time.Millisecond}), io.NopCloser(nil), nil
		},
		Clock: r.clock, CredentialsDir: r.credDir, AgentBuildsDir: r.builds,
		Front: &providerctl.FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "front.example.org:29443", CertPin: r.certPin, CertPEM: r.certPEM,
			FrpcDigests: map[string]string{"x86_64": r.frpc}},
		PollInterval: time.Second, BusyInterval: 500 * time.Millisecond, CreationGracePeriod: -1,
		TypedRecorder: r.typed,
	}
}

// newTypedRecorder is the Provider's typed recorder as the controller binary
// builds it: client-go's events.k8s.io recorder writing to the API server, with
// reportingController hpcburst-provider, wrapped by the counting wrapper.
func (r *evRig) newTypedRecorder() eventcount.IDRecorder {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	b := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})
	stop := make(chan struct{})
	b.StartRecordingToSink(stop)
	r.t.Cleanup(func() { close(stop); b.Shutdown() })
	return eventcount.Events(b.NewRecorder(scheme(), v1alpha1.FieldManagerProvider), r.counter, eventcount.ControllerProvider(r.prov))
}

// newAlloc creates an Allocation with the time limit and the credential the
// lifecycle controller would write.
func (r *evRig) newAlloc(name, limit string) *v1alpha1.Allocation {
	r.t.Helper()
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.ns},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: r.prov},
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
				Partition: rowPart, TimeLimit: v1alpha1.Duration(limit), GPUs: 1, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), AgentDigest: r.digest,
			}},
		},
	}
	if err := c.Create(r.ctx, a); err != nil {
		r.t.Fatal(err)
	}
	p := agentapi.AllocationCredentialPath(r.credDir, string(a.UID))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		r.t.Fatal(err)
	}
	cred := sha256.Sum256([]byte(a.UID))
	if err := os.WriteFile(p, []byte(hex.EncodeToString(cred[:])), 0o600); err != nil {
		r.t.Fatal(err)
	}
	return a
}

// get reads the Allocation from the API server; gone reports a deleted one.
func (r *evRig) get(name string) (a *v1alpha1.Allocation, gone bool) {
	r.t.Helper()
	a = &v1alpha1.Allocation{}
	err := c.Get(r.ctx, client.ObjectKey{Namespace: r.ns, Name: name}, a)
	if client.IgnoreNotFound(err) != nil {
		r.t.Fatal(err)
	}
	return a, err != nil
}

// pass moves the clock one second and reconciles the Allocation once: the
// second makes the Batch's cached queue listing stale, so a pass reads what the
// fake Slurm holds now. The returned error is the reconcile's, which a test
// that does not care about it ignores.
func (r *evRig) pass(name string) error {
	r.clock.Advance(time.Second)
	_, err := r.ctl.Reconciler.Reconcile(r.ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: r.ns, Name: name}})
	return err
}

func (r *evRig) passes(name string, n int) {
	for i := 0; i < n; i++ {
		_ = r.pass(name)
	}
}

// until runs passes until cond holds, at most max of them.
func (r *evRig) until(name, what string, max int, cond func(*v1alpha1.Allocation) bool) *v1alpha1.Allocation {
	r.t.Helper()
	for i := 0; i < max; i++ {
		_ = r.pass(name)
		if a, gone := r.get(name); !gone && cond(a) {
			return a
		}
	}
	a, _ := r.get(name)
	r.t.Fatalf("after %d passes, not %s: phase %q reason %q", max, what, a.Status.AtProvider.Phase, a.Status.AtProvider.Reason)
	return nil
}

// jobID is the recorded job's ID as the fake Slurm numbers it.
func (r *evRig) jobID(a *v1alpha1.Allocation) int {
	r.t.Helper()
	var id int
	if _, err := fmt.Sscanf(a.Annotations[v1alpha1.AnnotationJobID], "%d", &id); err != nil || id == 0 {
		r.t.Fatalf("the Allocation records no job ID: %q", a.Annotations[v1alpha1.AnnotationJobID])
	}
	return id
}

// ---- the Events the API holds ----

// apiEvents lists the typed events (action set) the API holds about the
// Allocation, reported by the Provider controller.
func (r *evRig) apiEvents(a *v1alpha1.Allocation) []eventsv1.Event {
	r.t.Helper()
	var list eventsv1.EventList
	if err := c.List(r.ctx, &list, client.InNamespace(r.ns)); err != nil {
		r.t.Fatal(err)
	}
	var out []eventsv1.Event
	for _, e := range list.Items {
		if e.Regarding.UID == a.UID && e.Action != "" && e.ReportingController == v1alpha1.FieldManagerProvider {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].CreationTimestamp.Before(&out[k].CreationTimestamp) })
	return out
}

// want is an event the API must hold at least one of: the type in action, and
// the reason.
type want struct{ action, reason string }

// waitEvents waits until the API holds at least one event of each wanted
// action and reason regarding the Allocation: the recorder delivers
// asynchronously, and merges isomorphic events, so a test never asks for a
// count here.
func (r *evRig) waitEvents(a *v1alpha1.Allocation, wants ...want) []eventsv1.Event {
	r.t.Helper()
	var got []eventsv1.Event
	missing := func() []want {
		got = r.apiEvents(a)
		var m []want
		for _, w := range wants {
			found := false
			for _, e := range got {
				if e.Action == w.action && e.Reason == w.reason {
					found = true
				}
			}
			if !found {
				m = append(m, w)
			}
		}
		return m
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if len(missing()) == 0 {
			return got
		}
		time.Sleep(100 * time.Millisecond)
	}
	var have []string
	for _, e := range got {
		have = append(have, e.Action+"/"+e.Reason)
	}
	r.t.Fatalf("the API lacks %v after 30 s; it holds %v", missing(), have)
	return nil
}

// noteOf returns the notes of the API's events of the reason.
func notesOf(evs []eventsv1.Event, reason string) []string {
	var out []string
	for _, e := range evs {
		if e.Reason == reason {
			out = append(out, e.Note)
		}
	}
	return out
}

// tokenIn reports whether s holds token as a whole token: not inside a longer
// run of letters, digits or underscores (a time stamp, a count, a path). A
// substring match would also find the digits of a job ID inside any longer
// number.
func tokenIn(s, token string) bool {
	for from := 0; ; {
		i := strings.Index(s[from:], token)
		if i < 0 {
			return false
		}
		i += from
		end := i + len(token)
		if (i == 0 || !isWord(s[i-1])) && (end == len(s) || !isWord(s[end])) {
			return true
		}
		from = i + 1
	}
}

func isWord(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// checkNotes fails the test when a typed event of the API names the job ID as a
// token, or the Provider's account or grant: the notes go to the durable log,
// which is kept and may be exported (HBEP-8, Allocation events, No job ID in a
// note). The job ID is the fake's synthetic value, which no other field holds.
func (r *evRig) checkNotes(evs []eventsv1.Event, jobID int) {
	r.t.Helper()
	token := fmt.Sprint(jobID)
	for _, e := range evs {
		for _, text := range []string{e.Note, e.Reason, e.Action} {
			for _, bad := range []string{token, rowAccount, rowGrant, rowUser, rowHome, rowCredsPath} {
				if tokenIn(text, bad) {
					r.t.Errorf("the %s event's text %q names %q", e.Reason, text, bad)
				}
			}
			if m := pathToken.FindString(text); m != "" {
				r.t.Errorf("the %s event's text %q holds the path-looking token %q", e.Reason, text, m)
			}
		}
	}
}

// pathToken finds a token that starts with a slash, as a path does, where it
// begins a word: an object's namespace/name pair does not.
var pathToken = regexp.MustCompile(`(^|[\s("'])/[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*`)

// ---- hpcburst_events_raised_total ----

// key is the counter's series of a typed reason under this Provider.
func (r *evRig) key(action, reason, typ string) eventcount.Key {
	return eventcount.Key{Controller: eventcount.ControllerProvider(r.prov), Reason: reason, Action: action, Type: typ}
}

// read is one read of the counter's /metrics text, for the series: the counter
// and the process collector are gathered and parsed as a scrape would be.
func (r *evRig) read(k eventcount.Key) eventcount.Read {
	r.t.Helper()
	sent := time.Now()
	fams, err := r.reg.Gather()
	if err != nil {
		r.t.Fatal(err)
	}
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, f := range fams {
		if err := enc.Encode(f); err != nil {
			r.t.Fatal(err)
		}
	}
	rd, err := eventcount.ParseRead(buf.Bytes(), k)
	if err != nil {
		r.t.Fatal(err)
	}
	rd.Sent, rd.Answered = sent, time.Now()
	return rd
}

// count is the series' count; the series must exist (created at 0, or raised).
func (r *evRig) count(k eventcount.Key) float64 {
	r.t.Helper()
	rd := r.read(k)
	if !rd.HasCount {
		return 0
	}
	return rd.Count
}

// newRegistry is a registry that gathers the counter and the process collector, as
// a serving copy's /metrics does.
func newRegistry(counter *eventcount.Counter) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(counter, collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// lineFaults is a Runner over the fake Slurm session that fails the commands
// whose line starts with a prefix, as often as armed: fakeslurm's FailNext fails
// whatever command comes next, and a row needs one kind of command to fail
// while the others run (a lift that does not take).
type lineFaults struct {
	inner slurm.Runner
	mu    sync.Mutex
	rules []*lineRule
}

type lineRule struct {
	prefix string
	left   int // < 0: every time
	err    error
}

// fail makes the next n commands starting with prefix fail with err without
// running; n < 0 fails all of them until clear.
func (f *lineFaults) fail(prefix string, n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = append(f.rules, &lineRule{prefix: prefix, left: n, err: err})
}

func (f *lineFaults) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = nil
}

func (f *lineFaults) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	f.mu.Lock()
	for _, rule := range f.rules {
		if rule.left != 0 && strings.HasPrefix(cmd.Line, rule.prefix) {
			if rule.left > 0 {
				rule.left--
			}
			f.mu.Unlock()
			return slurm.Result{}, rule.err
		}
	}
	f.mu.Unlock()
	return f.inner.Run(ctx, cmd)
}

func (f *lineFaults) Generation() uint64 { return f.inner.Generation() }
