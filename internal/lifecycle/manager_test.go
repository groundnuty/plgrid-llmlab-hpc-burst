package lifecycle

// The manager on a real API server (internal/testenv's assembled host): the
// Lease, the keyed work queue writing status, identity records as
// ControllerRevisions, the health endpoints, and the server-side apply of
// DialHomeUnreachable beside the Provider controller's own status writes. On a
// machine that runs several envtest suites at once, serialise them with a lock
// of your own:
//
//	flock -o <lock file> go test ./internal/lifecycle/ -run TestManagerOnControlPlane
//
// It uses #61's envtest block, 29204-29207, unless HPCBURST_TESTENV_PORT_BASE
// is set, so it does not collide with the other packages' blocks when go test
// runs packages in parallel; the health probe binds 127.0.0.1:29301 and the
// metrics server 127.0.0.1:29302 (#61's block).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-logr/logr"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

const (
	managerHealthAddr = "127.0.0.1:29301"
	// managerMetricsAddr is the metrics server's address in this test, in
	// #61's test block 29300-29319: controller-runtime's metrics server does
	// not report a kernel-chosen port.
	managerMetricsAddr = "127.0.0.1:29302"
	// managerPortBase is the first port of #61's envtest block, 29204-29207.
	// One control plane takes four loopback ports (etcd client, etcd peer,
	// kube-apiserver, kube-controller-manager) inside issue #40's range
	// 29200-29219 (internal/testenv), and two control planes cannot share a
	// block. go test runs packages in parallel, so each package that starts one
	// gets a block of its own; this one is #61's. It is used when
	// HPCBURST_TESTENV_PORT_BASE is unset.
	managerPortBase = "29204"
)

func installTestCRDs(ctx context.Context, cfg *rest.Config) error {
	s, err := Scheme()
	if err != nil {
		return err
	}
	if err := apiextensionsv1.AddToScheme(s); err != nil {
		return err
	}
	cl, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		return err
	}
	files, err := filepath.Glob(filepath.Join("..", "..", "config", "crd", "*.yaml"))
	if err != nil || len(files) != 5 {
		return fmt.Errorf("want the 5 committed CRDs in config/crd, found %d (%v)", len(files), err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(crd); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		if err := cl.Create(ctx, crd); err != nil {
			return fmt.Errorf("create %s: %w", crd.Name, err)
		}
	}
	return nil
}

func eventually(t *testing.T, limit time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(limit)
	last := ""
	for {
		ok, msg := cond()
		if ok {
			return
		}
		last = msg
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s; last: %s", what, limit, last)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// capturedEvent is one call of client-go's events recorder.
type capturedEvent struct{ eventType, reason, action, note, regarding, related string }

// captureRecorder is an events.EventRecorder that keeps what KubeRecorder
// hands client-go.
type captureRecorder struct{ got []capturedEvent }

func (c *captureRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventType, reason, action, note string, args ...any) {
	name := func(o runtime.Object) string {
		if o == nil {
			return ""
		}
		m, err := apimeta.Accessor(o)
		if err != nil {
			return "?"
		}
		return m.GetName()
	}
	c.got = append(c.got, capturedEvent{eventType, reason, action, fmt.Sprintf(note, args...), name(regarding), name(related)})
}

// TestKubeRecorderReasonIsTheCodeAlone: a Kubernetes Event's reason is the
// single CamelCase reason code, and HBEP-8's type is the events.k8s.io action;
// the "<type>/<Code>" form is gone (HBEP-7, Events and conditions). The type
// alone decides the severity: degraded and failing are Warning, ending-soon
// and the others Normal (HBEP-8). A related object is passed on. The
// expectations are literals.
func TestKubeRecorderReasonIsTheCodeAlone(t *testing.T) {
	c := &captureRecorder{}
	k := KubeRecorder{R: c}
	md := mdSpec("md")
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "md-abcde", Namespace: md.Namespace}}
	k.Event(md, nil, "degraded", "IdentityUnresolved", "class has no deployment ID")
	k.Event(in, nil, "ending-soon", "NoReplacementReady", "less than an hour left")
	k.Event(in, nil, "failing", "WorkerRestart", "no progress")
	k.Event(in, nil, "ready", "Ready", "routed")
	k.Event(md, in, "degraded", "CapacityDropped", "drain start reached with no replacement ready")
	want := []capturedEvent{
		{"Warning", "IdentityUnresolved", "degraded", "class has no deployment ID", "md", ""},
		{"Normal", "NoReplacementReady", "ending-soon", "less than an hour left", "md-abcde", ""},
		{"Warning", "WorkerRestart", "failing", "no progress", "md-abcde", ""},
		{"Normal", "Ready", "ready", "routed", "md-abcde", ""},
		{"Warning", "CapacityDropped", "degraded", "drain start reached with no replacement ready", "md", "md-abcde"},
	}
	if len(c.got) != len(want) {
		t.Fatalf("recorded %+v, want %+v", c.got, want)
	}
	for i := range want {
		if c.got[i] != want[i] {
			t.Errorf("event %d = %+v, want %+v", i, c.got[i], want[i])
		}
	}
}

// TestEventNoteIsCutAtAUTF8Boundary: the events API refuses a note above 1,024
// bytes, so a longer message is cut to at most 1,024 bytes, never inside a
// character. The message is 1,023 ASCII bytes and then two-byte characters, so
// a cut at byte 1,024 would split the first of them.
func TestEventNoteIsCutAtAUTF8Boundary(t *testing.T) {
	msg := strings.Repeat("a", 1023) + strings.Repeat("\u00e9", 100)
	c := &captureRecorder{}
	KubeRecorder{R: c}.Event(mdSpec("md"), nil, "failing", "WorkerRestart", msg)
	got := c.got[0].note
	if len(got) != 1023 || !utf8.ValidString(got) || !strings.HasPrefix(msg, got) {
		t.Fatalf("note of %d bytes, valid UTF-8 %v; want the 1,023-byte prefix", len(got), utf8.ValidString(got))
	}
	if short := "short note"; eventNote(short) != short {
		t.Fatalf("a short note was changed to %q", eventNote(short))
	}
	exact := strings.Repeat("b", 1024)
	if eventNote(exact) != exact {
		t.Fatal("a note of exactly 1,024 bytes was cut")
	}
}

func TestManagerOnControlPlane(t *testing.T) {
	if os.Getenv("HPCBURST_TESTENV_PORT_BASE") == "" {
		t.Setenv("HPCBURST_TESTENV_PORT_BASE", managerPortBase)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	// The manager must lead through the gapLock over leaderlock's lock, whose
	// release is retried (issue #131): the lock newLeaseLock returns counts
	// the elector's writes, and a manager that led through controller-runtime's
	// own lock would leave it at none (PR #159 review).
	lead := &countingLeaseLock{}
	realLock := newLeaseLock
	t.Cleanup(func() { newLeaseLock = realLock })
	newLeaseLock = func(cfg *rest.Config, ns, name string, renew time.Duration, log logr.Logger) (resourcelock.Interface, error) {
		inner, err := realLock(cfg, ns, name, renew, log)
		if err != nil {
			return nil, err
		}
		if !leaderlock.RetriesRelease(inner) {
			t.Errorf("leaderlock.New returned a %T, not its retrying lock", inner)
		}
		lead.Interface = inner
		return lead, nil
	}
	cp, err := testenv.Start(ctx, t.TempDir(), testenv.Options{BeforeControllerManager: installTestCRDs})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	defer func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	}()
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
	// Ready by v1alpha1.ProviderReady: a recent successful read as well.
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
	// A second deployment whose identity record name is taken by another ID:
	// its degraded event with reason IdentityUnresolved must reach the API
	// server as an events.k8s.io Event, reason the code alone and action the
	// HBEP-8 type (a rejected Event would be dropped silently).
	collide := mdSpec("md-collide")
	collide.Generation = 0
	collide.Spec.Source.Served.Flags = append(collide.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: "dtype", Value: "bfloat16"})
	tmpl, err := deploymentid.ChatTemplateSHA256("")
	if err != nil {
		t.Fatal(err)
	}
	cid, err := deploymentid.Compute(deploymentid.ServingSpec{
		Checkpoint:         deploymentid.Checkpoint{Repository: "example-org/example-model", Revision: rigRevision},
		Engine:             deploymentid.Engine{Name: "vllm", Version: "0.32.0", ImageDigest: rigDigest},
		Flags:              []deploymentid.Flag{{Name: "max-model-len", Value: "32768"}, {Name: "dtype", Value: "bfloat16"}},
		ChatTemplateSHA256: tmpl,
	}, deploymentid.Accelerator{GPUModel: rigGPU, CPUArch: rigArch}, rigFiles)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(identityData{Scheme: "v1", SHA256: cid.SHA256[:12] + strings.Repeat("0", 52), Canonical: json.RawMessage(`{}`)})
	if err := cl.Create(ctx, &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "id-" + cid.SHA256[:12], Namespace: rigNS},
		Data:       runtime.RawExtension{Raw: data}, Revision: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(ctx, collide); err != nil {
		t.Fatalf("create ModelDeployment: %v", err)
	}

	creds := t.TempDir()
	builds := t.TempDir()
	build := []byte("a stand-in for the aarch64 agent build")
	if err := os.WriteFile(filepath.Join(builds, "hb-agent-"+rigArch), build, 0o600); err != nil {
		t.Fatal(err)
	}
	buildSum := sha256.Sum256(build)
	// A configured front, an agent build and a Ready Provider: the controller
	// creates an Instance and an Allocation, which pass the real CRDs' schema
	// and CEL rules (the --max-num-seqs=<n> rule among them), unlike the rig's
	// fake client. FrontNotConfigured is covered by the rig and by the k3s run.
	mgr, backend, err := NewManager(Options{
		Config: cp.Config, CredentialsDir: creds, AgentBuildsDir: builds, FrontConfigured: true, HealthAddr: managerHealthAddr,
		MetricsAddr: managerMetricsAddr, LeaderElection: true, Checkpoints: &stubSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	mctx, stopMgr := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	// stop cancels the manager and waits for it once: done carries one value,
	// so a second receive would block for good (issue #136's 10-minute hang).
	stopped, stopOK := false, false
	stop := func() bool {
		if !stopped {
			stopped = true
			stopOK = stopManager(t, "first", stopMgr, done)
		}
		return stopOK
	}
	defer stop()

	eventually(t, time.Minute, "the lifecycle Lease is held", func() (bool, string) {
		var l coordinationv1.Lease
		if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
			return false, err.Error()
		}
		return l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity != "", "no holder"
	})
	if lead.writes.Load() == 0 {
		t.Fatal("the lifecycle Lease is held, but no write went through the lock newLeaseLock built: the manager leads through another lock")
	}
	eventually(t, time.Minute, "the Backend serves agents", func() (bool, string) {
		return backend.serving() == nil, "not serving"
	})
	eventually(t, time.Minute, "an Instance and its Allocation pass admission", func() (bool, string) {
		var got v1alpha1.ModelDeployment
		if err := cl.Get(ctx, client.ObjectKeyFromObject(md), &got); err != nil {
			return false, err.Error()
		}
		var insts v1alpha1.InstanceList
		var allocs v1alpha1.AllocationList
		if err := cl.List(ctx, &insts, client.InNamespace(rigNS), client.MatchingLabels{v1alpha1.LabelModelDeployment: "md"}); err != nil {
			return false, err.Error()
		}
		if err := cl.List(ctx, &allocs, client.InNamespace(rigNS), client.MatchingLabels{v1alpha1.LabelModelDeployment: "md"}); err != nil {
			return false, err.Error()
		}
		if len(insts.Items) != 1 || len(allocs.Items) != 1 || insts.Items[0].Status.Phase != v1alpha1.InstancePending {
			return false, fmt.Sprintf("%d Instances (phase %v), %d Allocations; conditions %+v", len(insts.Items), phases(insts.Items), len(allocs.Items), got.Status.Conditions)
		}
		if len(got.Status.CurrentIDs) != 1 {
			return false, fmt.Sprintf("current IDs %+v", got.Status.CurrentIDs)
		}
		name, _ := v1alpha1.IdentityRecordName(string(got.Status.CurrentIDs[0].ID))
		var rec appsv1.ControllerRevision
		if err := cl.Get(ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &rec); err != nil {
			return false, "identity record: " + err.Error()
		}
		return true, ""
	})
	var insts v1alpha1.InstanceList
	if err := cl.List(ctx, &insts, client.InNamespace(rigNS), client.MatchingLabels{v1alpha1.LabelModelDeployment: "md"}); err != nil {
		t.Fatal(err)
	}
	in := insts.Items[0]
	var seqs []string
	for _, c := range in.Spec.Launch.Command {
		if strings.HasPrefix(c, "--max-num-seqs") {
			seqs = append(seqs, c)
		}
	}
	if len(seqs) != 1 || seqs[0] != "--max-num-seqs=256" || in.Spec.Launch.MaxNumSeqs != 256 {
		t.Fatalf("launch max-num-seqs arguments %v (maxNumSeqs %d), want exactly --max-num-seqs=256", seqs, in.Spec.Launch.MaxNumSeqs)
	}
	var alloc v1alpha1.Allocation
	if err := cl.Get(ctx, client.ObjectKey{Namespace: rigNS, Name: in.Spec.AllocationRef.Name}, &alloc); err != nil {
		t.Fatalf("the Instance's Allocation: %v", err)
	}
	b := alloc.Spec.ForProvider.Batch
	if b == nil || b.TimeLimit != "48h0m0s" || b.GPUs != 1 || b.CPUsPerTask != 16 || b.Memory.String() != "120Gi" || b.Nodes != 1 ||
		b.AgentDigest != hex.EncodeToString(buildSum[:]) || alloc.Spec.ProviderConfigReference.Name != rigProvider {
		t.Fatalf("forProvider.batch %+v, providerConfigRef %+v", b, alloc.Spec.ProviderConfigReference)
	}
	if _, err := os.Stat(agentapi.AllocationCredentialPath(creds, string(alloc.UID))); err != nil {
		t.Fatalf("the Allocation's credential file: %v", err)
	}
	eventually(t, time.Minute, "the collision's event reaches the API server", func() (bool, string) {
		var evs eventsv1.EventList
		if err := cl.List(ctx, &evs, client.InNamespace(rigNS)); err != nil {
			return false, err.Error()
		}
		var seen []string
		for _, e := range evs.Items {
			seen = append(seen, e.Reason+"/"+e.Action+" on "+e.Regarding.Name)
			// Literals, not the code's constants: the wire form is the contract.
			if e.Reason == "IdentityUnresolved" && e.Action == "degraded" && e.Regarding.Name == "md-collide" && e.Type == "Warning" {
				return true, ""
			}
		}
		return false, fmt.Sprintf("events: %v", seen)
	})
	// A note above the events API's 1,024 bytes is cut at a UTF-8 boundary and
	// admitted; uncut, the API server refuses it and the event is lost.
	cs, err := kubernetes.NewForConfig(cp.Config)
	if err != nil {
		t.Fatal(err)
	}
	bc := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})
	bc.StartRecordingToSink(ctx.Done())
	defer bc.Shutdown()
	long := strings.Repeat("x", 1023) + strings.Repeat("\u00e9", 1000)
	KubeRecorder{R: bc.NewRecorder(s, "hpcburst-lifecycle-test")}.Event(md, nil, "failing", "LongNote", long)
	eventually(t, time.Minute, "the long note's event is admitted", func() (bool, string) {
		var evs eventsv1.EventList
		if err := cl.List(ctx, &evs, client.InNamespace(rigNS)); err != nil {
			return false, err.Error()
		}
		for _, e := range evs.Items {
			if e.Reason == "LongNote" {
				if len(e.Note) == 1023 && utf8.ValidString(e.Note) && strings.HasPrefix(long, e.Note) {
					return true, ""
				}
				return false, fmt.Sprintf("LongNote's note has %d bytes", len(e.Note))
			}
		}
		return false, "no LongNote event"
	})
	for _, path := range []string{"/healthz", "/readyz"} {
		if code := getStatus(t, "http://"+managerHealthAddr+path); code != http.StatusOK {
			t.Fatalf("GET %s: %d", path, code)
		}
	}
	// /metrics on loopback at the configured address (HBEP-8, Metrics): the
	// pending Instance's info series, and controller-runtime's own work-queue
	// metrics beside it.
	eventually(t, time.Minute, "/metrics serves the Instance's info series", func() (bool, string) {
		body := getBody(t, "http://"+managerMetricsAddr+"/metrics")
		info := fmt.Sprintf(`instance_uid="%s"`, in.UID)
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "hpcburst_instance_info{") && strings.Contains(line, info) && strings.HasSuffix(line, " 1") &&
				strings.Contains(body, "workqueue_") {
				return true, ""
			}
		}
		return false, "no hpcburst_instance_info line for " + string(in.UID)
	})
	// A first start, with no previous Lease, raises no ObservationGap (HBEP-8,
	// Gaps after a restart). The collision's event above shows the recorder
	// reaches the API server.
	if gaps := observationGaps(t, ctx, cl); len(gaps) != 0 {
		t.Fatalf("a first start raised %d ObservationGap events", len(gaps))
	}

	// DialHomeUnreachable by server-side apply, beside the Provider
	// controller's whole-status updates.
	r := &Reconciler{Client: cl, Reader: cl, Clock: SystemClock{}}
	now := time.Now().UTC().Truncate(time.Second)
	if err := r.setDialHome(ctx, rigProvider, true, now, "an Instance could not register", time.Time{}); err != nil {
		t.Fatalf("apply DialHomeUnreachable: %v", err)
	}
	var got v1alpha1.Provider
	if err := cl.Get(ctx, client.ObjectKey{Name: rigProvider}, &got); err != nil {
		t.Fatal(err)
	}
	if !apimeta.IsStatusConditionTrue(got.Status.Conditions, v1alpha1.ConditionDialHomeUnreachable) ||
		!apimeta.IsStatusConditionTrue(got.Status.Conditions, v1alpha1.ConditionProviderReady) {
		t.Fatalf("after the apply, want DialHomeUnreachable and Ready true: %+v", got.Status.Conditions)
	}
	owned := false
	for _, mf := range got.ManagedFields {
		if mf.Manager == v1alpha1.FieldManagerLifecycle && mf.Operation == metav1.ManagedFieldsOperationApply && mf.Subresource == "status" {
			owned = true
		}
	}
	if !owned {
		t.Fatalf("no status apply entry of %s in managedFields", v1alpha1.FieldManagerLifecycle)
	}
	// The Provider's controller writes its whole status as it found it.
	got.Status.LastSuccessfulStatusRead = &metav1.Time{Time: now}
	if err := cl.Status().Update(ctx, &got, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
		t.Fatal(err)
	}
	if err := r.setDialHome(ctx, rigProvider, false, now.Add(time.Minute), "an agent registered", now.Add(time.Minute)); err != nil {
		t.Fatalf("apply DialHomeUnreachable false: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKey{Name: rigProvider}, &got); err != nil {
		t.Fatal(err)
	}
	c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDialHomeUnreachable)
	if c == nil || c.Status != metav1.ConditionFalse || !apimeta.IsStatusConditionTrue(got.Status.Conditions, v1alpha1.ConditionProviderReady) {
		t.Fatalf("after the second apply, want DialHomeUnreachable false and Ready kept: %+v", got.Status.Conditions)
	}

	if !stop() {
		t.FailNow()
	}
	var released coordinationv1.Lease
	eventually(t, 30*time.Second, "the Lease is released on stop", func() (bool, string) {
		if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &released); err != nil {
			return false, err.Error()
		}
		return released.Spec.HolderIdentity == nil || *released.Spec.HolderIdentity == "", "still held"
	})
	if released.Spec.RenewTime == nil {
		t.Fatal("the released Lease has no renewTime")
	}
	restartAfterGap(t, ctx, cl, cp.Config, creds, builds)
	withoutLeaderElection(t, ctx, cl, cp.Config, creds, builds)
}

// collisionEvents counts the IdentityUnresolved events about md-collide, which
// each controller process raises once.
func collisionEvents(t *testing.T, ctx context.Context, cl client.Client) int {
	t.Helper()
	var evs eventsv1.EventList
	if err := cl.List(ctx, &evs, client.InNamespace(rigNS)); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range evs.Items {
		if e.Reason == "IdentityUnresolved" && e.Regarding.Name == "md-collide" {
			n++
		}
	}
	return n
}

// withoutLeaderElection runs the controller with --leader-elect=false, a
// development and test mode (HBEP-8, Observers, and Test plan, Observers that
// never start): it takes no Lease, so the lifecycle Lease is not written, and
// emits no ObservationGap, while its /readyz needs only synced caches. The
// run's own collision event reaching the API shows its recorder works, so the
// unchanged ObservationGap count is not silence for want of a recorder.
func withoutLeaderElection(t *testing.T, ctx context.Context, cl client.Client, cfg *rest.Config, creds, builds string) {
	t.Helper()
	var before coordinationv1.Lease
	eventually(t, 30*time.Second, "the second run released the Lease", func() (bool, string) {
		if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &before); err != nil {
			return false, err.Error()
		}
		return before.Spec.HolderIdentity == nil || *before.Spec.HolderIdentity == "", "still held"
	})
	collisions := collisionEvents(t, ctx, cl)
	mgr, _, err := NewManager(Options{
		Config: cfg, CredentialsDir: creds, AgentBuildsDir: builds, FrontConfigured: true, HealthAddr: managerHealthAddr,
		MetricsAddr: managerMetricsAddr, LeaderElection: false, Checkpoints: &stubSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	mctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	defer stopManager(t, "no-leader-election", stop, done)
	eventually(t, time.Minute, "/readyz passes with synced caches and no Lease", func() (bool, string) {
		code := getStatus(t, "http://"+managerHealthAddr+"/readyz")
		return code == http.StatusOK, fmt.Sprintf("/readyz %d", code)
	})
	eventually(t, time.Minute, "this run's own collision event reaches the API", func() (bool, string) {
		n := collisionEvents(t, ctx, cl)
		return n > collisions, fmt.Sprintf("%d IdentityUnresolved events on md-collide, %d before this run", n, collisions)
	})
	var after coordinationv1.Lease
	if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &after); err != nil {
		t.Fatal(err)
	}
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("the lifecycle Lease changed (resourceVersion %s to %s, holder %v) under --leader-elect=false", before.ResourceVersion, after.ResourceVersion, after.Spec.HolderIdentity)
	}
	if gaps := observationGaps(t, ctx, cl); len(gaps) != 1 {
		t.Fatalf("%d ObservationGap events after a run with --leader-elect=false, want still the one of the second run", len(gaps))
	}
}

// writeLifecycleLease writes the lifecycle Lease as another copy of the
// controller would: holder, renewTime now and a lease duration. It returns the
// renewTime as stored.
func writeLifecycleLease(t *testing.T, ctx context.Context, cl client.Client, holder string, seconds int32) time.Time {
	t.Helper()
	var l coordinationv1.Lease
	if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
		t.Fatal(err)
	}
	now := metav1.NewMicroTime(time.Now())
	l.Spec.HolderIdentity, l.Spec.LeaseDurationSeconds, l.Spec.RenewTime = &holder, &seconds, &now
	if err := cl.Update(ctx, &l); err != nil {
		t.Fatal(err)
	}
	return l.Spec.RenewTime.Time
}

// restartAfterGap starts the controller again after the first run's stop
// (HBEP-8, Test plan, Readiness and Restart). A second copy holds the
// lifecycle Lease, and the new run's caches' sync is held: /readyz fails while
// the caches have not synced, and still fails once they have while the second
// copy holds the Lease; /healthz answers throughout, and no ObservationGap
// exists, since the Lease is not taken. Once the second copy releases the
// Lease, /readyz passes, and exactly one ObservationGap regards the lifecycle
// Lease, naming the release's renewTime, the last one before the take, to the
// microsecond: one event, not a series of two.
func restartAfterGap(t *testing.T, ctx context.Context, cl client.Client, cfg *rest.Config, creds, builds string) {
	t.Helper()
	const second = "lifecycle-second-copy"
	writeLifecycleLease(t, ctx, cl, second, 3600)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	leaseRead := make(chan struct{})
	readOnce := sync.OnceFunc(func() { close(leaseRead) })
	mgr, _, err := NewManager(Options{
		Config: cfg, CredentialsDir: creds, AgentBuildsDir: builds, FrontConfigured: true, HealthAddr: managerHealthAddr,
		MetricsAddr: managerMetricsAddr, LeaderElection: true, Checkpoints: &stubSource{}, onLeaseRead: readOnce,
		newCache: func(c *rest.Config, o cache.Options) (cache.Cache, error) {
			inner, err := cache.New(c, o)
			if err != nil {
				return nil, err
			}
			return heldCache{Cache: inner, gate: gate}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	defer stopManager(t, "restarted", stop, done)
	eventually(t, time.Minute, "the health server answers", func() (bool, string) {
		return getStatus(t, "http://"+managerHealthAddr+"/healthz") == http.StatusOK, "no /healthz"
	})
	// Each state below is held by the test (the gate, a Lease of 3600 s), so
	// it cannot end on its own; the checks repeat a few times to see it hold.
	notReady := func(why string) {
		t.Helper()
		for i := 0; i < 5; i++ {
			if code := getStatus(t, "http://"+managerHealthAddr+"/readyz"); code == http.StatusOK {
				t.Fatalf("/readyz answered 200 %s", why)
			}
			if code := getStatus(t, "http://"+managerHealthAddr+"/healthz"); code != http.StatusOK {
				t.Fatalf("/healthz answered %d %s; it is liveness and answers", code, why)
			}
		}
		if gaps := observationGaps(t, ctx, cl); len(gaps) != 0 {
			t.Fatalf("%d ObservationGap events %s, before the Lease could be taken", len(gaps), why)
		}
	}
	notReady("while the caches' sync is held")
	select {
	case <-leaseRead:
		t.Fatal("the elector read the Lease while the caches' sync was held; leader election starts only after it")
	default:
	}
	release()
	// The caches synced and the elector read the Lease the second copy holds.
	select {
	case <-leaseRead:
	case <-time.After(2 * time.Minute):
		t.Fatal("the elector did not read the lifecycle Lease within 2 min of the caches' release")
	}
	notReady("while a second copy holds the lifecycle Lease")
	var l coordinationv1.Lease
	if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
		t.Fatal(err)
	}
	if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != second {
		t.Fatalf("the Lease's holder is %v, want the second copy still", l.Spec.HolderIdentity)
	}
	prev := writeLifecycleLease(t, ctx, cl, "", 1) // the second copy releases it, as client-go's release does
	eventually(t, time.Minute, "/readyz passes once the caches have synced and the Lease is held", func() (bool, string) {
		code := getStatus(t, "http://"+managerHealthAddr+"/readyz")
		return code == http.StatusOK, fmt.Sprintf("/readyz %d", code)
	})
	want := prev.UTC().Format(metav1.RFC3339Micro)
	eventually(t, time.Minute, "one ObservationGap regards the lifecycle Lease", func() (bool, string) {
		gaps := observationGaps(t, ctx, cl)
		if len(gaps) != 1 {
			return false, fmt.Sprintf("%d ObservationGap events", len(gaps))
		}
		e := gaps[0]
		if e.Action != "degraded" || e.Type != "Warning" || e.Regarding.Kind != "Lease" || e.Regarding.Name != v1alpha1.LeaseLifecycle || e.Regarding.Namespace != v1alpha1.ControllerNamespace {
			t.Fatalf("ObservationGap %s/%s regarding %+v; want degraded, Warning, the Lease %s/%s", e.Action, e.Type, e.Regarding, v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)
		}
		if !strings.Contains(e.Note, want) {
			t.Fatalf("ObservationGap note %q does not name the previous renewTime %s", e.Note, want)
		}
		// This check cannot see a second emission within the test's wait: the
		// recorder turns a second isomorphic event into a series in memory,
		// and the count reaches the API only every 30 minutes or once the
		// series has been quiet for 6 minutes (HBEP-8, What the recorder
		// merges and drops). It would catch only a series that is already
		// there. The proof that a process emits one ObservationGap is
		// gap_test.go, which counts the emitter's recorder calls.
		if e.Series != nil {
			t.Fatalf("ObservationGap is a series of %d: emitted more than once", e.Series.Count)
		}
		return true, ""
	})
}

// heldCache holds its sync until gate closes.
type heldCache struct {
	cache.Cache
	gate <-chan struct{}
}

func (h heldCache) WaitForCacheSync(ctx context.Context) bool {
	select {
	case <-h.gate:
	case <-ctx.Done():
		return false
	}
	return h.Cache.WaitForCacheSync(ctx)
}

// observationGaps lists the ObservationGap events in the controllers'
// namespace, where the lifecycle Lease is.
func observationGaps(t *testing.T, ctx context.Context, cl client.Client) []eventsv1.Event {
	t.Helper()
	var evs eventsv1.EventList
	if err := cl.List(ctx, &evs, client.InNamespace(v1alpha1.ControllerNamespace)); err != nil {
		t.Fatal(err)
	}
	var out []eventsv1.Event
	for _, e := range evs.Items {
		if e.Reason == "ObservationGap" {
			out = append(out, e)
		}
	}
	return out
}

// probeClient makes the health and metrics probes. It keeps no connection
// alive: a probe's idle connection from http.DefaultClient is still on the
// manager's health server when the manager stops, and net/http's
// Server.Shutdown, which controller-runtime runs on that server, waits up to
// 5 s for a connection it still counts as new (3 of 30 stops took 5.2 to
// 6.4 s). stopManager also closes the client's idle connections before it
// cancels.
var probeClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 30 * time.Second}

func getStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := probeClient.Get(url)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func getBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := probeClient.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// managerStopLimit bounds the wait for a cancelled manager's Start to return
// with every runnable stopped: HBEP-11's hpcburst down and the supervisor's
// restart rely on a controller that stops when told (@hpc-burst-science-agent,
// 1 Nov 2026). It is below controller-runtime's 30 s graceful shutdown
// timeout, so a runnable that does not stop is caught while it still hangs,
// and far above the second or so a stop takes.
const managerStopLimit = 10 * time.Second

// stopManager cancels a manager and waits up to managerStopLimit for its
// Start to return on done. Start returns nil only once every runnable has
// stopped. A manager that does not return in time, or returns an error, is
// reported with every goroutine's stack, which names the runnable that did
// not stop. It reports with Errorf, so a deferred call after an earlier
// failure can use it too. what names the manager in the log line that reports
// how long its stop took.
func stopManager(t *testing.T, what string, cancel context.CancelFunc, done <-chan error) bool {
	t.Helper()
	probeClient.CloseIdleConnections()
	cancel()
	start := time.Now()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s manager: %v; goroutines:\n%s", what, err, goroutines())
			return false
		}
		t.Logf("the %s manager returned %s after its cancel", what, time.Since(start).Round(time.Millisecond))
		return true
	case <-time.After(managerStopLimit):
		t.Errorf("the %s manager did not return within %s of its cancel; goroutines:\n%s", what, managerStopLimit, goroutines())
		return false
	}
}

func phases(items []v1alpha1.Instance) []v1alpha1.InstancePhase {
	var out []v1alpha1.InstancePhase
	for _, in := range items {
		out = append(out, in.Status.Phase)
	}
	return out
}

// countingLeaseLock counts the writes the elector makes through a Lease lock.
type countingLeaseLock struct {
	resourcelock.Interface
	writes atomic.Int64
}

func (c *countingLeaseLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	c.writes.Add(1)
	return c.Interface.Create(ctx, ler)
}

func (c *countingLeaseLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	c.writes.Add(1)
	return c.Interface.Update(ctx, ler)
}
