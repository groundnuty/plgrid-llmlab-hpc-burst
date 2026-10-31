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
// runs packages in parallel; the health probe binds 127.0.0.1:29301 (#61's
// block).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

const (
	managerHealthAddr = "127.0.0.1:29301"
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
		{corev1.EventTypeWarning, "IdentityUnresolved", "degraded", "class has no deployment ID", "md", ""},
		{corev1.EventTypeNormal, "NoReplacementReady", "ending-soon", "less than an hour left", "md-abcde", ""},
		{corev1.EventTypeWarning, "WorkerRestart", "failing", "no progress", "md-abcde", ""},
		{corev1.EventTypeNormal, "Ready", "ready", "routed", "md-abcde", ""},
		{corev1.EventTypeWarning, "CapacityDropped", "degraded", "drain start reached with no replacement ready", "md", "md-abcde"},
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
		LeaderElection: true, Checkpoints: &stubSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	mctx, stopMgr := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	stopped := false
	defer func() {
		if !stopped {
			stopMgr()
			<-done
		}
	}()

	eventually(t, time.Minute, "the lifecycle Lease is held", func() (bool, string) {
		var l coordinationv1.Lease
		if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
			return false, err.Error()
		}
		return l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity != "", "no holder"
	})
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
			if e.Reason == "IdentityUnresolved" && e.Action == "degraded" && e.Regarding.Name == "md-collide" && e.Type == corev1.EventTypeWarning {
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
		resp, err := http.Get("http://" + managerHealthAddr + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: %d", path, resp.StatusCode)
		}
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

	stopMgr()
	if err := <-done; err != nil {
		t.Fatalf("manager: %v", err)
	}
	stopped = true
	eventually(t, 30*time.Second, "the Lease is released on stop", func() (bool, string) {
		var l coordinationv1.Lease
		if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
			return false, err.Error()
		}
		return l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "", "still held"
	})
}

func phases(items []v1alpha1.Instance) []v1alpha1.InstancePhase {
	var out []v1alpha1.InstancePhase
	for _, in := range items {
		out = append(out, in.Status.Phase)
	}
	return out
}
