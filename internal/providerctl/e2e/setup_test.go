package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The controller as #61's main will run it: the supervisor, providerctl.Setup
// in a real manager with its informer cache and leader Lease, the real
// ProfileReader over a PlatformProfile, a Provider and mode-600 credentials,
// and a real slurm.Batch over the in-process fake Slurm. The API server checks
// every status the controller writes against the CRD. Counts come from the
// fake Slurm's call log.
func TestSetupInARealManager(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key, creds := filepath.Join(dir, "key"), filepath.Join(dir, "creds")
	for path, content := range map[string]string{
		key:   "example key\n",
		creds: "HostName login.example.org\nUser example-user\nIdentityFile " + key + "\n",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	credDir, builds := filepath.Join(dir, "credentials"), filepath.Join(dir, "builds")
	for _, d := range []string{credDir, builds} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	agent := []byte("#!/bin/sh\n# an example agent build\n")
	sum := sha256.Sum256(agent)
	digest := hex.EncodeToString(sum[:])
	if err := os.WriteFile(filepath.Join(builds, agentapi.AgentBuildName("x86_64")), agent, 0o700); err != nil {
		t.Fatal(err)
	}

	gpus := int32(4)
	maxTime, window := v1alpha1.Duration("48h"), v1alpha1.Duration("5m")
	pp := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "setup-profile"}, Spec: v1alpha1.PlatformProfileSpec{
		ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42", InflightWindow: &window,
		StagingDir: "$SCRATCH/stage",
		Partitions: []v1alpha1.PartitionProfile{{Name: "example-gpu", AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: "x86_64"},
			GPUsPerNode: &gpus, MaxTime: &maxTime}},
	}}
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "setup-prov"}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: pp.Name}, Account: "example-account",
		AllowedPartitions: []string{"example-gpu"}, CredentialsRef: &v1alpha1.CredentialsReference{File: creds},
	}}
	// A generated name: the control plane runs the namespace controller, but
	// nothing deleted the namespace, so a fixed name made a second run
	// (go test -count=2) fail with AlreadyExists. The cleanup below deletes it,
	// and the namespace controller removes the Allocations in it. Create sets
	// ns.Name; the rest of the test reads it.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "setup-allocs-"}}
	for _, o := range []client.Object{pp, prov, ns} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), prov)
		_ = c.Delete(context.Background(), pp)
		_ = c.Delete(context.Background(), ns)
	})

	sl := fakeslurm.New(slurm.RealClock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: "example-gpu", GPUOnly: true}}})
	session := sl.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
	sup := &providerctl.Supervisor{
		Client: c,
		NewManager: func(p string) (manager.Manager, error) {
			return providerctl.NewProviderManager(cfg, scheme(), p, providerctl.ManagerOptions{
				LeaseDuration: 4 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: 500 * time.Millisecond,
			})
		},
		Setup: func(mgr manager.Manager, p string) error {
			return providerctl.Setup(mgr, providerctl.Options{
				Provider: p,
				Profiles: &slurm.ProfileReader{Reader: mgr.GetClient(), Home: dir},
				NewPlatform: func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
					return slurm.NewBatch(session, v.Profile, v.Settings, slurm.Options{PollInterval: time.Second, BusyInterval: 500 * time.Millisecond}), io.NopCloser(nil), nil
				},
				CredentialsDir: credDir, AgentBuildsDir: builds,
				Front: &providerctl.FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "front.example.org:29443", CertPin: strings.Repeat("c", 64),
					FrpcDigests: map[string]string{"x86_64": strings.Repeat("d", 64)}},
				PollInterval: time.Second, BusyInterval: 500 * time.Millisecond, CreationGracePeriod: -1,
			})
		},
		Interval: 200 * time.Millisecond, MinBackoff: 300 * time.Millisecond, MaxBackoff: 2 * time.Second,
	}
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- sup.Start(sctx) }()
	defer func() {
		cancel()
		<-done
	}()

	newAlloc := func(name, provider string) *v1alpha1.Allocation {
		t.Helper()
		a := &v1alpha1.Allocation{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name},
			Spec: v1alpha1.AllocationSpec{
				ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: provider},
				ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
					Partition: "example-gpu", TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), AgentDigest: digest,
				}},
			},
		}
		if err := c.Create(ctx, a); err != nil {
			t.Fatal(err)
		}
		// The lifecycle controller writes the credential once the UID exists.
		p := agentapi.AllocationCredentialPath(credDir, string(a.UID))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		cred := sha256.Sum256([]byte(a.UID))
		if err := os.WriteFile(p, []byte(hex.EncodeToString(cred[:])), 0o600); err != nil {
			t.Fatal(err)
		}
		return a
	}
	get := func(name string) (*v1alpha1.Allocation, bool) {
		a := &v1alpha1.Allocation{}
		err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: name}, a)
		if apierrors.IsNotFound(err) {
			return nil, false
		}
		if err != nil {
			t.Fatal(err)
		}
		return a, true
	}

	ghost := newAlloc("for-another-provider", "setup-absent")
	a := newAlloc("alloc-a", prov.Name)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	eventually(t, "the Allocation's job is submitted, lifted and recorded", 60*time.Second, func() bool {
		a, _ = get("alloc-a")
		return a.Annotations[v1alpha1.AnnotationHoldLifted] != "" && a.Status.AtProvider.Phase == v1alpha1.AllocationPending
	})
	if got := meta.GetExternalName(a); got != tag {
		t.Errorf("external name %q, want the tag %q", got, tag)
	}
	if !controllerHas(a.Finalizers, v1alpha1.FinalizerCancelAll) {
		t.Errorf("finalizers %v lack %s", a.Finalizers, v1alpha1.FinalizerCancelAll)
	}
	if n, m := sl.Count("sbatch"), sl.Count("scontrol"); n != 1 || m != 1 {
		t.Errorf("%d sbatch and %d scontrol calls, want 1 and 1", n, m)
	}
	id, _ := strconv.Atoi(a.Annotations[v1alpha1.AnnotationJobID])
	if err := sl.Start(id); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the job is observed Running with its queue wait recorded", 60*time.Second, func() bool {
		a, _ = get("alloc-a")
		return a.Status.AtProvider.Phase == v1alpha1.AllocationRunning && a.Status.AtProvider.QueueWaitRecorded
	})
	if a.Status.Retention == nil || a.Status.Retention.EffectiveMode != v1alpha1.RetentionKeep {
		t.Errorf("status.retention %+v, want Keep", a.Status.Retention)
	}
	if !managedBy(a.ManagedFields, v1alpha1.FieldManagerProvider, "status") || !managedBy(a.ManagedFields, v1alpha1.FieldManagerProvider, "") {
		t.Errorf("managedFields do not show %s writing the status and the metadata: %v", v1alpha1.FieldManagerProvider, managers(a.ManagedFields))
	}
	var p v1alpha1.Provider
	eventually(t, "the Provider is Ready with its queue wait", 30*time.Second, func() bool {
		if err := c.Get(ctx, client.ObjectKey{Name: prov.Name}, &p); err != nil {
			return false
		}
		return v1alpha1.ProviderReady(&p, time.Now()) && len(p.Status.QueueWaits) == 1
	})
	if c := apimeta.FindStatusCondition(p.Status.Conditions, v1alpha1.ConditionProviderReady); c == nil || c.Reason != v1alpha1.ReasonPolled {
		t.Errorf("Ready %+v, want reason Polled", c)
	}
	if !managedBy(p.ManagedFields, v1alpha1.FieldManagerProvider, "status") {
		t.Errorf("the Provider's managedFields do not show %s writing its status: %v", v1alpha1.FieldManagerProvider, managers(p.ManagedFields))
	}

	// Another Provider's Allocation: this controller never acts on it.
	ghostTag := v1alpha1.JobTag(ghost.Namespace, ghost.Name, ghost.UID)
	for _, call := range sl.Calls() {
		if strings.Contains(strings.Join(call.Words, " "), ghostTag) {
			t.Errorf("a call names another Provider's Allocation: %v", call.Words)
		}
	}
	if g, _ := get(ghost.Name); len(g.Finalizers) != 0 || g.Annotations[meta.AnnotationKeyExternalName] != "" {
		t.Errorf("another Provider's Allocation was touched: finalizers %v, annotations %v", g.Finalizers, g.Annotations)
	}

	// The running job's allocated CPUs and GPUs come from the one accounting
	// read; the API server has checked the block against the CRD.
	eventually(t, "the running job's accounting is in the status", 60*time.Second, func() bool {
		a, _ = get("alloc-a")
		acc := a.Status.AtProvider.Accounting
		return acc != nil && acc.AllocatedCPUs != nil && acc.AllocatedGPUs != nil && acc.ElapsedSeconds != nil
	})
	if acc := a.Status.AtProvider.Accounting; *acc.AllocatedCPUs != 8 || *acc.AllocatedGPUs != 1 || *acc.ElapsedSeconds < 0 || *acc.ElapsedSeconds > 120 {
		t.Errorf("status.atProvider.accounting = cpus %d gpus %d elapsed %d s, want 8, 1 and a few seconds", *acc.AllocatedCPUs, *acc.AllocatedGPUs, *acc.ElapsedSeconds)
	}
	if n := countSacctJob(sl, id); n != 1 {
		t.Errorf("%d accounting reads of the running job, want the one for its allocated CPUs and GPUs", n)
	}
	// A second Allocation whose record already exists under acct-<UID>: the
	// controller must leave that record as it is. Its job is not started.
	b := newAlloc("alloc-b", prov.Name)
	eventually(t, "the second Allocation's job is recorded", 60*time.Second, func() bool {
		b, _ = get("alloc-b")
		return b.Annotations[v1alpha1.AnnotationJobID] != ""
	})
	earlier := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "acct-" + string(b.UID), Namespace: ns.Name},
		Data:       runtime.RawExtension{Raw: []byte(`{"basis":"allocated","marker":"written earlier"}`)},
		Revision:   1,
	}
	if err := c.Create(ctx, earlier); err != nil {
		t.Fatal(err)
	}

	// Deleting cancels the job, and then the finalizer goes.
	if err := c.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the deleted Allocation is gone", 60*time.Second, func() bool {
		_, ok := get("alloc-a")
		return !ok
	})
	// The accounting record outlives the Allocation: written once, with no
	// owner, holding the job's allocation and its times.
	var rec appsv1.ControllerRevision
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "acct-" + string(a.UID)}, &rec); err != nil {
		t.Fatalf("no accounting record after the Allocation is gone: %v", err)
	}
	if len(rec.OwnerReferences) != 0 || rec.Labels[v1alpha1.LabelRecord] != v1alpha1.RecordAccounting || rec.Labels[v1alpha1.LabelProvider] != prov.Name {
		t.Errorf("record owners %v labels %v, want no owner and record=accounting, provider=%s", rec.OwnerReferences, rec.Labels, prov.Name)
	}
	var data v1alpha1.AccountingRecordData
	if err := json.Unmarshal(rec.Data.Raw, &data); err != nil {
		t.Fatal(err)
	}
	if data.Provider != prov.Name || data.Partition != "example-gpu" || data.Tag != tag || !reflect.DeepEqual(data.JobIDs, []string{strconv.Itoa(id)}) ||
		data.Basis != "allocated" || len(data.Unknown) != 0 {
		t.Errorf("record data %s: want provider %s, partition example-gpu, tag %s, job %d, basis allocated and nothing unknown", rec.Data.Raw, prov.Name, tag, id)
	}
	if data.AllocatedCPUs == nil || *data.AllocatedCPUs != 8 || data.AllocatedGPUs == nil || *data.AllocatedGPUs != 1 ||
		data.ElapsedSeconds == nil || *data.ElapsedSeconds < 0 || *data.ElapsedSeconds > 120 || data.StartTime == nil || data.EndTime == nil {
		t.Errorf("record data %s: want 8 CPUs, 1 GPU, a few seconds elapsed, a start and an end", rec.Data.Raw)
	}
	if n := sl.Count("scancel"); n != 1 {
		t.Errorf("%d scancel calls, want 1", n)
	}
	if j, ok := sl.Job(id); !ok || j.State != "CANCELLED" {
		t.Errorf("job %d ended as %q, want CANCELLED", id, j.State)
	}

	// The record that existed before is not rewritten: same data, and the same
	// resourceVersion, which a write of any kind would move. The API server
	// refuses an update of a ControllerRevision's data, so only a delete and a
	// create could rewrite it; either moves the resourceVersion.
	if err := c.Delete(ctx, b); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the second Allocation is gone", 60*time.Second, func() bool {
		_, ok := get("alloc-b")
		return !ok
	})
	var kept appsv1.ControllerRevision
	if err := c.Get(ctx, client.ObjectKeyFromObject(earlier), &kept); err != nil {
		t.Fatal(err)
	}
	if string(kept.Data.Raw) != string(earlier.Data.Raw) || kept.ResourceVersion != earlier.ResourceVersion {
		t.Errorf("the existing record was rewritten: data %s, resourceVersion %s -> %s", kept.Data.Raw, earlier.ResourceVersion, kept.ResourceVersion)
	}
}

// countSacctJob counts the accounting reads of one job by its ID.
func countSacctJob(sl *fakeslurm.Platform, id int) int {
	n := 0
	for _, call := range sl.Calls() {
		if len(call.Words) == 0 || call.Words[0] != "sacct" {
			continue
		}
		for i, w := range call.Words {
			if w == "-j" && i+1 < len(call.Words) && call.Words[i+1] == strconv.Itoa(id) {
				n++
			}
		}
	}
	return n
}

func controllerHas(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// managedBy reports whether manager owns fields of the subresource ("" for
// the object itself).
func managedBy(fields []metav1.ManagedFieldsEntry, manager, subresource string) bool {
	for _, f := range fields {
		if f.Manager == manager && f.Subresource == subresource {
			return true
		}
	}
	return false
}

func managers(fields []metav1.ManagedFieldsEntry) []string {
	var out []string
	for _, f := range fields {
		out = append(out, f.Manager+"/"+f.Subresource)
	}
	return out
}
