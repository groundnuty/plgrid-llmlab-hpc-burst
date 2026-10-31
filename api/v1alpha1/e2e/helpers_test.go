package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Fixtures. The commit ID and the deployment ID are examples; the deployment ID
// is the golden vector of HBEP-3's worked example.
const (
	commit40 = "0123456789abcdef0123456789abcdef01234567"
	fullID   = "0e1dd69efe8a57c3eadd5e0293fd8c0f0cc185629e79297b6530de361b887852"
)

var digest = "sha256:" + strings.Repeat("2", 64)

// agentDigest is an allocation agent build's SHA-256, bare hex.
var agentDigest = strings.Repeat("3", 64)

// validBatch returns a batch job's submission parameters that admission accepts.
func validBatch() *v1alpha1.BatchParameters {
	return &v1alpha1.BatchParameters{
		Partition: "example-gpu", TimeLimit: "1h", GPUs: 1, CPUsPerTask: 16,
		Memory: resource.MustParse("120Gi"), AgentDigest: agentDigest,
	}
}

// validWorkload returns a workload with only the required fields.
func validWorkload() *v1alpha1.Workload {
	return &v1alpha1.Workload{
		ContextTokens:  v1alpha1.ContextTokens{Typical: 8192},
		OutputTokens:   v1alpha1.OutputTokens{Typical: 1024},
		TargetSessions: 8,
	}
}

// validLaunch returns an Instance's launch that admission accepts.
func validLaunch() v1alpha1.Launch {
	return v1alpha1.Launch{
		Image: "docker.io/vllm/vllm-openai", ImageDigest: digest,
		Command:    []string{"vllm", "serve", "/hpcburst/model", "--uds", "/hpcburst/run/worker-0.sock", "--dtype=bfloat16", "--max-num-seqs=256"},
		Checkpoint: v1alpha1.LaunchCheckpoint{Repository: "example-org/example-1b", Revision: commit40},
		GPUs:       1,
		MaxNumSeqs: 256,
	}
}

// installCRDs applies every CRD manifest under config/crd through the client,
// waits until each is established, and waits until its resource is served.
func installCRDs(ctx context.Context, cl client.Client) error {
	files, err := filepath.Glob(filepath.Join(configDir("crd"), "*.yaml"))
	if err != nil {
		return err
	}
	if len(files) != 5 {
		return fmt.Errorf("want 5 CRD manifests in config/crd, found %d", len(files))
	}
	sort.Strings(files)
	var crds []*apiextensionsv1.CustomResourceDefinition
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(crd); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		existing := &apiextensionsv1.CustomResourceDefinition{}
		switch err := cl.Get(ctx, client.ObjectKeyFromObject(crd), existing); {
		case apierrors.IsNotFound(err):
			err = cl.Create(ctx, crd)
			if err != nil {
				return fmt.Errorf("create %s: %w", crd.Name, err)
			}
		case err != nil:
			return err
		default:
			crd.ResourceVersion = existing.ResourceVersion
			if err := cl.Update(ctx, crd); err != nil {
				return fmt.Errorf("update %s: %w", crd.Name, err)
			}
		}
		crds = append(crds, crd)
	}
	return wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		for _, want := range crds {
			got := &apiextensionsv1.CustomResourceDefinition{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(want), got); err != nil {
				return false, nil
			}
			established := false
			for _, cond := range got.Status.Conditions {
				if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
					established = true
				}
			}
			if !established {
				return false, nil
			}
		}
		// Established is not served: list each kind until the API answers.
		lists := []client.ObjectList{&v1alpha1.ProviderList{}, &v1alpha1.PlatformProfileList{},
			&v1alpha1.ModelDeploymentList{}, &v1alpha1.AllocationList{}, &v1alpha1.InstanceList{}}
		for _, l := range lists {
			if err := cl.List(ctx, l); err != nil {
				return false, nil
			}
		}
		return true, nil
	})
}

var nameSeq = rand.Uint32N(1 << 20)

// uniq returns a name with a unique suffix, safe for a DNS-1123 label.
func uniq(prefix string) string {
	nameSeq++
	return fmt.Sprintf("%s-%05x", prefix, nameSeq)
}

func bg() context.Context { return context.Background() }

// newNamespace creates a namespace for one test and deletes it afterwards.
func newNamespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: uniq("hb")}}
	must(t, c.Create(bg(), ns), "create namespace")
	t.Cleanup(func() { _ = c.Delete(bg(), ns) })
	return ns.Name
}

// cleanup deletes obj when the test ends. The object may already be gone.
func cleanup(t *testing.T, obj client.Object) {
	t.Helper()
	t.Cleanup(func() {
		if err := c.Delete(bg(), obj); err != nil && !apierrors.IsNotFound(err) {
			t.Logf("cleanup %T %s: %v", obj, obj.GetName(), err)
		}
	})
}

func must(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// requireRefused fails unless err is an Invalid (HTTP 422) refusal whose text
// contains every want. An error of another kind, or none, fails the test: a
// refusal test that passes on any error proves nothing about the rule.
func requireRefused(t *testing.T, err error, want ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the object was accepted; want a refusal containing %q", want)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("want an Invalid (422) refusal, got %T: %v", err, err)
	}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Fatalf("the refusal does not mention %q: %v", w, err)
		}
	}
}

// eventually polls fn every 200 ms until it reports true.
func eventually(t *testing.T, timeout time.Duration, what string, fn func() (bool, error)) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(bg(), 200*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		ok, e := fn()
		last = e
		return ok, nil
	})
	if err != nil {
		t.Fatalf("timed out after %s waiting for %s (last error: %v)", timeout, what, last)
	}
}

// consistently fails the test if fn errors at any poll during d.
func consistently(t *testing.T, d time.Duration, what string, fn func() error) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if err := fn(); err != nil {
			t.Fatalf("%s did not hold: %v", what, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// goneByUID reports whether the object with this UID no longer exists. A new
// object of the same name does not count as the old one.
func goneByUID(obj client.Object, uid types.UID) (bool, error) {
	got := obj.DeepCopyObject().(client.Object)
	err := c.Get(bg(), client.ObjectKeyFromObject(obj), got)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return got.GetUID() != uid, nil
}

func ownerRef(owner client.Object, kind string) metav1.OwnerReference {
	t := true
	return metav1.OwnerReference{
		APIVersion: v1alpha1.GroupVersion.String(), Kind: kind, Name: owner.GetName(), UID: owner.GetUID(),
		Controller: &t, BlockOwnerDeletion: &t,
	}
}

// ---- valid base objects: every refusal test changes one thing in one of these ----

func newProvider(name string) *v1alpha1.Provider {
	return &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
			v1alpha1.LabelOrganization: "example-center", v1alpha1.LabelPlatform: "example-platform"}},
		Spec: v1alpha1.ProviderSpec{
			PlatformProfileRef: v1alpha1.ObjectReference{Name: "example-platform"},
			Account:            "example-account",
			AllowedPartitions:  []string{"example-gpu"},
			CredentialsRef:     &v1alpha1.CredentialsReference{File: "/run/hpcburst/example-credentials"},
		},
	}
}

func newProfile(name string) *v1alpha1.PlatformProfile {
	return &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.PlatformProfileSpec{
			ProviderKind: v1alpha1.PlatformBatch,
			Partitions: []v1alpha1.PartitionProfile{
				{Name: "example-gpu", AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"}},
				{Name: "example-cpu", AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "none", CPUArch: "x86_64"}},
			},
		},
	}
}

func newModelDeployment(ns, name string) *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: "example-27b",
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-1b", Revision: commit40},
				Engine:     v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", ImageDigest: digest, Image: "docker.io/vllm/vllm-openai"},
				Flags:      []v1alpha1.EngineFlag{{Name: "dtype", Value: "bfloat16"}, {Name: "max-model-len", Value: "32768"}},
			}},
			Instances: 2,
			Providers: v1alpha1.ProviderSelection{Names: []string{"helios-a"}},
			Workload:  validWorkload(),
		},
	}
}

func newAllocation(ns, name string) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	a.Spec.ProviderConfigReference = &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: "helios-a"}
	a.Spec.ForProvider = v1alpha1.AllocationParameters{Batch: validBatch()}
	return a
}

func newInstance(ns, name string) *v1alpha1.Instance {
	return &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.InstanceSpec{
			DeploymentID:   fullID,
			IdentityRecord: "id-" + fullID[:12],
			AllocationRef:  v1alpha1.ObjectReference{Name: "helios-a-abcde"},
			Launch:         validLaunch(),
		},
	}
}

// readAll reads the whole body of a file or fails.
func readAll(t *testing.T, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	must(t, err, "open "+path)
	defer f.Close()
	b, err := io.ReadAll(f)
	must(t, err, "read "+path)
	return b
}

func objKey(o client.Object) client.ObjectKey { return client.ObjectKeyFromObject(o) }

func jsonOf(t *testing.T, s string) *apiextensionsv1.JSON {
	t.Helper()
	if !json.Valid([]byte(s)) {
		t.Fatalf("not JSON: %s", s)
	}
	return &apiextensionsv1.JSON{Raw: []byte(s)}
}
