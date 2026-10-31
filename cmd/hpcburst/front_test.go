package main

// The front's supervision against a real k3s (issue #64): hpcburst up starts the real
// hpcburst-front beside a fake controller; the front's watch must see a ModelDeployment and an
// Instance created in the cluster, and the Instance's status written through the status
// subresource; a killed front is restarted, and its usage counter shows the gap across the kill
// (HBEP-5, Test plan, Usage). Judgements come from /proc, the front's own listeners and files, and
// the cluster, not from the launcher's report.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
)

var (
	frontBinOnce sync.Once
	frontBinDir  string
	frontBinErr  error
)

// frontBinary builds cmd/hpcburst-front once per run.
func frontBinary(t *testing.T) string {
	t.Helper()
	frontBinOnce.Do(func() {
		if frontBinDir, frontBinErr = os.MkdirTemp("", "hbfront"); frontBinErr != nil {
			return
		}
		cmd := exec.Command("go", "build", "-o", filepath.Join(frontBinDir, "hpcburst-front"), "../hpcburst-front")
		if out, err := cmd.CombinedOutput(); err != nil {
			frontBinErr = fmt.Errorf("go build of hpcburst-front: %v\n%s", err, out)
		}
	})
	if frontBinErr != nil {
		t.Fatal(frontBinErr)
	}
	return filepath.Join(frontBinDir, "hpcburst-front")
}

func removeFrontBinary() {
	if frontBinDir != "" {
		os.RemoveAll(frontBinDir)
	}
}

// The front's listeners in this test: inside the launcher tests' block 29220-29239 and outside
// the launcher's own base..base+7.
const (
	frontClientPort = 29230
	frontDialPort   = 29231
)

type frontEntryRecord struct {
	Name     string     `json:"name"`
	State    string     `json:"state"`
	Proc     *stateProc `json:"proc"`
	Restarts int        `json:"restarts"`
}

func frontEntry(t *testing.T, dir string) (frontEntryRecord, bool) {
	var r struct {
		Entries []frontEntryRecord `json:"entries"`
	}
	b, err := os.ReadFile(filepath.Join(dir, "controller", "supervisor.json"))
	if err != nil || json.Unmarshal(b, &r) != nil {
		return frontEntryRecord{}, false
	}
	for _, e := range r.Entries {
		if e.Name == "front" {
			return e, true
		}
	}
	return frontEntryRecord{}, false
}

type frontHealth struct {
	Synced      bool           `json:"synced"`
	Deployments map[string]int `json:"deployments"`
	Instances   int            `json:"instances"`
	LeaseEnds   int            `json:"leaseEnds"`
}

func getFrontHealth(addr string) (frontHealth, bool) {
	var h frontHealth
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + addr + "/healthz")
	if err != nil {
		return h, false
	}
	defer resp.Body.Close()
	return h, resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&h) == nil
}

func toU(t *testing.T, obj runtime.Object, kind string) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind(kind)
	return u
}

// usageCounters reads the counters of the front's usage records.
func usageCounters(t *testing.T, path string) []uint64 {
	t.Helper()
	b, _ := os.ReadFile(path)
	var out []uint64
	dec := json.NewDecoder(bytes.NewReader(b))
	for dec.More() {
		var r struct {
			Counter uint64 `json:"counter"`
		}
		if err := dec.Decode(&r); err != nil {
			t.Fatal(err)
		}
		out = append(out, r.Counter)
	}
	return out
}

func listModels(t *testing.T, key string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/v1/models", frontClientPort), nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	return resp.StatusCode, b.String()
}

func TestFrontSupervision(t *testing.T) {
	const base = 29220
	h := newHost(t)
	ctl, _ := fakeController(t)
	fb := frontBinary(t)
	work := shortTmp(t, false)
	clientKey := "front-e2e-client-key-" + strings.Repeat("7", 20)
	keys := filepath.Join(work, "client-keys.json")
	if err := os.WriteFile(keys, []byte(fmt.Sprintf(`{"keys":[{"id":"e2e","key":%q}]}`, clientKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(work, "front.json")
	usageDir := filepath.Join(work, "usage")
	cfgBody := fmt.Sprintf(`{"clientListen":"127.0.0.1:%d","dialHomeListen":"127.0.0.1:%d","clientKeysFile":%q,"usageDir":%q}`, frontClientPort, frontDialPort, keys, usageDir)
	if err := os.WriteFile(cfg, []byte(cfgBody), 0o600); err != nil {
		t.Fatal(err)
	}

	r := cli(t, h.upArgs(base, "--json", "--controller-bin", ctl, "--manifests", "../../config/crd", "--front-config", cfg, "--front-bin", fb)...)
	if r.code != exitOK {
		t.Fatalf("up exited %d: %s", r.code, r.err)
	}
	var up struct {
		FrontHealth string `json:"front_health"`
	}
	parseJSON(t, r.out, &up)
	healthAddr := fmt.Sprintf("127.0.0.1:%d", base+4)
	if up.FrontHealth != healthAddr {
		t.Errorf("up reports the front's health on %q, want %s", up.FrontHealth, healthAddr)
	}

	// ---- the front runs as a child of the supervisor, with exactly its command line ----
	var fe frontEntryRecord
	waitFor(t, 60*time.Second, "the front to run", func() bool {
		var ok bool
		fe, ok = frontEntry(t, h.dir)
		return ok && fe.Proc != nil && fe.State == "running"
	})
	pid := fe.Proc.PID
	sup := recordedProcs(t, h.dir)[1]
	if ppidOfPID(t, pid) != sup.PID {
		t.Fatalf("the front (pid %d) is not a child of the supervisor (pid %d)", pid, sup.PID)
	}
	cmdline := strings.Split(strings.TrimRight(fileOf(fmt.Sprintf("/proc/%d/cmdline", pid)), "\x00"), "\x00")
	wantArgv := []string{fb, "--config", cfg, "--state-dir", h.dir, "--port-base", "29220", "--health-addr", healthAddr}
	if !slices.Equal(cmdline, wantArgv) {
		t.Errorf("front argv\n got %q\nwant %q", cmdline, wantArgv)
	}
	if strings.Contains(fileOf(fmt.Sprintf("/proc/%d/cmdline", pid)), clientKey) {
		t.Error("the client key is on the front's command line")
	}
	waitFor(t, 60*time.Second, "the front's /healthz", func() bool { _, ok := getFrontHealth(healthAddr); return ok })

	// ---- the watch sees objects created in the cluster, status included ----
	c := h.clients()
	ctx := t.Context()
	prof := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "example-platform"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch,
		Partitions: []v1alpha1.PartitionProfile{{Name: "example-gpu", AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"}}}}}
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "helios-a", Labels: map[string]string{v1alpha1.LabelPlatform: "example-platform"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: "example-platform"}}}
	for _, o := range []struct {
		obj  runtime.Object
		kind string
	}{{prof, "PlatformProfile"}, {prov, "Provider"}} {
		gvr := inventory.GVRPlatformProfiles
		if o.kind == "Provider" {
			gvr = inventory.GVRProviders
		}
		if _, err := c.dyn.Resource(gvr).Create(ctx, toU(t, o.obj, o.kind), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create %s: %v", o.kind, err)
		}
	}
	md := frontTestMD()
	created, err := c.dyn.Resource(inventory.GVRModelDeployments).Namespace("default").Create(ctx, toU(t, md, "ModelDeployment"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create ModelDeployment: %v", err)
	}
	inst := frontTestInstance(created.GetName(), created.GetUID())
	ci, err := c.dyn.Resource(inventory.GVRInstances).Namespace("default").Create(ctx, toU(t, inst, "Instance"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create Instance: %v", err)
	}
	leaseEnd := metav1.NewTime(time.Now().Add(48 * time.Hour).Truncate(time.Second))
	if err := unstructured.SetNestedMap(ci.Object, map[string]any{"phase": "ready", "leaseEnd": leaseEnd.UTC().Format(time.RFC3339)}, "status"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.dyn.Resource(inventory.GVRInstances).Namespace("default").UpdateStatus(ctx, ci, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update the Instance's status: %v", err)
	}
	var fh frontHealth
	waitFor(t, 30*time.Second, "the front to see the Instance and its lease end", func() bool {
		var ok bool
		fh, ok = getFrontHealth(healthAddr)
		_, dep := fh.Deployments["default/front-e2e"]
		return ok && fh.Instances == 1 && fh.LeaseEnds == 1 && dep
	})
	// No transport is registered (HBEP-6's tunnel is not built), so the Instance is not routable:
	// the deployment has 0 routable Instances, and the model is listed, not routable.
	if fh.Deployments["default/front-e2e"] != 0 {
		t.Errorf("routable Instances %d without a transport", fh.Deployments["default/front-e2e"])
	}
	code, body := listModels(t, clientKey)
	if code != http.StatusOK || !strings.Contains(body, `"id":"example-27b"`) || !strings.Contains(body, `"routable":false`) {
		t.Fatalf("GET /v1/models = %d %s", code, body)
	}
	usagePath := filepath.Join(usageDir, "usage.jsonl")
	waitFor(t, 10*time.Second, "the first usage record", func() bool { return len(usageCounters(t, usagePath)) == 1 })

	// ---- a killed front is restarted, and its usage counter shows the gap ----
	verifiedKill(t, *fe.Proc)
	waitFor(t, 60*time.Second, "the front to be restarted", func() bool {
		e, ok := frontEntry(t, h.dir)
		return ok && e.Proc != nil && e.Proc.PID != pid && e.Restarts == 1 && e.State == "running"
	})
	waitFor(t, 60*time.Second, "the restarted front's /healthz", func() bool { h, ok := getFrontHealth(healthAddr); return ok && h.Instances == 1 })
	if code, _ := listModels(t, clientKey); code != http.StatusOK {
		t.Fatalf("after the restart GET /v1/models = %d", code)
	}
	waitFor(t, 10*time.Second, "the second usage record", func() bool { return len(usageCounters(t, usagePath)) == 2 })
	if got := usageCounters(t, usagePath); got[0] != 1 || got[1] != 1001 {
		t.Errorf("usage counters %v, want [1 1001]: the kill must show as a gap", got)
	}

	// ---- status reports the front ----
	st, scode := statusOf(t, h)
	var front *struct {
		Name      string `json:"name"`
		Running   bool   `json:"running"`
		PID       int    `json:"pid"`
		Restarts  int    `json:"restarts"`
		HealthzOK *bool  `json:"healthz_ok"`
		LastExit  *struct {
			Detail string `json:"detail"`
		} `json:"last_exit"`
	}
	for i := range st.Controllers {
		if st.Controllers[i].Name == "front" {
			front = &st.Controllers[i]
		}
	}
	if scode != exitOK || front == nil || !front.Running || front.Restarts != 1 || front.HealthzOK == nil || !*front.HealthzOK {
		t.Errorf("status exited %d and reports the front as %+v (reasons %v)", scode, front, st.Reasons)
	}
	for _, f := range []string{filepath.Join(h.dir, "logs", "front.log"), usagePath} {
		if strings.Contains(fileOf(f), clientKey) {
			t.Errorf("%s holds the client key", f)
		}
	}
}

func frontTestMD() *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "front-e2e", Namespace: "default"},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: "example-27b",
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-1b", Revision: strings.Repeat("a", 40)},
				Engine:     v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", ImageDigest: "sha256:" + strings.Repeat("b", 64), Image: "docker.io/vllm/vllm-openai"},
			}},
			Instances: 1,
			Providers: v1alpha1.ProviderSelection{Names: []string{"helios-a"}},
			Workload:  &v1alpha1.Workload{ContextTokens: v1alpha1.ContextTokens{Typical: 8192}, OutputTokens: v1alpha1.OutputTokens{Typical: 1024}, TargetSessions: 8},
		},
	}
}

func frontTestInstance(mdName string, mdUID types.UID) *v1alpha1.Instance {
	controller := true
	full := strings.Repeat("c", 64)
	return &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "front-e2e-0", Namespace: "default",
			Labels:          map[string]string{v1alpha1.LabelProvider: "helios-a", v1alpha1.LabelModelDeployment: mdName},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "ModelDeployment", Name: mdName, UID: mdUID, Controller: &controller}}},
		Spec: v1alpha1.InstanceSpec{
			DeploymentID: v1alpha1.FullDeploymentID(full), IdentityRecord: "id-" + full[:12],
			AllocationRef: v1alpha1.ObjectReference{Name: "helios-a-abcde"},
			Launch: v1alpha1.Launch{
				Image: "docker.io/vllm/vllm-openai", ImageDigest: "sha256:" + strings.Repeat("b", 64),
				Command:    []string{"vllm", "serve", "/hpcburst/model", "--uds", "/hpcburst/run/worker-0.sock", "--max-num-seqs=256"},
				Checkpoint: v1alpha1.LaunchCheckpoint{Repository: "example-org/example-1b", Revision: strings.Repeat("a", 40)},
				GPUs:       1, MaxNumSeqs: 256,
			},
		},
	}
}
