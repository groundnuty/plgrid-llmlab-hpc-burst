package conformance

// The accounting row of the alpha runbook (issue #194; HBEP-8, Graduation
// criteria, alpha): "the kept accounting record's GPU entry is checked once on a
// real GPU job on each alpha partition". One GPU job goes through the
// Provider's controller; the job writes down how many GPUs its node shows
// (nvidia-smi); when it has ended the Allocation is deleted, which writes the
// kept record acct-<UID>; and the record's allocatedGPUs is checked against that
// count. The record is built from the scheduler's accounting, so it is not
// checked against the scheduler: the node's own count is the independent
// witness (alpha.VerifyAccounting).
//
// A dry run (HPCBURST_ALPHA_DRY=1, for hack/livechecks) runs the same body over
// a fake Slurm in run mode, where the "node" shows what the fault says. It
// refuses to start when a live setting is present, and its log says it is dry.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// gpuAgent is the agent stand-in of the accounting row, run by the bootstrap with
// the Allocation's directory as its argument. It writes the number of GPUs its
// node shows into node-gpus there and holds them for a few seconds.
var gpuAgent = []byte("#!/bin/sh\nn=$(nvidia-smi -L 2>/dev/null | grep -c '^GPU')\nprintf '%s\\n' \"$n\" > \"$1/node-gpus\"\nsleep 20\nexit 0\n")

// dryGPUAgent is the same in a dry run, where the fake node's GPUs are the
// environment's FAKE_NODE_GPUS.
var dryGPUAgent = []byte("#!/bin/sh\nprintf '%s\\n' \"$FAKE_NODE_GPUS\" > \"$1/node-gpus\"\nsleep 2\nexit 0\n")

// nodeGPUsFile is the file name in the Allocation's directory (agentapi.AllocationDir).
const nodeGPUsFile = "node-gpus"

// dryFaults of this row: what the fake node shows. "" is the healthy node, with
// the one GPU the job asked for.
var dryGPUFaults = map[string]string{"": "1", "node-two": "2", "node-none": "0"}

// TestGPUAccounting is the run on one platform.
//
//livecheck:gpu-accounting-helios,gpu-accounting-athena
func TestGPUAccounting(t *testing.T) {
	if os.Getenv("HPCBURST_ALPHA_DRY") == "1" {
		dryGPUAccounting(t)
		return
	}
	s := load(t)
	w := buildWorld(t, s, worldKit{agent: gpuAgent, dial: func(v slurm.Validated) (slurm.Runner, io.Closer, error) {
		ssh, err := sshrunNew(s, v)
		return ssh, ssh, err
	}})
	gpuAccountingBody(t, w, "gpu-accounting-"+s.profile.Name)
}

func dryGPUAccounting(t *testing.T) {
	t.Helper()
	row := os.Getenv("HPCBURST_ALPHA_ROW")
	r, ok := alpha.RowByName(row)
	if !ok || !strings.HasPrefix(row, "gpu-accounting-") {
		t.Fatalf("HPCBURST_ALPHA_ROW is %q, not a gpu-accounting row", row)
	}
	for _, k := range os.Environ() {
		if strings.HasPrefix(k, "HPCBURST_CONF") {
			t.Fatalf("a dry run refuses to start with live settings present (%s)", strings.SplitN(k, "=", 2)[0])
		}
	}
	nodeGPUs, known := dryGPUFaults[os.Getenv("HPCBURST_ALPHA_DRY_FAULT")]
	if !known {
		t.Fatalf("the dry fault %q is not one of the row's", os.Getenv("HPCBURST_ALPHA_DRY_FAULT"))
	}
	out := os.Getenv("HPCBURST_ALPHA_OUT")
	if !filepath.IsAbs(out) {
		t.Fatal("HPCBURST_ALPHA_OUT must be an absolute path")
	}
	t.Logf("%s: DRY RUN: stand-in node over a fake Slurm in run mode; nothing reached a cluster", row)

	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	home, scratch := filepath.Join(root, "home"), filepath.Join(root, "scratch")
	key, creds := filepath.Join(dir, "key"), filepath.Join(dir, "creds")
	for _, d := range []string{home, scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for path, content := range map[string]string{key: "example key\n", creds: "HostName login.example.org\nUser example-user\nIdentityFile " + key + "\n"} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	vars := map[string]string{
		"HPCBURST_CONF_PROFILE": r.Cluster, "HPCBURST_CONF_PARTITION": r.Partition, "HPCBURST_CONF_CREDENTIALS": creds,
		"HPCBURST_CONF_ACCOUNT": "example-account", "HPCBURST_CONF_GPUS": "1", "HPCBURST_CONF_CPUS": "4", "HPCBURST_CONF_MEMORY": "8Gi",
		"HPCBURST_CONF_STAGING": "$SCRATCH/hpc-burst-conformance", "HPCBURST_CONF_HOST_LOCK": filepath.Join(dir, "lock"),
		"HPCBURST_CONF_CONTROL_DIR": filepath.Join(dir, "ctl"), "HPCBURST_CONF_OUT": out, "HPCBURST_CONF_QOS": r.QOS,
		"HPCBURST_CONF_PENDING_MAX_SECONDS": "60",
	}
	s, err := readSettings(env(vars))
	if err != nil {
		t.Fatal(err)
	}
	notListed := fakeslurm.NotListedHelios
	if r.Cluster == "athena" {
		notListed = fakeslurm.NotListedAthena
	}
	run := &fakeslurm.RunMode{Root: root, Env: map[string]string{"FAKE_NODE_GPUS": nodeGPUs}}
	if r.Cluster == "helios" {
		// The shipped Helios profile makes the job's temporary directory $MEMFS (tmpDir), which
		// a job gets by asking for the feature memfs.
		run.JobVars = []fakeslurm.JobVar{{Name: "MEMFS", Feature: "memfs"}}
	}
	plat := fakeslurm.New(slurm.RealClock, fakeslurm.Config{
		DataParser: s.profile.Spec.JSONDataParser, NotListed: notListed,
		Partitions: []fakeslurm.Partition{{Name: r.Partition, GPUOnly: true}}, Run: run,
	})
	t.Cleanup(func() {
		if err := plat.Close(); err != nil {
			t.Errorf("the fake cluster did not close cleanly: %v", err)
		}
	})
	session := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": home, "SCRATCH": scratch}})
	w := buildWorld(t, s, worldKit{agent: dryGPUAgent, dial: func(slurm.Validated) (slurm.Runner, io.Closer, error) {
		return session, io.NopCloser(nil), nil
	}})
	gpuAccountingBody(t, w, row)
}

// gpuAccountingBody is the run, live or dry.
func gpuAccountingBody(t *testing.T, w *world, rowName string) {
	t.Helper()
	ctx := context.Background()
	row, ok := alpha.RowByName(rowName)
	if !ok {
		t.Fatalf("%s is not a row of the runbook", rowName)
	}
	if w.s.gpus < 1 {
		// On a CPU partition 0 would agree with 0 and prove nothing.
		t.Fatal("the accounting row needs a GPU partition and a GPU request")
	}
	name := "gpu-accounting"
	a := w.newAlloc(name)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	uid := a.UID
	// The job waits in the queue (the first clock), runs, and ends by itself.
	w.until(name, "the job ends", w.s.pendingMax+10*time.Minute, func(a *v1alpha1.Allocation) bool {
		if ph := a.Status.AtProvider.Phase; ph == v1alpha1.AllocationFailed || ph == v1alpha1.AllocationRefused || ph == v1alpha1.AllocationLost {
			t.Fatalf("the Allocation is %s (reason %q), not a job that ended", ph, a.Status.AtProvider.Reason)
		}
		return a.Status.AtProvider.Phase == v1alpha1.AllocationEnded
	})
	nodeGPUs := w.nodeGPUs(ctx, tag)
	// Deleting the Allocation writes the kept record, then lets the finalizer go.
	if err := w.c.Delete(ctx, w.get(name)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Minute)
	for w.get(name) != nil && time.Now().Before(deadline) {
		w.reconcile(name)
		time.Sleep(2 * time.Second)
	}
	if w.get(name) != nil {
		t.Fatal("the deleted Allocation still holds its finalizer")
	}
	recName, err := v1alpha1.AccountingRecordName(uid)
	if err != nil {
		t.Fatal(err)
	}
	var rev appsv1.ControllerRevision
	if err := w.c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: recName}, &rev); err != nil {
		t.Fatalf("the kept accounting record was not written: %v", err)
	}
	var data v1alpha1.AccountingRecordData
	if err := json.Unmarshal(rev.Data.Raw, &data); err != nil {
		t.Fatalf("the accounting record's data is not JSON: %v", err)
	}
	obs := alpha.AccountingObs{Partition: data.Partition, RecordGPUs: data.AllocatedGPUs, Basis: data.Basis, RequestedGPUs: int(w.s.gpus), NodeGPUs: nodeGPUs}
	failures := alpha.VerifyAccounting(row, obs)
	verdict := "PASS"
	if len(failures) > 0 {
		verdict = "FAIL"
	}
	recGPUs := "unknown"
	if data.AllocatedGPUs != nil {
		recGPUs = fmt.Sprint(*data.AllocatedGPUs)
	}
	posts := fmt.Sprintf("%s: %s (posts to %s)\n- the kept record's allocatedGPUs: %s; the GPUs the job's node showed: %d; requested: %d; basis: %s\n",
		row.Name, verdict, row.PostsTo, recGPUs, nodeGPUs, w.s.gpus, data.Basis)
	if len(failures) > 0 {
		posts += "- failed checks: " + alpha.Summary(failures) + "\n"
	}
	w.p.save(alpha.PostsFile, []byte(posts))
	w.p.save(alpha.RecordFile, []byte(fmt.Sprintf("{\"row\": %q, \"verdict\": %q, \"nodeGPUs\": %d, \"requestedGPUs\": %d}\n", row.Name, verdict, nodeGPUs, w.s.gpus)))
	if len(failures) > 0 {
		t.Fatalf("%s failed: %s", row.Name, alpha.Summary(failures))
	}
	t.Logf("%s passed; the text to post is in %s", row.Name, alpha.PostsFile)
}

// nodeGPUs reads how many GPUs the job's node showed from the file the job wrote
// in the Allocation's directory. The file is read by its sha256 (the one file
// command both the real login node and the fake have), against the sums of the
// few counts a node can show.
func (w *world) nodeGPUs(ctx context.Context, tag string) int {
	w.t.Helper()
	paths, err := w.batch.Paths(ctx)
	if err != nil {
		w.t.Fatalf("the staging directory could not be resolved: %v", err)
	}
	res, err := w.cap.Run(ctx, slurm.Command{Line: "sha256sum " + paths.Staging + "/alloc/" + tag + "/" + nodeGPUsFile})
	if err != nil || res.ExitCode != 0 {
		w.t.Fatalf("the job did not leave its GPU count (err %v, exit %d)", err, res.ExitCode)
	}
	got := strings.Fields(string(res.Stdout))
	if len(got) == 0 {
		w.t.Fatal("the GPU count's sha256 is empty")
	}
	for n := 0; n <= 16; n++ {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d\n", n)))
		if got[0] == hex.EncodeToString(sum[:]) {
			return n
		}
	}
	w.t.Fatal("the job's GPU count file holds something that is not a count from 0 to 16")
	return -1
}
