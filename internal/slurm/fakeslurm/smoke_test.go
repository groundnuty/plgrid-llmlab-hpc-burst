package fakeslurm_test

// New. The grammar check: a slurm.Batch over a fakeslurm Session runs the
// plugin's real command lines. A mismatch between internal/slurm/dialect.go and
// the fake shows here first. The expected values are literals worked out from
// HBEP-4 and the Slurm rules, never read back from the fake's own output.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	smokePart    = "example-gpu"
	smokeAccount = "example-account"
	smokeTag     = "hb-0123456789ab"
	smokeScratch = "/scratch/example-user"
	smokeHome    = "/home/example-user"
	// stagingDir "$SCRATCH/hpcburst" resolves to this.
	smokeStaging = smokeScratch + "/hpcburst"
)

var smokeStart = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

type smokeRig struct {
	batch *slurm.Batch
	plat  *fakeslurm.Platform
	sess  *fakeslurm.Session
	clock *fake.Clock
}

func newSmoke(t *testing.T) *smokeRig {
	t.Helper()
	clock := fake.NewClock(smokeStart)
	plat := fakeslurm.New(clock, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: smokePart}},
	})
	sess := plat.Login(fakeslurm.Login{
		User: "example-user",
		Env:  map[string]string{"HOME": smokeHome, "SCRATCH": smokeScratch},
	})
	gpus := int32(4)
	maxTime := v1alpha1.Duration("1h")
	profile := provider.Profile{
		Name: "example",
		Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.42", StagingDir: "$SCRATCH/hpcburst"},
		Partition: v1alpha1.PartitionProfile{
			Name:             smokePart,
			AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "example-gpu-model", CPUArch: "aarch64"},
			GPUsPerNode:      &gpus,
			MaxTime:          &maxTime,
		},
	}
	settings := provider.Settings{Provider: "example", Account: smokeAccount, Partition: smokePart}
	batch := slurm.NewBatch(sess, profile, settings, slurm.Options{Clock: clock})
	return &smokeRig{batch: batch, plat: plat, sess: sess, clock: clock}
}

func smokeRequest(digest string, hold bool) provider.SubmitRequest {
	return provider.SubmitRequest{
		Tag: smokeTag, Hold: hold, Account: smokeAccount,
		Params: v1alpha1.BatchParameters{
			Partition: smokePart, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8,
			Memory: resource.MustParse("16Gi"), Nodes: 1, AgentDigest: digest,
		},
	}
}

func fileOf(content []byte, path string, mode fs.FileMode) provider.File {
	sum := sha256.Sum256(content)
	return provider.File{Path: path, SHA256: hex.EncodeToString(sum[:]), Mode: mode, Content: content}
}

func TestBatchOverFakeSlurm(t *testing.T) {
	ctx := context.Background()
	r := newSmoke(t)
	agent := []byte("example agent binary")
	digest := fileOf(agent, "", 0).SHA256
	secret := []byte("example-credential-0f3a9c")

	// Files first: the agent binary and the credential land once, with the modes asked for.
	agentFile := fileOf(agent, agentapi.StagedAgentPath(digest), 0o755)
	credFile := fileOf(secret, agentapi.StagedCredentialPath(smokeTag), 0o600)
	for _, f := range []provider.File{agentFile, credFile} {
		if err := r.batch.EnsureFile(ctx, f); err != nil {
			t.Fatalf("EnsureFile(%s): %v", f.Path, err)
		}
	}
	files := r.plat.Files()
	if got := files[smokeStaging+"/"+agentFile.Path]; string(got.Content) != string(agent) || got.Mode != 0o755 {
		t.Errorf("agent file = %q mode %o, want the agent bytes and mode 755", got.Content, got.Mode)
	}
	if got := files[smokeStaging+"/"+credFile.Path]; string(got.Content) != string(secret) || got.Mode != 0o600 {
		t.Errorf("credential = %q mode %o, want the secret and mode 600", got.Content, got.Mode)
	}
	if len(files) != 2 {
		t.Errorf("%d files after two EnsureFile calls, want 2 (no temporary file left): %v", len(files), files)
	}
	// umask 077 made every new directory 0777 &^ 077 = 0700.
	for _, d := range []string{smokeStaging, smokeStaging + "/alloc", smokeStaging + "/alloc/" + smokeTag} {
		if m, ok := r.plat.Dirs()[d]; !ok || m != 0o700 {
			t.Errorf("directory %s = %o (present %v), want mode 700", d, m, ok)
		}
	}

	// A second EnsureFile of each file makes no write: judged from the call log.
	before := len(r.plat.Calls())
	for _, f := range []provider.File{agentFile, credFile} {
		if err := r.batch.EnsureFile(ctx, f); err != nil {
			t.Fatalf("second EnsureFile(%s): %v", f.Path, err)
		}
	}
	for _, c := range r.plat.Calls()[before:] {
		if c.Words[0] != "sha256sum" {
			t.Errorf("second EnsureFile ran %v, want only sha256sum", c.Words)
		}
	}
	if n := len(r.plat.Calls()) - before; n != 2 {
		t.Errorf("second EnsureFile calls = %d, want 2 sha256sum checks", n)
	}
	for _, c := range r.plat.Calls() {
		if strings.Contains(strings.Join(c.Words, " "), string(secret)) {
			t.Errorf("the credential appears in the call log: %v", c.Words)
		}
	}

	// Submit held: one sbatch, the job registered with the request's parameters.
	id, outcome, err := r.batch.Submit(ctx, smokeRequest(digest, true))
	if err != nil || outcome != provider.Accepted || id != "1001" {
		t.Fatalf("Submit = (%q, %s, %v), want (1001, Accepted, nil)", id, outcome, err)
	}
	if n := r.plat.Count("sbatch"); n != 1 {
		t.Errorf("sbatch calls = %d, want 1", n)
	}
	j, _ := r.plat.Job(1001)
	wantSubmitted(t, j, digest)

	// ListByTag sees it held (a user hold); LiftHold releases it.
	listed, err := r.batch.ListByTag(ctx, smokeTag)
	if err != nil || len(listed) != 1 {
		t.Fatalf("ListByTag = %v, %v, want one job", listed, err)
	}
	if listed[0].ID != "1001" || listed[0].Hold != provider.HoldUser || listed[0].Reason != "JobHeldUser" ||
		listed[0].Phase != v1alpha1.AllocationPhase("Held") || listed[0].GPUs != 1 || listed[0].TimeLimit != 30*time.Minute {
		t.Errorf("held job = %+v", listed[0])
	}
	if err := r.batch.LiftHold(ctx, "1001"); err != nil {
		t.Fatalf("LiftHold: %v", err)
	}
	if j, _ := r.plat.Job(1001); j.Held {
		t.Errorf("job is still held after LiftHold")
	}

	// Start, then Observe: running, with the hard end start + limit.
	r.clock.Advance(5 * time.Minute)
	if err := r.plat.Start(1001); err != nil {
		t.Fatal(err)
	}
	got, found, err := r.batch.Observe(ctx, id)
	if err != nil || !found || got.StartTime == nil || got.HardEnd == nil {
		t.Fatalf("Observe = %+v, %v, %v, want a running job", got, found, err)
	}
	if want := smokeStart.Add(5 * time.Minute); !got.StartTime.Equal(want) {
		t.Errorf("StartTime = %v, want %v", got.StartTime, want)
	}
	if want := smokeStart.Add(35 * time.Minute); !got.HardEnd.Equal(want) {
		t.Errorf("HardEnd = %v, want %v", got.HardEnd, want)
	}

	// The job ends at 12:25:00. The Helios queue drops it at once, so Observe
	// finds it in accounting, ended; the single-job squeue answers an empty list.
	r.clock.Advance(20 * time.Minute)
	if err := r.plat.End(1001, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	r.clock.Advance(time.Second)
	got, found, err = r.batch.Observe(ctx, id)
	if err != nil || !found || got.Source != provider.SourceAccounting || got.EndTime == nil {
		t.Fatalf("Observe after the end = %+v, %v, %v, want an ended job from accounting", got, found, err)
	}
	if want := smokeStart.Add(25 * time.Minute); !got.EndTime.Equal(want) {
		t.Errorf("EndTime = %v, want %v", got.EndTime, want)
	}
	if _, listed, err := r.batch.QueueJob(ctx, id); err != nil || listed {
		t.Errorf("QueueJob 1 s after the end = listed %v, %v, want not listed (empty list)", listed, err)
	}
	if res := r.batch.Cancel(ctx, []string{id}); res[0].Outcome != provider.AlreadyEnded {
		t.Errorf("Cancel of an ended job = %+v, want AlreadyEnded", res[0])
	}
	r.clock.Advance(10 * time.Second) // MinJobAge passed: the error shape
	if _, listed, err := r.batch.QueueJob(ctx, id); err != nil || listed {
		t.Errorf("QueueJob 11 s after the end = listed %v, %v, want not listed (error shape)", listed, err)
	}
	// Purged from slurmctld, so scancel would say nothing at all, but accounting still shows the
	// job ended: AlreadyEnded. A job nothing shows is ErrNotFound (#199).
	if res := r.batch.Cancel(ctx, []string{id}); res[0].Outcome != provider.AlreadyEnded {
		t.Errorf("Cancel of a purged job that accounting shows = %+v, want AlreadyEnded", res[0])
	}
	if res := r.batch.Cancel(ctx, []string{"9999"}); !errors.Is(res[0].Err, provider.ErrNotFound) {
		t.Errorf("Cancel of an unknown job = %+v, want ErrNotFound", res[0])
	}

	// A second job, pending, is cancelled.
	req := smokeRequest(digest, false)
	req.Tag = "hb-ba9876543210"
	id2, outcome, err := r.batch.Submit(ctx, req)
	if err != nil || outcome != provider.Accepted || id2 != "1002" {
		t.Fatalf("second Submit = (%q, %s, %v)", id2, outcome, err)
	}
	if res := r.batch.Cancel(ctx, []string{id2}); res[0].Outcome != provider.Cancelled {
		t.Errorf("Cancel of a pending job = %+v, want Cancelled", res[0])
	}
	if j, _ := r.plat.Job(1002); j.State != fakeslurm.Cancelled {
		t.Errorf("job 1002 state = %s, want CANCELLED", j.State)
	}

	// RemoveAllocationFiles removes alloc/<tag>/ and nothing else; twice is fine.
	for i := 0; i < 2; i++ {
		if err := r.batch.RemoveAllocationFiles(ctx, smokeTag); err != nil {
			t.Fatalf("RemoveAllocationFiles #%d: %v", i+1, err)
		}
	}
	if _, ok := r.plat.Files()[smokeStaging+"/"+credFile.Path]; ok {
		t.Errorf("the credential is still there after RemoveAllocationFiles")
	}
	if _, ok := r.plat.Dirs()[smokeStaging+"/alloc/"+smokeTag]; ok {
		t.Errorf("the allocation directory is still there")
	}
	if _, ok := r.plat.Files()[smokeStaging+"/"+agentFile.Path]; !ok {
		t.Errorf("the staged agent was removed with the allocation directory")
	}

	// Paths: HOME and SCRATCH read once each for the connection, none again
	// until it is renewed.
	if n := r.plat.CountFor("example-user", "printenv"); n != 2 {
		t.Errorf("printenv calls = %d, want 2 (HOME, SCRATCH) for one connection", n)
	}
	paths, err := r.batch.Paths(ctx)
	if err != nil || paths.Home != smokeHome || paths.Staging != smokeStaging {
		t.Errorf("Paths = %+v, %v, want home %s staging %s", paths, err, smokeHome, smokeStaging)
	}
	r.sess.Reconnect()
	if _, err := r.batch.Paths(ctx); err != nil {
		t.Fatal(err)
	}
	if n := r.plat.CountFor("example-user", "printenv"); n != 4 {
		t.Errorf("printenv calls after a reconnect = %d, want 4", n)
	}

	// The dial-home key: missing, then placed.
	if present, err := r.batch.DialHomeKeysPresent(ctx); err != nil || present {
		t.Errorf("DialHomeKeysPresent with no files = %v, %v, want false", present, err)
	}
	dial := smokeHome + "/" + agentapi.DialHomeDir
	for _, f := range []string{agentapi.DialHomeKeyFile, agentapi.DialHomeKnownHostsFile} {
		if err := r.plat.WriteFile(dial+"/"+f, []byte("example"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if present, err := r.batch.DialHomeKeysPresent(ctx); err != nil || !present {
		t.Errorf("DialHomeKeysPresent with both files = %v, %v, want true", present, err)
	}

	// The plugin never reads squeue's text output, whose times are local.
	for _, c := range r.plat.Calls() {
		if c.Words[0] == "squeue" && !contains(c.Words, "--json") {
			t.Errorf("the plugin ran squeue without --json: %v", c.Words)
		}
	}
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func wantSubmitted(t *testing.T, j fakeslurm.Job, digest string) {
	t.Helper()
	if j.Name != smokeTag || j.Partition != smokePart || j.Account != smokeAccount || j.TimeLimit != 30 ||
		j.GPUs != 1 || j.CPUs != 8 || j.MemMiB != 16384 || !j.Held || j.HoldReason != "JobHeldUser" || j.State != fakeslurm.Pending {
		t.Errorf("registered job = %+v", j)
	}
	dir := smokeStaging + "/alloc/" + smokeTag
	if j.Output != dir+"/job.out" || j.Error != dir+"/job.err" {
		t.Errorf("output %q error %q, want files under %s", j.Output, j.Error, dir)
	}
	agent := smokeStaging + "/agent/" + digest + "/hb-agent"
	wrap := "[ $(sha256sum " + agent + " | cut -c1-64) = " + digest + " ] && exec " + agent + " " + dir
	if j.Wrap != wrap {
		t.Errorf("wrap = %q, want %q", j.Wrap, wrap)
	}
}
