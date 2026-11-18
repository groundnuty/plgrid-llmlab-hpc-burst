package slurm_test

// Sano as a Provider (#735; HBEP-4 as amended by #745): the QOS chosen by the GPU count, the per-job caps,
// the QOS's wall limit, and Sano's job_submit rules, over the fake Slurm in Sano's shape (fakeslurm.SanoConfig).
// The expectations are worked out by hand from the probes of 18 Nov 2026 and the science agent's #735 comment: 11 CPUs and
// 135,904 MiB per GPU, the QOS caps of probe 3, a 3-day wall. None is read back through the code under test, and the
// fixture's caps are its own literals, never read from profiles/sano.yaml, so an agreement between them is a finding.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// shippedSano reads the shipped profile profiles/sano.yaml. It gives no stagingDir (no fact names one, so the
// profile leaves it out); the rig sets one, as a Provider's operator would be given one.
func shippedSano(t *testing.T) *v1alpha1.PlatformProfile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "sano.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err) // a missing file fails; it never skips
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatalf("sano.yaml: %v", err)
	}
	pp.Spec.StagingDir = "$SCRATCH/stage"
	return &pp
}

// sanoRig is a Batch on the Sano-shaped fake, given the shipped profile with edit applied to its spec.
func sanoRig(t *testing.T, edit func(*v1alpha1.PlatformProfileSpec)) (*slurm.Batch, *fakeslurm.Platform, slurm.Runner) {
	t.Helper()
	pp := shippedSano(t)
	if edit != nil {
		edit(&pp.Spec)
	}
	var part v1alpha1.PartitionProfile
	for _, p := range pp.Spec.Partitions {
		if p.Name == "gpu" {
			part = p
		}
	}
	if part.Name == "" {
		t.Fatal("sano.yaml does not list the partition gpu")
	}
	clock := fake.NewClock(fakeStart)
	plat := fakeslurm.New(clock, fakeslurm.SanoConfig())
	sess := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": fakeHome, "SCRATCH": fakeScratch}})
	profile := provider.Profile{Name: "sano", Spec: pp.Spec, Partition: part}
	settings := provider.Settings{Provider: "example", Partition: "gpu"} // no account, and no qos: qosByGpus is the QOS's one source
	return slurm.NewBatch(sess, profile, settings, slurm.Options{Clock: clock}), plat, sess
}

func sanoReq(gpus, cpus int32, mem, limit string) provider.SubmitRequest {
	return provider.SubmitRequest{
		Tag: "hb-example-0123456789ab", Hold: true,
		Params: v1alpha1.BatchParameters{Partition: "gpu", TimeLimit: v1alpha1.Duration(limit), GPUs: gpus, CPUsPerTask: cpus,
			Memory: resource.MustParse(mem), Nodes: 1, AgentDigest: qosDigest},
	}
}

// wordsWithPrefix returns the words of the one sbatch call that start with the prefix.
func wordsWithPrefix(t *testing.T, plat *fakeslurm.Platform, prefix string) []string {
	t.Helper()
	calls := sbatchWords(plat)
	if len(calls) != 1 {
		t.Fatalf("%d sbatch calls, want 1", len(calls))
	}
	var got []string
	for _, w := range calls[0] {
		if strings.HasPrefix(w, prefix) {
			got = append(got, w)
		}
	}
	return got
}

// The QOS is the first entry whose gpus is at least the count, and the request is at the partition's per-GPU shares:
// 11 CPUs and 135,904 MiB per GPU (#735, the science agent's comment of 18 Nov 2026). Every row is accepted by the
// fixture's job_submit rules, which hold the request to the QOS's caps, so each shows Sano accepts what the plugin sends.
func TestSanoSubmitsUnderTheQOSOfItsGPUCount(t *testing.T) {
	for _, tc := range []struct {
		name         string
		gpus, cpus   int32
		mem          string
		qos, memWord string
	}{
		{"1 GPU", 1, 11, "135904Mi", "gpu1", "--mem=135904M"},
		{"2 GPUs", 2, 22, "271808Mi", "gpu2", "--mem=271808M"},
		{"3 GPUs take the next QOS up", 3, 33, "407712Mi", "gpu4", "--mem=407712M"},
		{"4 GPUs", 4, 44, "543616Mi", "gpu4", "--mem=543616M"},
		{"5 GPUs take the next QOS up", 5, 55, "679520Mi", "gpu8", "--mem=679520M"},
		// 88 CPUs are under gpu8's 92, and 8 x 135,904 MiB is exactly its memory cap, 1,061.75 GiB.
		{"8 GPUs, exactly at gpu8's memory cap", 8, 88, "1087232Mi", "gpu8", "--mem=1087232M"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, plat, _ := sanoRig(t, nil)
			id, outcome, err := b.Submit(context.Background(), sanoReq(tc.gpus, tc.cpus, tc.mem, "72h"))
			if outcome != provider.Accepted || err != nil || id == "" {
				t.Fatalf("Submit = %q, %s, %v; want it accepted by Sano's rules", id, outcome, err)
			}
			if got := wordsWithPrefix(t, plat, "--qos"); len(got) != 1 || got[0] != "--qos="+tc.qos {
				t.Errorf("qos words %q, want [--qos=%s]", got, tc.qos)
			}
			if got := wordsWithPrefix(t, plat, "--mem"); len(got) != 1 || got[0] != tc.memWord {
				t.Errorf("mem words %q, want [%s]", got, tc.memWord)
			}
			if got := wordsWithPrefix(t, plat, "--gres"); len(got) != 1 || got[0] != "--gres=gpu:"+itoa(tc.gpus) {
				t.Errorf("gres words %q, want the untyped gpu:%d", got, tc.gpus)
			}
			if got := wordsWithPrefix(t, plat, "--cpus-per-task"); len(got) != 1 || got[0] != "--cpus-per-task="+itoa(tc.cpus) {
				t.Errorf("cpus words %q, want [--cpus-per-task=%d]", got, tc.cpus)
			}
		})
	}
}

func itoa(n int32) string { return strconv.Itoa(int(n)) }

// A count above the last qosByGpus entry has no QOS: it is refused before any call, naming the partition and the count.
func TestSanoRefusesNineGPUsBeforeAnyCall(t *testing.T) {
	b, plat, _ := sanoRig(t, nil)
	id, outcome, err := b.Submit(context.Background(), sanoReq(9, 99, "1223136Mi", "72h"))
	if id != "" || outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) {
		t.Fatalf("Submit = %q, %s, %v; want no ID, Rejected and ErrInvalidRequest", id, outcome, err)
	}
	for _, want := range []string{"partition gpu", "9 GPUs"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not say %q", err, want)
		}
	}
	if n := len(plat.Calls()); n != 0 {
		t.Errorf("%d calls, want 0: %v", n, plat.Calls())
	}
}

// A request above the chosen QOS's per-job caps is refused before any call, though the fixture would refuse it too; a
// request exactly at them is accepted by both (each pair differs in the one value).
func TestSanoRefusesAboveThePerJobCapsBeforeAnyCall(t *testing.T) {
	for _, tc := range []struct {
		name       string
		gpus, cpus int32
		mem        string
		names      []string // what the refusal names; none: the request is accepted
	}{
		{"gpu1 at its caps", 1, 12, "143360Mi", nil},
		{"gpu1, 13 CPUs", 1, 13, "143360Mi", []string{"maxCpusPerJob 12", `"gpu1"`}},
		{"gpu1, 1 MiB above 140 GiB", 1, 12, "143361Mi", []string{"maxMemPerJob", "143360 MiB", `"gpu1"`}},
		{"gpu8 at its caps", 8, 92, "1087232Mi", nil},
		{"gpu8, 93 CPUs", 8, 93, "1087232Mi", []string{"maxCpusPerJob 92", `"gpu8"`}},
		{"gpu8, 1 MiB above 1,061.75 GiB", 8, 92, "1087233Mi", []string{"maxMemPerJob", "1087232 MiB", `"gpu8"`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, plat, _ := sanoRig(t, nil)
			_, outcome, err := b.Submit(context.Background(), sanoReq(tc.gpus, tc.cpus, tc.mem, "72h"))
			if tc.names == nil {
				if outcome != provider.Accepted || err != nil {
					t.Fatalf("Submit = %s, %v; want it accepted", outcome, err)
				}
				return
			}
			if outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) {
				t.Fatalf("Submit = %s, %v; want Rejected and ErrInvalidRequest", outcome, err)
			}
			for _, want := range tc.names {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal %q does not say %q", err, want)
				}
			}
			if n := len(plat.Calls()); n != 0 {
				t.Errorf("%d calls, want 0: %v", n, plat.Calls())
			}
		})
	}
}

// The GPU bound on a qosByGpus partition: a count above the chosen QOS's maxGpusPerJob is refused, not capped
// (#745). A profile copy whose gpu4 allows 2 GPUs per job refuses 3 GPUs, which gpu4 would take.
func TestSanoRefusesAboveTheChosenQOSsGPUCap(t *testing.T) {
	b, plat, _ := sanoRig(t, func(s *v1alpha1.PlatformProfileSpec) {
		for i := range s.QOS {
			if s.QOS[i].Name == "gpu4" {
				two := int32(2)
				s.QOS[i].MaxGPUsPerJob = &two
			}
		}
	})
	_, outcome, err := b.Submit(context.Background(), sanoReq(3, 33, "407712Mi", "72h"))
	if outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) ||
		!strings.Contains(err.Error(), "maxGpusPerJob 2") || !strings.Contains(err.Error(), `"gpu4"`) {
		t.Fatalf("Submit = %s, %v; want Rejected naming maxGpusPerJob 2 of gpu4", outcome, err)
	}
	if n := len(plat.Calls()); n != 0 {
		t.Errorf("%d calls, want 0", n)
	}
}

// The time limit is bounded by the chosen QOS's 3-day wall, not the partition's 7 days (HBEP-4, QOS limits; #745):
// 72 hours is sent as --time=4320, and 73 hours is refused before any call naming maxWall and gpu1.
func TestSanoTimeLimitIsBoundedByTheChosenQOSsWall(t *testing.T) {
	b, plat, _ := sanoRig(t, nil)
	if _, outcome, err := b.Submit(context.Background(), sanoReq(1, 11, "135904Mi", "72h")); outcome != provider.Accepted || err != nil {
		t.Fatalf("72h: Submit = %s, %v; want it accepted", outcome, err)
	}
	if got := wordsWithPrefix(t, plat, "--time"); len(got) != 1 || got[0] != "--time=4320" {
		t.Errorf("time words %q, want [--time=4320]", got)
	}
	b, plat, _ = sanoRig(t, nil)
	_, outcome, err := b.Submit(context.Background(), sanoReq(1, 11, "135904Mi", "73h"))
	if outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) {
		t.Fatalf("73h: Submit = %s, %v; want Rejected and ErrInvalidRequest", outcome, err)
	}
	for _, want := range []string{"maxWall", `"gpu1"`, `maxTime of partition "gpu" is longer`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not say %q", err, want)
		}
	}
	if n := len(plat.Calls()); n != 0 {
		t.Errorf("%d calls, want 0", n)
	}
}

// Sano's job_submit rules (spike S5; #735's probe 3): the plugin's own line meets them, and the fixture refuses the same
// line with each rule broken in turn, with the plugin's message shapes. So the first half shows the plugin sends what
// Sano requires, and the second that the fixture would have said so if it did not.
func TestSanoJobSubmitRulesAreMetByThePluginAndEnforcedByTheFixture(t *testing.T) {
	b, plat, sess := sanoRig(t, nil)
	if _, outcome, err := b.Submit(context.Background(), sanoReq(1, 11, "135904Mi", "72h")); outcome != provider.Accepted || err != nil {
		t.Fatalf("Submit = %s, %v; want it accepted", outcome, err)
	}
	sent := sbatchWords(plat)[0]

	// edit returns the words with the first one that starts with the prefix replaced by the replacement ("" drops it).
	edit := func(words []string, prefix, replacement string) []string {
		var out []string
		for _, w := range words {
			switch {
			case strings.HasPrefix(w, prefix) && replacement != "":
				out = append(out, replacement)
			case strings.HasPrefix(w, prefix):
			default:
				out = append(out, w)
			}
		}
		return out
	}
	with := func(prefix, replacement string) []string { return edit(sent, prefix, replacement) }
	run := func(words []string) (int, string) {
		line, err := slurm.Line(words...)
		if err != nil {
			t.Fatal(err)
		}
		res, err := sess.Run(context.Background(), slurm.Command{Line: line})
		if err != nil {
			t.Fatal(err)
		}
		return res.ExitCode, string(res.Stderr)
	}
	// The control: the plugin's own line, run again as it was sent, is accepted.
	if code, stderr := run(sent); code != 0 {
		t.Fatalf("the plugin's own line is refused (exit %d): %s", code, stderr)
	}
	for _, tc := range []struct {
		name  string
		words []string
		want  []string
	}{
		{"no --qos", with("--qos", ""), []string{"Missing required parameters", "--qos=<class> e.g. --qos=gpu1"}},
		{"no --mem", with("--mem", ""), []string{"Missing required parameters", "--mem=<size>"}},
		{"no --cpus-per-task", with("--cpus-per-task", ""), []string{"Missing required parameters", "--cpus-per-task=<n>"}},
		{"a generic job name", with("--job-name", "--job-name=wrap"), []string{"Missing required parameters", "--job-name=<name> 'wrap' says nothing about the computation"}},
		{"13 CPUs under gpu1", with("--cpus-per-task", "--cpus-per-task=13"), []string{"Job exceeds the limits of QOS 'gpu1': cores: requested 13, limit 12", "use --qos=gpu2 instead"}},
		{"2 GPUs under gpu1", with("--gres", "--gres=gpu:2"), []string{"Job exceeds the limits of QOS 'gpu1': GPUs: requested 2, limit 1", "use --qos=gpu2 instead"}},
		{"141 GiB under gpu1", with("--mem", "--mem=144384M"), []string{"memory: requested 144384 MB, limit 143360 MB", "use --qos=gpu2 instead"}},
		{"96 CPUs under gpu8", edit(edit(sent, "--qos", "--qos=gpu8"), "--cpus-per-task", "--cpus-per-task=96"), []string{"Job exceeds the limits of QOS 'gpu8': cores: requested 96, limit 92", "no QOS fits this request; reduce it"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stderr := run(tc.words)
			if code != 1 {
				t.Fatalf("exit %d, want 1 (a refusal): %s", code, stderr)
			}
			for _, want := range tc.want {
				if !strings.Contains(stderr, want) {
					t.Errorf("the refusal %q does not say %q", stderr, want)
				}
			}
		})
	}
}
