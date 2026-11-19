package slurm_test

// Sano as a Provider (#735; HBEP-4 as amended by #745): the QOS chosen by the GPU count, the per-job caps,
// the QOS's wall limit, and Sano's job_submit rules, over the fake Slurm in Sano's shape (fakeslurm.SanoConfig).
// The expectations are worked out by hand from the probes of 18 Nov 2026 and the science agent's #735 comment: the node's maximum of 11 CPUs and
// 135,904 MiB per GPU (the edge each QOS's caps are tested at; the shipped profile asks for 4 CPUs and 64 GiB since 19 Nov 2026), the QOS caps of probe 3, a 3-day wall. None is read back through the code under test, and the
// fixture's caps are its own literals, never read from profiles/sano.yaml, so an agreement between them is a finding.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

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

// The QOS is the first entry whose gpus is at least the count, and the request is at the node's maximum per-GPU shares:
// 11 CPUs and 135,904 MiB per GPU (#735, the science agent's comment of 18 Nov 2026), or at the shipped profile's own, 4 CPUs and
// 64 GiB per GPU (the science agent's decision of 19 Nov 2026). The request carries its own sizes, which the Batch does not read
// from the profile, so the maximum rows stay as the edge of each QOS's caps. Every row is accepted by the fixture's job_submit
// rules, which hold the request to the QOS's caps, so each shows Sano accepts what the plugin sends.
func TestSanoSubmitsUnderTheQOSOfItsGPUCount(t *testing.T) {
	for _, tc := range []struct {
		name         string
		gpus, cpus   int32
		mem          string
		qos, memWord string
	}{
		{"1 GPU", 1, 11, "135904Mi", "gpu1", "--mem=135904M"},
		{"1 GPU at the shipped profile's shares", 1, 4, "65536Mi", "gpu1", "--mem=65536M"},
		{"4 GPUs at the shipped profile's shares", 4, 16, "262144Mi", "gpu4", "--mem=262144M"},
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

// An estimate on Sano reads epoch seconds (probe 4 of #735, the shipped profile's startEstimateTime) and carries the flags of
// the real job: the QOS of its GPU count, --mem and --cpus-per-task, which Sano's job_submit plugin requires of an
// sbatch --test-only as of a submission (the source of probe 3's plugin rules is "sbatch --test-only with each flag left out in
// turn"), under a job name that is not one the plugin calls generic. The fake's --test-only path does not run the plugin's
// refusal (fakeslurm/testonly.go), so "not refused" is read here from the words the plugin requires, hand-written below, and
// from the words of the real Submit of the same request, never from the estimate's own builder.
func TestSanoEstimateCarriesTheRealJobsFlagsAndReadsEpochSeconds(t *testing.T) {
	b, plat, _ := sanoRig(t, nil)
	want := fakeStart.Add(95 * time.Minute)
	plat.SetTestOnlyStart(want)
	req := sanoReq(2, 22, "271808Mi", "72h") // 2 GPUs: gpu2, 22 CPUs and 271,808 MiB, which are 2 x 11 and 2 x 135,904
	res, err := b.Estimate(context.Background(), req)
	if err != nil || res.Unknown != "" || res.Start == nil || !res.Start.Equal(want) {
		t.Fatalf("Estimate = %+v, %v; want the fake's start %s read as epoch seconds", res, err, want)
	}
	if plat.CountTestOnly() != 1 || plat.CountSubmits() != 0 {
		t.Fatalf("%d --test-only calls and %d submissions, want 1 and 0", plat.CountTestOnly(), plat.CountSubmits())
	}
	var call fakeslurm.Call
	for _, c := range plat.Calls() {
		if c.TestOnly() {
			call = c
		}
	}
	if len(call.Words) < 4 || call.Words[0] != "env" || call.Words[1] != "SLURM_TIME_FORMAT=%s" {
		t.Fatalf("the estimate's call %q lacks the epoch time format prefix", call.Words)
	}
	est := call.Command()
	if id, outcome, err := b.Submit(context.Background(), req); outcome != provider.Accepted || err != nil || id == "" {
		t.Fatalf("Submit = %q, %s, %v; want it accepted, to compare its words", id, outcome, err)
	}
	subs := sbatchWords(plat)
	if len(subs) != 1 {
		t.Fatalf("%d submissions, want 1", len(subs))
	}
	sub := subs[0]
	with := func(words []string, prefix string) []string {
		var got []string
		for _, w := range words {
			if strings.HasPrefix(w, prefix) {
				got = append(got, w)
			}
		}
		return got
	}
	// What the plugin requires, written out: the QOS, the memory and the CPUs of the job.
	for _, w := range []struct{ prefix, word string }{
		{"--qos=", "--qos=gpu2"}, {"--mem=", "--mem=271808M"}, {"--cpus-per-task=", "--cpus-per-task=22"},
		{"--gres=", "--gres=gpu:2"}, {"--partition=", "--partition=gpu"},
	} {
		if got := with(est, w.prefix); !slices.Equal(got, []string{w.word}) {
			t.Errorf("the estimate's %s words are %q, want [%s]", w.prefix, got, w.word)
		}
		if got := with(sub, w.prefix); !slices.Equal(got, with(est, w.prefix)) {
			t.Errorf("the submission's %s words are %q, the estimate's are %q: an estimate must carry the real job's", w.prefix, got, with(est, w.prefix))
		}
	}
	// The name is the estimate's own, and not generic; the estimate is never held, and it is a --test-only.
	name := with(est, "--job-name=")
	if len(name) != 1 || name[0] != "--job-name=hpcburst-estimate" {
		t.Errorf("the estimate's job names are %q, want [--job-name=hpcburst-estimate]", name)
	}
	for _, generic := range []string{"wrap", "job", "bash"} {
		if slices.Contains(est, "--job-name="+generic) {
			t.Errorf("the estimate's job name is %q, which Sano's plugin refuses as generic", generic)
		}
	}
	if slices.Contains(est, "--hold") || !slices.Contains(sub, "--hold") {
		t.Errorf("hold: estimate %v, submission %v; want the estimate unheld and the (held) submission held", slices.Contains(est, "--hold"), slices.Contains(sub, "--hold"))
	}
}
