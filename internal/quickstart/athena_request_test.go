package quickstart

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// The quickstart's Athena steps apply profiles/athena.yaml, examples/provider-athena-a100.yaml and
// examples/modeldeployment-athena-qwen25-instruct.yaml, on a host that runs in host-dialed mode.
// This file runs the request that the lifecycle controller makes from those three files through
// the check that the Slurm provider runs before it submits, as request_test.go does for Helios.
// The expected values are worked out by hand from profiles/athena.yaml, never read back through the
// code under test:
//   - partition plgrid-gpu-a100: maxTime 48h, cpusPerGpu 16, memPerGpu 128000Mi (the node's
//     1024000 MiB over its 8 GPUs);
//   - the example Provider sets no qos, and the partition's own QOS, plgrid, types only a limit of
//     1000 submitted jobs, so no wall, GPU, CPU or memory limit narrows the request;
//   - the deployment needs one GPU (no tensor or pipeline parallel flag), so the request is 1 GPU,
//     16 CPUs and 128000Mi, and its time limit is the partition's 48h;
//   - the lead an Instance needs is the profile's drainWindow 2h10m + handoverMargin 30m +
//     health.startTimeout 1h = 3h40m, below 48h.

var athenaInput = planInput{
	profile:    "profiles/athena.yaml",
	provider:   "examples/provider-athena-a100.yaml",
	deployment: "examples/modeldeployment-athena-qwen25-instruct.yaml",
	hostDialed: true,
}

func TestQuickstartAthenaRequestIsAcceptedOnA100(t *testing.T) {
	plan := planFiles(t, athenaInput, nil)
	if plan.degraded != nil && plan.degraded.Status == metav1.ConditionTrue {
		t.Logf("Degraded: %s: %s", plan.degraded.Reason, plan.degraded.Message)
	}
	if len(plan.allocs) != 1 {
		t.Fatalf("the lifecycle controller made %d Allocations from the quickstart's Athena objects, want 1 (Degraded: %v)", len(plan.allocs), plan.degraded)
	}
	b := plan.allocs[0].Spec.ForProvider.Batch
	if b == nil {
		t.Fatal("the Allocation has no batch parameters")
	}
	want := resource.MustParse("128000Mi")
	if b.Partition != "plgrid-gpu-a100" || b.GPUs != 1 || b.CPUsPerTask != 16 || b.Nodes != 1 || b.Memory.Cmp(want) != 0 {
		t.Errorf("the request is %+v; want partition plgrid-gpu-a100, 1 GPU, 16 CPUs, 1 node, 128000Mi", *b)
	}
	if b.TimeLimit != "48h0m0s" {
		t.Errorf("the time limit is %q; want 48h0m0s, the partition's maxTime", b.TimeLimit)
	}
	if plan.profileOK != nil {
		t.Fatalf("the Slurm provider cannot read the example Provider with the Athena profile: %v", plan.profileOK)
	}
	if plan.req.QOS != "" {
		t.Errorf("the request names QOS %q; the example Provider sets none", plan.req.QOS)
	}
	if plan.checkErr != nil {
		t.Errorf("the Slurm provider refuses the quickstart's Athena request before it submits: %v", plan.checkErr)
	}
}

// Controls: each shows the test above can see what it claims to.
func TestQuickstartAthenaRequestControls(t *testing.T) {
	t.Run("a request above the partition's 48h is refused by the check", func(t *testing.T) {
		plan := planFiles(t, athenaInput, nil)
		if len(plan.allocs) != 1 || plan.profileOK != nil {
			t.Fatalf("no request to alter: %d Allocations, %v", len(plan.allocs), plan.profileOK)
		}
		req := plan.req
		req.Params.TimeLimit = "49h0m0s"
		err := slurm.CheckRequest(req, plan.profile, plan.settings)
		if err == nil || !strings.Contains(err.Error(), "maxTime") {
			t.Errorf("a 49h request on a 48h partition: %v; want a refusal that names maxTime", err)
		}
	})
	t.Run("a lease under the lead gives PartitionTooShort and no Allocation", func(t *testing.T) {
		plan := planFiles(t, athenaInput, func(_ *v1alpha1.Provider, md *v1alpha1.ModelDeployment) {
			d := v1alpha1.Duration("3h0m0s")
			md.Spec.LeaseLimit = &d
		})
		if len(plan.allocs) != 0 {
			t.Fatalf("a 3h lease made %d Allocations; the lead is 3h40m", len(plan.allocs))
		}
		if plan.degraded == nil || plan.degraded.Reason != v1alpha1.ReasonPartitionTooShort {
			t.Errorf("Degraded is %v; want PartitionTooShort", plan.degraded)
		}
	})
}

// "Only profiles/athena-now.yaml accepts a deployment on plgrid-now" (Alpha's limits). Worked out
// by hand from the two profiles:
//   - athena.yaml's lead is 3h40m, and plgrid-now's time limit is 1h, so the deployment is
//     Degraded with PartitionTooShort and gets no Allocation;
//   - athena-now.yaml's margins are drainWindow 7m, handoverMargin 1m and health.startTimeout 12m,
//     a lead of 20m, so a lease of 30m is placed and one of 20m is not (a time limit must be longer
//     than the lead). The request is within the partition's QOS plgrid-now: 1 GPU, 16 CPUs and
//     125Gi, which is 128000Mi.
func TestQuickstartPlgridNowTakesADeploymentOnlyUnderTheNowProfile(t *testing.T) {
	onNow := func(p *v1alpha1.Provider) { p.Spec.AllowedPartitions = []string{"plgrid-now"} }
	underNow := func(p *v1alpha1.Provider) {
		onNow(p)
		p.Spec.PlatformProfileRef.Name = "athena-now"
	}
	lease := func(s string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { d := v1alpha1.Duration(s); md.Spec.LeaseLimit = &d }
	}
	tooShort := func(t *testing.T, plan quickstartPlan) {
		t.Helper()
		if len(plan.allocs) != 0 {
			t.Fatalf("the controller made %d Allocations; want none", len(plan.allocs))
		}
		if plan.degraded == nil || plan.degraded.Reason != v1alpha1.ReasonPartitionTooShort {
			t.Errorf("Degraded is %v; want %s", plan.degraded, v1alpha1.ReasonPartitionTooShort)
		}
	}
	nowInput := athenaInput
	nowInput.profile = "profiles/athena-now.yaml"

	t.Run("under athena.yaml the deployment is PartitionTooShort", func(t *testing.T) {
		tooShort(t, planFiles(t, athenaInput, func(p *v1alpha1.Provider, _ *v1alpha1.ModelDeployment) { onNow(p) }))
	})
	t.Run("under athena-now.yaml a lease of 30m is placed and accepted", func(t *testing.T) {
		plan := planFiles(t, nowInput, func(p *v1alpha1.Provider, md *v1alpha1.ModelDeployment) { underNow(p); lease("30m0s")(md) })
		if len(plan.allocs) != 1 || plan.profileOK != nil {
			t.Fatalf("%d Allocations, profile read: %v; want 1 and nil (Degraded: %v)", len(plan.allocs), plan.profileOK, plan.degraded)
		}
		b := plan.allocs[0].Spec.ForProvider.Batch
		want := resource.MustParse("128000Mi")
		if b.Partition != "plgrid-now" || b.GPUs != 1 || b.CPUsPerTask != 16 || b.Nodes != 1 || b.Memory.Cmp(want) != 0 {
			t.Errorf("the request is %+v; want partition plgrid-now, 1 GPU, 16 CPUs, 1 node, 128000Mi", *b)
		}
		if b.TimeLimit != "30m0s" {
			t.Errorf("the time limit is %q; want the lease, 30m0s", b.TimeLimit)
		}
		if plan.checkErr != nil {
			t.Errorf("the Slurm provider refuses the request on plgrid-now: %v", plan.checkErr)
		}
	})
	t.Run("under athena-now.yaml a lease equal to the lead is too short", func(t *testing.T) {
		tooShort(t, planFiles(t, nowInput, func(p *v1alpha1.Provider, md *v1alpha1.ModelDeployment) { underNow(p); lease("20m0s")(md) }))
	})

	// The page says it, with the reason's own name and the limits the profile holds.
	page := readRepoFile(t, "docs/quickstart.md")
	if !strings.Contains(page, "`"+v1alpha1.ReasonPartitionTooShort+"`") {
		t.Errorf("docs/quickstart.md does not name %s, the reason a deployment on plgrid-now has under athena.yaml", v1alpha1.ReasonPartitionTooShort)
	}
}

// The quickstart says plgrid-now gives one GPU and one job at a time, for one hour. The numbers are
// read from profiles/athena-now.yaml, which holds them as typed fields.
func TestQuickstartStatesPlgridNowsLimitsAsTheProfileHasThem(t *testing.T) {
	var prof struct {
		Spec struct {
			Partitions []struct {
				Name    string `json:"name"`
				MaxTime string `json:"maxTime"`
			} `json:"partitions"`
			QOS []struct {
				Name             string `json:"name"`
				MaxSubmitPerUser int    `json:"maxSubmitPerUser"`
				MaxGPUsPerUser   int    `json:"maxGpusPerUser"`
			} `json:"qos"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "profiles/athena-now.yaml")), &prof); err != nil {
		t.Fatal(err)
	}
	var maxTime time.Duration
	for _, p := range prof.Spec.Partitions {
		if p.Name == "plgrid-now" {
			d, err := time.ParseDuration(p.MaxTime)
			if err != nil {
				t.Fatalf("plgrid-now's maxTime %q: %v", p.MaxTime, err)
			}
			maxTime = d
		}
	}
	jobs, gpus := 0, 0
	for _, q := range prof.Spec.QOS {
		if q.Name == "plgrid-now" {
			jobs, gpus = q.MaxSubmitPerUser, q.MaxGPUsPerUser
		}
	}
	if maxTime != time.Hour || jobs != 1 || gpus != 1 {
		t.Errorf("profiles/athena-now.yaml gives plgrid-now %v, %d jobs and %d GPUs; the quickstart says one hour, one job and one GPU", maxTime, jobs, gpus)
	}
	if want := "one GPU and one job at a time, for one hour"; !strings.Contains(readRepoFile(t, "docs/quickstart.md"), want) {
		t.Errorf("docs/quickstart.md no longer says %q", want)
	}
}

// The codes the quickstart names are the ones the code uses, so that renaming one shows here.
func TestCodesNamedInTheQuickstartAreTheCodesOfTheCode(t *testing.T) {
	page := readRepoFile(t, "docs/quickstart.md")
	for _, code := range []string{
		v1alpha1.ConditionCredentialsInvalid,
		v1alpha1.ReasonFrontNotConfigured,
		v1alpha1.ReasonPartitionTooShort,
		v1alpha1.ReasonSubmitLimitReached,
	} {
		if !strings.Contains(page, "`"+code+"`") {
			t.Errorf("docs/quickstart.md does not name `%s`", code)
		}
	}
}
