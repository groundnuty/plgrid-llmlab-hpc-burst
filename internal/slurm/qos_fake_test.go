package slurm_test

// The QOS rules end to end over the fake Slurm: the flag in the argv log, the
// refusals before any call, and the limit refusal Slurm gives. Counts come from
// the fake's call log, never from return values (fakeslurm doc, Call log).

import (
	"context"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const qosDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func qosRig(t *testing.T, qos string, limits []fakeslurm.QOS) (*slurm.Batch, *fakeslurm.Platform) {
	t.Helper()
	clock := fake.NewClock(fakeStart)
	plat := fakeslurm.New(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: fakePart}}, QOS: limits})
	sess := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": fakeHome, "SCRATCH": fakeScratch}})
	profile := exampleProfile("$SCRATCH/stage")
	one := int32(1)
	profile.Spec.QOS = []v1alpha1.QOSProfile{{Name: "now", MaxGPUsPerUser: &one}, {Name: "normal"}}
	settings := provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart, QOS: qos}
	return slurm.NewBatch(sess, profile, settings, slurm.Options{Clock: clock}), plat
}

func qosReq(tag, qos string) provider.SubmitRequest {
	return provider.SubmitRequest{
		Tag: tag, Hold: true, Account: "example-account", QOS: qos,
		Params: v1alpha1.BatchParameters{Partition: fakePart, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8,
			Memory: resource.MustParse("16Gi"), Nodes: 1, AgentDigest: qosDigest},
	}
}

func sbatchWords(plat *fakeslurm.Platform) [][]string {
	var out [][]string
	for _, c := range plat.Calls() {
		if c.Words[0] == "sbatch" {
			out = append(out, c.Words)
		}
	}
	return out
}

// A Provider with qos now sends --qos=now; one with no qos sends no --qos at
// all, neither the flag nor an empty value (HBEP-4, Test plan).
func TestSubmitSendsTheQOSFlagOnlyWhenSet(t *testing.T) {
	for _, c := range []struct {
		qos  string
		want string // the --qos word, "" for none
	}{{"now", "--qos=now"}, {"", ""}} {
		t.Run("qos="+c.qos, func(t *testing.T) {
			b, plat := qosRig(t, c.qos, nil)
			if _, outcome, err := b.Submit(context.Background(), qosReq("hb-example-0123456789ab", c.qos)); outcome != provider.Accepted {
				t.Fatalf("Submit = %s, %v, want Accepted", outcome, err)
			}
			calls := sbatchWords(plat)
			if len(calls) != 1 {
				t.Fatalf("%d sbatch calls, want 1", len(calls))
			}
			var got []string
			for _, w := range calls[0] {
				if strings.HasPrefix(w, "--qos") || w == "" {
					got = append(got, w)
				}
			}
			switch {
			case c.want == "" && len(got) != 0:
				t.Errorf("no qos is set, but the argv holds %q", got)
			case c.want != "" && (len(got) != 1 || got[0] != c.want):
				t.Errorf("argv qos words %q, want [%s]", got, c.want)
			}
		})
	}
}

// A request that HBEP-4 refuses before any call makes no call, the QOS ones
// included: the limits and the flag are checked before sbatch runs.
func TestQOSRefusalsMakeNoCall(t *testing.T) {
	b, plat := qosRig(t, "now", nil)
	req := qosReq("hb-example-0123456789ab", "now")
	req.Params.GPUs = 2 // within gpusPerNode 4 of the partition, above now's maxGpusPerUser 1
	_, outcome, err := b.Submit(context.Background(), req)
	if outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) || !strings.Contains(err.Error(), "maxGpusPerUser") {
		t.Fatalf("Submit = %s, %v, want Rejected, ErrInvalidRequest and the bound named", outcome, err)
	}
	if n := len(plat.Calls()); n != 0 {
		t.Errorf("%d calls, want 0: %v", n, plat.Calls())
	}
}

// A full per-user submit limit: the fake refuses with both lines, and Submit
// returns a PolicyRefusal for the reason name with the outcome Rejected, and
// records no job. After the other job ends, the next attempt is accepted.
func TestSubmitUnderAFullSlotIsAPolicyRefusal(t *testing.T) {
	limits := []fakeslurm.QOS{{Name: "now", MaxSubmitPerUser: fakeslurm.Cap(1)}, {Name: "normal"}}
	b, plat := qosRig(t, "now", limits)
	other := plat.OutsideJob("example-user", "another-project-job", fakePart, "now", 1)

	id, outcome, err := b.Submit(context.Background(), qosReq("hb-example-0123456789ab", "now"))
	var pr *provider.PolicyRefusal
	if id != "" || outcome != provider.Rejected || !errors.As(err, &pr) {
		t.Fatalf("Submit = %q, %s, %v; want no ID, Rejected and a PolicyRefusal", id, outcome, err)
	}
	if pr.Reason != "QOSMaxSubmitJobPerUserLimit" || !pr.SubmitLimit() || pr.Line != "sbatch: error: QOSMaxSubmitJobPerUserLimit" {
		t.Errorf("refusal %+v, want the submit-limit reason and its reason line", pr)
	}
	if n := plat.Count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls, want 1 (the outside job is no call)", n)
	}

	if err := plat.End(other, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	if _, outcome, err := b.Submit(context.Background(), qosReq("hb-example-0123456789ab", "now")); outcome != provider.Accepted {
		t.Fatalf("after the other job ended: Submit = %s, %v, want Accepted", outcome, err)
	}
}

// A refusal whose reason names another limit is a PolicyRefusal that is no
// submit limit; one with no reason line is no PolicyRefusal at all.
func TestSubmitOtherRefusals(t *testing.T) {
	b, plat := qosRig(t, "now", nil)
	plat.Script(fakeslurm.RejectPolicy("QOSMaxWallDurationPerJobLimit"), fakeslurm.Reject("Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"))

	_, outcome, err := b.Submit(context.Background(), qosReq("hb-example-0123456789ab", "now"))
	var pr *provider.PolicyRefusal
	if outcome != provider.Rejected || !errors.As(err, &pr) || pr.Reason != "QOSMaxWallDurationPerJobLimit" || pr.SubmitLimit() {
		t.Fatalf("Submit = %s, %v; want Rejected and a PolicyRefusal that is no submit limit", outcome, err)
	}
	_, outcome, err = b.Submit(context.Background(), qosReq("hb-example-0123456789ab", "now"))
	if outcome != provider.Rejected || err == nil || errors.As(err, &pr) {
		t.Fatalf("Submit = %s, %v; want an ordinary Rejected outcome, no PolicyRefusal", outcome, err)
	}
	if !strings.Contains(err.Error(), "Job violates accounting/QOS policy") {
		t.Errorf("the ordinary Rejected text %q lost Slurm's words", err)
	}
}
