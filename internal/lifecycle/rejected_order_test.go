package lifecycle

// Review of PR 171 (Opus n5): with two Allocations refused for the same
// reason, the Degraded message lists them in the order of their names, on every
// pass. The Allocations are kept in a map, whose iteration order changes from
// one range to the next, so an unsorted loop shows a different order from pass
// to pass.

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestSubmissionRejectedMessagesAreInNameOrder(t *testing.T) {
	rg := newRig(t)
	refusal := pfake.Script{Outcome: provider.Rejected, Reason: "example refusal"}
	rg.batch.Enqueue(refusal, refusal)
	md := mdSpec("md")
	md.Spec.Instances = 2
	rg.create(md)
	rg.until(time.Minute, 10*time.Second, "both Allocations refused", func() bool {
		n := 0
		for _, a := range rg.allocations("md") {
			if s := a.Status.AtProvider.Submission; s != nil && s.State == v1alpha1.SubmissionRejected {
				n++
			}
		}
		return n == 2
	})
	var names []string
	for _, a := range rg.allocations("md") {
		names = append(names, a.Name)
	}
	sort.Strings(names)
	for i := range 40 { // the same instant, so both stay refused
		if err := rg.reconcileOf("md"); err != nil {
			t.Fatal(err)
		}
		msg := rg.condition("md", v1alpha1.ConditionDegraded).Message
		first, second := strings.Index(msg, "Allocation "+names[0]), strings.Index(msg, "Allocation "+names[1])
		if first < 0 || second < first {
			t.Fatalf("pass %d: message %q does not list %s before %s", i, msg, names[0], names[1])
		}
	}
}
