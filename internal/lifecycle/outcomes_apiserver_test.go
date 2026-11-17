package lifecycle

// The start-up outcomes on a real API server (#604 PR 4; #620's CEL rules). The rig's fake client runs no CEL, so this
// test starts internal/testenv's control plane with the shipped CRDs, writes a status the controller computed
// (recordStartOutcomes, its counts from classCounts) and reads it back; the same status with qualificationFailures
// off by one is refused, so the rule is shown to run. It uses #61's envtest block, 29204-29207, so run it under
// envtest.lock.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

func TestRecordedOutcomesPassTheAPIServersRules(t *testing.T) {
	t.Setenv("HPCBURST_TESTENV_PORT_BASE", managerPortBase)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cp, err := testenv.Start(ctx, t.TempDir(), testenv.Options{BeforeControllerManager: installTestCRDs})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	defer func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	}()
	s, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := client.New(cp.Config, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: rigNS}}); err != nil {
		t.Fatal(err)
	}
	md := mdSpec(hoMD)
	if err := cl.Create(ctx, md); err != nil {
		t.Fatalf("create the deployment: %v", err)
	}

	// Three Allocations' first Instances: one ready, one failed verification on a rehash, one failed qualification.
	r, e := hoT(8, 1, 10), hoT(8, 5, 0)
	insts := []*v1alpha1.Instance{
		outcomeInstance("i0", "a0", hoT(8, 0, 0), &r, nil, "", ""),
		outcomeInstance("i1", "a1", hoT(8, 0, 1), nil, &e, v1alpha1.ReasonVerificationFailed, "ver-rehash"),
		outcomeInstance("i2", "a2", hoT(8, 0, 2), nil, &e, v1alpha1.ReasonQualificationFailed, ""),
	}
	allocs := []*v1alpha1.Allocation{started("a0", "u0"), started("a1", "u1"), started("a2", "u2")}
	p := outcomePass(insts, allocs, map[string]string{"ver-rehash": causeRehash})

	var got v1alpha1.ModelDeployment
	if err := cl.Get(ctx, client.ObjectKeyFromObject(md), &got); err != nil {
		t.Fatal(err)
	}
	p.recordStartOutcomes(&got.Status)
	c := got.Status.StartOutcomes[0]
	if c.Count != 3 || c.Failures != 2 || c.QualificationFailures != 2 {
		t.Fatalf("computed %d of %d, %d in qualification; want 2 of 3, 2", c.Failures, c.Count, c.QualificationFailures)
	}
	if err := cl.Status().Update(ctx, &got); err != nil {
		t.Fatalf("the API server refused the status the controller computed: %v", err)
	}
	var back v1alpha1.ModelDeployment
	if err := cl.Get(ctx, client.ObjectKeyFromObject(md), &back); err != nil {
		t.Fatal(err)
	}
	if b := back.Status.StartOutcomes; len(b) != 1 || b[0].Count != 3 || b[0].QualificationFailures != 2 || len(b[0].Outcomes) != 3 {
		t.Fatalf("read back %+v, want the 3 outcomes, 2 in qualification", b)
	}
	// The same status with qualificationFailures off by one: the rule refuses it.
	back.Status.StartOutcomes[0].QualificationFailures = 1
	err = cl.Status().Update(ctx, &back)
	if err == nil || !strings.Contains(err.Error(), "qualificationFailures") {
		t.Fatalf("a status with qualificationFailures off by one was written (%v): the rule did not run, so the test proves nothing", err)
	}
}
