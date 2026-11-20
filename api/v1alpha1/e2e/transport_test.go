package e2e

import (
	"fmt"
	"testing"

	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// transport.loginHostBudget is an integer from LoginHostBudgetMin to LoginHostBudgetMax (HBEP-4, open question 17,
// amended 2026-11-20): 5 leaves a cap of 2 beside one master, and 17 a cap of 8, the relay keeper's bound on a set of
// Allocations. The two bounds are admission rules whose literals cannot name the constants, so the rows below are
// written from the constants: a constant moved without its rule, or a rule without its constant, fails here, and
// hack/mutate-cel.sh neutralises each rule alone to show that this test fails without it.
func TestLoginHostBudgetBounds(t *testing.T) {
	for _, tc := range []struct {
		budget int32
		want   string // "" accepts; else the refusal's words
	}{
		{v1alpha1.LoginHostBudgetMin - 1, fmt.Sprintf("must be at least %d", v1alpha1.LoginHostBudgetMin)},
		{0, "must be at least"},
		{-3, "must be at least"},
		{v1alpha1.LoginHostBudgetMin, ""},
		{9, ""},
		{11, ""},
		{v1alpha1.LoginHostBudgetMax, ""},
		{v1alpha1.LoginHostBudgetMax + 1, fmt.Sprintf("must be at most %d", v1alpha1.LoginHostBudgetMax)},
		{1000, "must be at most"},
	} {
		t.Run(fmt.Sprint(tc.budget), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Transport = &v1alpha1.TransportProfile{LoginHostBudget: ptr.To(tc.budget)}
			if tc.want != "" {
				requireRefused(t, c.Create(bg(), p), "loginHostBudget", tc.want)
				return
			}
			must(t, c.Create(bg(), p), fmt.Sprintf("create with loginHostBudget %d", tc.budget))
			cleanup(t, p)
			var got v1alpha1.PlatformProfile
			must(t, c.Get(bg(), client.ObjectKeyFromObject(p), &got), "read it back")
			if got.Spec.Transport == nil || got.Spec.Transport.LoginHostBudget == nil || *got.Spec.Transport.LoginHostBudget != tc.budget {
				t.Fatalf("read back %+v, want loginHostBudget %d (a field the CRD does not hold is pruned)", got.Spec.Transport, tc.budget)
			}
		})
	}
}

// An absent transport, and a transport with no budget, are admitted: the field is optional, and a public profile leaves
// it unset (the cap is then 2).
func TestLoginHostBudgetIsOptional(t *testing.T) {
	p := newProfile(uniq("prof"))
	must(t, c.Create(bg(), p), "create with no transport")
	cleanup(t, p)
	q := newProfile(uniq("prof"))
	q.Spec.Transport = &v1alpha1.TransportProfile{}
	must(t, c.Create(bg(), q), "create with an empty transport")
	cleanup(t, q)
}
