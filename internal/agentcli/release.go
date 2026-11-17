package agentcli

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// maxReleaseNames is the most Allocations a Release names (api/v1alpha1: release.allocations).
const maxReleaseNames = 64

// cmdRelease is hpcburst release <allocation>... | -l <selector>.
func (rt *cli) cmdRelease(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs, o := newFlags("release", stderr, true)
	var selector string
	fs.StringVar(&selector, "l", "", "release the Allocations that match this label selector")
	fs.StringVar(&selector, "selector", "", "release the Allocations that match this label selector (same as -l)")
	names, code, done := parseArgs(fs, args)
	if done {
		return code
	}
	switch {
	case len(names) == 0 && selector == "":
		return failf(stderr, exitUsage, "release: name the Allocations to release, or give -l <selector>")
	case len(names) > 0 && selector != "":
		return failf(stderr, exitUsage, "release: name Allocations or give -l <selector>, not both")
	}
	if code := checkOptions(o, stderr); code != exitOK {
		return code
	}
	cl, ns, code := rt.open(ctx, o, stderr)
	if code != exitOK {
		return code
	}
	groups, code := findAllocations(ctx, cl.c, ns, names, selector, stderr)
	if code != exitOK {
		return code
	}
	reqs, code := rt.releaseRequests(o, groups, selector, stderr)
	if code != exitOK {
		return code
	}
	return rt.createFlow(ctx, o, reqs, stdin, stdout, stderr)
}

// allocationGroup is the Allocations of one ModelDeployment, by name.
type allocationGroup struct {
	md    string
	names []string
}

// findAllocations reads the named Allocations, or lists those the selector matches, and groups them by
// spec.modelDeploymentRef.name: never by the model-deployment label, which a long name shortens. It
// creates nothing; a name that is not an Allocation, or a selector that matches none, is a refusal.
func findAllocations(ctx context.Context, c client.Client, ns string, names []string, selector string, stderr io.Writer) ([]allocationGroup, int) {
	var allocs []v1alpha1.Allocation
	if selector != "" {
		sel, err := metav1.ParseToLabelSelector(selector)
		if err != nil {
			return nil, failf(stderr, exitUsage, "release: -l %q is not a label selector: %v", selector, err)
		}
		ls, err := metav1.LabelSelectorAsSelector(sel)
		if err != nil {
			return nil, failf(stderr, exitUsage, "release: -l %q is not a label selector: %v", selector, err)
		}
		var list v1alpha1.AllocationList
		if err := c.List(ctx, &list, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: ls}); err != nil {
			return nil, failf(stderr, exitFail, "list Allocations: %v", err)
		}
		if len(list.Items) == 0 {
			return nil, failf(stderr, exitFail, "release: no Allocation in namespace %s matches %q; nothing was created", ns, selector)
		}
		allocs = list.Items
	}
	for _, n := range dedupe(names) {
		a := v1alpha1.Allocation{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: n}, &a); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, failf(stderr, exitFail, "release: Allocation %s not found in namespace %s; nothing was created", n, ns)
			}
			return nil, failf(stderr, exitFail, "read Allocation %s: %v", n, err)
		}
		allocs = append(allocs, a)
	}
	groups, orphans := groupAllocations(allocs)
	for _, n := range orphans {
		if selector == "" {
			return nil, failf(stderr, exitFail, "release: Allocation %s names no ModelDeployment; nothing was created", n)
		}
		fmt.Fprintf(stderr, "hpcburst: Allocation %s names no ModelDeployment: skipped\n", n)
	}
	if len(groups) == 0 {
		return nil, failf(stderr, exitFail, "release: no Allocation names a ModelDeployment; nothing was created")
	}
	return groups, exitOK
}

// groupAllocations groups Allocations by spec.modelDeploymentRef.name, in the order of the names, each
// group's names sorted. Allocations that name none are returned apart.
func groupAllocations(allocs []v1alpha1.Allocation) (groups []allocationGroup, orphans []string) {
	by := map[string][]string{}
	for i := range allocs {
		a := &allocs[i]
		if a.Spec.ModelDeploymentRef == nil || a.Spec.ModelDeploymentRef.Name == "" {
			orphans = append(orphans, a.Name)
			continue
		}
		by[a.Spec.ModelDeploymentRef.Name] = append(by[a.Spec.ModelDeploymentRef.Name], a.Name)
	}
	for md, ns := range by {
		sort.Strings(ns)
		groups = append(groups, allocationGroup{md: md, names: slices.Compact(ns)})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].md < groups[j].md })
	sort.Strings(orphans)
	return groups, orphans
}

// releaseRequests makes one Operation request per ModelDeployment. Named Allocations go into
// release.allocations and a selector goes into release.selector, whole: the controller keeps only the
// matches of the Operation's own ModelDeployment. A name past 253 characters is refused here, before
// anything is created.
func (rt *cli) releaseRequests(o *options, groups []allocationGroup, selector string, stderr io.Writer) ([]opRequest, int) {
	var sel *metav1.LabelSelector
	if selector != "" {
		sel, _ = metav1.ParseToLabelSelector(selector) // parsed once already
	}
	var reqs []opRequest
	for _, g := range groups {
		rel := &v1alpha1.ReleaseParameters{Selector: sel}
		if sel == nil {
			if len(g.names) > maxReleaseNames {
				return nil, failf(stderr, exitUsage, "release: %d Allocations of ModelDeployment %s; an Operation names at most %d. Use -l <selector>; nothing was created", len(g.names), g.md, maxReleaseNames)
			}
			rel.Allocations = g.names
		}
		name := rt.nameFor(o)
		if o.key != "" && len(groups) > 1 {
			name = derivedKey(o.key, g.md)
		}
		if len(name) > maxName {
			return nil, failf(stderr, exitUsage, "release: the name %q... for ModelDeployment %s is %d characters; at most %d. Give a shorter --key; nothing was created", name[:40], g.md, len(name), maxName)
		}
		reqs = append(reqs, opRequest{name: name, spec: v1alpha1.OperationSpec{
			Action: v1alpha1.OperationActionRelease, ModelDeploymentRef: v1alpha1.ObjectReference{Name: g.md}, Release: rel,
		}})
	}
	return reqs, exitOK
}

// dedupe drops repeated names, keeping the first of each.
func dedupe(names []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, n := range names {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}
