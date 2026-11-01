package launcher

import (
	"context"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func schemaGVR(group, version, resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
}

// ObjRec is what an upgrade must preserve about an object: its identity and its creation time.
type ObjRec struct {
	UID     string `json:"uid"`
	Created string `json:"created"`
}

// Snapshot maps "group/resource/namespace/name" to the object's record.
type Snapshot map[string]ObjRec

// ephemeral kinds are left out of upgrade checks: Events expire by design (a TTL of seven days),
// and Leases are renewed and recreated by controllers that restart with the host.
var ephemeral = map[string]bool{
	"/events":                    true,
	"events.k8s.io/events":       true,
	"coordination.k8s.io/leases": true,
}

// TakeSnapshot lists every object of every listable resource the apiserver serves (the
// preferred version of each), except the ephemeral kinds. Any resource that cannot be listed
// is an error: a snapshot that silently skips a kind could not show that the kind was kept.
func TakeSnapshot(ctx context.Context, cfg *rest.Config) (Snapshot, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	lists, err := dc.ServerPreferredResources()
	if err != nil {
		return nil, fmt.Errorf("discovery: %w", err)
	}
	snap := Snapshot{}
	for _, rl := range lists {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			return nil, err
		}
		for _, r := range rl.APIResources {
			if strings.Contains(r.Name, "/") || !hasVerb(r.Verbs, "list") || ephemeral[gv.Group+"/"+r.Name] {
				continue
			}
			gvr := gv.WithResource(r.Name)
			opts := metav1.ListOptions{Limit: 500}
			for {
				l, err := dyn.Resource(gvr).List(ctx, opts)
				if err != nil {
					return nil, fmt.Errorf("list %s: %w", gvr, err)
				}
				for _, o := range l.Items {
					key := fmt.Sprintf("%s/%s/%s/%s", gv.Group, r.Name, o.GetNamespace(), o.GetName())
					snap[key] = ObjRec{UID: string(o.GetUID()), Created: o.GetCreationTimestamp().UTC().Format("2006-01-02T15:04:05.999999999Z")}
				}
				if l.GetContinue() == "" {
					break
				}
				opts.Continue = l.GetContinue()
			}
		}
	}
	return snap, nil
}

func hasVerb(vs metav1.Verbs, v string) bool {
	for _, x := range vs {
		if x == v {
			return true
		}
	}
	return false
}

// Verification is the outcome of comparing two snapshots across an upgrade.
type Verification struct {
	Before  int      `json:"objects_before"`
	After   int      `json:"objects_after"`
	Kept    int      `json:"objects_kept"`
	Added   int      `json:"objects_added"`
	Missing []string `json:"missing,omitempty"`
	Changed []string `json:"changed,omitempty"`
}

// OK is true when every object from before is still there with its UID and creation time.
func (v Verification) OK() bool {
	return v.Before > 0 && v.Kept == v.Before && len(v.Missing) == 0 && len(v.Changed) == 0
}

// compareSnapshots is a subset check: the new version may add objects (default roles, say),
// but every object from before must be present with the same UID and creation time.
func compareSnapshots(before, after Snapshot) Verification {
	v := Verification{Before: len(before), After: len(after)}
	for k, b := range before {
		a, ok := after[k]
		switch {
		case !ok:
			v.Missing = append(v.Missing, k)
		case a != b:
			v.Changed = append(v.Changed, fmt.Sprintf("%s: uid %s->%s created %s->%s", k, b.UID, a.UID, b.Created, a.Created))
		default:
			v.Kept++
		}
	}
	for k := range after {
		if _, ok := before[k]; !ok {
			v.Added++
		}
	}
	sort.Strings(v.Missing)
	sort.Strings(v.Changed)
	return v
}
