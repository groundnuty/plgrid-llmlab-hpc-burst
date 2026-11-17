package lifecycle

// The fixture of the owner tests (owner.go; HBEP-10, Owner and purpose; issue #662): a recorder over the rig's client
// that keeps the labels and annotations of every Allocation and Instance as they were at the Create call, and every
// write to a ModelDeployment's metadata. A test reads what the controller sent, not what a helper built. Every expected
// value in these tests is written out by hand; none comes from ownerMeta or OwnerHash.

import (
	"context"
	"maps"
	"sync"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The keys and values the owner tests expect, spelled out. The hashes are worked out by hand (python's hashlib and
// sha256sum) and are the same ones api/v1alpha1/ownerhash_test.go pins.
const (
	keyOwner       = "hpcburst.groundnuty.github.io/owner"
	keyPurpose     = "hpcburst.groundnuty.github.io/purpose"
	keyOwnerHash   = "hpcburst.groundnuty.github.io/owner-hash"
	keyHedgeGroup  = "hpcburst.groundnuty.github.io/hedge-group"
	keyPredecessor = "hpcburst.groundnuty.github.io/predecessor"

	ownerA = "system:serviceaccount:hpcburst-agents:agent-a"
	ownerB = "system:serviceaccount:hpcburst-agents:agent-b"
	hashA  = "u-77d125ed9ec1"
	hashB  = "u-494c5a26e66f"

	wrongHash = "u-000000000000"
)

// ownerRecord is an Allocation or an Instance as the Create call carried it.
type ownerRecord struct {
	kind        string // "Allocation" or "Instance"
	name        string
	labels      map[string]string
	annotations map[string]string
}

// ownerPatch is a Patch call on a ModelDeployment: the JSON merge patch it sent and its field manager.
type ownerPatch struct {
	name, body, manager string
}

// ownerLog records the creates of Allocations and Instances, the Patch and Update calls on ModelDeployments, and the
// Patch calls on Allocations and Instances (their bodies), all made through the rig's client, the tests' own included:
// a test reads the counts before and after the pass it judges.
type ownerLog struct {
	mu           sync.Mutex
	created      []ownerRecord
	mdPatches    []ownerPatch
	mdUpdates    int
	childPatches []string
	// failMDPatch, when set, is what every Patch of a ModelDeployment returns, before anything is written.
	failMDPatch error
}

func (l *ownerLog) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			var kind string
			switch obj.(type) {
			case *v1alpha1.Allocation:
				kind = "Allocation"
			case *v1alpha1.Instance:
				kind = "Instance"
			}
			rec := ownerRecord{kind: kind, name: obj.GetName(), labels: maps.Clone(obj.GetLabels()), annotations: maps.Clone(obj.GetAnnotations())}
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			if kind != "" {
				l.mu.Lock()
				l.created = append(l.created, rec)
				l.mu.Unlock()
			}
			return nil
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			l.mu.Lock()
			fail := l.failMDPatch
			l.mu.Unlock()
			if _, ok := obj.(*v1alpha1.ModelDeployment); ok && fail != nil {
				return fail
			}
			body, err := patch.Data(obj) // before the call: the call overwrites obj with the stored object
			if err != nil {
				return err
			}
			var po client.PatchOptions
			po.ApplyOptions(opts)
			if err := c.Patch(ctx, obj, patch, opts...); err != nil {
				return err
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			switch obj.(type) {
			case *v1alpha1.ModelDeployment:
				l.mdPatches = append(l.mdPatches, ownerPatch{name: obj.GetName(), body: string(body), manager: po.FieldManager})
			case *v1alpha1.Allocation, *v1alpha1.Instance:
				l.childPatches = append(l.childPatches, string(body))
			}
			return nil
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*v1alpha1.ModelDeployment); ok {
				l.mu.Lock()
				l.mdUpdates++
				l.mu.Unlock()
			}
			return c.Update(ctx, obj, opts...)
		},
	}
}

// failPatches makes every Patch of a ModelDeployment fail with err from now on; nil lets them through.
func (l *ownerLog) failPatches(err error) {
	l.mu.Lock()
	l.failMDPatch = err
	l.mu.Unlock()
}

// createdOf returns the records of one kind created for a deployment, in creation order.
func (l *ownerLog) createdOf(kind, md string) []ownerRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []ownerRecord
	for _, r := range l.created {
		if r.kind == kind && r.labels[v1alpha1.LabelModelDeployment] == md {
			out = append(out, r)
		}
	}
	return out
}

// record returns the create record of the named object.
func (l *ownerLog) record(t *testing.T, kind, name string) ownerRecord {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.created {
		if r.kind == kind && r.name == name {
			return r
		}
	}
	t.Fatalf("no %s %s was created", kind, name)
	return ownerRecord{}
}

// patchesOf returns the Patch calls made on the named ModelDeployment.
func (l *ownerLog) patchesOf(md string) []ownerPatch {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []ownerPatch
	for _, p := range l.mdPatches {
		if p.name == md {
			out = append(out, p)
		}
	}
	return out
}

// mdWrites counts the Patch and Update calls on every ModelDeployment so far.
func (l *ownerLog) mdWrites() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.mdPatches) + l.mdUpdates
}

// ownedMD is mdSpec with an owner and a purpose annotation, each left out when empty.
func ownedMD(name, owner, purpose string) *v1alpha1.ModelDeployment {
	md := mdSpec(name)
	md.Annotations = map[string]string{}
	if owner != "" {
		md.Annotations[keyOwner] = owner
	}
	if purpose != "" {
		md.Annotations[keyPurpose] = purpose
	}
	if len(md.Annotations) == 0 {
		md.Annotations = nil
	}
	return md
}

// without returns m without the keys, as a new map.
func without(m map[string]string, keys ...string) map[string]string {
	out := maps.Clone(m)
	for _, k := range keys {
		delete(out, k)
	}
	return out
}

// sameMap reports whether two maps hold the same entries; nil and empty are the same.
func sameMap(a, b map[string]string) bool { return len(a) == len(b) && maps.Equal(a, b) }

// wantMap fails the test unless got holds exactly want.
func wantMap(t *testing.T, what string, got, want map[string]string) {
	t.Helper()
	if !sameMap(got, want) {
		t.Errorf("%s = %v, want exactly %v", what, got, want)
	}
}

// mdMeta returns a stored ModelDeployment's labels and annotations.
func (rg *rig) mdMeta(name string) (labels, annotations map[string]string) {
	rg.t.Helper()
	md := rg.md(name)
	return md.Labels, md.Annotations
}
