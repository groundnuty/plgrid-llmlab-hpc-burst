package lifecycle

import (
	"fmt"
	"maps"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Owner and purpose (HBEP-10, Owner and purpose; issue #662). The lifecycle controller copies a ModelDeployment's
// owner and purpose annotations to every Allocation and Instance it creates, adds the owner-hash label that selects an
// owner's objects, and keeps the ModelDeployment's own label true to its owner annotation. An empty annotation is no
// annotation: the inventory shows an absent owner as unknown, and an owner-hash of no name selects nothing a person
// could name. Nothing is backfilled: an object created before this copy keeps what it has, and shows its owner as
// unknown until it is replaced (`@hpc-burst-science-agent`, 17 Nov 2026, #662, Q5).

// ownerMeta returns the annotations and labels to stamp on an Allocation or Instance created for md (HBEP-10, Owner and
// purpose):
//   - with an owner annotation: the owner annotation and the owner-hash label of its value;
//   - with a purpose annotation: the purpose annotation, owner or not (`@hpc-burst-science-agent`, 17 Nov 2026, #662,
//     S5: a purpose is public text by design);
//   - with no owner annotation: no owner annotation and no label.
//
// A nil map is returned when there is nothing to stamp in it.
func ownerMeta(md *v1alpha1.ModelDeployment) (annotations, labels map[string]string) {
	if o := md.Annotations[v1alpha1.AnnotationOwner]; o != "" {
		annotations = map[string]string{v1alpha1.AnnotationOwner: o}
		labels = map[string]string{v1alpha1.LabelOwnerHash: v1alpha1.OwnerHash(o)}
	}
	if pu := md.Annotations[v1alpha1.AnnotationPurpose]; pu != "" {
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[v1alpha1.AnnotationPurpose] = pu
	}
	return annotations, labels
}

// stampOwner adds ownerMeta(md) to the metadata of an Allocation or Instance about to be created. It adds keys and
// replaces no map, so every annotation and label that the create already sets stays; call it after the create has set
// them all, as a later assignment of a whole map would still drop what it stamped. A ModelDeployment with nothing to
// stamp leaves a nil map nil. It changes obj in place, which is the point of a stamp on a new object.
func stampOwner(obj *metav1.ObjectMeta, md *v1alpha1.ModelDeployment) {
	annotations, labels := ownerMeta(md)
	obj.Annotations = withKeys(obj.Annotations, annotations)
	obj.Labels = withKeys(obj.Labels, labels)
}

// withKeys returns dst with add's entries, as a new map; dst when add is empty.
func withKeys(dst, add map[string]string) map[string]string {
	if len(add) == 0 {
		return dst
	}
	out := maps.Clone(dst)
	if out == nil {
		out = make(map[string]string, len(add))
	}
	maps.Copy(out, add)
	return out
}

// syncOwnerLabel keeps the ModelDeployment's own owner-hash label true to its owner annotation, once per pass, right
// after the deployment is read (`@hpc-burst-science-agent`, 17 Nov 2026, #662, Q4): it sets the label when it is
// absent, corrects a wrong one (an agent can set any label at create, and the capacity guard only stops a later edit),
// and removes it when there is no owner annotation. The write is a merge patch of the labels alone, under the
// lifecycle controller's field manager, so it touches no spec, annotation or other label and carries no
// resourceVersion to conflict on. It writes nothing when the label is right, and nothing for a deployment being
// deleted. On success p.md is the stored object, so the pass's status write carries the new resourceVersion.
func (p *pass) syncOwnerLabel() error {
	md := p.md
	if md == nil || !md.DeletionTimestamp.IsZero() {
		return nil
	}
	_, want := ownerMeta(md)
	wantHash, wanted := want[v1alpha1.LabelOwnerHash]
	gotHash, has := md.Labels[v1alpha1.LabelOwnerHash]
	if wanted == has && wantHash == gotHash {
		return nil // right already; with neither wanted nor present both hashes are empty
	}
	next := md.DeepCopy()
	if wanted {
		next.Labels = withKeys(next.Labels, want)
	} else {
		delete(next.Labels, v1alpha1.LabelOwnerHash)
	}
	if err := p.r.Client.Patch(p.ctx, next, client.MergeFrom(md), owner); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // deleted since the read: the next pass finds it gone
		}
		return fmt.Errorf("set the owner-hash label of ModelDeployment %s: %w", p.key, err)
	}
	*p.md = *next
	return nil
}
