// Package managedrecord keeps the identity record of a managed catalog entry's deployment ID (HBEP-9, the
// identity record of a managed catalog entry; HBEP-11, Identity records; issue #574): a ControllerRevision
// id-<first 12 hex> holding the canonical managed object and the full hash, created when a catalog loop first
// sees the entry, never rewritten, and with no owner reference, so it outlives the entry and every Provider
// that listed it. Both catalog loops use it: the managed controller (internal/managed) and the stand-in
// (pkg/provider/fake). The served path's record (internal/lifecycle, ensureIdentityRecord) has the same
// data shape and the same create-if-absent, read-and-compare rule.
package managedrecord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// data is a record's data, the shape a served ID's record has (internal/lifecycle's identityData).
type data struct {
	Scheme    string          `json:"scheme"`
	SHA256    string          `json:"sha256"`
	Canonical json.RawMessage `json:"canonical"`
}

// ErrCollision is a display-form collision: the record id-<first 12 hex> holds another deployment ID (HBEP-3,
// the collision rule). The ID is refused.
var ErrCollision = errors.New("display-form collision")

// Collision is ErrCollision with the record it met.
type Collision struct {
	Record, ResourceVersion, Stored, Want string
}

func (e *Collision) Error() string {
	return fmt.Sprintf("%v: identity record %s holds deployment ID %q, not %s", ErrCollision, e.Record, e.Stored, e.Want)
}

func (e *Collision) Unwrap() error { return ErrCollision }

// Ensure makes sure the identity record of entry's managed ID exists in namespace: it creates the record when
// absent, and otherwise reads it and compares its stored hash, never rewriting it. It returns the record's
// name. A record that holds another ID is a *Collision; any other failure to read or create it is returned as
// it is. r reads past any cache, so a record another writer just created is seen.
func Ensure(ctx context.Context, c client.Client, r client.Reader, namespace string, entry deploymentid.CatalogEntry, fieldOwner string) (string, error) {
	id, err := deploymentid.ComputeManaged(entry)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(data{Scheme: deploymentid.Scheme, SHA256: id.SHA256, Canonical: json.RawMessage(id.Canonical)})
	if err != nil {
		return "", err
	}
	rec, err := v1alpha1.NewManagedIdentityRecord(namespace, entry.Platform, entry.CatalogName, id.SHA256, raw)
	if err != nil {
		return "", err
	}
	var got appsv1.ControllerRevision
	err = r.Get(ctx, client.ObjectKeyFromObject(rec), &got)
	switch {
	case apierrors.IsNotFound(err):
		cerr := c.Create(ctx, rec, client.FieldOwner(fieldOwner))
		if cerr == nil {
			return rec.Name, nil
		}
		if !apierrors.IsAlreadyExists(cerr) {
			return "", fmt.Errorf("create identity record %s/%s: %w", namespace, rec.Name, cerr)
		}
		// Another writer created it first: read what it holds.
		if err := r.Get(ctx, client.ObjectKeyFromObject(rec), &got); err != nil {
			return "", fmt.Errorf("read identity record %s/%s: %w", namespace, rec.Name, err)
		}
	case err != nil:
		return "", fmt.Errorf("read identity record %s/%s: %w", namespace, rec.Name, err)
	}
	var stored data
	if err := json.Unmarshal(got.Data.Raw, &stored); err != nil || stored.SHA256 != id.SHA256 {
		return "", &Collision{Record: rec.Name, ResourceVersion: got.ResourceVersion, Stored: stored.SHA256, Want: id.SHA256}
	}
	return rec.Name, nil
}
