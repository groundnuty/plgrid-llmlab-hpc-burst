package lifecycle

// The plan's ID (HBEP-10, Operations, The plan): the first 16 hex characters of SHA-256 over a canonical
// JSON form. It is opaque to clients, who compare and quote it and never parse it (the CRD says so).
//
// It covers the action; the ModelDeployment's UID and generation; each target's kind, name, UID,
// generation, effect and needsApproval, sorted by kind then name; and each target's policies by name, UID
// and generation. It leaves out the costs and every time, which age, and the approvalRule text, which
// only explains needsApproval: a plan whose only change is a figure keeps its ID, so the owner's confirm
// is never made stale by a queue wait that moved.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// idForm is the canonical form: fixed field order, sorted lists.
type idForm struct {
	Action     v1alpha1.OperationAction `json:"action"`
	Deployment idIdentity               `json:"modelDeployment"`
	Targets    []idTarget               `json:"targets"`
}

type idIdentity struct {
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
}

type idTarget struct {
	Kind          string     `json:"kind"`
	Name          string     `json:"name"`
	UID           string     `json:"uid"`
	Generation    int64      `json:"generation"`
	Effect        string     `json:"effect"`
	NeedsApproval bool       `json:"needsApproval"`
	Policies      []idPolicy `json:"policies"`
}

type idPolicy struct {
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
}

// targetBefore orders targets by kind, then name.
func targetBefore(a, b v1alpha1.PlanTarget) bool {
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	return a.Name < b.Name
}

// planID computes the ID of a plan of an action on a deployment with these targets.
func planID(action v1alpha1.OperationAction, md v1alpha1.PlannedDeployment, targets []v1alpha1.PlanTarget) string {
	f := idForm{Action: action, Deployment: idIdentity{UID: string(md.UID), Generation: md.Generation}, Targets: []idTarget{}}
	sorted := slices.Clone(targets)
	slices.SortFunc(sorted, func(a, b v1alpha1.PlanTarget) int {
		switch {
		case targetBefore(a, b):
			return -1
		case targetBefore(b, a):
			return 1
		}
		return 0
	})
	for _, t := range sorted {
		it := idTarget{
			Kind: string(t.Kind), Name: t.Name, UID: string(t.UID), Generation: t.Generation,
			Effect: string(t.Effect), NeedsApproval: t.NeedsApproval, Policies: []idPolicy{},
		}
		for _, p := range t.Policies {
			it.Policies = append(it.Policies, idPolicy{Name: p.Name, UID: string(p.UID), Generation: p.Generation})
		}
		slices.SortFunc(it.Policies, func(a, b idPolicy) int {
			switch {
			case a.Name < b.Name:
				return -1
			case a.Name > b.Name:
				return 1
			}
			return 0
		})
		f.Targets = append(f.Targets, it)
	}
	raw, err := json.Marshal(f)
	if err != nil {
		panic("lifecycle: a plan's canonical form does not marshal: " + err.Error()) // fixed types, cannot fail
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:16]
}
