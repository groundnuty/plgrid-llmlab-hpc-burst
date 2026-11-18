// Package inventory is HBEP-10's inventory (issue #662, beta step 5): one read-only query that shows
// every ModelDeployment, Allocation and Instance the caller can read, with owner, purpose, state and
// provider facts, each Provider's last successful read with its age, and the Operations waiting for
// approval. docs/agent-cli.md, "The inventory", is its contract: what each row shows and the shape of
// --json.
//
// The package has two halves that stay apart. The lister (list.go) reads through a client.Reader, so it
// cannot write by type, and returns an Inventory of rows. The printer (print.go) takes an Inventory and
// writes text or JSON; it reads nothing. Neither imports internal/lifecycle or internal/agentcli: the
// lifecycle package's end-to-end test imports the command package, and the command package calls this
// one. The rules the rows follow, retention's effective mode, the idle anchor and the owner label, are
// api/v1alpha1's helpers.
package inventory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// SchemaVersion is the "schemaVersion" of the JSON output. A change that breaks a field raises it.
const SchemaVersion = 1

// Options say what to list.
type Options struct {
	// Namespace is the namespace to list. It is ignored when AllNamespaces is set.
	Namespace string
	// AllNamespaces lists every namespace the caller can list: one cluster-wide list per kind.
	AllNamespaces bool
	// Owner, when set, selects the objects of one owner (docs/agent-cli.md, "--owner USER").
	Owner string
}

// Fact is a value that may be unknown: the value, or the reason it is not known. An unknown is never a
// zero or an empty string. The zero Fact is neither, and the rows leave it out of the JSON.
type Fact struct {
	value   any // a string or an int32
	unknown string
}

// Text, Instant, Span and Count are known facts. An Instant is shown as RFC 3339 in UTC, to the second;
// a Span as a Go duration string.
func Text(s string) Fact { return Fact{value: s} }

func Instant(t time.Time) Fact {
	return Fact{value: t.UTC().Truncate(time.Second).Format(time.RFC3339)}
}

func Span(d time.Duration) Fact { return Fact{value: d.String()} }

func Count(n int32) Fact { return Fact{value: n} }

// Unknown is a fact that is not known, with the reason, which is never empty.
func Unknown(reason string) Fact { return Fact{unknown: reason} }

// IsUnknown reports whether the fact is an unknown.
func (f Fact) IsUnknown() bool { return f.unknown != "" }

// IsZero reports whether the fact is neither known nor unknown: a field that does not apply. encoding/json
// leaves such a field out of an object when it is tagged omitzero.
func (f Fact) IsZero() bool { return f.value == nil && f.unknown == "" }

// MarshalJSON writes the value, or {"unknown": "<reason>"}. HTML escaping is off, as in WriteJSON: a purpose holds
// "<" and ">" and means them, and an encoder's setting does not reach the output of a Marshaler.
func (f Fact) MarshalJSON() ([]byte, error) {
	if f.unknown != "" {
		return noEscape(struct {
			Unknown string `json:"unknown"`
		}{f.unknown})
	}
	return noEscape(f.value)
}

func noEscape(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(b.Bytes(), "\n"), nil
}

// String is the fact as the text output shows it: the value, or "unknown (<reason>)".
func (f Fact) String() string {
	if f.unknown != "" {
		return "unknown (" + f.unknown + ")"
	}
	return fmt.Sprint(f.value)
}

// Inventory is what the lister returns and the printer prints. Every list is non-nil, so the JSON never
// holds a null where a list belongs.
type Inventory struct {
	SchemaVersion int `json:"schemaVersion"`
	// ObservedAt is the clock the command read once; every age in the inventory counts from it.
	ObservedAt string `json:"observedAt"`
	Scope      Scope  `json:"scope"`
	// SelectedOwner is Options.Owner; it is left out without one.
	SelectedOwner    string          `json:"selectedOwner,omitempty"`
	ModelDeployments []DeploymentRow `json:"modelDeployments"`
	Allocations      []AllocationRow `json:"allocations"`
	Instances        []InstanceRow   `json:"instances"`
	Providers        []ProviderRow   `json:"providers"`
	Operations       []OperationRow  `json:"operationsAwaitingApproval"`
}

// Scope is the namespace the inventory covers, or every namespace.
type Scope struct {
	Namespace     string `json:"namespace,omitempty"`
	AllNamespaces bool   `json:"allNamespaces,omitempty"`
}

// DeploymentRow is one ModelDeployment.
type DeploymentRow struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Owner     Fact   `json:"owner"`
	Purpose   Fact   `json:"purpose"`
	// Instances is spec.instances; ReadyInstances is the controller's count in status.readyInstances.
	Instances      int32 `json:"instances"`
	ReadyInstances Fact  `json:"readyInstances"`
	// EndTime is spec.endTime, RFC 3339 in UTC; it is left out when the deployment has none.
	EndTime string `json:"endTime,omitempty"`
	// Providers are the Providers of the deployment's Allocations that are not over, sorted.
	Providers []string `json:"providers"`
}

// AllocationRow is one Allocation. Its owner and purpose are its own annotations: never its
// deployment's.
type AllocationRow struct {
	Namespace       string       `json:"namespace"`
	Name            string       `json:"name"`
	ModelDeployment Fact         `json:"modelDeployment"`
	Owner           Fact         `json:"owner"`
	Purpose         Fact         `json:"purpose"`
	Phase           Fact         `json:"phase"`
	Provider        Fact         `json:"provider"`
	HardEnd         Fact         `json:"hardEnd"`
	Retention       RetentionRow `json:"retention"`
	// IdleSince is the idle anchor; it is left out when the Allocation is not idle.
	IdleSince Fact `json:"idleSince,omitzero"`
	// KeptNoInstance is the marker "kept, no Instance".
	KeptNoInstance bool `json:"keptNoInstance,omitempty"`
}

// RetentionRow is an Allocation's retention. Mode is always set; the other facts are left out when they
// do not apply.
type RetentionRow struct {
	// Mode is Keep or ReleaseWhenIdle: the policy's, with an unset mode read as Keep.
	Mode string `json:"mode"`
	// Until is the policy's keep date.
	Until Fact `json:"until,omitzero"`
	// IdleLimit is the policy's idle limit.
	IdleLimit Fact `json:"idleLimit,omitzero"`
	// ProfileMaxIdle is the maxIdle of the PlatformProfile of the Allocation's Provider.
	ProfileMaxIdle Fact `json:"profileMaxIdle,omitzero"`
}

// InstanceRow is one Instance. Its owner and purpose are its own annotations.
type InstanceRow struct {
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	Owner      Fact   `json:"owner"`
	Purpose    Fact   `json:"purpose"`
	Phase      Fact   `json:"phase"`
	Allocation string `json:"allocation"`
}

// ProviderRow is one Provider's last successful read and its age at the command's clock.
type ProviderRow struct {
	Name                     string `json:"name"`
	LastSuccessfulStatusRead Fact   `json:"lastSuccessfulStatusRead"`
	Age                      Fact   `json:"age"`
}

// OperationRow is one Operation in AwaitingApproval.
type OperationRow struct {
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	ModelDeployment string `json:"modelDeployment"`
	Action          string `json:"action"`
	PlanID          Fact   `json:"planID"`
	Owner           string `json:"owner"`
}
