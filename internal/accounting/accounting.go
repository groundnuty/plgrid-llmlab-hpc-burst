// Package accounting totals the billed units of a researcher's Allocations per
// grant and account, from the kept accounting records and the live Allocations
// (beta; HBEP-8, Provider accounting, Billing, Totals per grant). It is pure:
// the caller reads the objects, so the grouping and the sums are tested over
// objects written by hand, and the command that reads the API is a thin layer
// over Summarize.
//
// Every figure is "allocated, not billed" (HBEP-8): an Allocation's billed units
// are the scheduler's billing count for its job times its elapsed hours, in its
// partition's billing unit, which a person-run comparison with a center's report
// has not yet confirmed.
//
// Origin: new, written for HBEP-8's amendment of 16 Nov 2026 (#625). It reuses
// v1alpha1.AllocationAccounting.BilledUnitSeconds for the arithmetic.
package accounting

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Label is the label every figure carries (HBEP-8: "allocated, not billed").
const Label = "allocated, not billed"

// Record is one kept accounting record: its object's name and its data.
type Record struct {
	// Name is the ControllerRevision's name, acct-<Allocation UID>.
	Name string
	Data v1alpha1.AccountingRecordData
}

// ParseRecord reads the data of a ControllerRevision of the accounting kind. It
// fails when the data is not an accounting record's JSON.
func ParseRecord(cr appsv1.ControllerRevision) (Record, error) {
	var d v1alpha1.AccountingRecordData
	if err := json.Unmarshal(cr.Data.Raw, &d); err != nil {
		return Record{}, fmt.Errorf("accounting record %s/%s: %w", cr.Namespace, cr.Name, err)
	}
	return Record{Name: cr.Name, Data: d}, nil
}

// Inputs are the objects the totals are made from. A record names its Provider
// and partition, an Allocation names its Provider (spec.providerConfigRef) and
// partition, and a Provider names its PlatformProfile.
type Inputs struct {
	Records     []Record
	Allocations []v1alpha1.Allocation
	Providers   []v1alpha1.Provider
	Profiles    []v1alpha1.PlatformProfile
}

// Group is the total of one (grant, account, unit). An empty Grant, Account or
// Unit is unknown: the members that could not be placed there (a Provider that
// is gone, a profile that lists no unit) form a group of their own, and are
// counted, never merged into another.
type Group struct {
	Grant   string               `json:"grant"`
	Account string               `json:"account"`
	Unit    v1alpha1.BillingUnit `json:"unit"`

	// Billed is the sum of the known members' billed units: billing count times
	// elapsed seconds over 3,600, in Unit. It never includes an unknown member.
	Billed float64 `json:"billed"`
	// Known and Unknown count the members whose figure is known and unknown.
	// Members is their sum. An unknown member is a live Allocation that is still
	// pending, a member whose billing count or elapsed time is unknown, a member
	// whose unit is unknown (a record's own unit, never the profile's now), and a
	// record written before the beta fields (it has no billing count and does
	// not name it in its unknown list).
	Known   int `json:"known"`
	Unknown int `json:"unknown"`
	Members int `json:"members"`

	// GrantEnd is the earliest grantEnd of the Providers whose members are in the
	// group, when any sets one; DaysLeft is the whole days from now to it,
	// floor((GrantEnd - now) / 24h), negative once it has passed.
	GrantEnd *metav1.Time `json:"grantEnd,omitempty"`
	DaysLeft *int         `json:"daysLeft,omitempty"`
}

// Report is the totals, labelled as the figures are.
type Report struct {
	// Label is Label: every figure is allocated, not billed.
	Label string `json:"label"`
	// Now is the time the days left count from.
	Now    metav1.Time `json:"now"`
	Groups []Group     `json:"groups"`
	// Unreadable names the records whose data could not be read, which the
	// caller found and counted in no group.
	Unreadable []string `json:"unreadable,omitempty"`
}

type key struct {
	grant, account string
	unit           v1alpha1.BillingUnit
}

// member is one record or live Allocation, placed.
type member struct {
	key       key
	unitSecs  int64
	known     bool
	grantEnd  *metav1.Time
	providers string
}

// Summarize groups the records and the live Allocations by (grant, account,
// unit) and sums each group's billed units. now is injected; nothing reads the
// clock.
//
//   - A record's grant, account and unit are its own when it has them. A record
//     written before the beta fields has none, so they are the Provider's grant
//     and account and the unit of its partition in the Provider's PlatformProfile
//     as they stand now: a grant has one name for its life, and a new grant is a
//     new Provider (HBEP-8), so the Provider's are the record's. A Provider or a
//     partition that is gone leaves them unknown. The profile's unit only PLACES
//     a record whose own unit is unknown: the record counts as unknown and adds
//     nothing to any sum, since the unit may have changed since it was written.
//   - A group's grantEnd and days left are those of the Providers of its members,
//     as their specs say now, whatever grant a record names: a record whose own
//     grant differs from its Provider's current grant still shows that Provider's
//     grantEnd.
//   - A live Allocation's are the Provider's and its partition's, the same way.
//     An Allocation that has a record (the deleting pass writes the record
//     before the finalizer goes) counts once, by its record.
//   - A running Allocation's figure is its stored accounting as of that block's
//     readTime, which the Provider controller refreshes at least once a minute
//     while it runs.
//
// A member with the billing count or the elapsed time unknown is counted
// unknown, and adds nothing to the sum. The sums are of whole unit-seconds, so no
// rounding enters before the division by 3,600.
func Summarize(in Inputs, now time.Time) Report {
	providers := map[string]v1alpha1.Provider{}
	for _, p := range in.Providers {
		providers[p.Name] = p
	}
	profiles := map[string]v1alpha1.PlatformProfile{}
	for _, p := range in.Profiles {
		profiles[p.Name] = p
	}
	r := resolver{providers: providers, profiles: profiles}

	var members []member
	kept := map[string]bool{}
	for _, rec := range in.Records {
		kept[rec.Name] = true
		members = append(members, r.record(rec.Data))
	}
	for _, a := range in.Allocations {
		if name, err := v1alpha1.AccountingRecordName(a.UID); err == nil && kept[name] {
			continue // its record is the figure
		}
		members = append(members, r.allocation(a))
	}
	return Report{Label: Label, Now: metav1.NewTime(now.UTC()), Groups: groupsOf(members, now)}
}

// resolver places a member from the Provider and the profile objects.
type resolver struct {
	providers map[string]v1alpha1.Provider
	profiles  map[string]v1alpha1.PlatformProfile
}

func (r resolver) record(d v1alpha1.AccountingRecordData) member {
	m := r.place(d.Provider, d.Partition, d.Grant, d.Account, d.BillingUnit)
	acct := v1alpha1.AllocationAccounting{AllocatedBilling: d.AllocatedBilling, ElapsedSeconds: d.ElapsedSeconds}
	m.unitSecs, m.known = acct.BilledUnitSeconds()
	// The record's own unit is the unit its count is in. The profile's unit now may
	// have changed since the record was written, so a record without one (written
	// before the field, or with it named unknown) is unknown, never summed under
	// the profile's current unit: that would add one unit's hours to another's. It
	// is still placed by the profile's unit now, so it is counted in a group.
	m.known = m.known && d.BillingUnit != ""
	return m
}

func (r resolver) allocation(a v1alpha1.Allocation) member {
	providerName, partition := "", ""
	if ref := a.Spec.ProviderConfigReference; ref != nil {
		providerName = ref.Name
	}
	if b := a.Spec.ForProvider.Batch; b != nil {
		partition = b.Partition
	}
	m := r.place(providerName, partition, "", "", "")
	if acct := a.Status.AtProvider.Accounting; acct != nil { // nil: pending, and unknown
		m.unitSecs, m.known = acct.BilledUnitSeconds()
		m.known = m.known && m.key.unit != ""
	}
	return m
}

// place gives a member its key and its Provider's grantEnd. grant, account and
// unit are the member's own, when it has them, and "" when it has none; each
// empty one is the Provider's now.
func (r resolver) place(providerName, partition, grant, account string, unit v1alpha1.BillingUnit) member {
	m := member{providers: providerName}
	p, found := r.providers[providerName]
	if found {
		if grant == "" {
			grant = p.Spec.Grant
		}
		if account == "" {
			account = p.Spec.Account
		}
		m.grantEnd = p.Spec.GrantEnd
		if unit == "" {
			unit = r.unitOf(p, partition)
		}
	}
	m.key = key{grant: grant, account: account, unit: unit}
	return m
}

// unitOf is the billingUnit of the named partition in the Provider's profile.
func (r resolver) unitOf(p v1alpha1.Provider, partition string) v1alpha1.BillingUnit {
	pp, ok := r.profiles[p.Spec.PlatformProfileRef.Name]
	if !ok {
		return ""
	}
	for _, part := range pp.Spec.Partitions {
		if part.Name == partition {
			return part.BillingUnit
		}
	}
	return ""
}

// groupsOf sums the members per key, in the order of the keys: grant, account,
// unit, a name before an unknown one.
func groupsOf(members []member, now time.Time) []Group {
	byKey := map[key]*Group{}
	ends := map[key]*metav1.Time{}
	var order []key
	for _, m := range members {
		g, ok := byKey[m.key]
		if !ok {
			g = &Group{Grant: m.key.grant, Account: m.key.account, Unit: m.key.unit}
			byKey[m.key] = g
			order = append(order, m.key)
		}
		g.Members++
		if m.known {
			g.Known++
			g.Billed += float64(m.unitSecs) // unit-seconds for now; divided once below
		} else {
			g.Unknown++
		}
		if m.grantEnd != nil && (ends[m.key] == nil || m.grantEnd.Before(ends[m.key])) {
			ends[m.key] = m.grantEnd
		}
	}
	sort.Slice(order, func(i, j int) bool { return keyLess(order[i], order[j]) })
	out := make([]Group, 0, len(order))
	for _, k := range order {
		g := *byKey[k]
		g.Billed /= 3600
		if e := ends[k]; e != nil {
			end := *e
			days := int(math.Floor(end.Sub(now).Hours() / 24))
			g.GrantEnd, g.DaysLeft = &end, &days
		}
		out = append(out, g)
	}
	return out
}

// keyLess orders grants, then accounts, then units; an empty (unknown) name
// follows the named ones.
func keyLess(a, b key) bool {
	if a.grant != b.grant {
		return nameLess(a.grant, b.grant)
	}
	if a.account != b.account {
		return nameLess(a.account, b.account)
	}
	return nameLess(string(a.unit), string(b.unit))
}

func nameLess(a, b string) bool {
	if (a == "") != (b == "") {
		return b == ""
	}
	return a < b
}
