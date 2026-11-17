package accounting

import (
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// HBEP-8's totals row (beta; Test plan, Totals per grant), from records written
// by hand and live objects. The expected totals are literals worked out from the
// row, never the package's own sum:
//
//	g1/a1, gpu-hour: billing 1 for 7,200 s = 2.0, billing 3 for 5,400 s = 4.5, a
//	  running Allocation with billing 1 at 1,800 s = 0.5; unknown: a record with
//	  its billing unknown, a record written before the fields, a pending
//	  Allocation. 2.0 + 4.5 + 0.5 = 7.0 GPU-hours with 3 unknown.
//	g1/a2, gpu-hour: billing 4 for 43,200 s = 48.0.
//	g1/c1, cpu-hour: billing 4 for 1,800 s = 2.0.
//
// With grantEnd 2026-12-31T00:00:00Z and the clock at 2026-11-16T00:00:00Z the
// time left is 14 days of November and 31 of December: 45.
//
// Every record also has 16 CPUs and 2 GPUs, so a total built from either count
// instead of the billing count shows. Grant and account names are synthetic.

var now = time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)

func provider(name, grant, account, profile string, end *time.Time) v1alpha1.Provider {
	p := v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.ProviderSpec{
		Grant: grant, Account: account, PlatformProfileRef: v1alpha1.ObjectReference{Name: profile},
		AllowedPartitions: []string{"example-part"},
	}}
	if end != nil {
		t := metav1.NewTime(*end)
		p.Spec.GrantEnd = &t
	}
	return p
}

func profile(name, partition string, unit v1alpha1.BillingUnit) v1alpha1.PlatformProfile {
	return v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.PlatformProfileSpec{
		Partitions: []v1alpha1.PartitionProfile{{Name: partition, BillingUnit: unit}},
	}}
}

// rec is a record of the Provider's partition with 16 CPUs and 2 GPUs, the given
// billing count (nil: unknown) and elapsed seconds.
func rec(name, providerName, partition string, billing *int32, elapsed int64, mod func(*v1alpha1.AccountingRecordData)) Record {
	d := v1alpha1.AccountingRecordData{
		Provider: providerName, Partition: partition, Tag: "example-tag", JobIDs: []string{"1234567"},
		AllocatedCPUs: ptr.To(int32(16)), AllocatedGPUs: ptr.To(int32(2)), ElapsedSeconds: ptr.To(elapsed),
		AllocatedBilling: billing, Basis: v1alpha1.AccountingBasisAllocated,
	}
	if mod != nil {
		mod(&d)
	}
	return Record{Name: name, Data: d}
}

// beta sets the record's beta names, as a record written by the beta controller has them.
func beta(grant, account string, unit v1alpha1.BillingUnit) func(*v1alpha1.AccountingRecordData) {
	return func(d *v1alpha1.AccountingRecordData) { d.Grant, d.Account, d.BillingUnit = grant, account, unit }
}

func alloc(name string, uid types.UID, providerName, partition string, acct *v1alpha1.AllocationAccounting) v1alpha1.Allocation {
	return v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "example-ns", UID: uid},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: providerName},
			ForProvider:             v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{Partition: partition}},
		},
		Status: v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{Accounting: acct}},
	}
}

func running(billing int32, elapsed int64) *v1alpha1.AllocationAccounting {
	return &v1alpha1.AllocationAccounting{
		AllocatedCPUs: ptr.To(int32(16)), AllocatedGPUs: ptr.To(int32(2)), AllocatedBilling: ptr.To(billing),
		ElapsedSeconds: ptr.To(elapsed), ReadTime: metav1.NewTime(now),
	}
}

func grantEnd() *time.Time { t := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC); return &t }

// totalsRow is the HBEP's row, as the objects of one namespace.
func totalsRow() Inputs {
	return Inputs{
		Providers: []v1alpha1.Provider{
			provider("prov-a", "g1", "a1", "prof-gpu", grantEnd()),
			provider("prov-b", "g1", "a2", "prof-gpu", nil),
			provider("prov-c", "g1", "c1", "prof-cpu", nil),
		},
		Profiles: []v1alpha1.PlatformProfile{
			profile("prof-gpu", "gpu-part", v1alpha1.BillingUnitGPUHour),
			profile("prof-cpu", "cpu-part", v1alpha1.BillingUnitCPUHour),
		},
		Records: []Record{
			rec("acct-r1", "prov-a", "gpu-part", ptr.To(int32(1)), 7200, beta("g1", "a1", v1alpha1.BillingUnitGPUHour)),
			rec("acct-r2", "prov-a", "gpu-part", ptr.To(int32(3)), 5400, beta("g1", "a1", v1alpha1.BillingUnitGPUHour)),
			// Its billing count is unknown, and named so.
			rec("acct-r3", "prov-a", "gpu-part", nil, 3600, func(d *v1alpha1.AccountingRecordData) {
				beta("g1", "a1", v1alpha1.BillingUnitGPUHour)(d)
				d.Unknown = []string{v1alpha1.AccountingFieldAllocatedBilling}
			}),
			// Written before the fields: no billing count, no names, nothing named unknown.
			rec("acct-r4", "prov-a", "gpu-part", nil, 3600, nil),
			rec("acct-r5", "prov-b", "gpu-part", ptr.To(int32(4)), 43200, beta("g1", "a2", v1alpha1.BillingUnitGPUHour)),
			rec("acct-r6", "prov-c", "cpu-part", ptr.To(int32(4)), 1800, beta("g1", "c1", v1alpha1.BillingUnitCPUHour)),
		},
		Allocations: []v1alpha1.Allocation{
			alloc("live-running", "uid-live-running", "prov-a", "gpu-part", running(1, 1800)),
			alloc("live-pending", "uid-live-pending", "prov-a", "gpu-part", nil),
		},
	}
}

// want is one group's expected line, written out.
type want struct {
	grant, account string
	unit           v1alpha1.BillingUnit
	billed         float64
	known, unknown int
	daysLeft       *int
}

func checkGroups(t *testing.T, got []Group, wants []want) {
	t.Helper()
	if len(got) != len(wants) {
		t.Fatalf("%d groups %+v, want %d", len(got), got, len(wants))
	}
	for i, w := range wants {
		g := got[i]
		if g.Grant != w.grant || g.Account != w.account || g.Unit != w.unit {
			t.Errorf("group %d is %q/%q/%q, want %q/%q/%q", i, g.Grant, g.Account, g.Unit, w.grant, w.account, w.unit)
		}
		if g.Billed != w.billed {
			t.Errorf("group %s/%s/%s: billed %v, want %v", w.grant, w.account, w.unit, g.Billed, w.billed)
		}
		if g.Known != w.known || g.Unknown != w.unknown || g.Members != w.known+w.unknown {
			t.Errorf("group %s/%s/%s: %d known, %d unknown of %d, want %d known and %d unknown", w.grant, w.account, w.unit, g.Known, g.Unknown, g.Members, w.known, w.unknown)
		}
		switch {
		case w.daysLeft == nil && g.DaysLeft != nil:
			t.Errorf("group %s/%s/%s: %d days left, want no grantEnd", w.grant, w.account, w.unit, *g.DaysLeft)
		case w.daysLeft != nil && (g.DaysLeft == nil || *g.DaysLeft != *w.daysLeft):
			t.Errorf("group %s/%s/%s: days left %v, want %d", w.grant, w.account, w.unit, g.DaysLeft, *w.daysLeft)
		}
	}
}

// HBEP-8's totals row: a1 7.0 GPU-hours with 3 unknown, a2 48.0 GPU-hours, c1 2.0
// CPU-hours, 45 days left to a1's grantEnd, and no group that mixes the units.
func TestTotalsRow(t *testing.T) {
	r := Summarize(totalsRow(), now)
	if r.Label != "allocated, not billed" {
		t.Errorf("label %q, want %q", r.Label, "allocated, not billed")
	}
	checkGroups(t, r.Groups, []want{
		{"g1", "a1", v1alpha1.BillingUnitGPUHour, 7.0, 3, 3, ptr.To(45)},
		{"g1", "a2", v1alpha1.BillingUnitGPUHour, 48.0, 1, 0, nil},
		{"g1", "c1", v1alpha1.BillingUnitCPUHour, 2.0, 1, 0, nil},
	})
}

// A record that has its own unit, grant and account keeps them, whatever the
// Provider and the profile say now.
func TestARecordsOwnNamesWinOverTheProviderNow(t *testing.T) {
	in := totalsRow()
	in.Records = []Record{rec("acct-x", "prov-a", "gpu-part", ptr.To(int32(2)), 3600, beta("g9", "z9", v1alpha1.BillingUnitCPUHour))}
	in.Allocations = nil
	checkGroups(t, Summarize(in, now).Groups, []want{{"g9", "z9", v1alpha1.BillingUnitCPUHour, 2.0, 1, 0, ptr.To(45)}})
}

// An Allocation whose record exists counts once, by the record: the deleting
// pass writes the record before the finalizer goes, so both exist for a while.
func TestAnAllocationWithItsRecordCountsOnce(t *testing.T) {
	in := totalsRow()
	in.Records = []Record{rec("acct-uid-both", "prov-a", "gpu-part", ptr.To(int32(3)), 3600, beta("g1", "a1", v1alpha1.BillingUnitGPUHour))}
	in.Allocations = []v1alpha1.Allocation{
		alloc("both", "uid-both", "prov-a", "gpu-part", running(1, 7200)), // its status says 2.0; the record says 3.0
		alloc("other", "uid-other", "prov-a", "gpu-part", running(1, 3600)),
	}
	checkGroups(t, Summarize(in, now).Groups, []want{{"g1", "a1", v1alpha1.BillingUnitGPUHour, 4.0, 2, 0, ptr.To(45)}})
}

// A known 0 billing count is a known 0, not unknown; an unknown elapsed time
// makes the member unknown.
func TestAKnownZeroIsKnownAndAnUnknownElapsedTimeIsNot(t *testing.T) {
	in := totalsRow()
	noElapsed := rec("acct-n", "prov-a", "gpu-part", ptr.To(int32(3)), 0, beta("g1", "a1", v1alpha1.BillingUnitGPUHour))
	noElapsed.Data.ElapsedSeconds = nil
	in.Records = []Record{
		rec("acct-z", "prov-a", "gpu-part", ptr.To(int32(0)), 3600, beta("g1", "a1", v1alpha1.BillingUnitGPUHour)),
		noElapsed,
	}
	in.Allocations = nil
	checkGroups(t, Summarize(in, now).Groups, []want{{"g1", "a1", v1alpha1.BillingUnitGPUHour, 0, 1, 1, ptr.To(45)}})
}

// A member whose Provider is gone, or whose partition the profile does not list,
// is placed in a group of its own with the missing name empty, counted unknown,
// and sorts after the named groups; its figure is never added to another group.
func TestMembersThatCannotBePlacedFormAGroupOfTheirOwn(t *testing.T) {
	in := totalsRow()
	in.Records = []Record{
		rec("acct-gone", "prov-gone", "gpu-part", nil, 3600, nil),  // pre-beta, Provider deleted
		rec("acct-nopart", "prov-a", "other-part", nil, 3600, nil), // pre-beta, partition not in the profile
		rec("acct-ok", "prov-b", "gpu-part", ptr.To(int32(4)), 3600, beta("g1", "a2", v1alpha1.BillingUnitGPUHour)),
	}
	in.Allocations = nil
	checkGroups(t, Summarize(in, now).Groups, []want{
		{"g1", "a1", "", 0, 0, 1, ptr.To(45)}, // the Provider is known; the unit is not
		{"g1", "a2", v1alpha1.BillingUnitGPUHour, 4.0, 1, 0, nil},
		{"", "", "", 0, 0, 1, nil},
	})
}

// The days left are whole days, floored: 44.5 days is 44, and a grant that ended
// 12 hours ago is -1.
func TestDaysLeftAreFlooredWholeDays(t *testing.T) {
	in := totalsRow()
	in.Records, in.Allocations = totalsRow().Records[:1], nil
	for _, c := range []struct {
		at   time.Time
		days int
	}{
		{time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC), 44},
		{time.Date(2026, 12, 30, 23, 59, 59, 0, time.UTC), 0},
		{time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC), 0},
		{time.Date(2026, 12, 31, 12, 0, 0, 0, time.UTC), -1},
	} {
		g := Summarize(in, c.at).Groups[0]
		if g.DaysLeft == nil || *g.DaysLeft != c.days {
			t.Errorf("at %s: days left %v, want %d", c.at.Format(time.RFC3339), g.DaysLeft, c.days)
		}
	}
}

// A group takes the earliest grantEnd of its Providers.
func TestAGroupTakesTheEarliestGrantEnd(t *testing.T) {
	in := totalsRow()
	later := time.Date(2027, 1, 31, 0, 0, 0, 0, time.UTC)
	in.Providers = []v1alpha1.Provider{
		provider("prov-a", "g1", "a1", "prof-gpu", &later),
		provider("prov-a2", "g1", "a1", "prof-gpu", grantEnd()),
	}
	in.Records = []Record{
		rec("acct-1", "prov-a", "gpu-part", ptr.To(int32(1)), 3600, beta("g1", "a1", v1alpha1.BillingUnitGPUHour)),
		rec("acct-2", "prov-a2", "gpu-part", ptr.To(int32(1)), 3600, beta("g1", "a1", v1alpha1.BillingUnitGPUHour)),
	}
	in.Allocations = nil
	checkGroups(t, Summarize(in, now).Groups, []want{{"g1", "a1", v1alpha1.BillingUnitGPUHour, 2.0, 2, 0, ptr.To(45)}})
}

func TestParseRecord(t *testing.T) {
	cr := appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "acct-u", Namespace: "example-ns"},
		Data: runtime.RawExtension{Raw: []byte(`{"provider":"prov-a","partition":"gpu-part","tag":"t","jobIDs":["1234567"],"elapsedSeconds":60,"allocatedBilling":3,` +
			`"billingUnit":"gpu-hour","grant":"g1","account":"a1","basis":"allocated"}`)}}
	got, err := ParseRecord(cr)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "acct-u" || got.Data.Provider != "prov-a" || got.Data.AllocatedBilling == nil || *got.Data.AllocatedBilling != 3 ||
		got.Data.BillingUnit != v1alpha1.BillingUnitGPUHour || got.Data.Grant != "g1" || got.Data.Account != "a1" {
		t.Errorf("record %+v", got)
	}
	cr.Data.Raw = []byte(`not json`)
	if _, err := ParseRecord(cr); err == nil {
		t.Error("a record that is not JSON was accepted")
	}
}

// A record whose own billing unit is unknown, with its billing count known, is an
// unknown member of its group and adds nothing to any unit's sum. The profile's
// unit may have changed since the record was written, so it is never read as the
// record's. Expectations by hand: in HBEP-8's row a1 gains one unknown member,
// 7 members with 4 unknown and the same 7.0; and with the profile now saying
// cpu-hour, a record of billing 2 for 3,600 s and no unit is placed in the
// cpu-hour group as one unknown with 0.0 billed, while the record that names
// gpu-hour keeps its 2.0 in the gpu-hour group.
func TestARecordWithAnUnknownUnitIsUnknown(t *testing.T) {
	noUnit := func(d *v1alpha1.AccountingRecordData) {
		d.Grant, d.Account = "g1", "a1"
		d.Unknown = []string{v1alpha1.AccountingFieldBillingUnit}
	}
	in := totalsRow()
	in.Records = append(in.Records, rec("acct-nounit", "prov-a", "gpu-part", ptr.To(int32(2)), 3600, noUnit))
	checkGroups(t, Summarize(in, now).Groups, []want{
		{"g1", "a1", v1alpha1.BillingUnitGPUHour, 7.0, 3, 4, ptr.To(45)},
		{"g1", "a2", v1alpha1.BillingUnitGPUHour, 48.0, 1, 0, nil},
		{"g1", "c1", v1alpha1.BillingUnitCPUHour, 2.0, 1, 0, nil},
	})

	in = Inputs{
		Providers: []v1alpha1.Provider{provider("prov-a", "g1", "a1", "prof-now", nil)},
		Profiles:  []v1alpha1.PlatformProfile{profile("prof-now", "gpu-part", v1alpha1.BillingUnitCPUHour)}, // changed since the records
		Records: []Record{
			rec("acct-named", "prov-a", "gpu-part", ptr.To(int32(2)), 3600, beta("g1", "a1", v1alpha1.BillingUnitGPUHour)),
			rec("acct-nounit", "prov-a", "gpu-part", ptr.To(int32(2)), 3600, noUnit),
		},
	}
	checkGroups(t, Summarize(in, now).Groups, []want{
		{"g1", "a1", v1alpha1.BillingUnitCPUHour, 0.0, 0, 1, nil},
		{"g1", "a1", v1alpha1.BillingUnitGPUHour, 2.0, 1, 0, nil},
	})
}
