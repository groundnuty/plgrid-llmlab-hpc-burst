package providerctl

// HBEP-8's billing row (beta; Provider accounting, Billing; Test plan). Over the
// real slurm.Batch and the fake Slurm, reading the Allocation's STORED status and
// the created acct- ControllerRevision through the API client.
//
// The job has 2 GPUs and 16 CPUs, started at 12:01:00, and is read at 13:31:00,
// 5,400 s on. The fake Slurm's accounting reply lists billing 3 by hand
// (Platform.SetBilling), a count that equals neither the CPUs (16) nor the GPUs
// (2), so a read of either entry in its place shows. Every expected number is a
// literal worked out from those: 3 x 5,400 / 3,600 = 4.5 billed units, 2 x 1.5 =
// 3.0 GPU-hours, 16 x 1.5 = 24.0 CPU-hours. Grant and account names are
// synthetic.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// billingEnv starts alloc-a's job (2 GPUs, 16 CPUs) at 12:01:00 on the fake
// Slurm, with the billing entry of its accounting reply set to billing (nil
// leaves the entry out). The Provider's grant is g1, its account a1 and the
// partition's unit gpu-hour, unless mods change the profile.
func billingEnv(t *testing.T, billing *int64, mods ...func(*slurm.Validated)) (*realEnv, *fakeslurm.Platform, string) {
	t.Helper()
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
		e.profiles.set(func(v *slurm.Validated) {
			v.Settings.Grant, v.Settings.Account = "g1", "a1"
			v.Profile.Partition.BillingUnit = v1alpha1.BillingUnitGPUHour
			for _, m := range mods {
				m(v)
			}
		})
	})
	e.newAlloc("alloc-a", 2, func(a *v1alpha1.Allocation) {
		a.Spec.ForProvider.Batch.CPUsPerTask = 16
		a.Spec.ForProvider.Batch.TimeLimit = "3h"
	})
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	if err := sl.SetBilling(jobNumber(t, id), billing); err != nil {
		t.Fatal(err)
	}
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	return e, sl, id
}

func i64p(v int64) *int64 { return &v }

// The status after 5,400 s: allocatedBilling 3, and the hours worked out by hand.
// Then the job ends at 13:31:00, the Allocation is deleted, and the record holds
// the same, with the unit, the grant and the account, and nothing unknown.
func TestBillingRowStatusAndRecord(t *testing.T) {
	e, sl, id := billingEnv(t, i64p(3))
	e.clock.Set(at(13, 31, 0)) // 5,400 s after the start
	e.run("alloc-a", 1)
	acct := e.accStatus("alloc-a")
	wantAcctStatus(t, "after 5,400 s", acct, 16, 2, 5400, at(13, 31, 0))
	if acct.AllocatedBilling == nil || *acct.AllocatedBilling != 3 {
		t.Fatalf("allocatedBilling = %v, want 3", acct.AllocatedBilling)
	}
	if s, ok := acct.BilledUnitSeconds(); !ok || s != 16200 {
		t.Errorf("billed unit-seconds = %d, %v; want 16,200, whose hours are 3 x 5,400 / 3,600 = 4.5", s, ok)
	}
	if h, ok := acct.GPUHours(); !ok || h != 3.0 {
		t.Errorf("GPU-hours = %v, %v; want 3.0 (2 x 1.5)", h, ok)
	}
	if h, ok := acct.CPUHours(); !ok || h != 24.0 {
		t.Errorf("CPU-hours = %v, %v; want 24.0 (16 x 1.5)", h, ok)
	}

	endJob(t, sl, id, fakeslurm.Completed) // 13:31:00
	e.poll("alloc-a", 1)
	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id)
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":16,"allocatedGPUs":2,"elapsedSeconds":5400,`+
		`"allocatedBilling":3,"billingUnit":"gpu-hour","grant":"g1","account":"a1",`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T13:31:00Z","basis":"allocated"}`)
}

// A reply with no billing entry: allocatedBilling is absent from the status and
// named in the record's unknown, while the GPU-hours stay 3.0 and the CPU-hours
// 24.0.
func TestBillingRowReplyWithNoBillingEntry(t *testing.T) {
	e, sl, id := billingEnv(t, nil)
	e.clock.Set(at(13, 31, 0))
	e.run("alloc-a", 1)
	acct := e.accStatus("alloc-a")
	wantAcctStatus(t, "after 5,400 s", acct, 16, 2, 5400, at(13, 31, 0))
	if acct.AllocatedBilling != nil {
		t.Errorf("allocatedBilling = %d, want absent: the reply has no billing entry", *acct.AllocatedBilling)
	}
	if s, ok := acct.BilledUnitSeconds(); ok {
		t.Errorf("billed unit-seconds = %d, want unknown", s)
	}
	if h, ok := acct.GPUHours(); !ok || h != 3.0 {
		t.Errorf("GPU-hours = %v, %v; want a known 3.0", h, ok)
	}

	endJob(t, sl, id, fakeslurm.Completed)
	e.poll("alloc-a", 1)
	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id)
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":16,"allocatedGPUs":2,"elapsedSeconds":5400,`+
		`"billingUnit":"gpu-hour","grant":"g1","account":"a1",`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T13:31:00Z","basis":"allocated",`+
		`"unknown":["allocatedBilling"]}`)
}

// The unit is the partition's billingUnit as the profile reads when the record
// is written, and the grant and account are the Provider's then: the profile
// changes from gpu-hour to cpu-hour between the status and the record, and the
// record has the later one. A Provider with no grant, account or unit has each
// absent and named, after the alpha fields, in the order allocatedBilling,
// billingUnit, grant, account.
func TestBillingRecordCopiesTheProfileAsReadWhenItIsWritten(t *testing.T) {
	e, sl, id := billingEnv(t, i64p(3))
	e.clock.Set(at(13, 31, 0))
	e.run("alloc-a", 1)
	endJob(t, sl, id, fakeslurm.Completed)
	e.poll("alloc-a", 1)
	e.profiles.set(func(v *slurm.Validated) {
		v.Profile.Partition.BillingUnit = v1alpha1.BillingUnitCPUHour
		v.Settings.Grant, v.Settings.Account = "g2", "c1"
	})
	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id)
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":16,"allocatedGPUs":2,"elapsedSeconds":5400,`+
		`"allocatedBilling":3,"billingUnit":"cpu-hour","grant":"g2","account":"c1",`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T13:31:00Z","basis":"allocated"}`)
}

func TestBillingRecordNamesAMissingUnitGrantAndAccount(t *testing.T) {
	e, sl, id := billingEnv(t, i64p(3), func(v *slurm.Validated) {
		v.Settings.Grant, v.Settings.Account = "", ""
		v.Profile.Partition.BillingUnit = ""
	})
	e.clock.Set(at(13, 31, 0))
	e.run("alloc-a", 1)
	endJob(t, sl, id, fakeslurm.Completed)
	e.poll("alloc-a", 1)
	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id)
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":16,"allocatedGPUs":2,"elapsedSeconds":5400,"allocatedBilling":3,`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T13:31:00Z","basis":"allocated",`+
		`"unknown":["billingUnit","grant","account"]}`)
}
