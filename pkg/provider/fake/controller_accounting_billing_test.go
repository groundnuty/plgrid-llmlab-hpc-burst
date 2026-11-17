package fake

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The beta fields of the stand-in's record and status (HBEP-8, Provider
// accounting, Billing). The Allocation asks for 1 GPU and 8 CPUs, so a billing
// count of 3, set by hand with Batch.SetBilling, equals neither. The job starts
// at 12:05:00 and the Allocation is deleted at 12:10:00 (300 s): the billing
// count is 3, and its Provider's grant and account and its partition's unit are
// the Controller's. Names are synthetic.

func (e *env) recordData(name string) v1alpha1.AccountingRecordData {
	e.t.Helper()
	cr, ok := e.acct(name)
	if !ok {
		e.t.Fatalf("no accounting record for %s", name)
	}
	var d v1alpha1.AccountingRecordData
	if err := json.Unmarshal(cr.Data.Raw, &d); err != nil {
		e.t.Fatalf("record data %s: %v", cr.Data.Raw, err)
	}
	return d
}

func TestRecordCopiesTheBillingCountAndTheControllersNames(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.batch.SetBilling("998", i32(3))
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	if a := e.obs("a1").Accounting; a == nil || a.AllocatedBilling == nil || *a.AllocatedBilling != 3 {
		t.Fatalf("live accounting %+v, want allocatedBilling 3", a)
	}
	e.del("a1")
	e.sync()
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), secs: i64(300), start: tp(at(12, 5, 0)), end: tp(at(12, 10, 0)), jobIDs: []string{"998"}})
	d := e.recordData("a1")
	if d.AllocatedBilling == nil || *d.AllocatedBilling != 3 || d.BillingUnit != v1alpha1.BillingUnitGPUHour ||
		d.Grant != "example-grant" || d.Account != "example-account" {
		t.Errorf("record = billing %s, unit %q, grant %q, account %q; want 3, gpu-hour, example-grant, example-account",
			show(d.AllocatedBilling), d.BillingUnit, d.Grant, d.Account)
	}
}

// Without a billing entry in accounting's reply, and with a Controller that was
// given no grant, account or unit, each of the four is absent and named, after
// the alpha fields, in the order allocatedBilling, billingUnit, grant, account.
func TestRecordNamesEveryBetaFieldThatIsUnknown(t *testing.T) {
	e := newEnv(t)
	e.ctl.Grant, e.ctl.Account, e.ctl.BillingUnit = "", "", ""
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.batch.SetBilling("998", nil)
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	if a := e.obs("a1").Accounting; a == nil || a.AllocatedBilling != nil {
		t.Fatalf("live accounting %+v, want allocatedBilling absent", a)
	}
	e.del("a1")
	e.sync()
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), secs: i64(300), start: tp(at(12, 5, 0)), end: tp(at(12, 10, 0)), jobIDs: []string{"998"},
		unknown: []string{"allocatedBilling", "billingUnit", "grant", "account"}})
	d := e.recordData("a1")
	if d.AllocatedBilling != nil || d.BillingUnit != "" || d.Grant != "" || d.Account != "" {
		t.Errorf("record = billing %s, unit %q, grant %q, account %q; want all absent", show(d.AllocatedBilling), d.BillingUnit, d.Grant, d.Account)
	}
}
