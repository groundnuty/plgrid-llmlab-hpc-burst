package slurm

import "testing"

// The billing entry of tres.allocated (beta; HBEP-8, Provider accounting,
// Billing), read as the cpu entry is: the count of the first entry of type
// billing, unknown when the entry is missing or its count cannot be read. The
// expected counts are written by hand from the fixtures' raw JSON, or are given
// with the inline list; -1 stands for unknown.

// Two real replies list billing: S7's Helios CPU job (v0.0.42, job 4242,
// tres.allocated billing 4 beside 4 CPUs and 6,144 MB) and Athena's GPU job
// (v0.0.40, job 4501, billing 1 beside 4 CPUs, 32,768 MB and one GPU). Athena's
// job is the one that tells the billing entry from the CPU entry (1 against 4);
// the inline cases below tell it from the GPU entry (3 against 2).
func TestAccountingBillingOfRealRecords(t *testing.T) {
	athena := realAcct(t, "v0.0.40", "v0.0.40-athena-real", "sacct-completed.json", "4501")
	if b := athena.Accounting.AllocatedBilling; b == nil || *b != 1 {
		t.Errorf("Athena job 4501: AllocatedBilling = %v, want 1", b)
	}
	if c := athena.Accounting.AllocatedCPUs; c == nil || *c != 4 {
		t.Errorf("Athena job 4501: AllocatedCPUs = %v, want 4 (the billing entry is not the CPU entry)", c)
	}
	helios := realAcct(t, "v0.0.42", "", "sacct-completed.json", "4242")
	if b := helios.Accounting.AllocatedBilling; b == nil || *b != 4 {
		t.Errorf("Helios job 4242: AllocatedBilling = %v, want 4", b)
	}
}

func TestAccountingAllocatedBillingValidation(t *testing.T) {
	for _, c := range []struct {
		name    string
		tres    string
		billing int32
	}{
		// Billing 3 beside 16 CPUs and 2 GPUs equals neither of them.
		{"billing 3 beside 16 CPUs and 2 GPUs", `[{"type":"cpu","count":16},{"type":"billing","count":3},{"type":"gres","name":"gpu","count":2}]`, 3},
		{"no billing entry", `[{"type":"cpu","count":16},{"type":"gres","name":"gpu","count":2}]`, -1},
		{"a known 0", `[{"type":"cpu","count":16},{"type":"billing","count":0}]`, 0},
		{"null count", `[{"type":"billing","count":null}]`, -1},
		{"no count", `[{"type":"billing"}]`, -1},
		{"negative sentinel", `[{"type":"billing","count":-2}]`, -1},
		{"above MaxInt32", `[{"type":"billing","count":2147483648}]`, -1},
		{"at MaxInt32", `[{"type":"billing","count":2147483647}]`, 2147483647},
		{"unreadable first entry, readable second", `[{"type":"billing","count":null},{"type":"billing","count":4}]`, -1},
		// Other entries are not billing, whatever they are named or count.
		{"a gres named billing is not billing", `[{"type":"gres","name":"billing","count":7}]`, -1},
		{"a cpu entry named billing is not billing", `[{"type":"cpu","name":"billing","count":7}]`, -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := allocatedOf(t, c.tres)
			switch {
			case c.billing < 0 && a.AllocatedBilling != nil:
				t.Errorf("AllocatedBilling = %d, want unknown", *a.AllocatedBilling)
			case c.billing >= 0 && (a.AllocatedBilling == nil || *a.AllocatedBilling != c.billing):
				t.Errorf("AllocatedBilling = %v, want %d", a.AllocatedBilling, c.billing)
			}
		})
	}
}

// Only tres.allocated is read: the job's requested list in the same fixture
// carries billing 1, and an allocated list without the entry stays unknown.
func TestAccountingBillingIsNotTakenFromTheRequestedList(t *testing.T) {
	a := allocatedOf(t, `[{"type":"cpu","count":4}]`)
	if a.AllocatedBilling != nil {
		t.Errorf("AllocatedBilling = %d, want unknown: the requested list's billing entry is not the allocated one", *a.AllocatedBilling)
	}
}
