package slurm_test

// FindByTag's merge of the queue listing and accounting (HBEP-4, Core
// operations: "from accounting since since for any job the listing does not
// show"). "Shows" is by job ID, whatever name the listing gives the job (Astra
// B1 on PR 217): a job the listing shows under another name is not returned,
// and its accounting record, which has the tag, does not stand in for it, since
// accounting never carries COMPLETING. The job is then read by ID, listing
// first, as Observe does.

import (
	"context"
	"testing"
	"time"
)

func TestFindByTagDoesNotLetAccountingStandInForAJobTheListingShowsUnderAnotherName(t *testing.T) {
	const tag = "hb-example-0123456789ab"
	r := newFakeRig(t, "$SCRATCH/stage")
	ctx := context.Background()
	id := r.plat.OutsideJob("example-user", tag, fakePart, "", 1)
	if err := r.plat.Start(id); err != nil {
		t.Fatal(err)
	}
	if err := r.plat.End(id, "CANCELLED"); err != nil {
		t.Fatal(err)
	}
	if err := r.plat.HoldCompleting(id, true); err != nil {
		t.Fatal(err)
	}
	since := r.clock.Now().Add(-time.Hour)
	find := func(what string) []string {
		t.Helper()
		r.clock.Advance(time.Minute) // past the poll interval: the listing is read again
		jobs, err := r.batch.FindByTag(ctx, tag, since)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		var ids []string
		for _, j := range jobs {
			ids = append(ids, j.ID)
			if j.ID == "1001" && !j.Completing {
				t.Errorf("%s: job 1001 comes back without the flag, from %s", what, j.Source)
			}
		}
		return ids
	}
	// Control: listed under the tag, the job is found, with the flag.
	if got := find("under the tag"); len(got) != 1 || got[0] != "1001" {
		t.Fatalf("control: FindByTag gives %v, want [1001]", got)
	}
	// The listing now names it otherwise; accounting still has the tag, ended.
	if err := r.plat.RenameListed(id, "ExampleRenamed"); err != nil {
		t.Fatal(err)
	}
	if got := find("under another name"); len(got) != 0 {
		t.Errorf("FindByTag gives %v while the listing shows job 1001 under another name, want none: its accounting record must not stand in for it", got)
	}
	j, found, err := r.batch.Observe(ctx, "1001")
	if err != nil || !found || !j.Completing || j.Name != "ExampleRenamed" {
		t.Errorf("Observe by ID gives %+v, found %v, %v; want the listing's job, ExampleRenamed, completing", j, found, err)
	}
	// Control: once the listing no longer shows it (the flag has cleared and
	// the job is not listed), accounting is the evidence again.
	if err := r.plat.HoldCompleting(id, false); err != nil {
		t.Fatal(err)
	}
	r.clock.Advance(time.Minute)
	jobs, err := r.batch.FindByTag(ctx, tag, since)
	if err != nil || len(jobs) != 1 || jobs[0].ID != "1001" || jobs[0].Source != "accounting" || jobs[0].Completing {
		t.Errorf("after the flag cleared FindByTag gives %+v, %v; want job 1001 from accounting, without the flag", jobs, err)
	}
}
