package fakeslurm

// New. #198 (Astra B1 on PR 217): Platform.RenameListed makes squeue and sacct
// disagree about a job's name, the fixture of the tests of "queue evidence wins
// by job ID". squeue lists the new name, by --me and by --jobs; sacct keeps the
// submitted name, so a lookup by the submitted name still finds the job in
// accounting and no lookup by the new name does.

import "testing"

func TestRenameListedChangesWhatSqueueSaysAndNotWhatSacctSays(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--job-name=submitted-tag")
	r.start(id)
	r.end(id, Cancelled)
	if err := r.p.HoldCompleting(id, true); err != nil { // listed in every mode
		t.Fatal(err)
	}
	if err := r.p.RenameListed(id, "changed-name"); err != nil {
		t.Fatal(err)
	}
	if got := r.squeueJob(id)["name"]; got != "changed-name" {
		t.Errorf("squeue --jobs lists the name %v, want changed-name", got)
	}
	if got := jobsOf(t, r.squeue()); len(got) != 1 || got[0]["name"] != "changed-name" {
		t.Errorf("squeue --me lists %v, want the one job under changed-name", got)
	}
	if got := jobsOf(t, r.sacct("-X", "--name=submitted-tag")); len(got) != 1 || got[0]["name"] != "submitted-tag" {
		t.Errorf("sacct by the submitted name lists %v, want the one job under submitted-tag", got)
	}
	if got := jobsOf(t, r.sacct("-X", "--name=changed-name")); len(got) != 0 {
		t.Errorf("sacct by the new name lists %v, want none: accounting never heard of it", got)
	}
	if err := r.p.RenameListed(id, ""); err != nil {
		t.Fatal(err)
	}
	if got := r.squeueJob(id)["name"]; got != "submitted-tag" {
		t.Errorf("after an empty rename squeue lists %v, want submitted-tag", got)
	}
}
