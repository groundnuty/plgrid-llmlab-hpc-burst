package lifecycle

// Retention (HBEP-7, Retention; Test plan: "Retention", and "A job ends and
// leaves the queue listing seconds later"). An Allocation is empty when it is
// started and hosts no live Instance. Every test here first makes it empty and
// checks that it is, because a retention test whose Allocation never becomes
// empty proves nothing: the default policy keeps a started Allocation, so an
// Allocation that was simply never emptied would also "stay".

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// retentionEmpty serves one Instance of "md" with the policy, then sets
// spec.instances to 0 and steps until the Instance has ended. The Allocation is
// then started and empty; the clock is the time of the pass that saw it empty,
// which is also the pass that starts its idle clock.
func retentionEmpty(t *testing.T, rg *rig, policy *v1alpha1.RetentionPolicy) (v1alpha1.Allocation, time.Time) {
	t.Helper()
	md := mdSpec("md")
	md.Spec.RetentionPolicy = policy
	rg.create(md)
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	name := rg.liveInstances("md")[0].Name
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	allocUntilEnded(rg, name, 2*time.Minute)
	a, ok := rg.allocation(rg.allocations("md")[0].Name)
	if !ok || !a.DeletionTimestamp.IsZero() || a.Status.AtProvider.StartTime == nil || len(rg.liveInstances("md")) != 0 {
		t.Fatalf("the Allocation must be started, empty and kept at first: %+v", a)
	}
	return *a, rg.clock.Now()
}

// retentionReleaseAt steps in ticks of tick and fails if the Allocation is
// released before release, or is still kept 10 s after it.
func retentionReleaseAt(t *testing.T, rg *rig, name string, release time.Time, tick time.Duration) {
	t.Helper()
	for rg.clock.Now().Before(release) {
		if allocReleased(rg, name) {
			t.Fatalf("Allocation released at %s, before %s", rg.clock.Now().Format(time.TimeOnly), release.Format(time.TimeOnly))
		}
		rg.clock.Advance(min(tick, release.Sub(rg.clock.Now())))
		rg.Step()
	}
	if !allocReleased(rg, name) {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	if !allocReleased(rg, name) {
		t.Fatalf("Allocation still kept at %s, want released at %s", rg.clock.Now().Format(time.TimeOnly), release.Format(time.TimeOnly))
	}
}

// TestRetentionKeepUntilDateStaysThenReleases covers "an Allocation kept until a
// date stays, with no Instance, until that date and is then released". The date
// is 09:00:00, an hour after the rig's start; the job's hard end is 48 h away.
func TestRetentionKeepUntilDateStaysThenReleases(t *testing.T) {
	rg := newRig(t)
	until := metav1.NewTime(rigStart.Add(time.Hour))
	a, _ := retentionEmpty(t, rg, &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: &until})
	retentionReleaseAt(t, rg, a.Name, until.Time, time.Minute)
	rg.run(time.Minute, 10*time.Second)
	if _, there := rg.allocation(a.Name); there {
		t.Error("the released Allocation is still there a minute later")
	}
	if n := allocCount(rg.batch, "scancel", allocJobID(&a)); n < 1 {
		t.Errorf("scancel calls for the released job = %d, want at least 1", n)
	}
}

// TestRetentionReleaseWhenIdleAfterIdleLimit covers "one under release-when-idle
// is released after its idle limit": 20 min after the Allocation became empty.
func TestRetentionReleaseWhenIdleAfterIdleLimit(t *testing.T) {
	rg := newRig(t)
	a, emptyAt := retentionEmpty(t, rg, &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(20 * time.Minute)})
	retentionReleaseAt(t, rg, a.Name, emptyAt.Add(20*time.Minute), 10*time.Second)
}

// TestRetentionProfileMaxIdleReleasesUnderKeep covers "one on a profile with
// maxIdle is released after it under keep": the policy is unset, which means
// Keep, and the profile's maxIdle of 30 min applies.
func TestRetentionProfileMaxIdleReleasesUnderKeep(t *testing.T) {
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = dur(30 * time.Minute) }))
	a, emptyAt := retentionEmpty(t, rg, nil)
	if m := rg.md("md").Status.Retention; m == nil || m.EffectiveMode != v1alpha1.RetentionKeep {
		t.Fatalf("status.retention = %+v, want effectiveMode Keep", m)
	}
	retentionReleaseAt(t, rg, a.Name, emptyAt.Add(30*time.Minute), 10*time.Second)
}

// TestRetentionEffectiveModeFollowsPolicy covers "status.retention.effectiveMode
// is Keep for an unset policy", and that it is not a constant: a policy of
// ReleaseWhenIdle shows as that.
func TestRetentionEffectiveModeFollowsPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy *v1alpha1.RetentionPolicy
		want   v1alpha1.RetentionMode
	}{
		{"no policy", nil, v1alpha1.RetentionKeep},
		{"a policy without a mode", &v1alpha1.RetentionPolicy{}, v1alpha1.RetentionKeep},
		{"release when idle", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(time.Hour)}, v1alpha1.RetentionReleaseWhenIdle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			md := mdSpec("md")
			md.Spec.RetentionPolicy = tc.policy
			rg.create(md)
			rg.run(time.Minute, 10*time.Second)
			if got := rg.md("md").Status.Retention; got == nil || got.EffectiveMode != tc.want {
				t.Errorf("status.retention = %+v, want effectiveMode %s", got, tc.want)
			}
		})
	}
}

// TestRetentionNoneOutlivesHardEnd covers "none outlives its hard end": a policy
// that would keep the Allocation past the lease (a date a day away, or an idle
// limit of ten hours, on a 4 h lease) still releases it when its job ends.
func TestRetentionNoneOutlivesHardEnd(t *testing.T) {
	day := metav1.NewTime(rigStart.Add(24 * time.Hour))
	for _, tc := range []struct {
		name   string
		policy *v1alpha1.RetentionPolicy
	}{
		{"keep until a day away", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: &day}},
		{"release when idle after ten hours", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(10 * time.Hour)}},
		{"the default policy", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, withMaxTime(4*time.Hour))
			a, _ := retentionEmpty(t, rg, tc.policy)
			// The job started at 08:00:20 (the smoke test's calculation): hard end 12:00:20.
			hardEnd := time.Date(2026, 11, 2, 12, 0, 20, 0, time.UTC)
			if he := a.Status.AtProvider.HardEnd; he == nil || !he.Time.Equal(hardEnd) {
				t.Fatalf("hard end = %v, want %s", he, hardEnd)
			}
			retentionReleaseAt(t, rg, a.Name, hardEnd, time.Minute)
		})
	}
}

// TestRetentionJobEndsAndLeavesListing covers "A job ends and leaves the queue
// listing seconds later: the controller reads it as ended, from accounting, and
// submits nothing again for it". The empty Allocation is kept until its hard end
// (12:00:20). The fake lists a finished job for MinJobAge, 10 s, then only
// accounting knows it. In the first case the controller does not look again
// until 40 s after the end, so its first read of the ended job is from
// accounting (a sacct call for the job ID, which a queue read does not make);
// in the second it looks every 10 s and sees the end in the listing.
// Submissions are counted as sbatch calls, which a count of Allocations would
// not show.
func TestRetentionJobEndsAndLeavesListing(t *testing.T) {
	for _, tc := range []struct {
		name string
		jump bool
	}{{"the controller looks after the job left the listing", true}, {"the controller looks every 10 s", false}} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, withMaxTime(4*time.Hour))
			a, _ := retentionEmpty(t, rg, nil)
			job, hardEnd := allocJobID(&a), a.Status.AtProvider.HardEnd.Time
			allocRunTo(rg, hardEnd.Add(-time.Minute), time.Minute)
			subs := rg.submissions()
			if n := allocCount(rg.batch, "sacct", job); n != 0 {
				t.Fatalf("%d sacct calls for the job before it ended, want 0", n)
			}
			if tc.jump {
				rg.clock.Advance(time.Minute + 40*time.Second) // hardEnd + 40 s
				rg.Step()
			} else {
				allocRunTo(rg, hardEnd.Add(20*time.Second), 10*time.Second)
			}
			got, there := rg.allocation(a.Name)
			if there && got.Status.AtProvider.Phase != v1alpha1.AllocationEnded {
				t.Errorf("phase %q after the job ended, want Ended", got.Status.AtProvider.Phase)
			}
			if !allocReleased(rg, a.Name) {
				t.Error("the Allocation of the ended job is not released")
			}
			if n := allocCount(rg.batch, "sacct", job); tc.jump && n == 0 {
				t.Error("no sacct call for the job: its end was not read from accounting")
			}
			rg.run(10*time.Minute, time.Minute)
			if rg.submissions() != subs || len(rg.allocations("md")) != 0 {
				t.Errorf("sbatch calls %d (was %d), Allocations %d: nothing may be submitted again", rg.submissions(), subs, len(rg.allocations("md")))
			}
		})
	}
}
