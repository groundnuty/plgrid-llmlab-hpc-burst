package slurm

// The QOS rules of HBEP-4 (The Slurm dialect; Submission, Refused before any
// call; QOS limits; Core operations, ReadProfile). Every expected value is
// worked out by hand from the HBEP, never read back through the code under
// test: the limits are written as literals, and the bound each case hits is
// named in a comment.

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func dur(s string) *v1alpha1.Duration { d := v1alpha1.Duration(s); return &d }
func i32(n int32) *int32              { return &n }
func qty(s string) *resource.Quantity { q := resource.MustParse(s); return &q }

// qosFixture is a request, profile and settings under the Provider qos `now`,
// on a partition of 8 GPUs and maxTime 48h. The profile lists `now` (12h wall
// limit, 1 job, 1 GPU, 72 CPUs, 122400Mi) and `plgrid` (2000 jobs). The
// request is exactly at every cap of `now`: 12h, 1 GPU, 72 CPUs, 122400Mi, so
// each case changes one thing and the cap is the only bound it can hit.
func qosFixture(edit func(req *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, part *v1alpha1.PartitionProfile)) (provider.SubmitRequest, provider.Profile, provider.Settings) {
	req, profile, settings := exampleRequest()
	eight := int32(8)
	profile.Partition.GPUsPerNode = &eight
	profile.Spec.QOS = []v1alpha1.QOSProfile{
		{Name: "now", MaxWall: dur("12h"), MaxSubmitPerUser: i32(1), MaxGPUsPerUser: i32(1), MaxCPUsPerUser: i32(72), MaxMemPerUser: qty("122400Mi")},
		{Name: "plgrid", MaxSubmitPerUser: i32(2000)},
	}
	settings.QOS, req.QOS = "now", "now"
	req.Params.GPUs, req.Params.CPUsPerTask, req.Params.Memory, req.Params.TimeLimit = 1, 72, resource.MustParse("122400Mi"), "12h"
	if edit != nil {
		edit(&req, &profile.Spec, &profile.Partition)
	}
	return req, profile, settings
}

func refusedWith(t *testing.T, req provider.SubmitRequest, profile provider.Profile, settings provider.Settings, names ...string) {
	t.Helper()
	_, err := checkRequest(req, profile, settings)
	if !errors.Is(err, provider.ErrInvalidRequest) {
		t.Fatalf("checkRequest = %v, want a refusal wrapping ErrInvalidRequest", err)
	}
	for _, n := range names {
		if !strings.Contains(err.Error(), n) {
			t.Errorf("the refusal %q does not name %q", err, n)
		}
	}
}

func accepted(t *testing.T, req provider.SubmitRequest, profile provider.Profile, settings provider.Settings) submission {
	t.Helper()
	s, err := checkRequest(req, profile, settings)
	if err != nil {
		t.Fatalf("checkRequest = %v, want the request accepted", err)
	}
	return s
}

// The request at every cap of `now` passes: the fixture is a control for the
// refusals below.
func TestQOSFixtureAtItsCapsPasses(t *testing.T) {
	req, profile, settings := qosFixture(nil)
	if s := accepted(t, req, profile, settings); s.qos != "now" || s.minutes != 720 || s.gpus != 1 || s.cpusPerTask != 72 || s.memMiB != 122400 {
		t.Errorf("submission %+v, want qos now, 720 minutes, 1 GPU, 72 CPUs, 122400 MiB", s)
	}
}

// HBEP-4, Test plan, The time limit is the smaller. maxTime 48h under maxWall
// 12h: 12h passes, 12h1m is refused. With maxWall 72h: 48h passes, 48h1m is
// refused. A partition QOS's maxWall of 1h applies whatever the Provider's QOS
// sets.
func TestTimeLimitIsTheSmallerOfMaxTimeAndMaxWall(t *testing.T) {
	wall := func(w string) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(_ *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
			spec.QOS[0].MaxWall = dur(w)
		}
	}
	limit := func(l string) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(r *provider.SubmitRequest, _ *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
			r.Params.TimeLimit = v1alpha1.Duration(l)
		}
	}
	both := func(fs ...func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile)) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(r *provider.SubmitRequest, s *v1alpha1.PlatformProfileSpec, p *v1alpha1.PartitionProfile) {
			for _, f := range fs {
				f(r, s, p)
			}
		}
	}
	partWall := func(w string) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(_ *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, p *v1alpha1.PartitionProfile) {
			spec.QOS[1].MaxWall = dur(w)
			p.QOS = "plgrid"
		}
	}
	for _, c := range []struct {
		name  string
		edit  func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile)
		ok    bool
		minut int64 // minutes sent, when ok
		names []string
	}{
		{"maxWall 12h, a 12h request", limit("12h"), true, 720, nil},
		{"maxWall 12h, a request one minute over", limit("12h1m"), false, 0, []string{"maxWall", "12h", `"now"`}},
		{"maxWall 72h, a 48h request (maxTime is the smaller)", both(wall("72h"), limit("48h")), true, 2880, nil},
		{"maxWall 72h, a request one minute over maxTime", both(wall("72h"), limit("48h1m")), false, 0, []string{"maxTime", "48h"}},
		{"maxWall 72h is no lift of maxTime", both(wall("72h"), limit("72h")), false, 0, []string{"maxTime"}},
		{"a partition QOS wall of 1h, a 1h request", both(partWall("1h"), limit("1h")), true, 60, nil},
		{"a partition QOS wall of 1h beats the Provider's 12h", both(partWall("1h"), limit("1h1m")), false, 0, []string{"maxWall", "plgrid"}},
		{"a partition QOS wall of 1h beats the Provider's 12h, 12h asked", both(partWall("1h"), limit("12h")), false, 0, []string{"maxWall", "plgrid"}},
		// The job QOS with overPartQos puts its own wall first.
		{"overPartQos on the job QOS: its 12h wins over the partition's 1h", both(partWall("1h"), limit("12h"),
			func(_ *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
				spec.QOS[0].OverPartQOS = true
			}), true, 720, nil},
		{"no wall limit anywhere: maxTime alone", both(
			func(_ *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
				spec.QOS[0].MaxWall = nil
			},
			limit("48h")), true, 2880, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			req, profile, settings := qosFixture(c.edit)
			if c.ok {
				if s := accepted(t, req, profile, settings); s.minutes != c.minut {
					t.Errorf("minutes = %d, want %d", s.minutes, c.minut)
				}
				return
			}
			refusedWith(t, req, profile, settings, c.names...)
		})
	}
}

// HBEP-4, Test plan, The GPU bound: maxGpusPerUser 1 on gpusPerNode 8: 2 GPUs
// are refused, 1 passes. The cap equal to gpusPerNode proves nothing, so the
// fixture's is the smaller by 7.
func TestGPUBoundIsTheSmallerOfGPUsPerNodeAndMaxGPUsPerUser(t *testing.T) {
	gpus := func(n int32) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(r *provider.SubmitRequest, _ *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
			r.Params.GPUs = n
		}
	}
	req, profile, settings := qosFixture(gpus(2))
	refusedWith(t, req, profile, settings, "maxGpusPerUser", "gpus 2")
	req, profile, settings = qosFixture(gpus(1))
	accepted(t, req, profile, settings)

	// With no QOS cap, gpusPerNode is the bound: 8 pass, 9 are refused.
	noCap := func(n int32) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(r *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
			spec.QOS[0].MaxGPUsPerUser = nil
			r.Params.GPUs = n
		}
	}
	req, profile, settings = qosFixture(noCap(8))
	accepted(t, req, profile, settings)
	req, profile, settings = qosFixture(noCap(9))
	refusedWith(t, req, profile, settings, "gpusPerNode 8")

	// The cap is not applied when the Provider names no qos: nothing limits
	// the request but the hardware.
	req, profile, settings = qosFixture(func(r *provider.SubmitRequest, _ *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
		r.Params.GPUs = 4
	})
	req.QOS, settings.QOS = "", ""
	accepted(t, req, profile, settings)
}

// An effective maxSubmitPerUser of 0 means no Allocation can be hosted
// (`@hpc-burst-science-agent`, 1 Nov 2026, the ruling on a GPU cap or a submit
// limit of 0). Slurm would accept nothing under it, and the refusal would read
// as a full slot that never ends, so the request is refused before any call, as
// a GPU cap of 0 already is (PR 168's review, Opus N6). The limit that applies
// is the first set in Slurm's order, so a partition QOS's 2000 beats the job
// QOS's 0, and a partition QOS's 0 beats the job QOS's 1.
func TestASubmitLimitOfZeroIsRefusedBeforeAnyCall(t *testing.T) {
	type edit = func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile)
	jobZero := func(_ *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
		spec.QOS[0].MaxSubmitPerUser = i32(0)
	}
	partition := func(sub *int32) edit {
		return func(_ *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, p *v1alpha1.PartitionProfile) {
			spec.QOS[1].MaxSubmitPerUser = sub
			p.QOS = "plgrid"
		}
	}
	both := func(fs ...edit) edit {
		return func(r *provider.SubmitRequest, s *v1alpha1.PlatformProfileSpec, p *v1alpha1.PartitionProfile) {
			for _, f := range fs {
				f(r, s, p)
			}
		}
	}
	for _, c := range []struct {
		name  string
		edit  edit
		ok    bool
		names []string
	}{
		{"the job QOS's limit is 0", jobZero, false, []string{"maxSubmitPerUser", `"now"`, "0"}},
		{"the partition QOS's 2000 comes before the job QOS's 0", both(jobZero, partition(i32(2000))), true, nil},
		{"the partition QOS's 0 comes before the job QOS's 1", partition(i32(0)), false, []string{"maxSubmitPerUser", `"plgrid"`}},
		{"the job QOS's limit is 1 (the fixture)", nil, true, nil},
		{"the partition QOS sets none and the job QOS's 0 applies", both(jobZero, partition(nil)), false, []string{"maxSubmitPerUser", `"now"`}},
	} {
		t.Run(c.name, func(t *testing.T) {
			req, profile, settings := qosFixture(c.edit)
			if c.ok {
				accepted(t, req, profile, settings)
				return
			}
			refusedWith(t, req, profile, settings, c.names...)
			runner := &countingRunner{}
			id, outcome, err := NewBatch(runner, profile, settings, Options{}).Submit(t.Context(), req)
			if id != "" || outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) {
				t.Errorf("Submit = %q, %s, %v; want no ID, Rejected and ErrInvalidRequest", id, outcome, err)
			}
			if n := runner.calls.Load(); n != 0 {
				t.Errorf("the runner was called %d times, want 0: a limit of 0 is refused before any call", n)
			}
		})
	}
	// With no Provider qos only the partition QOS's limits apply; its 0 is refused.
	req, profile, settings := qosFixture(partition(i32(0)))
	req.QOS, settings.QOS = "", ""
	refusedWith(t, req, profile, settings, "maxSubmitPerUser", `"plgrid"`)
}

// More CPUs or memory than the QOS's caps are refused the same way. Memory is
// compared in the whole MiB that go to Slurm, rounded up: 122400Mi passes,
// 122401Mi is refused, and 122400Mi plus one byte rounds up to 122401 MiB and
// is refused. Athena's 125Gi is exactly 128000 MiB (Slurm's G is 2^30), the
// same as A100's memPerGpu.
func TestCPUAndMemoryCaps(t *testing.T) {
	cpus := func(n int32) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(r *provider.SubmitRequest, _ *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
			r.Params.CPUsPerTask = n
		}
	}
	mem := func(m string) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(r *provider.SubmitRequest, _ *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
			r.Params.Memory = resource.MustParse(m)
		}
	}
	req, profile, settings := qosFixture(cpus(73))
	refusedWith(t, req, profile, settings, "maxCpusPerUser", "73")
	req, profile, settings = qosFixture(cpus(72))
	accepted(t, req, profile, settings)

	req, profile, settings = qosFixture(mem("122401Mi"))
	refusedWith(t, req, profile, settings, "maxMemPerUser", "122401")
	req, profile, settings = qosFixture(mem("122400Mi"))
	accepted(t, req, profile, settings)
	// 122400Mi is 128,345,702,400 bytes; one more byte rounds up to 122401 MiB.
	req, profile, settings = qosFixture(mem("128345702401"))
	refusedWith(t, req, profile, settings, "maxMemPerUser")

	athena := func(m string) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(r *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
			spec.QOS[0].MaxMemPerUser = qty("125Gi")
			r.Params.Memory = resource.MustParse(m)
		}
	}
	req, profile, settings = qosFixture(athena("128000Mi"))
	if s := accepted(t, req, profile, settings); s.memMiB != 128000 {
		t.Errorf("memMiB = %d, want 128000", s.memMiB)
	}
	req, profile, settings = qosFixture(athena("128001Mi"))
	refusedWith(t, req, profile, settings, "maxMemPerUser")

	// No cap, no refusal.
	req, profile, settings = qosFixture(func(r *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
		spec.QOS[0].MaxCPUsPerUser, spec.QOS[0].MaxMemPerUser = nil, nil
		r.Params.CPUsPerTask, r.Params.Memory = 500, resource.MustParse("900Gi")
	})
	accepted(t, req, profile, settings)
}

// A memory cap that is not a whole number of MiB: Kubernetes reads 100G as
// 100,000,000,000 bytes, which is 95367.43 MiB, and 125G as 119209.29 MiB, not
// Slurm's G of 2^30. The cap is rounded down to whole MiB and the request up
// (the whole MiB that go to Slurm), so a job never exceeds the cap: 95367Mi
// passes under 100G and 95368Mi is refused, and 119209Mi passes under 125G and
// 119210Mi is refused. A request spelled in the same decimal unit is not whole
// MiB and rounds up past the cap (100G is 95368 MiB), so a caller that sizes a
// request from a cap must clamp it to whole MiB itself (PR 168's review, Opus
// N10, and PR 171's B2); this pins the provider's side of that contract.
func TestAMemoryCapThatIsNotWholeMiBRoundsDown(t *testing.T) {
	withCap := func(capQty, request string) func(*provider.SubmitRequest, *v1alpha1.PlatformProfileSpec, *v1alpha1.PartitionProfile) {
		return func(r *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
			spec.QOS[0].MaxMemPerUser = qty(capQty)
			r.Params.Memory = resource.MustParse(request)
		}
	}
	for _, c := range []struct {
		capQty, request string
		ok              bool
		memMiB          int64 // sent, when ok
		capMiB          string
	}{
		{"100G", "95367Mi", true, 95367, ""},
		{"100G", "95368Mi", false, 0, "95367"},
		{"100G", "100G", false, 0, "95367"}, // 100,000,000,000 bytes round up to 95368 MiB
		{"125G", "119209Mi", true, 119209, ""},
		{"125G", "119210Mi", false, 0, "119209"},
		{"125G", "125G", false, 0, "119209"}, // 119210 MiB
	} {
		t.Run(c.capQty+" cap, "+c.request+" asked", func(t *testing.T) {
			req, profile, settings := qosFixture(withCap(c.capQty, c.request))
			if c.ok {
				if s := accepted(t, req, profile, settings); s.memMiB != c.memMiB {
					t.Errorf("memMiB = %d, want %d", s.memMiB, c.memMiB)
				}
				return
			}
			refusedWith(t, req, profile, settings, "maxMemPerUser", "("+c.capMiB+" MiB)")
		})
	}
}

// The flag, the safe pattern and the sources of the limits.
func TestQOSValueIsCheckedBeforeAnyCall(t *testing.T) {
	// An unsafe qos that the profile lists: only the safe pattern can refuse
	// it, so the "not listed" check is not what these cases test.
	for _, bad := range []string{"now;id", "now id", "$(id)", "now\nid", "now`id`", "now'x"} {
		t.Run(bad, func(t *testing.T) {
			req, profile, settings := qosFixture(func(r *provider.SubmitRequest, spec *v1alpha1.PlatformProfileSpec, _ *v1alpha1.PartitionProfile) {
				spec.QOS[0].Name = bad
			})
			req.QOS, settings.QOS = bad, bad
			refusedWith(t, req, profile, settings, "qos")
			runner := &countingRunner{}
			id, outcome, err := NewBatch(runner, profile, settings, Options{}).Submit(t.Context(), req)
			if id != "" || outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) {
				t.Errorf("Submit = %q, %s, %v; want no ID, Rejected and ErrInvalidRequest", id, outcome, err)
			}
			if n := runner.calls.Load(); n != 0 {
				t.Errorf("the runner was called %d times, want 0", n)
			}
		})
	}
	// The request's qos is the Provider's: a request that names another is
	// refused, so the flag and the limits never come from two sources.
	req, profile, settings := qosFixture(nil)
	req.QOS = "plgrid"
	refusedWith(t, req, profile, settings, "plgrid", "now")
	req, profile, settings = qosFixture(nil)
	req.QOS = ""
	refusedWith(t, req, profile, settings, "now")

	// A qos or a partition qos that the profile does not list is refused: a
	// stored object is not trusted blindly.
	req, profile, settings = qosFixture(nil)
	req.QOS, settings.QOS = "unlisted", "unlisted"
	refusedWith(t, req, profile, settings, "unlisted")
	req, profile, settings = qosFixture(func(_ *provider.SubmitRequest, _ *v1alpha1.PlatformProfileSpec, p *v1alpha1.PartitionProfile) {
		p.QOS = "unlisted-partition-qos"
	})
	refusedWith(t, req, profile, settings, "unlisted-partition-qos")
}

// HBEP-4, The Slurm dialect: --qos=<qos> follows --account and precedes --time;
// with no qos no flag is sent at all.
func TestSubmitLineQOS(t *testing.T) {
	s := exampleSubmission()
	s.qos = "now"
	line, err := submitLine(s)
	if err != nil {
		t.Fatal(err)
	}
	if want := "--account=example-account --qos=now --time=2880 "; !strings.Contains(line, want) {
		t.Errorf("the line lacks %q: %s", want, line)
	}
	args := sbatchArgv(t, line)
	n := 0
	for _, a := range args {
		if strings.HasPrefix(a, "--qos") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d --qos words in %q, want 1", n, args)
	}

	s.qos = ""
	line, err = submitLine(s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(line, "--qos") {
		t.Errorf("no qos is set, but the line sends the flag: %s", line)
	}
}

// ReadProfile fills Settings.QOS and checks it (HBEP-4, Core operations).
func TestReadProfileQOS(t *testing.T) {
	listed := func(r *profileRig) {
		r.pp.Spec.QOS = []v1alpha1.QOSProfile{{Name: "now", MaxWall: dur("12h"), MaxSubmitPerUser: i32(1)}, {Name: "plgrid"}}
	}
	r := newProfileRig(t)
	listed(r)
	r.prov.Spec.QOS = "now"
	v, err := r.read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Settings.QOS != "now" {
		t.Errorf("Settings.QOS = %q, want now", v.Settings.QOS)
	}
	// No qos: none in the settings.
	r = newProfileRig(t)
	listed(r)
	if v, err := r.read(); err != nil || v.Settings.QOS != "" {
		t.Errorf("no qos: Settings.QOS = %q, error %v; want none", v.Settings.QOS, err)
	}
	// The partition's qos, listed, passes.
	r = newProfileRig(t)
	listed(r)
	r.pp.Spec.Partitions[0].QOS = "plgrid"
	r.prov.Spec.QOS = "now"
	if _, err := r.read(); err != nil {
		t.Errorf("a listed partition qos: %v", err)
	}

	for _, c := range []struct {
		name  string
		mod   func(r *profileRig)
		field string
	}{
		{"a qos the profile does not list", func(r *profileRig) { listed(r); r.prov.Spec.QOS = "normal" }, "provider.spec.qos"},
		{"a qos with an empty list", func(r *profileRig) { r.prov.Spec.QOS = "now" }, "provider.spec.qos"},
		{"an unsafe qos, listed", func(r *profileRig) {
			r.pp.Spec.QOS = []v1alpha1.QOSProfile{{Name: "now;id"}}
			r.prov.Spec.QOS = "now;id"
		}, "provider.spec.qos"},
		{"a partition qos the profile does not list", func(r *profileRig) { listed(r); r.pp.Spec.Partitions[0].QOS = "unlisted" }, "spec.partitions[0].qos"},
		{"a partition qos with an empty list", func(r *profileRig) { r.pp.Spec.Partitions[0].QOS = "plgrid" }, "spec.partitions[0].qos"},
		{"a wall limit that is not a duration", func(r *profileRig) {
			listed(r)
			r.pp.Spec.QOS[0].MaxWall = dur("soon")
		}, "spec.qos[0].maxWall"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newProfileRig(t)
			c.mod(r)
			_, err := r.read()
			var inv *provider.InvalidProfileError
			if !errors.As(err, &inv) {
				t.Fatalf("got %v, want an InvalidProfileError", err)
			}
			for _, f := range inv.Fields {
				if f.Field == c.field {
					return
				}
			}
			t.Errorf("field %s not named in %v", c.field, inv.Fields)
		})
	}
}
