package e2e

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestPlatformProfileTypedFields(t *testing.T) {
	refused := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
		want   string
	}{
		{"unknown provider kind", func(p *v1alpha1.PlatformProfile) { p.Spec.ProviderKind = "cloud" }, "Unsupported value"},
		{"no provider kind", func(p *v1alpha1.PlatformProfile) { p.Spec.ProviderKind = "" }, "providerKind"},
		{"lostGrace is not a duration", func(p *v1alpha1.PlatformProfile) { p.Spec.LostGrace = ptr.To(v1alpha1.Duration("soon")) }, "lostGrace"},
		{"inflightWindow is not a duration", func(p *v1alpha1.PlatformProfile) { p.Spec.InflightWindow = ptr.To(v1alpha1.Duration("5")) }, "inflightWindow"},
		{"unknownBound is not a duration", func(p *v1alpha1.PlatformProfile) { p.Spec.UnknownBound = ptr.To(v1alpha1.Duration("10 minutes")) }, "unknownBound"},
		{"partition without a GPU model", func(p *v1alpha1.PlatformProfile) { p.Spec.Partitions[0].GPUModel = "" }, "gpuModel"},
		{"partition without a CPU architecture", func(p *v1alpha1.PlatformProfile) { p.Spec.Partitions[0].CPUArch = "" }, "cpuArch"},
		{"timeZone is not a zone name", func(p *v1alpha1.PlatformProfile) { p.Spec.TimeZone = "Warsaw time" }, "timeZone"},
		{"timeZone is empty text of spaces", func(p *v1alpha1.PlatformProfile) { p.Spec.TimeZone = " " }, "timeZone"},
		{"minJobAge is not a duration", func(p *v1alpha1.PlatformProfile) { p.Spec.MinJobAge = ptr.To(v1alpha1.Duration("ten seconds")) }, "minJobAge"},
		{"jobName.maxLength is zero", func(p *v1alpha1.PlatformProfile) {
			p.Spec.JobName = &v1alpha1.JobNameRules{MaxLength: ptr.To(int32(0))}
		}, "maxLength"},
		{"jobName.maxLength is below the tag's 56 characters", func(p *v1alpha1.PlatformProfile) {
			p.Spec.JobName = &v1alpha1.JobNameRules{MaxLength: ptr.To(int32(55))}
		},
			"jobName.maxLength must allow the job tag's 56 characters"},
		{"jsonDataParser has a space", func(p *v1alpha1.PlatformProfile) { p.Spec.JSONDataParser = "data parser" }, "jsonDataParser"},
		{"two entries for one partition", func(p *v1alpha1.PlatformProfile) { p.Spec.Partitions[1].Name = p.Spec.Partitions[0].Name }, "Duplicate value"},
	}
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			tc.mutate(p)
			requireRefused(t, c.Create(bg(), p), tc.want)
		})
	}
	accepted := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
	}{
		{"the base profile", func(*v1alpha1.PlatformProfile) {}},
		{"every typed field set", func(p *v1alpha1.PlatformProfile) {
			p.Spec.HoldRelease = true
			p.Spec.LostGrace = ptr.To(v1alpha1.Duration("90s"))
			p.Spec.InflightWindow = ptr.To(v1alpha1.Duration("1m30s"))
			p.Spec.UnknownBound = ptr.To(v1alpha1.Duration("15m"))
		}},
		{"every scheduling fact set", func(p *v1alpha1.PlatformProfile) {
			p.Spec.TimeZone = "Europe/Warsaw"
			p.Spec.RequeueDefault = ptr.To(true)
			p.Spec.MinJobAge = ptr.To(v1alpha1.Duration("10s"))
			p.Spec.JobName = &v1alpha1.JobNameRules{MaxLength: ptr.To(int32(64)), Pattern: `^[A-Za-z0-9_.-]+$`}
			p.Spec.JobCommentAccounted = ptr.To(false)
			p.Spec.JSONDataParser = "data_parser/v0.0.42"
		}},
		{"a job-name limit of exactly the tag's 56 characters", func(p *v1alpha1.PlatformProfile) {
			p.Spec.JobName = &v1alpha1.JobNameRules{MaxLength: ptr.To(int32(v1alpha1.JobTagLength))}
		}},
		{"job-name rules with a pattern and no limit", func(p *v1alpha1.PlatformProfile) {
			p.Spec.JobName = &v1alpha1.JobNameRules{Pattern: `^[a-z]+$`}
		}},
		{"UTC as the time zone", func(p *v1alpha1.PlatformProfile) { p.Spec.TimeZone = "UTC" }},
		{"a managed platform without partitions", func(p *v1alpha1.PlatformProfile) {
			p.Spec.ProviderKind, p.Spec.Partitions = v1alpha1.PlatformManaged, nil
		}},
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			tc.mutate(p)
			must(t, c.Create(bg(), p), "create PlatformProfile")
			cleanup(t, p)
		})
	}
}

// stagingDir names no ".." segment, because the agent's configuration refuses
// one (agentapi.AgentConfig). A ".." inside a longer segment is an ordinary
// name. This is the rule platformprofiles:$.spec.stagingDir#0.
func TestStagingDirRefusesDotDotSegments(t *testing.T) {
	const msg = "stagingDir must not contain a '..' segment"
	for _, dir := range []string{"/scratch/../etc", "$SCRATCH/..", "/scratch/a/../b", "/.."} {
		t.Run("refuse "+dir, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.StagingDir = dir
			requireRefused(t, c.Create(bg(), p), msg)
		})
	}
	for _, dir := range []string{"/scratch/a..b/x", "$SCRATCH/hpcburst", "/scratch/.../x", "/scratch/..a/b.."} {
		t.Run("accept "+dir, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.StagingDir = dir
			must(t, c.Create(bg(), p), "create with stagingDir "+dir)
			cleanup(t, p)
		})
	}
}

// A partition's tmpDir has stagingDir's form: an absolute path, or one that
// starts with a variable the job sets (science-agent ruling, 1 Nov 2026, issue
// #125). The pattern refuses a relative path, ~, a space and a substitution.
func TestPartitionTmpDirForm(t *testing.T) {
	for _, dir := range []string{"memfs", "~/tmp", "$MEMFS/a b", "$(id)", "${MEMFS}", "$1/x", ""} {
		t.Run("refuse "+dir, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Partitions[0].TmpDir = dir
			if dir == "" {
				// An empty string is the absent field (omitempty): admitted.
				must(t, c.Create(bg(), p), "create with no tmpDir")
				cleanup(t, p)
				return
			}
			requireRefused(t, c.Create(bg(), p), "tmpDir")
		})
	}
	for _, dir := range []string{"$MEMFS", "$MEMFS/hb", "/dev/shm", "$SCRATCH_LOCAL/t-1"} {
		t.Run("accept "+dir, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Partitions[0].TmpDir = dir
			must(t, c.Create(bg(), p), "create with tmpDir "+dir)
			var got v1alpha1.PlatformProfile
			must(t, c.Get(bg(), client.ObjectKeyFromObject(p), &got), "read back")
			if got.Spec.Partitions[0].TmpDir != dir {
				t.Errorf("tmpDir read back as %q, want %q", got.Spec.Partitions[0].TmpDir, dir)
			}
			cleanup(t, p)
		})
	}
}

// A partition's tmpDir names no ".." segment, like stagingDir. A ".." inside a
// longer segment is an ordinary name. This is the rule
// platformprofiles:$.spec.partitions[].tmpDir#0.
func TestPartitionTmpDirRefusesDotDotSegments(t *testing.T) {
	const msg = "tmpDir must not contain a '..' segment"
	for _, dir := range []string{"$MEMFS/..", "/dev/shm/../etc", "$MEMFS/a/../b", "/.."} {
		t.Run("refuse "+dir, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Partitions[0].TmpDir = dir
			requireRefused(t, c.Create(bg(), p), msg)
		})
	}
	for _, dir := range []string{"$MEMFS/a..b", "/dev/shm/.../x", "$MEMFS/..a/b.."} {
		t.Run("accept "+dir, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Partitions[0].TmpDir = dir
			must(t, c.Create(bg(), p), "create with tmpDir "+dir)
			cleanup(t, p)
		})
	}
}

// The scheduling facts of spikes S4 and S5 are typed fields, and a fact that is
// not stated is absent: an absent bool is unknown, not false, and it round-trips
// as absent.
func TestPlatformProfileSchedulingFactsRoundTrip(t *testing.T) {
	full := newProfile(uniq("prof"))
	full.Spec.TimeZone = "Europe/Warsaw"
	full.Spec.RequeueDefault = ptr.To(false)
	full.Spec.MinJobAge = ptr.To(v1alpha1.Duration("300s"))
	full.Spec.JobName = &v1alpha1.JobNameRules{MaxLength: ptr.To(int32(1024))}
	full.Spec.JobCommentAccounted = ptr.To(false)
	full.Spec.JSONDataParser = "data_parser/v0.0.42"
	must(t, c.Create(bg(), full), "create")
	cleanup(t, full)
	got := &v1alpha1.PlatformProfile{}
	must(t, c.Get(bg(), objKey(full), got), "get")
	s := got.Spec
	if s.TimeZone != "Europe/Warsaw" || s.JSONDataParser != "data_parser/v0.0.42" || s.MinJobAge == nil || *s.MinJobAge != "300s" ||
		s.JobName == nil || s.JobName.MaxLength == nil || *s.JobName.MaxLength != 1024 {
		t.Fatalf("spec = %+v", s)
	}
	// false is a stated fact and stays; absent is unknown and stays absent.
	if s.RequeueDefault == nil || *s.RequeueDefault || s.JobCommentAccounted == nil || *s.JobCommentAccounted {
		t.Fatalf("a stated false came back as %v / %v", s.RequeueDefault, s.JobCommentAccounted)
	}
	bare := newProfile(uniq("prof"))
	must(t, c.Create(bg(), bare), "create a profile that states none")
	cleanup(t, bare)
	must(t, c.Get(bg(), objKey(bare), got), "get")
	s = got.Spec
	if s.TimeZone != "" || s.RequeueDefault != nil || s.MinJobAge != nil || s.JobName != nil || s.JobCommentAccounted != nil || s.JSONDataParser != "" {
		t.Fatalf("unstated facts came back as %+v", s)
	}
	if v1alpha1.JobTagLength != len("hb-")+40+len("-")+12 {
		t.Errorf("JobTagLength = %d; HBEP-3's tag hb-<40>-<12 hex> has %d characters", v1alpha1.JobTagLength, len("hb-")+40+len("-")+12)
	}
}

// The kind of provider a platform offers is spec.providerKind, not spec.kind,
// which reads like the object's own kind. The check reads the stored object.
func TestPlatformProfileProviderKindKey(t *testing.T) {
	p := newProfile(uniq("prof"))
	must(t, c.Create(bg(), p), "create")
	cleanup(t, p)
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("PlatformProfile"))
	must(t, c.Get(bg(), objKey(p), u), "get unstructured")
	spec, _, _ := unstructured.NestedMap(u.Object, "spec")
	if spec["providerKind"] != string(v1alpha1.PlatformBatch) {
		t.Errorf("spec.providerKind = %v; keys %v", spec["providerKind"], spec)
	}
	if _, wrong := spec["kind"]; wrong {
		t.Errorf("spec.kind is written: %v", spec)
	}
}

// The untyped block keeps whatever the profile holds beyond the typed fields.
func TestPlatformProfileKeepsTheUntypedBlock(t *testing.T) {
	p := newProfile(uniq("prof"))
	p.Spec.Profile = jsonOf(t, `{"loginNodePolicy":"example","extraFact":true,"nested":{"a":[1,2,3]}}`)
	must(t, c.Create(bg(), p), "create PlatformProfile with a profile block")
	cleanup(t, p)
	got := &v1alpha1.PlatformProfile{}
	must(t, c.Get(bg(), objKey(p), got), "get")
	if got.Spec.Profile == nil || string(got.Spec.Profile.Raw) != `{"extraFact":true,"loginNodePolicy":"example","nested":{"a":[1,2,3]}}` {
		t.Fatalf("the untyped profile block was pruned or changed: %v", got.Spec.Profile)
	}
}
