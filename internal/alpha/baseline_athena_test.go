package alpha

// The Athena twin of the baseline row (baseline.go, AthenaBaselineRow): its row, its example, its ID, its settings and its plan, with the values written out as
// literals of the test's own, never read back from the code that holds them. The flow, phases and verdicts are Row 1's code (baseline_test.go,
// baselinejudge_test.go) and are not tested twice; dry_baseline_athena_test.go holds the Athena twin's dry runs.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The row is alpha-athena-now-serve's shape (plgrid-now under athena-now.yaml, the lease of 48 minutes, a start wait of 10, the slot hold of 10 + 48 + 2 and the
// owner's go of plgrid-now) with Row 1's cap at the Athena value, the managed LLMLab leg and the generator, and the Athena example. The expected values
// are written out here: the science agent's ruling of 19 Nov 2026 on #764 ("a straight copy of Row 1's flow on athena-now.yaml, with the Qwen3-VL example at my H3
// cap of 3").
func TestTheAthenaBaselineRowIsRowAsShapeWithTheAthenaCap(t *testing.T) {
	r := AthenaBaselineRow()
	if r.Name != "beta-athena-now-llmlab-baseline" || r.Cluster != "athena" || r.ProfileName() != "athena-now" || r.Partition != "plgrid-now" || r.QOS != "" || r.GPUs != 1 || r.MostJobs != 1 {
		t.Errorf("the row is %+v", r)
	}
	if r.Lease != 48*time.Minute || r.StartWait != 10*time.Minute || r.SlotHold != 60*time.Minute || r.Budget != 115*time.Minute || r.OwnerGo != "LIVE_OWNER_GO_ATHENA_NOW" {
		t.Errorf("lease %s, start wait %s, slot hold %s, budget %s, owner's go %s", r.Lease, r.StartWait, r.SlotHold, r.Budget, r.OwnerGo)
	}
	if r.Timing != AthenaNowTiming() || r.Clock().Lead() != 20*time.Minute {
		t.Errorf("the clock is %+v with a lead of %s, want athena-now.yaml's 20 minutes", r.Timing, r.Clock().Lead())
	}
	if r.InstanceCap != 3 || CapAthena != 3 || !r.LLMLab || !r.Studyload || r.ProviderStaging || r.PostsTo != "#764" ||
		r.DeploymentFile() != "modeldeployment-athena-now-qwen3-vl-8b-instruct.yaml" {
		t.Errorf("the row's cap %d, LLMLab %t, generator %t, Provider staging %t, posts to %s, example %s", r.InstanceCap, r.LLMLab, r.Studyload, r.ProviderStaging, r.PostsTo, r.DeploymentFile())
	}
	if !IsBaselineRow(r.Name) || !IsBaselineRow(BetaBaselineName) || IsBaselineRow("alpha-athena-now-serve") {
		t.Error("IsBaselineRow does not name exactly the two baseline rows")
	}
	// Row 1 is as it was.
	if s := BaselineRow(); s.Name != "beta-sano-llmlab-baseline" || s.Cluster != "sano" || s.InstanceCap != 11 || s.Lease != 106*time.Minute || s.Budget != 150*time.Minute || !s.ProviderStaging {
		t.Errorf("Row 1 changed: %+v", s)
	}
	// P1 and P1-long with N = 3, P2 and P3 with N = 5 (cap + k, k = 2).
	got := DefaultPhases(int(r.InstanceCap))
	want := map[string]int{PhaseP1: 3, PhaseP1Long: 3, PhaseP2: 5, PhaseP3: 5}
	for _, p := range got {
		if want[p.Name] != p.Sessions {
			t.Errorf("%s has N = %d, want %d", p.Name, p.Sessions, want[p.Name])
		}
	}
}

// The example the row serves is the Sano example's checkpoint, engine and flags as shipped, with overAllocation 0 and the cap, and its deployment ID is the
// one the row pins: derived here from the example, the Hub's file list of the checkpoint (internal/slurm/testdata/checkpoints) and the class of
// athena-now.yaml's partition, by the deployment-ID package. internal/slurm derives the same ID in its own test and holds the pin as a literal; this one holds
// the row's constant to it.
func TestTheAthenaBaselineExampleAndItsDeploymentIDArePinned(t *testing.T) {
	repo := filepath.Join("..", "..")
	var md v1alpha1.ModelDeployment
	readYAML(t, filepath.Join(repo, "examples", AthenaQwen3VLDeployment), &md)
	s := md.Spec.Source.Served
	if s == nil || md.Spec.Model != BaselineModel || s.Checkpoint.Repository != BaselineModel || s.Checkpoint.Revision != BaselineRevision {
		t.Fatalf("the example's model, repository or revision is not %s at %s", BaselineModel, BaselineRevision)
	}
	if s.Engine.ImageDigest != "sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34" || s.Engine.Version != "0.32.0" || len(s.Flags) != 1 || s.Flags[0].Name != "max-model-len" || s.Flags[0].Value != "32768" {
		t.Errorf("the engine is %s %s with flags %v, want vLLM 0.32.0 at its digest with max-model-len 32768 alone (as shipped)", s.Engine.Version, s.Engine.ImageDigest, s.Flags)
	}
	if md.Spec.OverAllocation == nil || *md.Spec.OverAllocation != 0 || md.Spec.Instances != 1 || md.Spec.Routing == nil || md.Spec.Routing.InstanceCap == nil || *md.Spec.Routing.InstanceCap != 3 {
		t.Errorf("overAllocation %v, instances %d, routing %+v: want 0, 1 and instanceCap 3", md.Spec.OverAllocation, md.Spec.Instances, md.Spec.Routing)
	}
	if AthenaBaselineDeploymentID != "965fa2409193" || len(AthenaBaselineDeploymentID) != v1alpha1.DisplayIDLength {
		t.Errorf("the deployment ID the Athena row checks is %q, want 965fa2409193 (12 hex characters)", AthenaBaselineDeploymentID)
	}
	// Derived, not copied: the example, the checkpoint's files and the partition's class.
	var pp v1alpha1.PlatformProfile
	readYAML(t, filepath.Join(repo, "profiles", "athena-now.yaml"), &pp)
	var part v1alpha1.PartitionProfile
	for _, p := range pp.Spec.Partitions {
		if p.Name == "plgrid-now" {
			part = p
		}
	}
	var fx struct {
		ChatTemplateSHA256 string `json:"chatTemplateSha256"`
		Files              []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	raw, err := os.ReadFile(filepath.Join(repo, "internal", "slurm", "testdata", "checkpoints", "qwen3-vl-8b-instruct-0c351dd01ed8.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	spec := deploymentid.ServingSpec{Checkpoint: deploymentid.Checkpoint{Repository: s.Checkpoint.Repository, Revision: s.Checkpoint.Revision},
		Engine: deploymentid.Engine{Name: s.Engine.Name, Version: s.Engine.Version, ImageDigest: s.Engine.ImageDigest}, ChatTemplateSHA256: fx.ChatTemplateSHA256}
	for _, f := range s.Flags {
		spec.Flags = append(spec.Flags, deploymentid.Flag{Name: f.Name, Value: f.Value})
	}
	var files []deploymentid.File
	for _, f := range fx.Files {
		files = append(files, deploymentid.File{Path: f.Path, SHA256: f.SHA256})
	}
	id, err := deploymentid.Compute(spec, deploymentid.Accelerator{GPUModel: part.GPUModel, CPUArch: part.CPUArch}, files)
	if err != nil {
		t.Fatal(err)
	}
	if id.Name != AthenaBaselineDeploymentID {
		t.Errorf("the example derives the deployment ID %s on plgrid-now, the row pins %s", id.Name, AthenaBaselineDeploymentID)
	}
	if got := baselineFactsOf(BetaAthenaBaselineName); got.DeploymentID != AthenaBaselineDeploymentID || got.Cluster != "Athena" || got.KV != KVAthenaDerived() {
		t.Errorf("the Athena row's facts are %+v", got)
	}
	if got := baselineFactsOf(BetaBaselineName); got.DeploymentID != BaselineDeploymentID || got.Cluster != "Sano" || got.KV != KVSano {
		t.Errorf("Row 1's facts are %+v", got)
	}
}

func readYAML(t *testing.T, path string, into any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.UnmarshalStrict(b, into); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// The world's deployment for the Athena row keeps the routing block with the cap of 3 and the deadline, one Instance and no hedge, on the lease of 48 minutes, in
// the run's own Provider alone: what applyPlan makes of the example.
func TestTheAthenaBaselineDeploymentKeepsItsRoutingBlockAndItsLease(t *testing.T) {
	var base v1alpha1.ModelDeployment
	readYAML(t, filepath.Join("..", "..", "examples", AthenaQwen3VLDeployment), &base)
	two, hedge := int32(2), int32(2)
	base.Spec.Instances, base.Spec.HedgeWidth = two, &hedge
	md := applyPlan(&base, AthenaBaselineRow(), "alpha-0123456789")
	if r := md.Spec.Routing; r == nil || r.InstanceCap == nil || *r.InstanceCap != 3 || r.MaxRequestDuration == nil || *r.MaxRequestDuration != "5m0s" {
		t.Fatalf("the routing block is %+v, want instanceCap 3 and maxRequestDuration 5m0s", r)
	}
	if md.Spec.Instances != 1 || md.Spec.HedgeWidth == nil || *md.Spec.HedgeWidth != 1 {
		t.Errorf("instances %d, hedgeWidth %v; want 1 and 1", md.Spec.Instances, md.Spec.HedgeWidth)
	}
	if got := md.Spec.Providers.Names; len(got) != 1 || got[0] != "alpha-0123456789" {
		t.Errorf("the deployment allows %v, want the run's Provider alone", got)
	}
	if md.Spec.LeaseLimit == nil || *md.Spec.LeaseLimit != "48m" {
		t.Errorf("leaseLimit %v, want 48m", md.Spec.LeaseLimit)
	}
}

// The settings of the Athena twin: the generator's binary and LLMLab's credentials, as Row 1 takes them; host-dialed mode alone (athena-now.yaml lists
// no other: ssh is refused and direct is no profile value); no Provider staging directory (the profile has its own, $SCRATCH); the script's profile is
// athena-now and not the cluster's.
func TestTheAthenaBaselineRowsSettings(t *testing.T) {
	good := func() map[string]string {
		m := liveSettings(BetaAthenaBaselineName)
		m[envProfile], m[envMode] = "athena-now", "host-dialed"
		delete(m, envAddr)
		return m
	}
	cfg, skip, err := ConfigFromEnv(env(good()), BetaAthenaBaselineName)
	if skip != "" || err != nil {
		t.Fatalf("skip %q, err %v", skip, err)
	}
	if cfg.StudyloadBin != "/private/studyload" || cfg.LLMLabCredentials != "/private/llmlab-creds.json" || len(cfg.Phases) != 4 || cfg.StagingDir != "" {
		t.Errorf("config %+v", cfg)
	}
	var ns []string
	for _, p := range cfg.Phases {
		ns = append(ns, p.Name+"="+strconv.Itoa(p.Sessions))
	}
	if strings.Join(ns, ",") != "P1=3,P1-long=3,P2=5,P3=5" {
		t.Errorf("the config's phases %v, want P1=3,P1-long=3,P2=5,P3=5", ns)
	}
	for _, c := range []struct {
		name string
		edit func(map[string]string)
		want string
	}{
		{"direct mode", func(m map[string]string) { m[envMode], m[envAddr] = "direct", "gateway.example:29227" }, envMode + " must be host-dialed for " + BetaAthenaBaselineName},
		{"ssh mode", func(m map[string]string) {
			m[envMode], m[envAddr], m[envKey] = "ssh", "gateway.example:29227", "/private/key"
		}, envMode + " must be host-dialed for " + BetaAthenaBaselineName},
		{"no generator", func(m map[string]string) { delete(m, envStudyloadBin) }, envStudyloadBin + " is not set"},
		{"no credentials", func(m map[string]string) { delete(m, envLLMLabCreds) }, envLLMLabCreds + " is not set"},
		{"a Provider staging directory", func(m map[string]string) { m[envStagingDir] = "/path/to/shared/example-stage" }, envStagingDir + " is set for a row whose profile gives the staging directory"},
		{"the cluster's profile name", func(m map[string]string) { m[envProfile] = "athena" }, "the script's profile differs from the row's"},
	} {
		m := good()
		c.edit(m)
		if _, _, err := ConfigFromEnv(env(m), BetaAthenaBaselineName); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want one naming %q", c.name, err, c.want)
		}
	}
}

// The example, applied as it stands, targets the row's own platform: the Provider it names is an example Provider whose profile is the row's (athena-now) and which
// allows exactly the row's partition (plgrid-now), and not plgrid-gpu-a100's Provider, which the live row's rewriting of the Provider selection would have hidden
// (point 7 of PR 809's review). The Provider is found among the example files by the name the deployment lists, as an operator applying both would find it.
func TestTheAthenaBaselineExampleNamesAProviderOnThePartitionItRunsOn(t *testing.T) {
	repo := filepath.Join("..", "..")
	row := AthenaBaselineRow()
	var md v1alpha1.ModelDeployment
	readYAML(t, filepath.Join(repo, "examples", row.DeploymentFile()), &md)
	if len(md.Spec.Providers.Names) != 1 {
		t.Fatalf("the example allows Providers %v, want one", md.Spec.Providers.Names)
	}
	files, err := filepath.Glob(filepath.Join(repo, "examples", "provider-*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no Provider examples (%v)", err)
	}
	var found []v1alpha1.Provider
	for _, f := range files {
		var p v1alpha1.Provider
		readYAML(t, f, &p)
		if p.Name == md.Spec.Providers.Names[0] {
			found = append(found, p)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d example Providers are called %q, want exactly one", len(found), md.Spec.Providers.Names[0])
	}
	p := found[0]
	if p.Spec.PlatformProfileRef.Name != row.ProfileName() || len(p.Spec.AllowedPartitions) != 1 || p.Spec.AllowedPartitions[0] != row.Partition ||
		p.Labels[v1alpha1.LabelPartition] != row.Partition {
		t.Errorf("the Provider %q names profile %q, partitions %v and partition label %q; the row runs under profile %q on partition %q",
			p.Name, p.Spec.PlatformProfileRef.Name, p.Spec.AllowedPartitions, p.Labels[v1alpha1.LabelPartition], row.ProfileName(), row.Partition)
	}
	if p.Spec.QOS != "" {
		t.Errorf("the Provider sets qos %q: plgrid-now's own QOS applies and the row sets none (Row.QOS %q)", p.Spec.QOS, row.QOS)
	}
}
