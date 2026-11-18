package alpha

// The baseline row's tests (baseline.go): the caps from their inputs, the phases as config and their way to the generator, the shipped example, the world's
// deployment, the generator's binary run as a process, and the settings. The judge and the sequencer are tested through the dry runs (dry_baseline_test.go,
// baselinejudge_test.go).

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// The caps are computed from the inputs by hand and by the rule, never read back from the constants: KV 389,120 tokens as shipped (24,320 blocks of 16;
// the first load on Sano, #764 comment 6982083163), the 0.95 margin and the 32,768 of max-model-len give 0.95 x 11.875 = 11.28, so 11. The same rule at the
// edges of its range: 379,419 to 413,911 tokens all give 11 (#764's cap comment), and one token either side of them gives 10 and 12. Athena's
// is derived from Sano's: its memory beside the KV cache is 0.92 x 80 GiB less 53.4375 GiB (389,120 tokens x 144 KiB), 20.1625 GiB, the same on the
// 40 GB card, so its KV is 0.92 x 40 GiB less that, 16.6375 GiB, 121,150 tokens by hand, 3.70 sessions, and 0.95 x 3.70 = 3.51: 3.
func TestTheCapsAreComputedFromTheirInputs(t *testing.T) {
	if KVSano != 24_320*16 {
		t.Errorf("KVSano %d is not 24,320 blocks of 16", KVSano)
	}
	if got := 389_120.0 * 144 * 1024 / (1 << 30); got != 53.4375 {
		t.Fatalf("389,120 tokens at 144 KiB is %v GiB, want 53.4375", got)
	}
	if KVBytesPerToken != 36*8*128*2*2 {
		t.Errorf("KVBytesPerToken %d is not 36 layers x 8 heads x 128 x K and V x 2 bytes", KVBytesPerToken)
	}
	floor := func(kv float64) int64 { return int64(math.Floor(0.95 * kv / 32768)) }
	for _, c := range []struct {
		kv   int64
		want int64
	}{{389_120, 11}, {379_419, 11}, {413_911, 11}, {379_418, 10}, {413_912, 12}, {32_768, 0}, {345_000, 10}} {
		if got := CapOf(c.kv, MaxModelLen); got != c.want || got != floor(float64(c.kv)) {
			t.Errorf("CapOf(%d) = %d, the float rule says %d, want %d", c.kv, got, floor(float64(c.kv)), c.want)
		}
	}
	if got := CapOf(KVSano, MaxModelLen); got != CapSano || CapSano != 11 {
		t.Errorf("the Sano cap is the constant %d, the rule on its inputs gives %d, want 11", CapSano, got)
	}
	kvA := KVAthenaDerived()
	gibA := 0.92*40 - (0.92*80 - 53.4375) // 16.6375 GiB
	if want := int64(gibA * (1 << 30) / 147456); kvA < want-1 || kvA > want+1 || kvA < 121_100 || kvA > 121_200 {
		t.Errorf("Athena's derived KV is %d tokens, want about 121,150 (by hand %d)", kvA, want)
	}
	if got := CapOf(kvA, MaxModelLen); got != CapAthena || CapAthena != 3 {
		t.Errorf("the Athena cap is the constant %d, the rule on its derived KV gives %d, want 3", CapAthena, got)
	}
	// The ruled margin: --language-model-only's KV (404,896, +4.1%) changes neither cap.
	if CapOf(404_896, MaxModelLen) != CapSano {
		t.Errorf("--language-model-only's KV moves Sano's cap")
	}
	// The cap fits the engine with the margin: cap x max-model-len is at most 95% of the KV.
	if int64(CapSano)*MaxModelLen*100 > int64(KVSano)*CapMarginPercent {
		t.Errorf("%d sessions of %d tokens do not fit 95%% of %d tokens", CapSano, MaxModelLen, KVSano)
	}
	if LLMLabK != 2 || LLMLabGuard != 3*LLMLabK*24 {
		t.Errorf("k is %d and the guard %d, want 2 and 3 x k x 24 turns = 144", LLMLabK, LLMLabGuard)
	}
}

// The phases' defaults are the brief's, typed by hand; and a config change reaches the generator's config and nothing else.
func TestTheDefaultPhasesAreTheBriefs(t *testing.T) {
	ph := DefaultPhases(11)
	want := []Phase{
		{Name: "P1", Kind: KindControl, Sessions: 11, Turns: 24, Context: 1024, Turn: 128, Output: 128, Think: 100 * time.Millisecond, Bound: 10 * time.Minute},
		{Name: "P1-long", Kind: KindLong, Sessions: 11, Turns: 2, Context: 26000, Turn: 128, Output: 128, Think: time.Second, Bound: 15 * time.Minute},
		{Name: "P2", Kind: KindFull, Sessions: 13, Turns: 24, Context: 1024, Turn: 128, Output: 128, Think: 100 * time.Millisecond, Bound: 10 * time.Minute},
		{Name: "P3", Kind: KindDetach, Sessions: 13, Turns: 24, Context: 1024, Turn: 128, Output: 128, Think: 100 * time.Millisecond, Bound: 10 * time.Minute},
	}
	if len(ph) != len(want) {
		t.Fatalf("%d phases, want %d", len(ph), len(want))
	}
	for i := range want {
		if ph[i] != want[i] {
			t.Errorf("phase %d is %+v, want %+v", i, ph[i], want[i])
		}
		if err := ph[i].Check(); err != nil {
			t.Errorf("phase %s: %v", ph[i].Name, err)
		}
	}
}

// The generator's config of P1, typed by hand from #768's README ("Config"): the keys are the generator's own, which refuses an unknown one.
func TestTheStudyConfigIsTheGeneratorsOwnSchema(t *testing.T) {
	got, err := StudyConfig(DefaultPhases(11)[0], StudyRun{BaseURL: "http://127.0.0.1:29713/v1", APIKeyFile: "/rec/studyload.key", RunID: "p1-0123456789", RequestTimeout: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{
  "baseURL": "http://127.0.0.1:29713/v1",
  "model": "Qwen/Qwen3-VL-8B-Instruct",
  "apiKeyFile": "/rec/studyload.key",
  "sessionHeader": "x-session-id",
  "runId": "p1-0123456789",
  "requestTimeout": "5m0s",
  "workloads": [
    {
      "name": "P1",
      "sessions": 11,
      "turns": 24,
      "contextTokens": 1024,
      "turnTokens": 128,
      "outputTokens": 128,
      "thinkTime": "100ms"
    }
  ]
}
`
	if string(got) != want {
		t.Errorf("the generator's config is\n%s\nwant\n%s", got, want)
	}
	// min_tokens left out is the one switch of the whole run.
	omit, _ := StudyConfig(DefaultPhases(11)[0], StudyRun{BaseURL: "http://127.0.0.1:1/v1", RunID: "p1-0123456789", RequestTimeout: time.Minute, OmitMinTokens: true})
	if !strings.Contains(string(omit), `"omitMinTokens": true`) || strings.Contains(string(got), "omitMinTokens") {
		t.Errorf("omitMinTokens is not exactly the run's switch:\n%s", omit)
	}
	// A one-turn phase has no turn tokens, a shape the generator refuses otherwise; a stagger and a think time of zero are left out.
	one := Phase{Name: "x", Kind: KindControl, Sessions: 3, Turns: 1, Context: 50, Output: 5, Bound: time.Minute, Stagger: 2 * time.Second}
	b, err := StudyConfig(one, StudyRun{BaseURL: "http://127.0.0.1:1/v1", RunID: "x", RequestTimeout: time.Minute})
	if err != nil || !strings.Contains(string(b), `"startStagger": "2s"`) || strings.Contains(string(b), "thinkTime") {
		t.Errorf("a one-turn phase: %v\n%s", err, b)
	}
	one.Turn = 7
	if _, err := StudyConfig(one, StudyRun{BaseURL: "http://127.0.0.1:1/v1", RunID: "x", RequestTimeout: time.Minute}); err == nil {
		t.Error("a one-turn phase with turn tokens was written: the generator would refuse it")
	}
}

// Changing H0's shape changes only config (the science agent's ruling H0): an override in the settings reaches the generator's config of that phase,
// and no other phase's, and a shape the generator would refuse is refused before the run.
func TestAChangedPhaseReachesTheGenerator(t *testing.T) {
	base := DefaultPhases(11)
	got, err := PhasesFromEnv(base, `{"P1-long":{"contextTokens":27000,"turns":3,"thinkTime":"2s"},"P2":{"sessions":14}}`)
	if err != nil {
		t.Fatal(err)
	}
	cfg := func(p Phase) string {
		b, err := StudyConfig(p, StudyRun{BaseURL: "http://127.0.0.1:1/v1", RunID: "r-0123456789", RequestTimeout: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	for i, p := range got {
		changed := p.Name == "P1-long" || p.Name == "P2"
		if (cfg(p) != cfg(base[i])) != changed {
			t.Errorf("%s: its generator config changed: %t, want %t", p.Name, cfg(p) != cfg(base[i]), changed)
		}
	}
	for _, w := range []string{`"contextTokens": 27000`, `"turns": 3`, `"thinkTime": "2s"`} {
		if !strings.Contains(cfg(got[1]), w) {
			t.Errorf("P1-long's generator config lacks %s", w)
		}
	}
	if !strings.Contains(cfg(got[2]), `"sessions": 14`) {
		t.Error("P2's generator config lacks 14 sessions")
	}
	if base[1].Context != 26000 {
		t.Error("the defaults were changed in place")
	}
	for _, c := range []struct{ name, env, why string }{
		{"an unknown phase", `{"P9":{"sessions":3}}`, `names no phase "P9"`},
		{"an unknown field", `{"P1":{"session":3}}`, "known fields"},
		{"not JSON", `sessions=3`, "not a JSON object"},
		{"a bad duration", `{"P1":{"thinkTime":"soon"}}`, "not a Go duration"},
		{"turn tokens with one turn", `{"P1":{"turns":1}}`, "turnTokens must be 0 for a one-turn phase"},
		{"no sessions", `{"P1":{"sessions":0}}`, "sessions must be at least 1"},
	} {
		if _, err := PhasesFromEnv(base, c.env); err == nil || !strings.Contains(err.Error(), c.why) {
			t.Errorf("%s: err %v, want one naming %q", c.name, err, c.why)
		}
	}
	if same, err := PhasesFromEnv(base, " "); err != nil || len(same) != 4 || same[0] != base[0] {
		t.Errorf("an empty value is no override: %v %v", same, err)
	}
}

// Two phases and two runs never share a session ID, in the generator's 32 characters.
func TestPhaseRunIDsFitTheGenerator(t *testing.T) {
	seen := map[string]bool{}
	for _, ph := range []string{PhaseP1, PhaseP1Long, PhaseP2, PhaseP3} {
		id := PhaseRunID("0123456789", ph)
		if !runIDForm.MatchString(id) || seen[id] {
			t.Errorf("run ID %q for %s is not unique or not the generator's form", id, ph)
		}
		seen[id] = true
	}
	if id := PhaseRunID(strings.Repeat("a", 40), "P1-long"); len(id) != 32 {
		t.Errorf("a long run ID is cut to %d characters, want 32", len(id))
	}
}

func TestTheLoggedKVIsReadFromTheOneLineTheReadPrints(t *testing.T) {
	for _, c := range []struct {
		in string
		kv int64
		ok bool
	}{{"389,120\n", 389120, true}, {"404896", 404896, true}, {"", 0, false}, {"0", 0, false}, {"-5", 0, false}, {"389,120\n11,000\n", 0, false},
		{"GPU KV cache size: 389,120 tokens", 0, false}, {"12.5", 0, false}} {
		if kv, ok := ParseLoggedKV([]byte(c.in)); kv != c.kv || ok != c.ok {
			t.Errorf("ParseLoggedKV(%q) = %d, %t, want %d, %t", c.in, kv, ok, c.kv, c.ok)
		}
	}
}

func TestTheGeneratorsRecordsAreJoinedByRequestID(t *testing.T) {
	// Lines as #768's README shows a record, and one with no answer from the front.
	in := `{"workload":"P2","session":"p2-0123456789-P2-000","turn":2,"start":"2026-11-18T12:00:00.5Z","end":"2026-11-18T12:00:00.9Z","status":200,"ttftMs":38.2,"requestId":"req-1","emitter":"front","provider":"sano-a","deploymentId":"dep-1","instance":"ns/inst-1"}

{"workload":"P2","session":"p2-0123456789-P2-001","turn":1,"start":"2026-11-18T12:00:00.5Z","end":"2026-11-18T12:00:00.9Z","status":0,"error":"transport error"}
`
	got, err := ParseGenRecords(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	want := []GenRow{{Session: "p2-0123456789-P2-000", Turn: 2, RequestID: "req-1", Emitter: "front", Provider: "sano-a", Instance: "ns/inst-1"}, {Session: "p2-0123456789-P2-001", Turn: 1}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("records %+v, want %+v", got, want)
	}
	for _, bad := range []string{"not json\n", `{"turn":1}` + "\n", `{"session":"s","turn":0}` + "\n"} {
		if _, err := ParseGenRecords(strings.NewReader(bad)); err == nil {
			t.Errorf("%q was read as a record", bad)
		}
	}
}

// The served example, which #735's rows PR owns (examples/modeldeployment-sano-qwen3-vl-8b-instruct.yaml: the client model name, the pinned revision, as
// shipped, max-model-len 32768, overAllocation 0), is the pinned model as shipped: the checkpoint's revision, the engine's digest, max-model-len, and no
// --language-model-only. The row sets the rest of the served deployment itself (applyPlan): one Instance, no hedge, the cap and the deadline. Until
// that PR's example is merged into this branch this test fails, by design: the row serves that file by name.
func TestTheServedExampleIsTheModelAsShipped(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "examples", SanoQwen3VLDeployment))
	if err != nil {
		t.Fatal(err)
	}
	var md v1alpha1.ModelDeployment
	if err := yaml.UnmarshalStrict(b, &md); err != nil {
		t.Fatal(err)
	}
	s := md.Spec.Source.Served
	if s == nil || md.Spec.Model != BaselineModel || s.Checkpoint.Repository != BaselineModel || s.Checkpoint.Revision != BaselineRevision {
		t.Fatalf("the example's model, repository or revision is not %s at %s", BaselineModel, BaselineRevision)
	}
	if s.Engine.ImageDigest != "sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34" || s.Engine.Version != "0.32.0" {
		t.Errorf("the engine is %s %s, not the pinned vLLM 0.32.0 at its digest", s.Engine.Version, s.Engine.ImageDigest)
	}
	flags := map[string]string{}
	for _, f := range s.Flags {
		flags[f.Name] = f.Value
	}
	if len(flags) != 1 || flags["max-model-len"] != "32768" {
		t.Errorf("the flags are %v, want max-model-len 32768 alone (as shipped: no --language-model-only)", flags)
	}
	if md.Spec.OverAllocation == nil || *md.Spec.OverAllocation != 0 {
		t.Errorf("the example's overAllocation is not 0")
	}
	// The row's deployment file is this one.
	if r := BaselineRow(); r.DeploymentFile() != SanoQwen3VLDeployment || !r.ProviderStaging || r.InstanceCap != CapSano || r.MostJobs != 1 || r.PostsTo != "#764" {
		t.Errorf("the baseline row is %+v", r)
	}
	if BaselineDeploymentID != "965fa2409193" || len(BaselineDeploymentID) != v1alpha1.DisplayIDLength {
		t.Errorf("the deployment ID the row checks is %q, want 965fa2409193 (12 hex characters)", BaselineDeploymentID)
	}
}

// The world's deployment keeps the routing block: the cap and the request deadline, with one Instance and no hedge. Today's rows replaced the block with
// the deadline alone, which loses the cap.
func TestTheServedDeploymentKeepsItsRoutingBlock(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "examples", SanoQwen3VLDeployment))
	if err != nil {
		t.Fatal(err)
	}
	var base v1alpha1.ModelDeployment
	if err := yaml.UnmarshalStrict(b, &base); err != nil {
		t.Fatal(err)
	}
	two, hedge := int32(2), int32(2)
	base.Spec.Instances, base.Spec.HedgeWidth = two, &hedge // the example's own are overwritten by the row's
	baseRouting := base.Spec.Routing
	md := applyPlan(&base, BaselineRow(), "alpha-0123456789")
	if r := md.Spec.Routing; r == nil || r.InstanceCap == nil || *r.InstanceCap != 11 || r.MaxRequestDuration == nil || *r.MaxRequestDuration != "5m0s" {
		t.Fatalf("the routing block is %+v, want instanceCap 11 and maxRequestDuration 5m0s", r)
	}
	if md.Spec.Instances != 1 || md.Spec.HedgeWidth == nil || *md.Spec.HedgeWidth != 1 {
		t.Errorf("instances %d, hedgeWidth %v; want 1 and 1", md.Spec.Instances, md.Spec.HedgeWidth)
	}
	if got := md.Spec.Providers.Names; len(got) != 1 || got[0] != "alpha-0123456789" {
		t.Errorf("the deployment allows %v, want the run's Provider alone", got)
	}
	if md.Spec.LeaseLimit == nil || *md.Spec.LeaseLimit != "106m" {
		t.Errorf("leaseLimit %v, want 106m", md.Spec.LeaseLimit)
	}
	// A row with no cap of its own leaves the engine's.
	plain := applyPlan(&base, SanoServeRow(), "alpha-0123456789")
	if plain.Spec.Routing == nil || plain.Spec.Routing.InstanceCap != nil || plain.Spec.Instances != 2 {
		t.Errorf("another row's deployment has a cap or lost its instance count: %+v", plain.Spec)
	}
	if base.Spec.Routing != baseRouting || base.Spec.Instances != 2 {
		t.Error("applyPlan changed the world's deployment in place")
	}
}

// The managed leg is read from the shipped files, with the run's credentials file in the Provider, and the names the row holds.
func TestTheManagedLegIsTheShippedExamples(t *testing.T) {
	repo := filepath.Join("..", "..")
	objs, md, err := llmlabObjects(repo, Config{LLMLabCredentials: "/private/llmlab.json"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 || md == nil {
		t.Fatalf("%d objects and a managed deployment %v", len(objs), md)
	}
	prov, ok := objs[1].(*v1alpha1.Provider)
	if !ok || prov.Name != LLMLabProviderName || prov.Spec.CredentialsRef == nil || prov.Spec.CredentialsRef.File != "/private/llmlab.json" ||
		prov.Spec.PlatformProfileRef.Name != "llmlab" {
		t.Errorf("the managed Provider is %+v", prov)
	}
	if md.Name != LLMLabDeploymentName || md.Spec.Model != BaselineModel || md.Spec.Source.Managed == nil || md.Spec.Source.Managed.Platform != "llmlab" ||
		md.Spec.Source.Managed.CatalogName != BaselineModel || len(md.Spec.Providers.Names) != 1 || md.Spec.Providers.Names[0] != LLMLabProviderName {
		t.Errorf("the managed ModelDeployment is %+v", md.Spec)
	}
}

// The settings: the row takes the generator's binary and the LLMLab credentials, and no other row does; the phases' override and the min_tokens switch are
// refused on a row with none; a dry run refuses the live ones.
func TestTheBaselineRowsSettings(t *testing.T) {
	m := liveSettings(BetaBaselineName)
	cfg, skip, err := ConfigFromEnv(env(m), BetaBaselineName)
	if skip != "" || err != nil {
		t.Fatalf("skip %q, err %v", skip, err)
	}
	if cfg.StudyloadBin != "/private/studyload" || cfg.LLMLabCredentials != "/private/llmlab-creds.json" || len(cfg.Phases) != 4 || cfg.OmitMinTokens {
		t.Errorf("config %+v", cfg)
	}
	m[envOmitMinTokens], m[envBaselinePhases] = "1", `{"P2":{"sessions":15}}`
	if cfg, _, err = ConfigFromEnv(env(m), BetaBaselineName); err != nil || !cfg.OmitMinTokens || cfg.Phases[2].Sessions != 15 {
		t.Errorf("the overrides did not reach the config: %v %+v", err, cfg.Phases)
	}
	for _, c := range []struct {
		name, key, val, want string
	}{
		{"no generator", envStudyloadBin, "", envStudyloadBin + " is not set"},
		{"a relative generator", envStudyloadBin, "studyload", envStudyloadBin + " must be an absolute path"},
		{"no credentials", envLLMLabCreds, "", envLLMLabCreds + " is not set"},
		{"relative credentials", envLLMLabCreds, "llmlab.json", envLLMLabCreds + " must be an absolute path"},
		{"a bad switch", envOmitMinTokens, "yes", envOmitMinTokens + " must be 1 or 0"},
		{"a bad override", envBaselinePhases, `{"P1":{"turns":0}}`, "turns must be 1 to 1000"},
	} {
		bad := liveSettings(BetaBaselineName)
		if c.val == "" {
			delete(bad, c.key)
		} else {
			bad[c.key] = c.val
		}
		if _, _, err := ConfigFromEnv(env(bad), BetaBaselineName); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want one naming %q", c.name, err, c.want)
		}
	}
	// Another row takes none of them; the message names the setting and never its value.
	for _, k := range []string{envStudyloadBin, envLLMLabCreds, envOmitMinTokens, envBaselinePhases} {
		other := liveSettings(SanoServeName)
		other[k] = "/private/SECRETVALUE"
		if _, _, err := ConfigFromEnv(env(other), SanoServeName); err == nil || !strings.Contains(err.Error(), k+" is set for a row that") || strings.Contains(err.Error(), "SECRETVALUE") {
			t.Errorf("%s on another row: err %v", k, err)
		}
	}
	dry := map[string]string{envRow: BetaBaselineName, envOut: "/o", envDry: "1", envStudyloadBin: "/x/studyload", EnvFrontLabel: testFrontLabel}
	if _, _, err := ConfigFromEnv(env(dry), BetaBaselineName); err == nil || !strings.Contains(err.Error(), "a dry run refuses to start with live settings present ("+envStudyloadBin) {
		t.Errorf("a dry run with the generator's binary set: %v", err)
	}
	delete(dry, envStudyloadBin)
	dry[envLLMLabCreds] = "/x/llmlab.json"
	if _, _, err := ConfigFromEnv(env(dry), BetaBaselineName); err == nil || !strings.Contains(err.Error(), envLLMLabCreds) {
		t.Errorf("a dry run with the LLMLab credentials set: %v", err)
	}
}

// The live world runs the generator's binary as a process: with its config and its key file in the run's private record directory, both of mode 600, the
// key on no command line, one config per phase, and its records read for the join. The binary here is a script that records what it was given; exit
// status 1 (some request failed) is a run, and any other status a run that broke.
func TestTheLiveGeneratorRunsTheBinaryWithAPrivateConfigAndKey(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	rec, err := NewRecorder(dir)
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "studyload")
	script := `#!/bin/sh
# -config <file> -out <file>
cfg=$2; out=$4
echo "$@" > "$cfg.args"
stat -c %a "$cfg" > "$cfg.mode"
key=$(sed -n 's/.*"apiKeyFile": "\(.*\)",/\1/p' "$cfg")
stat -c %a "$key" > "$cfg.keymode"
cat "$key" > "$cfg.key"
printf '%s\n' '{"workload":"P1","session":"s0","turn":1,"requestId":"r1","emitter":"instance","provider":"p","instance":"ns/i"}' > "$out"
exit ${STUDYLOAD_EXIT:-0}
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	const secret = "client-key-that-must-reach-no-command-line"
	r := &liveRun{t: t, cfg: Config{Out: dir, RunID: "0123456789", StudyloadBin: bin, Phases: DefaultPhases(11)}, tm: BaselineRow().Clock(),
		h: &stack.Host{ClientKey: secret, Ports: stack.Ports{ClientAddr: "127.0.0.1:29713"}}}
	b := &liveBaseline{r: r, p: Plan{Row: BaselineRow(), Rec: rec}}
	g, err := b.StartGenerator(t.Context(), DefaultPhases(11)[2])
	if err != nil {
		t.Fatal(err)
	}
	<-g.Done()
	rows, err := g.Result()
	if err != nil || len(rows) != 1 || rows[0].RequestID != "r1" || rows[0].Instance != "ns/i" {
		t.Fatalf("the records: %+v, %v", rows, err)
	}
	read := func(name string) string {
		t.Helper()
		x, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(x))
	}
	cfgPath := filepath.Join(dir, "studyload-P2.json")
	if args := read("studyload-P2.json.args"); args != "-config "+cfgPath+" -out "+filepath.Join(dir, "studyload-P2.jsonl") || strings.Contains(args, secret) {
		t.Errorf("the binary was run as %q", args)
	}
	if read("studyload-P2.json.mode") != "600" || read("studyload-P2.json.keymode") != "600" {
		t.Errorf("the config's mode %s and the key file's %s, want 600 and 600", read("studyload-P2.json.mode"), read("studyload-P2.json.keymode"))
	}
	if read("studyload-P2.json.key") != secret {
		t.Error("the key file does not hold the stack's client key")
	}
	cfgText := read("studyload-P2.json")
	for _, w := range []string{`"baseURL": "http://127.0.0.1:29713/v1"`, `"name": "P2"`, `"sessions": 13`, `"runId": "p2-0123456789"`, `"apiKeyFile": "` + filepath.Join(dir, "studyload.key") + `"`} {
		if !strings.Contains(cfgText, w) {
			t.Errorf("the config lacks %s:\n%s", w, cfgText)
		}
	}
	if strings.Contains(cfgText, secret) {
		t.Error("the config holds the key itself")
	}
	// Exit status 1 is a run some requests of which failed; 2 is a run that broke.
	for _, c := range []struct {
		exit string
		ok   bool
	}{{"1", true}, {"2", false}} {
		t.Run("exit "+c.exit, func(t *testing.T) {
			// The generator's own environment is PATH alone, so the status is passed by a wrapper that sets it.
			wrapper := filepath.Join(t.TempDir(), "studyload")
			if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nexport STUDYLOAD_EXIT="+c.exit+"\nexec "+bin+" \"$@\"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			r.cfg.StudyloadBin = wrapper
			g, err := b.StartGenerator(t.Context(), Phase{Name: "X" + c.exit, Kind: KindControl, Sessions: 1, Turns: 1, Context: 5, Output: 1, Bound: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			<-g.Done()
			if _, err := g.Result(); (err == nil) != c.ok {
				t.Errorf("exit %s: err %v, want ok %t", c.exit, err, c.ok)
			}
		})
	}
	// A binary that cannot be run is an error naming no path.
	r.cfg.StudyloadBin = filepath.Join(t.TempDir(), "no-such-binary")
	if _, err := b.StartGenerator(t.Context(), DefaultPhases(11)[0]); err == nil || strings.Contains(err.Error(), "no-such-binary") {
		t.Errorf("a missing binary: %v", err)
	}
}
