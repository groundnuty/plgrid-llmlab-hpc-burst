package register

// The register's check (HBEP-4, Known-issues register, Who reads it; Test plan, The register's check).
// The fixtures are YAML files written by hand under testdata/: none is written by the loader or the
// checker. Every expectation is a failure the HBEP names, with the reason in the error, and the control
// file differs from each failing one by that one defect.

import (
	"path/filepath"
	"strings"
	"testing"
)

func fixtureLayout(envoyPins string) Layout {
	repo := filepath.Join("testdata", "repo")
	return Layout{
		Root:     repo,
		Profiles: filepath.Join(repo, "profiles"),
		Pins: map[string]string{
			"envoy": filepath.Join(repo, "pins", envoyPins),
			"frp":   filepath.Join(repo, "pins", "frp-0.71.1.txt"),
		},
	}
}

// load reads a fixture, then checks it, and returns the stage that failed ("" if none) with its error.
func load(t *testing.T, file string, l Layout) (stage string, err error) {
	t.Helper()
	entries, err := Load(filepath.Join("testdata", "entries", file))
	if err != nil {
		return "load", err
	}
	if err := Check(entries, l); err != nil {
		return "check", err
	}
	return "", nil
}

func TestKnownIssuesRegister(t *testing.T) {
	t.Run("the real register passes the check", func(t *testing.T) {
		root := filepath.Join("..", "..")
		entries, err := Load(filepath.Join(root, "register", "known-issues.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if err := Check(entries, RepoLayout(root)); err != nil {
			t.Fatal(err)
		}
		checkStartingEntries(t, entries)
	})

	envoy := "envoy-1.40.2.txt"
	for _, c := range []struct {
		file  string
		pins  string   // the fixture pins file for envoy; envoy-1.40.2.txt when empty
		stage string   // the stage that must fail: load, check, or "" for a file that passes
		want  []string // every one must be in the error
	}{
		// The control, and what passes.
		{file: "valid.yaml"},
		{file: "envoy-fixed-in-the-pinned-version-workaround-none.yaml"},
		{file: "envoy-fixed-above-the-pinned-version-workaround-named.yaml"},
		{file: "envoy-fixed-above-the-pinned-version-workaround-none.yaml"},
		{file: "vllm-fixed-in-its-version-workaround-named.yaml"},
		{file: "engine-and-site-entries-never-fail-by-version.yaml"},

		// A field missing or unknown, an id twice or malformed.
		{file: "missing-symptom.yaml", stage: "load", want: []string{"symptom is missing"}},
		{file: "missing-test-name.yaml", stage: "load", want: []string{"test.name is missing"}},
		{file: "missing-workaround.yaml", stage: "load", want: []string{"workaround.where is missing", "workaround.what is missing"}},
		{file: "missing-source.yaml", stage: "load", want: []string{"source is missing"}},
		{file: "unknown-field.yaml", stage: "load", want: []string{`unknown field "owner"`}},
		{file: "unknown-nested-field.yaml", stage: "load", want: []string{`unknown field "how"`}},
		{file: "bad-id.yaml", stage: "load", want: []string{`id "KI-12" is not KI- and four digits`}},
		{file: "duplicate-id.yaml", stage: "check", want: []string{"KI-0001", "the id is used twice"}},
		{file: "unknown-component.yaml", stage: "load", want: []string{`component "kubernetes"`}},

		// A version that does not parse.
		{file: "unparseable-fixedin.yaml", stage: "load", want: []string{"fixedIn", `"0.33.x" is not a version`}},
		{file: "unparseable-affected.yaml", stage: "load", want: []string{"affected"}},

		// A platform that profiles/ does not ship, a partition its typed list lacks, a partition with no platform.
		{file: "platform-not-shipped.yaml", stage: "check", want: []string{`"example-nowhere" is not a platform that profiles/ ships`}},
		{file: "platform-only-in-a-draft.yaml", stage: "check", want: []string{`"example-draft" is not a platform that profiles/ ships`}},
		{file: "partition-not-in-typed-list.yaml", stage: "check", want: []string{`"part-three" is in no typed partitions list`, "example-alpha"}},
		{file: "partition-of-a-draft.yaml", stage: "check", want: []string{`"part-draft" is in no typed partitions list`}},
		{file: "partition-with-no-platform.yaml", stage: "check", want: []string{"named with no platform"}},

		// A test.name that is no such function in the package's test files.
		{file: "test-name-missing.yaml", stage: "check", want: []string{`test.name "TestNoSuchTest"`}},
		{file: "test-name-not-a-test.yaml", stage: "check", want: []string{`"TestWrongSignature"`, `"TestMethod"`, `"notATest"`}},
		{file: "test-package-missing.yaml", stage: "check", want: []string{"test.package pkg/none", "no _test.go file"}},
		{file: "test-package-outside-the-root.yaml", stage: "check", want: []string{"is not a directory under the repository root"}},

		// A binary pinned in a pins file whose fixedIn the pinned version has reached, workaround still named.
		{file: "envoy-fixed-in-the-pinned-version-workaround-named.yaml", stage: "check", want: []string{"envoy is fixed in 1.40.2", "pins up to 1.40.2", `write "none"`}},
		{file: "envoy-fixed-below-the-pinned-version-workaround-named.yaml", stage: "check", want: []string{"envoy is fixed in 1.40.0"}},
		// 1.10.0 is above 1.9.0 as numbers, though below it as text: the highest of two pins is 1.10.0.
		{file: "envoy-fixed-in-the-highest-of-several-pins.yaml", pins: "envoy-1.9.0-and-1.10.0.txt", stage: "check", want: []string{"envoy is fixed in 1.10.0", "pins up to 1.10.0"}},
		{file: "frp-fixed-in-the-pinned-version-workaround-named.yaml", stage: "check", want: []string{"frp is fixed in 0.71.1"}},
	} {
		t.Run(c.file, func(t *testing.T) {
			pins := c.pins
			if pins == "" {
				pins = envoy
			}
			stage, err := load(t, c.file, fixtureLayout(pins))
			if stage != c.stage {
				t.Fatalf("stage %q failed (error %v); want %q", stage, err, c.stage)
			}
			for _, w := range c.want {
				if err == nil || !strings.Contains(err.Error(), w) {
					t.Errorf("the error %v does not hold %q", err, w)
				}
			}
		})
	}
}

// checkStartingEntries: the register holds the four entries beta starts with, each with its existing
// test, and only the Apptainer one is scoped (HBEP-4, Known-issues register, The entries beta starts
// with). The pairs and the scope are written here by hand from that list.
func checkStartingEntries(t *testing.T, entries []Entry) {
	t.Helper()
	for _, want := range []struct {
		pkg, test, component string
		scoped               bool
	}{
		{"internal/front", "TestKeepAliveIdleTimeoutIsBelowTheEngines", "vllm", false},
		{"internal/agent", "TestAWorkerBehindTheRouterGetsTheKeepAlive", "vllm", false},
		{"internal/agent", "TestEnvoyRouteTimeoutOutlivesTheDefault", "envoy", false},
		{"internal/lifecycle", "TestShippedAthenaNowHoldsPlgridNowOnlyAndSendsASandbox", "apptainer", true},
	} {
		var hits []Entry
		for _, e := range entries {
			if e.Test.Package == want.pkg && e.Test.Name == want.test {
				hits = append(hits, e)
			}
		}
		if len(hits) != 1 {
			t.Errorf("%d entries for %s %s, want 1", len(hits), want.pkg, want.test)
			continue
		}
		e := hits[0]
		if e.Component != want.component {
			t.Errorf("%s: component %q, want %q", e.ID, e.Component, want.component)
		}
		if want.scoped != (e.Scope != nil) {
			t.Errorf("%s: scoped %v, want %v", e.ID, e.Scope != nil, want.scoped)
		}
		if !want.scoped {
			continue
		}
		if got := strings.Join(e.Scope.Platforms, ","); got != "athena,athena-now" {
			t.Errorf("%s: scope.platforms %q, want athena,athena-now", e.ID, got)
		}
		if got := strings.Join(e.Scope.Partitions, ","); got != "plgrid-gpu-a100,plgrid-now" {
			t.Errorf("%s: scope.partitions %q, want plgrid-gpu-a100,plgrid-now", e.ID, got)
		}
	}
}

func TestParseVersion(t *testing.T) {
	for _, s := range []string{"1.40.2", "0.32.0", "1.4", "24", "1.10.0"} {
		if _, err := ParseVersion(s); err != nil {
			t.Errorf("ParseVersion(%q) = %v, want a version", s, err)
		}
	}
	for _, s := range []string{"", "v1.2.3", "1.x", "1..2", "1.2.", ".1", "1.2-rc1", " 1.2", "1.2 ", "-1", "1,2"} {
		if _, err := ParseVersion(s); err == nil {
			t.Errorf("ParseVersion(%q) parses, want an error", s)
		}
	}
}

// Versions compare as numbers: 1.10.0 is above 1.9.0, and a missing part counts as 0.
func TestVersionCompare(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want int
	}{
		{"1.10.0", "1.9.0", 1},
		{"1.9.0", "1.10.0", -1},
		{"1.40.2", "1.40.2", 0},
		{"1.40", "1.40.0", 0},
		{"1.40.0.1", "1.40", 1},
		{"0.32.0", "0.32.1", -1},
		{"2", "1.99.99", 1},
	} {
		a, errA := ParseVersion(c.a)
		b, errB := ParseVersion(c.b)
		if errA != nil || errB != nil {
			t.Fatalf("parse %q, %q: %v, %v", c.a, c.b, errA, errB)
		}
		if got := a.Compare(b); got != c.want {
			t.Errorf("Compare(%s, %s) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

// An affected range is one version, or comma-separated bounds (>=, >, <=, <) and a version each.
func TestAffectedRanges(t *testing.T) {
	for _, s := range []string{"0.32.0", ">=1.2.0,<1.4.0", "<1.4.0", ">=1.2.0, <1.4.0", ">1.0,<=2.0"} {
		if err := checkRange(s); err != nil {
			t.Errorf("checkRange(%q) = %v, want a range", s, err)
		}
	}
	for _, s := range []string{">=1.2.0,<", "~1.2", "=1.2.0", "1.2.0,1.3.0", ",", ">=", "1.2.x", ">>1.0"} {
		if err := checkRange(s); err == nil {
			t.Errorf("checkRange(%q) is a range, want an error", s)
		}
	}
}

// A pins file that cannot be read as versions, and a layout that names none, fail the entry that needs
// it, loudly: an unchecked entry would pass for any version (HBEP-4, Known-issues register).
func TestAPinnedBinaryEntryNeedsAReadablePinsFile(t *testing.T) {
	const file = "envoy-fixed-above-the-pinned-version-workaround-named.yaml"
	for _, c := range []struct {
		name   string
		layout Layout
		want   string
	}{
		{"a pins file with no version", fixtureLayout("empty.txt"), "pins no version"},
		{"a pins file whose first column is no version", fixtureLayout("bad.txt"), "is not a version"},
		{"a pins file that does not exist", fixtureLayout("absent.txt"), "read the pins file"},
		{"no pins file for the component", func() Layout { l := fixtureLayout("envoy-1.40.2.txt"); delete(l.Pins, "envoy"); return l }(), "no pins file is given for envoy"},
	} {
		t.Run(c.name, func(t *testing.T) {
			stage, err := load(t, file, c.layout)
			if stage != "check" || err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("stage %q, error %v; want a check error holding %q", stage, err, c.want)
			}
		})
	}
}

// The register's own file and a profile directory that holds no profile are errors, not empty passes.
func TestLoadAndCheckRefuseWhatIsNotThere(t *testing.T) {
	if _, err := Load(filepath.Join("testdata", "entries", "absent.yaml")); err == nil {
		t.Error("Load of a file that does not exist succeeds")
	}
	if _, err := Load(filepath.Join("testdata", "repo", "pins", "empty.txt")); err == nil || !strings.Contains(err.Error(), "holds no entry") {
		t.Errorf("Load of a file with no entry = %v, want an error saying it holds no entry", err)
	}
	entries, err := Load(filepath.Join("testdata", "entries", "valid.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	l := fixtureLayout("envoy-1.40.2.txt")
	l.Profiles = filepath.Join("testdata", "repo", "pins")
	if err := Check(entries, l); err == nil || !strings.Contains(err.Error(), "ships no PlatformProfile") {
		t.Errorf("Check over a directory with no profile = %v, want an error saying it ships none", err)
	}
}
