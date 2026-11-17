package register

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"
)

// Entry is one known issue (HBEP-4, Known-issues register, The format).
type Entry struct {
	// ID is "KI-" and four digits, unique and never reused: a workaround's comment cites it.
	ID string `json:"id"`
	// Symptom is what a client, a log or a test sees, on one line.
	Symptom string `json:"symptom"`
	// Component is the upstream part: one of Components.
	Component string `json:"component"`
	// Affected is the versions the issue is seen in: one version, or a range such as ">=1.2.0,<1.4.0".
	Affected string `json:"affected"`
	// FixedIn is the first version that fixes it, once known; optional.
	FixedIn string `json:"fixedIn,omitempty"`
	// Scope is where the workaround applies; nil means everywhere (an engine-wide issue).
	Scope *Scope `json:"scope,omitempty"`
	// Workaround is the configuration that answers the bug.
	Workaround Workaround `json:"workaround"`
	// Test is the entry's one regression test.
	Test TestRef `json:"test"`
	// Source is where the issue was found: at least one upstream link, lesson ID or issue comment.
	Source []string `json:"source"`
}

// Scope names where an entry's workaround applies. A partition is always named with its platform.
type Scope struct {
	// Platforms are PlatformProfile names, as profiles/ ships them.
	Platforms []string `json:"platforms,omitempty"`
	// Partitions are partition names; allowed only with Platforms, and each must be in the typed
	// partitions list of at least one of those platforms.
	Partitions []string `json:"partitions,omitempty"`
}

// Workaround is the configuration that answers the bug.
type Workaround struct {
	// Where is a file and field, or a package and setting, on one line.
	Where string `json:"where"`
	// What is the setting, on one line, or "none" once the workaround is removed.
	What string `json:"what"`
}

// None is the Workaround.What of an entry whose workaround was removed; the entry and its test stay.
const None = "none"

// TestRef is an entry's regression test.
type TestRef struct {
	// Package is the Go package's directory, relative to the repository root.
	Package string `json:"package"`
	// Name is a test function of that package: func <Name>(t *testing.T) in its _test.go files.
	Name string `json:"name"`
}

// Components are the upstream parts an entry may name (HBEP-4, Known-issues register).
var Components = []string{"vllm", "envoy", "vllm-router", "frp", "apptainer", "slurm", "llmlab"}

// binaryComponents are the components pinned in a pins file, the ones whose entries fail by version.
// An engine, the router, the site's Slurm and Apptainer, and LLMLab never do: the framework pins no
// engine image of its own (HBEP-15, Notes), and the site's software is a profile fact.
var binaryComponents = []string{"envoy", "frp"}

var idPattern = regexp.MustCompile(`^KI-[0-9]{4}$`)

// Load reads the register file strictly: an unknown or duplicate key, a value of the wrong type, a
// field that is missing, an id, component or version that is malformed, or an empty register is an
// error, and the error names every problem it found.
func Load(path string) ([]Entry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the register: %w", err)
	}
	var entries []Entry
	if err := yaml.UnmarshalStrict(raw, &entries); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%s holds no entry", path)
	}
	var errs []error
	for i, e := range entries {
		for _, p := range e.problems() {
			errs = append(errs, fmt.Errorf("entry %d (%s): %s", i+1, e.ID, p))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return entries, nil
}

// problems are what is wrong with the entry alone: a field missing, a malformed value.
func (e Entry) problems() []string {
	var out []string
	need := func(field, value string) {
		if strings.TrimSpace(value) == "" {
			out = append(out, field+" is missing")
		} else if strings.ContainsAny(value, "\r\n") {
			out = append(out, field+" is not one line")
		}
	}
	switch {
	case e.ID == "":
		out = append(out, "id is missing")
	case !idPattern.MatchString(e.ID):
		out = append(out, fmt.Sprintf("id %q is not KI- and four digits", e.ID))
	}
	need("symptom", e.Symptom)
	need("workaround.where", e.Workaround.Where)
	need("workaround.what", e.Workaround.What)
	need("test.package", e.Test.Package)
	need("test.name", e.Test.Name)
	switch {
	case e.Component == "":
		out = append(out, "component is missing")
	case !contains(Components, e.Component):
		out = append(out, fmt.Sprintf("component %q is none of %s", e.Component, strings.Join(Components, ", ")))
	}
	if e.Affected == "" {
		out = append(out, "affected is missing")
	} else if err := checkRange(e.Affected); err != nil {
		out = append(out, "affected: "+err.Error())
	}
	if e.FixedIn != "" {
		if _, err := ParseVersion(e.FixedIn); err != nil {
			out = append(out, "fixedIn: "+err.Error())
		}
	}
	if len(e.Source) == 0 {
		out = append(out, "source is missing: name at least one upstream link, lesson ID or issue comment")
	}
	for _, s := range e.Source {
		if strings.TrimSpace(s) == "" {
			out = append(out, "source holds an empty item")
		}
	}
	if e.Scope != nil {
		for _, s := range append(append([]string{}, e.Scope.Platforms...), e.Scope.Partitions...) {
			if strings.TrimSpace(s) == "" {
				out = append(out, "scope holds an empty name")
			}
		}
	}
	return out
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// Version is a dotted version: its numbers, most significant first.
type Version []int

// ParseVersion parses digits and dots only ("1.40.2"): no prefix, no suffix, no empty part.
func ParseVersion(s string) (Version, error) {
	if s == "" {
		return nil, errors.New("the version is empty")
	}
	parts := strings.Split(s, ".")
	v := make(Version, len(parts))
	for i, p := range parts {
		if p == "" || strings.Trim(p, "0123456789") != "" {
			return nil, fmt.Errorf("%q is not a version: digits and dots only, such as 1.40.2", s)
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("%q is not a version: %w", s, err)
		}
		v[i] = n
	}
	return v, nil
}

// Compare returns -1, 0 or 1 as v is below, equal to or above w, comparing numbers, so 1.10.0 is above
// 1.9.0, and a missing part counts as 0 (1.40 equals 1.40.0).
func (v Version) Compare(w Version) int {
	for i := 0; i < len(v) || i < len(w); i++ {
		var a, b int
		if i < len(v) {
			a = v[i]
		}
		if i < len(w) {
			b = w[i]
		}
		if a != b {
			if a < b {
				return -1
			}
			return 1
		}
	}
	return 0
}

// checkRange checks an affected range: one version, or comma-separated bounds, each an operator (>=, >,
// <= or <) and a version, such as ">=1.2.0,<1.4.0".
func checkRange(s string) error {
	if !strings.Contains(s, ",") && !strings.ContainsAny(s, "<>=") {
		_, err := ParseVersion(s)
		return err
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		v := strings.TrimLeft(part, "<>=")
		switch op := part[:len(part)-len(v)]; op {
		case ">=", ">", "<=", "<":
		default:
			return fmt.Errorf("%q: a bound is >=, >, <= or < and a version", part)
		}
		if _, err := ParseVersion(v); err != nil {
			return err
		}
	}
	return nil
}
