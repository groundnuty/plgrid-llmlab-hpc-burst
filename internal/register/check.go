package register

import (
	"bufio"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// Layout says where Check reads what the register refers to, so a test can point it at fixtures.
type Layout struct {
	// Root is the directory that each entry's test.package is relative to.
	Root string
	// Profiles is the directory of the shipped PlatformProfile manifests, *.yaml; a *.draft.yaml
	// file is a draft and ships nothing.
	Profiles string
	// Pins maps a binary component (envoy, frp) to its pins file, whose first column is a version.
	Pins map[string]string
}

// RepoLayout is the layout of the repository at root.
func RepoLayout(root string) Layout {
	return Layout{
		Root:     root,
		Profiles: filepath.Join(root, "profiles"),
		Pins: map[string]string{
			"envoy": filepath.Join(root, "internal", "agent", "envoypin", "envoy-pins.txt"),
			"frp":   filepath.Join(root, "internal", "launcher", "frp-pins.txt"),
		},
	}
}

// Check checks the entries against what they refer to (HBEP-4, Known-issues register, Who reads it)
// and returns every problem it finds, or nil:
//   - an id used twice;
//   - a platform that profiles/ does not ship, a partition that none of the entry's platforms lists in
//     its typed partitions, or a partition named with no platform;
//   - a test.name that is no func <name>(t *testing.T) in the _test.go files of test.package;
//   - an entry for a binary pinned in a pins file (envoy, frp) whose fixedIn the pinned version has
//     reached, while its workaround is still named: the PR that reaches it writes "none" and keeps the
//     entry and its test.
//
// Entries of every other component never fail by version. Check does not run the tests.
func Check(entries []Entry, l Layout) error {
	var errs []error
	fail := func(e Entry, format string, a ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", e.ID, fmt.Sprintf(format, a...)))
	}

	seen := map[string]bool{}
	for _, e := range entries {
		if seen[e.ID] {
			fail(e, "the id is used twice")
		}
		seen[e.ID] = true
	}

	platforms, err := shippedPlatforms(l.Profiles)
	if err != nil {
		return err
	}
	tests := newTestIndex()
	pins := map[string]Version{}
	for _, e := range entries {
		checkScope(e, platforms, fail)
		checkTest(e, l.Root, tests, fail)
		if err := checkPins(e, l, pins); err != nil {
			fail(e, "%v", err)
		}
	}
	return errors.Join(errs...)
}

func checkScope(e Entry, platforms map[string]map[string]bool, fail func(Entry, string, ...any)) {
	if e.Scope == nil {
		return
	}
	if len(e.Scope.Partitions) > 0 && len(e.Scope.Platforms) == 0 {
		fail(e, "scope.partitions %v are named with no platform: a partition is always named with its platform", e.Scope.Partitions)
	}
	for _, p := range e.Scope.Platforms {
		if _, ok := platforms[p]; !ok {
			fail(e, "scope.platforms: %q is not a platform that profiles/ ships (%s)", p, strings.Join(sortedKeys(platforms), ", "))
		}
	}
	for _, part := range e.Scope.Partitions {
		listed := false
		for _, p := range e.Scope.Platforms {
			listed = listed || platforms[p][part]
		}
		if len(e.Scope.Platforms) > 0 && !listed {
			fail(e, "scope.partitions: %q is in no typed partitions list of the platforms %v", part, e.Scope.Platforms)
		}
	}
}

func checkTest(e Entry, root string, idx *testIndex, fail func(Entry, string, ...any)) {
	if e.Test.Package == "" || e.Test.Name == "" {
		return // Load reports it
	}
	if !filepath.IsLocal(e.Test.Package) {
		fail(e, "test.package %q is not a directory under the repository root", e.Test.Package)
		return
	}
	names, err := idx.funcs(filepath.Join(root, filepath.FromSlash(e.Test.Package)))
	if err != nil {
		fail(e, "test.package %s: %v", e.Test.Package, err)
		return
	}
	if !names[e.Test.Name] {
		fail(e, "test.name %q is no func Test...(t *testing.T) in the _test.go files of %s", e.Test.Name, e.Test.Package)
	}
}

// checkPins fails an entry for a pinned binary whose fixedIn the pins file has reached while its
// workaround is named. pins caches each file's highest version.
func checkPins(e Entry, l Layout, pins map[string]Version) error {
	if !contains(binaryComponents, e.Component) || e.FixedIn == "" || e.Workaround.What == None {
		return nil
	}
	fixed, err := ParseVersion(e.FixedIn)
	if err != nil {
		return nil // Load reports it
	}
	file := l.Pins[e.Component]
	if file == "" {
		return fmt.Errorf("no pins file is given for %s, so fixedIn %s cannot be checked", e.Component, e.FixedIn)
	}
	high, ok := pins[file]
	if !ok {
		if high, err = highestPinned(file); err != nil {
			return err
		}
		pins[file] = high
	}
	if high.Compare(fixed) >= 0 {
		return fmt.Errorf("%s is fixed in %s and the pins file pins up to %s, yet workaround.what still names the workaround: write %q and keep the entry and its test",
			e.Component, e.FixedIn, versionString(high), None)
	}
	return nil
}

func versionString(v Version) string {
	parts := make([]string, len(v))
	for i, n := range v {
		parts[i] = fmt.Sprint(n)
	}
	return strings.Join(parts, ".")
}

// highestPinned is the highest version in the first column of a pins file, whose lines are blank,
// comments (#) or a version followed by more columns.
func highestPinned(path string) (Version, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read the pins file: %w", err)
	}
	defer f.Close()
	var high Version
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		v, err := ParseVersion(strings.Fields(line)[0])
		if err != nil {
			return nil, fmt.Errorf("the pins file %s: %w", filepath.Base(path), err)
		}
		if high == nil || v.Compare(high) > 0 {
			high = v
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read the pins file: %w", err)
	}
	if high == nil {
		return nil, fmt.Errorf("the pins file %s pins no version", filepath.Base(path))
	}
	return high, nil
}

// shippedPlatforms maps each shipped platform, a PlatformProfile's name in a non-draft manifest of
// dir, to the names in its typed partitions list; a managed platform has none.
func shippedPlatforms(dir string) (map[string]map[string]bool, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, ".draft.yaml") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("read a profile: %w", err)
		}
		var pp struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Partitions []struct {
					Name string `json:"name"`
				} `json:"partitions"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(raw, &pp); err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(f), err)
		}
		if pp.Kind != "PlatformProfile" || pp.Metadata.Name == "" {
			return nil, fmt.Errorf("%s is no PlatformProfile with a name", filepath.Base(f))
		}
		if _, dup := out[pp.Metadata.Name]; dup {
			return nil, fmt.Errorf("%s: the platform %s is shipped twice", filepath.Base(f), pp.Metadata.Name)
		}
		parts := map[string]bool{}
		for _, p := range pp.Spec.Partitions {
			parts[p.Name] = true
		}
		out[pp.Metadata.Name] = parts
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s ships no PlatformProfile", dir)
	}
	return out, nil
}

// testIndex finds the test functions of a package's _test.go files, parsed, never grepped.
type testIndex struct{ byDir map[string]map[string]bool }

func newTestIndex() *testIndex { return &testIndex{byDir: map[string]map[string]bool{}} }

// funcs are the names of the package's functions of the form func TestX(t *testing.T): a plain
// function (no receiver), a name that starts with Test, one parameter of type *testing.T, no result.
func (x *testIndex) funcs(dir string) (map[string]bool, error) {
	if names, ok := x.byDir[dir]; ok {
		return names, nil
	}
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errors.New("the directory holds no _test.go file")
	}
	names := map[string]bool{}
	fset := token.NewFileSet()
	for _, f := range files {
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", filepath.Base(f), err)
		}
		for _, d := range file.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && isTestFunc(fn) {
				names[fn.Name.Name] = true
			}
		}
	}
	x.byDir[dir] = names
	return names, nil
}

func isTestFunc(fn *ast.FuncDecl) bool {
	if fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Test") || fn.Type.Results != nil || fn.Type.TypeParams != nil {
		return false
	}
	params := fn.Type.Params.List
	if len(params) != 1 || len(params[0].Names) > 1 {
		return false
	}
	star, ok := params[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "testing" && sel.Sel.Name == "T"
}

func sortedKeys(m map[string]map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
