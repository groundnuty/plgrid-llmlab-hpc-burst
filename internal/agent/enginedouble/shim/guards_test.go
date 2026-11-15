package main

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// doublePrefix is the import path every package of the engine double and the shim has.
const doublePrefix = "github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"

// shimPath is the shim's own directory in the module, as a script would name it to build it.
const shimPath = "internal/agent/enginedouble/shim"

func moduleRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("the module root is not at %s: %v", root, err)
	}
	return root
}

// No shipped binary reaches the double or the shim: the dependency closure of every cmd/* package, for each OS and
// architecture the binaries are built for, holds no package under internal/agent/enginedouble (issue #521, option (A)).
func TestNoShippedBinaryReachesTheDoubleOrTheShim(t *testing.T) {
	root := moduleRoot(t)
	cmds, err := filepath.Glob(filepath.Join(root, "cmd", "*"))
	if err != nil || len(cmds) < 4 {
		t.Fatalf("cmd/*: %v, %v; want the four shipped commands at least", cmds, err)
	}
	for _, target := range []struct{ goos, goarch string }{{"linux", "amd64"}, {"linux", "arm64"}} {
		args := []string{"list", "-deps", "-f", "{{.ImportPath}}"}
		for _, c := range cmds {
			args = append(args, "./cmd/"+filepath.Base(c))
		}
		cmd := exec.Command("go", args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOPROXY=off", "GOTOOLCHAIN=local", "CGO_ENABLED=0", "GOOS="+target.goos, "GOARCH="+target.goarch)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go list -deps of cmd/* for %s/%s: %v", target.goos, target.goarch, err)
		}
		deps := strings.Fields(string(out))
		if len(deps) < 50 {
			t.Fatalf("go list -deps of cmd/* for %s/%s listed %d packages: expected the shipped binaries' whole closure", target.goos, target.goarch, len(deps))
		}
		for _, d := range reachesTheDouble(deps) {
			t.Errorf("a shipped binary (cmd/*, %s/%s) reaches %s: the double and the shim are test-only", target.goos, target.goarch, d)
		}
	}
}

// reachesTheDouble are the import paths of a dependency list that are the double's or under it.
func reachesTheDouble(deps []string) []string {
	var out []string
	for _, d := range deps {
		if d == doublePrefix || strings.HasPrefix(d, doublePrefix+"/") {
			out = append(out, d)
		}
	}
	return out
}

// The closure check itself: the double and anything under it are named, a package that only shares a prefix of its name is not.
func TestReachesTheDouble(t *testing.T) {
	deps := []string{"fmt", "github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent", doublePrefix, doublePrefix + "/shim",
		"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedoubles"}
	if got := reachesTheDouble(deps); strings.Join(got, " ") != doublePrefix+" "+doublePrefix+"/shim" {
		t.Errorf("reachesTheDouble = %q, want the double and the shim alone", got)
	}
	if got := reachesTheDouble(deps[:2]); len(got) != 0 {
		t.Errorf("reachesTheDouble of a clean list = %q", got)
	}
}

// Nothing of the product can select the shim: no script, workflow or non-test source names its path, except the live
// checks script, which builds it for the bulk row alone. Its tests (_test.go files) may.
func TestNoProductConfigurationNamesTheShim(t *testing.T) {
	scanned, named := namesOfTheShim(t, moduleRoot(t))
	for _, rel := range named {
		t.Errorf("%s names the bulk row's test shim (%s): only hack/live-checks.sh may build it", rel, shimPath)
	}
	if scanned < 100 {
		t.Errorf("scanned %d sources and scripts, expected the module's whole tree", scanned)
	}
}

// The scan itself, over a tree of its own: a script or a non-test source that names the shim is found, the live checks script and a test file are
// not.
func TestTheShimScanFindsAProductThatNamesIt(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("hack/live-checks.sh", "go build ./"+shimPath+"\n")
	write("internal/x/x_test.go", "package x // "+shimPath+"\n")
	write("hack/build-agents.sh", "go build ./cmd/hb-agent\n")
	if _, named := namesOfTheShim(t, root); len(named) != 0 {
		t.Fatalf("the allowed tree: %q named", named)
	}
	write("hack/build-agents.sh", "go build ./"+shimPath+"\n")
	write(".github/workflows/release.yml", "run: go build ./"+shimPath+"\n")
	write("cmd/hpcburst/main.go", "package main // "+shimPath+"\n")
	if _, named := namesOfTheShim(t, root); strings.Join(named, " ") != ".github/workflows/release.yml cmd/hpcburst/main.go hack/build-agents.sh" {
		t.Errorf("namesOfTheShim = %q, want the workflow, the command and the agents' build", named)
	}
}

// namesOfTheShim reads every script, workflow and non-test source under root, but the shim's own and the spikes', and returns how many it read and
// the ones that name the shim's path, sorted, the live checks script aside.
func namesOfTheShim(t *testing.T, root string) (scanned int, named []string) {
	t.Helper()
	allowed := map[string]bool{"hack/live-checks.sh": true}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "testdata":
				return filepath.SkipDir
			}
			if rel == shimPath || strings.HasPrefix(rel, "spikes/") || strings.HasPrefix(rel, "third_party/") {
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(p)
		isSource := ext == ".go" && !strings.HasSuffix(p, "_test.go")
		isScript := ext == ".sh" || ext == ".yml" || ext == ".yaml" || d.Name() == "Makefile" || d.Name() == "Dockerfile"
		if !isSource && !isScript {
			return nil
		}
		scanned++
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), shimPath) && !allowed[rel] {
			named = append(named, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(named)
	return scanned, named
}
