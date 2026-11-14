package main

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
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
		for _, d := range deps {
			if d == doublePrefix || strings.HasPrefix(d, doublePrefix+"/") {
				t.Errorf("a shipped binary (cmd/*, %s/%s) reaches %s: the double and the shim are test-only", target.goos, target.goarch, d)
			}
		}
	}
}

// Nothing of the product can select the shim: no script, workflow or non-test source names its path, except the live
// checks script, which builds it for the bulk row alone. Its tests (_test.go files) may.
func TestNoProductConfigurationNamesTheShim(t *testing.T) {
	root := moduleRoot(t)
	allowed := map[string]bool{"hack/live-checks.sh": true}
	scanned := 0
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
			t.Errorf("%s names the bulk row's test shim (%s): only hack/live-checks.sh may build it", rel, shimPath)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 100 {
		t.Errorf("scanned %d sources and scripts, expected the module's whole tree", scanned)
	}
}
