package testenv

// Helpers of the tests of hack/build-agents.sh (build_agents_test.go): the copy of the sources at
// another path that the reproducibility test builds from.

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func fileSum(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// copyFile copies src to dst with src's mode.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fi.Mode().Perm())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

// sourceCopy copies what the script builds from into a new directory, which has another path and
// no .git: go.mod, go.sum, the two scripts, and every file but the tests of each package of this
// module that cmd/hb-agent imports for any of the architectures, with the files they embed. It
// is a copy of the working tree, not of a commit, so uncommitted edits are in it.
func sourceCopy(t *testing.T) string {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	// go list prints the directories it resolved, which differ from root when a parent of the
	// checkout is a symlink.
	if root, err = filepath.EvalSymlinks(root); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	for _, f := range []string{"go.mod", "go.sum", "hack/build-agents.sh", "hack/cpu-arch.sh"} {
		copyFile(t, filepath.Join(root, f), filepath.Join(dst, f))
	}
	dirs := map[string]bool{}
	embeds := map[string]bool{}
	for _, want := range agentWant {
		cmd := exec.Command("go", "list", "-deps", "-f",
			`{{if not .Standard}}D {{.Dir}}{{"\n"}}{{range .EmbedFiles}}E {{$.Dir}}/{{.}}{{"\n"}}{{end}}{{end}}`, "./cmd/hb-agent")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOPROXY=off", "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+want.goarch)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go list for %s: %v", want.goarch, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			kind, path, _ := strings.Cut(line, " ")
			if path, err = filepath.EvalSymlinks(path); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(path, root+string(filepath.Separator)) {
				continue // a dependency outside this module is read from the module cache
			}
			if kind == "D" {
				dirs[path] = true
			} else {
				embeds[path] = true
			}
		}
	}
	if len(dirs) == 0 {
		t.Fatal("go list found no package of this module under cmd/hb-agent")
	}
	for dir := range dirs {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			rel, err := filepath.Rel(root, filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			copyFile(t, filepath.Join(dir, e.Name()), filepath.Join(dst, rel))
		}
	}
	for f := range embeds {
		rel, err := filepath.Rel(root, f)
		if err != nil {
			t.Fatal(err)
		}
		copyFile(t, f, filepath.Join(dst, rel))
	}
	return dst
}
