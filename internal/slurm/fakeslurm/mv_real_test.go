package fakeslurm

// The fake's mv is compared with the real one. The fake used to refuse a
// directory as the destination, which hid that GNU mv -f moves the file INSIDE
// it and exits 0 (the install of PR 157's frpc and certificate depends on mv -fT
// failing there). A model checked only against itself would not have shown it,
// so each scenario runs the real mv in a temporary directory and the fake on the
// same layout, and compares the exit status class and the resulting tree.

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFakeMvMatchesTheRealMv(t *testing.T) {
	probe := t.TempDir()
	if err := os.WriteFile(filepath.Join(probe, "a"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("mv", "-fT", filepath.Join(probe, "a"), filepath.Join(probe, "b")).CombinedOutput(); err != nil {
		t.Skipf("the real mv has no -T (not GNU coreutils): %v: %s", err, out)
	}
	// Every scenario starts from the same layout: files d/a ("A"), d/b ("B"),
	// d/dir/keep ("K") and d/other/a/keep ("O"), so d/dir and d/other are
	// directories, and d/other/a is a directory too.
	layoutFiles := map[string]string{"d/a": "A", "d/b": "B", "d/dir/keep": "K", "d/other/a/keep": "O"}
	for _, tc := range []struct {
		name string
		args []string // the words after mv, relative to the layout's root
	}{
		{"a new name", []string{"-f", "d/a", "d/c"}},
		{"onto a file", []string{"-f", "d/a", "d/b"}},
		{"onto a file with -T", []string{"-fT", "d/a", "d/b"}},
		{"into a directory", []string{"-f", "d/a", "d/dir"}},
		{"into a directory, trailing slash", []string{"-f", "d/a", "d/dir/"}},
		{"onto a directory with -T", []string{"-fT", "d/a", "d/dir"}},
		{"onto a directory with -T, trailing slash", []string{"-fT", "d/a", "d/dir/"}},
		{"into a directory that holds a directory of the name", []string{"-f", "d/a", "d/other"}},
		{"separate options", []string{"-f", "-T", "d/a", "d/dir"}},
		{"options in the other order", []string{"-Tf", "d/a", "d/dir"}},
		{"no destination directory", []string{"-f", "d/a", "nodir/c"}},
		{"no destination directory with -T", []string{"-fT", "d/a", "nodir/c"}},
		{"no source", []string{"-f", "d/nope", "d/c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for rel, content := range layoutFiles {
				p := filepath.Join(root, rel)
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			realCode := 0
			cmd := exec.Command("mv", tc.args...)
			cmd.Dir = root
			var ee *exec.ExitError
			switch err := cmd.Run(); {
			case errors.As(err, &ee):
				realCode = ee.ExitCode()
			case err != nil:
				t.Fatal(err)
			}
			realFiles, realDirs := walk(t, root)

			r := newRig(t, Config{})
			for rel, content := range layoutFiles {
				if err := r.p.WriteFile("/w/"+rel, []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			words := make([]string, len(tc.args))
			for i, a := range tc.args {
				words[i] = a
				if !strings.HasPrefix(a, "-") {
					words[i] = "/w/" + a
				}
			}
			_, _, fakeCode := r.run("mv " + strings.Join(words, " "))
			fakeFiles, fakeDirs := map[string]string{}, map[string]bool{}
			for p, f := range r.p.Files() {
				if rel, ok := strings.CutPrefix(p, "/w/"); ok {
					fakeFiles[rel] = string(f.Content)
				}
			}
			for p := range r.p.Dirs() {
				if rel, ok := strings.CutPrefix(p, "/w/"); ok {
					fakeDirs[rel] = true
				}
			}
			if (realCode == 0) != (fakeCode == 0) || (realCode != 0 && fakeCode != realCode) {
				t.Errorf("mv %v: the real mv exits %d, the fake %d", tc.args, realCode, fakeCode)
			}
			if !reflect.DeepEqual(realFiles, fakeFiles) {
				t.Errorf("mv %v: files after the real mv %v, after the fake %v", tc.args, realFiles, fakeFiles)
			}
			if !reflect.DeepEqual(realDirs, fakeDirs) {
				t.Errorf("mv %v: directories after the real mv %v, after the fake %v", tc.args, realDirs, fakeDirs)
			}
		})
	}
}

// walk returns the files (relative path to content) and the directories below
// root.
func walk(t *testing.T, root string) (map[string]string, map[string]bool) {
	t.Helper()
	files, dirs := map[string]string{}, map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil || rel == "." {
			return err
		}
		if d.IsDir() {
			dirs[rel] = true
			return nil
		}
		b, err := os.ReadFile(p)
		files[rel] = string(bytes.TrimSpace(b))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files, dirs
}
