package fakeslurm

// New. A direct test of the run-mode file system's mkdirAll on paths outside
// its root. Under a mutant that dropped the root check, its walk up from such
// a path once never ended and grew without bound (path.Dir("/") is "/"); the
// walk now refuses at a fixed point of path.Dir. Each call runs under a
// deadline, and a call that does not return panics, which ends the test
// binary at once rather than letting the walk eat memory.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mkdirAllWithin calls d.mkdirAll and panics if it has not returned after 5 s,
// about a million times what a call takes, and before a runaway walk could
// reach a 4 GB memory cap.
func mkdirAllWithin(d *diskFS, p string) error {
	done := make(chan error, 1)
	go func() { done <- d.mkdirAll(p, 0o755) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		panic(fmt.Sprintf("diskFS.mkdirAll(%q) has not returned after 5 s: its walk does not end", p))
	}
}

func TestDiskFSMkdirAllRefusesPathsOutsideTheRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	d := newTestDiskFS(t, root)
	for _, p := range []string{
		"/", ".", "", "relative", "relative/dir",
		parent, parent + "/sibling/a", root + "x/a", // root+"x" shares the root's prefix but is not under it
	} {
		if err := mkdirAllWithin(d, p); err == nil {
			t.Errorf("mkdirAll(%q) outside the root %s: no error", p, root)
		}
	}
	for _, p := range []string{parent + "/sibling", root + "x", "relative"} {
		if _, err := os.Stat(p); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s exists after the refused calls: %v", p, err)
		}
	}
	// The control: a path under the root is made, and the root itself is
	// already there.
	for _, p := range []string{root, root + "/a/b"} {
		if err := mkdirAllWithin(d, p); err != nil {
			t.Errorf("mkdirAll(%q) under the root: %v", p, err)
		}
	}
	if fi, err := os.Stat(root + "/a/b"); err != nil || !fi.IsDir() {
		t.Errorf("%s/a/b was not made: %v", root, err)
	}
}

// newTestDiskFS is a diskFS on root, closed with the test.
func newTestDiskFS(t *testing.T, root string) *diskFS {
	t.Helper()
	d, err := newDiskFS(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.rt.Close() })
	return d
}
