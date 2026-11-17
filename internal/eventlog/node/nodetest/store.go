package nodetest

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// storeDir is where nats-server keeps its JetStream store beneath the log's directory.
func (d *Dir) storeDir() string { return filepath.Join(d.Path, "jetstream") }

// CopyStore copies the log's store to a new directory and returns it. The log must be stopped: a
// copy of a live store is not consistent. It emulates a backup, and with RestoreStore a power loss
// that took the log's tail, as S8 did (HBEP-8, Test plan, "A lost tail").
func (d *Dir) CopyStore(t testing.TB) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "store-copy")
	if err := copyTree(d.storeDir(), dst); err != nil {
		t.Fatalf("copy the store: %v", err)
	}
	return dst
}

// RestoreStore replaces the log's store with a copy CopyStore made. The log must be stopped.
func (d *Dir) RestoreStore(t testing.TB, copyDir string) {
	t.Helper()
	if err := os.RemoveAll(d.storeDir()); err != nil {
		t.Fatalf("remove the store: %v", err)
	}
	if err := copyTree(copyDir, d.storeDir()); err != nil {
		t.Fatalf("restore the store: %v", err)
	}
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		info, err := e.Info()
		if err != nil {
			return err
		}
		if e.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm()|0o700)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		if err := out.Sync(); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
}
