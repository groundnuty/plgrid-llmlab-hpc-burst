package fakeslurm

// New. Run mode's file system: a real directory, so that the job's processes
// see what the login-node commands wrote, as compute nodes share the login
// node's file system. A path is the real path itself, and only paths under
// the root are reachable: anything outside it reads as missing and refuses
// writes, so a command can never touch the host outside the test's directory.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// errOutside is the error of a write outside the run-mode root.
var errOutside = errors.New("Permission denied")

// diskFS is the file system under root, an absolute, cleaned directory.
type diskFS struct{ root string }

var _ fileSystem = (*diskFS)(nil)

// in reports whether p is the root or under it.
func (d *diskFS) in(p string) bool { return p == d.root || strings.HasPrefix(p, d.root+"/") }

func (d *diskFS) stat(p string) (fs.FileInfo, bool) {
	if !d.in(p) {
		return nil, false
	}
	fi, err := os.Stat(p)
	return fi, err == nil
}

func (d *diskFS) isDir(p string) bool {
	fi, ok := d.stat(p)
	return ok && fi.IsDir()
}

func (d *diskFS) isFile(p string) bool {
	fi, ok := d.stat(p)
	return ok && fi.Mode().IsRegular()
}

func (d *diskFS) read(p string) ([]byte, bool) {
	if !d.isFile(p) {
		return nil, false
	}
	b, err := os.ReadFile(p)
	return b, err == nil
}

// write keeps an existing file's mode, as a shell redirection does; a new file
// gets mode exactly, whatever the process umask is.
func (d *diskFS) write(p string, content []byte, mode fs.FileMode, _ string) error {
	if !d.in(p) {
		return errOutside
	}
	if d.isFile(p) {
		return os.WriteFile(p, content, 0)
	}
	return d.create(p, content, mode)
}

func (d *diskFS) put(p string, content []byte, mode fs.FileMode) error {
	if !d.in(p) {
		return errOutside
	}
	if d.isFile(p) {
		if err := os.WriteFile(p, content, 0); err != nil {
			return err
		}
		return os.Chmod(p, mode.Perm())
	}
	return d.create(p, content, mode)
}

func (d *diskFS) create(p string, content []byte, mode fs.FileMode) error {
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm())
	if err != nil {
		return err
	}
	_, werr := f.Write(content)
	cerr := f.Close()
	if err := errors.Join(werr, cerr); err != nil {
		return err
	}
	return os.Chmod(p, mode.Perm())
}

func (d *diskFS) mkdirAll(p string, mode fs.FileMode) error {
	if !d.in(p) {
		return fmt.Errorf("cannot create directory '%s': %v", p, errOutside)
	}
	var chain []string
	for q := p; q != d.root && q != "/"; q = path.Dir(q) { // in(p) means it ends at root
		chain = append(chain, q)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		q := chain[i]
		fi, err := os.Lstat(q)
		switch {
		case err == nil && !fi.IsDir():
			return fmt.Errorf("cannot create directory '%s': File exists", q)
		case err == nil:
			continue
		case !errors.Is(err, fs.ErrNotExist):
			return fmt.Errorf("cannot create directory '%s': %v", q, err)
		}
		if err := os.Mkdir(q, mode.Perm()); err != nil {
			return fmt.Errorf("cannot create directory '%s': %v", q, err)
		}
		if err := os.Chmod(q, mode.Perm()); err != nil {
			return fmt.Errorf("cannot create directory '%s': %v", q, err)
		}
	}
	return nil
}

func (d *diskFS) chmod(p string, mode fs.FileMode) (bool, error) {
	if _, ok := d.stat(p); !ok {
		return false, nil
	}
	return true, os.Chmod(p, mode.Perm())
}

func (d *diskFS) rename(from, to string) error {
	if !d.in(from) || !d.in(to) {
		return errOutside
	}
	return os.Rename(from, to)
}

// removeTree removes p and everything below it. The root itself stays, as
// "/" does in memory: only what is under it goes.
func (d *diskFS) removeTree(p string) error {
	if !d.in(p) {
		return errOutside
	}
	if p != d.root {
		return os.RemoveAll(p)
	}
	entries, err := os.ReadDir(p)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(p, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// snapshotFiles and snapshotDirs are test views: an entry that a job removes
// during the walk, or that cannot be read, is left out.
func (d *diskFS) snapshotFiles() map[string]File {
	out := map[string]File{}
	_ = filepath.WalkDir(d.root, func(p string, e fs.DirEntry, err error) error {
		if err != nil || !e.Type().IsRegular() {
			return nil
		}
		b, rerr := os.ReadFile(p)
		fi, serr := e.Info()
		if rerr == nil && serr == nil {
			out[p] = File{Content: b, Mode: fi.Mode().Perm()}
		}
		return nil
	})
	return out
}

func (d *diskFS) snapshotDirs() map[string]fs.FileMode {
	out := map[string]fs.FileMode{}
	_ = filepath.WalkDir(d.root, func(p string, e fs.DirEntry, err error) error {
		if err != nil || !e.IsDir() {
			return nil
		}
		if fi, ierr := e.Info(); ierr == nil {
			out[p] = fi.Mode().Perm()
		}
		return nil
	})
	return out
}
