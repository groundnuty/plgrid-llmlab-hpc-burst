package fakeslurm

// New. The platform's file system, shared by every Session: in memory (files
// with content, mode and owner, and a set of directories with modes), or in
// run mode a real directory on disk (diskfs.go).

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"path"
	"strings"
)

// File is one file of the fake file system.
type File struct {
	Content []byte
	Mode    fs.FileMode
	// Owner is the user whose command wrote the file; "" for a file placed
	// with WriteFile.
	Owner string
}

// fileSystem is what the commands and the setup helpers need of a file system.
// Every path is absolute and cleaned. The caller holds Platform.mu.
type fileSystem interface {
	isDir(p string) bool
	isFile(p string) bool
	// sum is the SHA-256 of a file's content. found is false for anything
	// but a file; err is why a file's sum could not be taken (run mode: it
	// is over the size limit).
	sum(p string) (sum [sha256.Size]byte, found bool, err error)
	// write replaces a file's content and keeps its mode and owner, or
	// creates the file with mode and owner.
	write(p string, content []byte, mode fs.FileMode, owner string) error
	// put creates or replaces a file with mode and no owner (test setup).
	put(p string, content []byte, mode fs.FileMode) error
	// mkdirAll creates p and any missing ancestor with mode. A file in the
	// way is an error, and nothing is created below it.
	mkdirAll(p string, mode fs.FileMode) error
	// chmod sets the mode of a file or a directory; false when p is neither.
	chmod(p string, mode fs.FileMode) (bool, error)
	// rename moves the file from to to; the caller has checked both.
	rename(from, to string) error
	// removeTree removes p and everything below it; a missing p is no error.
	removeTree(p string) error
	// snapshotFiles copies every file. The in-memory one cannot fail; the
	// disk one reports a file or a tree over its limits.
	snapshotFiles() (map[string]File, error)
	snapshotDirs() map[string]fs.FileMode
}

// fsState is the in-memory file system.
type fsState struct {
	files map[string]*File
	dirs  map[string]fs.FileMode
}

var _ fileSystem = (*fsState)(nil)

func newFS() *fsState {
	return &fsState{files: map[string]*File{}, dirs: map[string]fs.FileMode{"/": 0o755}}
}

func (f *fsState) sum(p string) ([sha256.Size]byte, bool, error) {
	file := f.files[p]
	if file == nil {
		return [sha256.Size]byte{}, false, nil
	}
	return sha256.Sum256(file.Content), true, nil
}

func (f *fsState) write(p string, content []byte, mode fs.FileMode, owner string) error {
	content = append([]byte(nil), content...)
	if file := f.files[p]; file != nil {
		file.Content = content
		return nil
	}
	f.files[p] = &File{Content: content, Mode: mode, Owner: owner}
	return nil
}

func (f *fsState) put(p string, content []byte, mode fs.FileMode) error {
	f.files[p] = &File{Content: append([]byte(nil), content...), Mode: mode.Perm()}
	return nil
}

func (f *fsState) chmod(p string, mode fs.FileMode) (bool, error) {
	switch {
	case f.files[p] != nil:
		f.files[p].Mode = mode.Perm()
	case f.isDir(p):
		f.dirs[p] = mode.Perm()
	default:
		return false, nil
	}
	return true, nil
}

func (f *fsState) rename(from, to string) error {
	file := f.files[from]
	delete(f.files, from)
	f.files[to] = file
	return nil
}

func (f *fsState) snapshotFiles() (map[string]File, error) {
	out := make(map[string]File, len(f.files))
	for name, file := range f.files {
		out[name] = File{Content: append([]byte(nil), file.Content...), Mode: file.Mode, Owner: file.Owner}
	}
	return out, nil
}

func (f *fsState) snapshotDirs() map[string]fs.FileMode {
	out := make(map[string]fs.FileMode, len(f.dirs))
	for name, m := range f.dirs {
		out[name] = m
	}
	return out
}

// isDir and isFile look a cleaned absolute path up.
func (f *fsState) isDir(p string) bool  { _, ok := f.dirs[p]; return ok }
func (f *fsState) isFile(p string) bool { _, ok := f.files[p]; return ok }

// mkdirAll creates p and any missing ancestor with mode. A file in the way is
// an error and nothing is created below it.
func (f *fsState) mkdirAll(p string, mode fs.FileMode) error {
	var chain []string
	for q := p; q != "/" && q != "."; q = path.Dir(q) {
		chain = append(chain, q)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		q := chain[i]
		switch {
		case f.isFile(q):
			return fmt.Errorf("cannot create directory '%s': File exists", q)
		case !f.isDir(q):
			f.dirs[q] = mode
		}
	}
	return nil
}

// removeTree removes p and everything below it.
func (f *fsState) removeTree(p string) error {
	delete(f.files, p)
	if p == "/" {
		return nil
	}
	delete(f.dirs, p)
	prefix := p + "/"
	for q := range f.files {
		if strings.HasPrefix(q, prefix) {
			delete(f.files, q)
		}
	}
	for q := range f.dirs {
		if strings.HasPrefix(q, prefix) {
			delete(f.dirs, q)
		}
	}
	return nil
}

func absolute(name string) (string, error) {
	if !strings.HasPrefix(name, "/") {
		return "", fmt.Errorf("fakeslurm: %q is not an absolute path", name)
	}
	return path.Clean(name), nil
}

// WriteFile places a file for test setup, creating missing parent directories
// with mode 0755. name must be absolute.
func (p *Platform) WriteFile(name string, content []byte, mode fs.FileMode) error {
	abs, err := absolute(name)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fs.isDir(abs) {
		return fmt.Errorf("fakeslurm: %s is a directory", abs)
	}
	if err := p.fs.mkdirAll(path.Dir(abs), 0o755); err != nil {
		return err
	}
	return p.fs.put(abs, content, mode)
}

// MkdirAll creates a directory and its missing parents for test setup.
func (p *Platform) MkdirAll(name string, mode fs.FileMode) error {
	abs, err := absolute(name)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fs.mkdirAll(abs, mode.Perm())
}

// Remove deletes a file, or a directory with everything below it, for test
// setup. A path that does not exist is not an error.
func (p *Platform) Remove(name string) error {
	abs, err := absolute(name)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fs.removeTree(abs)
}

// Files returns a copy of every file, by absolute path. In run mode it reads
// every regular file under the root, and Owner is "". The read is bounded: a
// file over 16 MiB, files over 64 MiB in all, or more than 50,000 files are a
// test bug (a root that holds staged models or a cache is not a tree to
// snapshot), and Files panics with the file that broke the limit rather than
// return a partial map. It holds no lock while it reads the disk.
func (p *Platform) Files() map[string]File {
	files, err := p.snapshotFiles()
	if err != nil {
		panic(err)
	}
	return files
}

func (p *Platform) snapshotFiles() (map[string]File, error) {
	if p.run == nil {
		p.mu.Lock()
		defer p.mu.Unlock()
	}
	return p.fs.snapshotFiles()
}

// Dirs returns a copy of every directory with its mode, by absolute path. In
// run mode it lists the root and every directory under it, with no lock held.
func (p *Platform) Dirs() map[string]fs.FileMode {
	if p.run == nil {
		p.mu.Lock()
		defer p.mu.Unlock()
	}
	return p.fs.snapshotDirs()
}
