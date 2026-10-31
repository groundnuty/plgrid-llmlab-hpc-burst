package fakeslurm

// New. The platform's in-memory file system: files (content, mode, owner) and a
// set of directories (with modes). The file system is shared by every Session.

import (
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

type fsState struct {
	files map[string]*File
	dirs  map[string]fs.FileMode
}

func newFS() fsState {
	return fsState{files: map[string]*File{}, dirs: map[string]fs.FileMode{"/": 0o755}}
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
func (f *fsState) removeTree(p string) {
	delete(f.files, p)
	if p == "/" {
		return
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
	p.fs.files[abs] = &File{Content: append([]byte(nil), content...), Mode: mode.Perm()}
	return nil
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
	p.fs.removeTree(abs)
	return nil
}

// Files returns a copy of every file, by absolute path.
func (p *Platform) Files() map[string]File {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]File, len(p.fs.files))
	for name, f := range p.fs.files {
		out[name] = File{Content: append([]byte(nil), f.Content...), Mode: f.Mode, Owner: f.Owner}
	}
	return out
}

// Dirs returns a copy of every directory with its mode, by absolute path.
func (p *Platform) Dirs() map[string]fs.FileMode {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]fs.FileMode, len(p.fs.dirs))
	for name, m := range p.fs.dirs {
		out[name] = m
	}
	return out
}
