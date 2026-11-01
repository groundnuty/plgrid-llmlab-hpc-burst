package fakeslurm

// New. Run mode's file system: a real directory, so that the job's processes
// see what the login-node commands wrote, as compute nodes share the login
// node's file system. A path is the real path itself. Every operation goes
// through an os.Root opened on the directory, so it cannot leave it: a path
// outside the root, a ".." that climbs out of it, and a symbolic link that
// resolves outside it (a job's real processes can make one) all read as
// missing or refuse the write. A link that stays inside works if it is
// relative, as the links of a Hugging Face cache are; os.Root refuses an
// absolute link even when its target is inside. The root is the guarantee of
// the fake's own operations, not a sandbox: the job's processes run as the
// test does and can touch whatever the test can.

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"syscall"
)

// errOutside is the error of a write outside the run-mode root.
var errOutside = errors.New("Permission denied")

// errNotRegular is the error of an open that found something other than a
// regular file: a FIFO, a device, a directory.
var errNotRegular = errors.New("not a regular file")

// diskFS is the file system under root, an absolute, cleaned directory.
type diskFS struct {
	root string
	rt   *os.Root // opened on root; its finalizer closes it with the Platform
	// Limits on what one read takes into memory or time (diskfs_read.go).
	hashLimit, fileLimit, totalLimit int64
	maxFiles                         int
}

var _ fileSystem = (*diskFS)(nil)

// newDiskFS opens the root directory.
func newDiskFS(root string) (*diskFS, error) {
	rt, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	return &diskFS{
		root: root, rt: rt,
		hashLimit: maxHashBytes, fileLimit: maxFileBytes, totalLimit: maxTotalBytes, maxFiles: maxSnapshotFiles,
	}, nil
}

// rel is p relative to the root, "." for the root itself, and false for a
// path that is not under it. p must be cleaned: "root/../x" is refused, not
// resolved.
func (d *diskFS) rel(p string) (string, bool) {
	if path.Clean(p) != p {
		return "", false
	}
	if p == d.root {
		return ".", true
	}
	return strings.CutPrefix(p, d.root+"/")
}

// cause is the error without the "openat name:" prefix of an os.Root
// operation, which a shell does not print.
func cause(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}

func (d *diskFS) stat(p string) (fs.FileInfo, bool) {
	rel, ok := d.rel(p)
	if !ok {
		return nil, false
	}
	fi, err := d.rt.Stat(rel)
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

// openRegular opens rel (inside the root) and returns it only if it is a
// regular file. O_NONBLOCK makes the open of a FIFO return at once instead of
// waiting for the other end, and the fstat then refuses the FIFO, a device
// and a directory; the flag is cleared again, since a regular file does not
// need it and a child that inherits the descriptor should not see it. A path
// is checked by name before it is opened, and a job can swap it in between:
// this check is on the opened file itself.
func (d *diskFS) openRegular(rel string, flag int, perm fs.FileMode) (*os.File, error) {
	f, err := d.rt.OpenFile(rel, flag|syscall.O_NONBLOCK, perm)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, &fs.PathError{Op: "open", Path: rel, Err: errNotRegular}
	}
	if err := clearNonblock(f); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// clearNonblock puts the open file description back in blocking mode.
func clearNonblock(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := rc.Control(func(fd uintptr) { serr = syscall.SetNonblock(int(fd), false) }); err != nil {
		return err
	}
	return serr
}

// sum is the SHA-256 of a regular file, streamed, for at most hashLimit bytes.
func (d *diskFS) sum(p string) (sum [sha256.Size]byte, found bool, err error) {
	rel, ok := d.rel(p)
	if !ok || !d.isFile(p) {
		return sum, false, nil
	}
	f, err := d.openRegular(rel, os.O_RDONLY, 0)
	if err != nil {
		return sum, false, nil // gone or swapped since the check: not a file
	}
	defer f.Close()
	h := sha256.New()
	if err := copyCapped(h, f, d.hashLimit); err != nil {
		return sum, true, err
	}
	h.Sum(sum[:0])
	return sum, true, nil
}

// write keeps an existing file's mode, as a shell redirection does; a new file
// gets mode exactly, whatever the process umask is.
func (d *diskFS) write(p string, content []byte, mode fs.FileMode, _ string) error {
	rel, ok := d.rel(p)
	if !ok {
		return errOutside
	}
	if d.isFile(p) {
		return d.overwrite(rel, content)
	}
	return d.create(rel, content, mode)
}

func (d *diskFS) put(p string, content []byte, mode fs.FileMode) error {
	rel, ok := d.rel(p)
	if !ok {
		return errOutside
	}
	if d.isFile(p) {
		if err := d.overwrite(rel, content); err != nil {
			return err
		}
		return cause(d.rt.Chmod(rel, mode.Perm()))
	}
	return d.create(rel, content, mode)
}

// overwrite replaces the content of an existing regular file. The file is
// truncated after the open has shown it is one: O_TRUNC on a name would
// truncate whatever a job swapped in.
func (d *diskFS) overwrite(rel string, content []byte) error {
	f, err := d.openRegular(rel, os.O_WRONLY, 0)
	if err != nil {
		return cause(err)
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return err
	}
	_, werr := f.Write(content)
	return errors.Join(werr, f.Close())
}

// create makes a new file; O_EXCL refuses an existing name, a symbolic link
// included.
func (d *diskFS) create(rel string, content []byte, mode fs.FileMode) error {
	f, err := d.rt.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm())
	if err != nil {
		return cause(err)
	}
	_, werr := f.Write(content)
	cherr := f.Chmod(mode.Perm()) // exactly mode, whatever the umask is
	return errors.Join(werr, cherr, f.Close())
}

func (d *diskFS) mkdirAll(p string, mode fs.FileMode) error {
	if _, ok := d.rel(p); !ok {
		return fmt.Errorf("cannot create directory '%s': %v", p, errOutside)
	}
	var chain []string
	for q := p; q != d.root; q = path.Dir(q) {
		// A fixed point of path.Dir ("/" or ".") before the root means p
		// is not under it: a second guard, so the walk always ends.
		if path.Dir(q) == q {
			return fmt.Errorf("cannot create directory '%s': %v", p, errOutside)
		}
		chain = append(chain, q)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		q := chain[i]
		rel, _ := d.rel(q)        // under the root, as p is
		fi, err := d.rt.Stat(rel) // follows a link that stays inside; one that leaves is an error
		switch {
		case err == nil && !fi.IsDir():
			return fmt.Errorf("cannot create directory '%s': File exists", q)
		case err == nil:
			continue
		case !errors.Is(err, fs.ErrNotExist):
			return fmt.Errorf("cannot create directory '%s': %v", q, cause(err))
		}
		if err := d.rt.Mkdir(rel, mode.Perm()); err != nil {
			return fmt.Errorf("cannot create directory '%s': %v", q, cause(err))
		}
		if err := d.rt.Chmod(rel, mode.Perm()); err != nil {
			return fmt.Errorf("cannot create directory '%s': %v", q, cause(err))
		}
	}
	return nil
}

func (d *diskFS) chmod(p string, mode fs.FileMode) (bool, error) {
	rel, ok := d.rel(p)
	if !ok {
		return false, nil
	}
	if _, ok := d.stat(p); !ok {
		return false, nil
	}
	return true, cause(d.rt.Chmod(rel, mode.Perm()))
}

func (d *diskFS) rename(from, to string) error {
	rf, ok1 := d.rel(from)
	rt, ok2 := d.rel(to)
	if !ok1 || !ok2 {
		return errOutside
	}
	return cause(d.rt.Rename(rf, rt))
}

// removeTree removes p and everything below it; a link is removed itself and
// never followed. The root itself stays, as "/" does in memory: only what is
// under it goes.
func (d *diskFS) removeTree(p string) error {
	rel, ok := d.rel(p)
	if !ok {
		return errOutside
	}
	if rel != "." {
		return cause(d.rt.RemoveAll(rel))
	}
	dir, err := d.rt.Open(".")
	if err != nil {
		return cause(err)
	}
	names, err := dir.Readdirnames(-1)
	if cerr := dir.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return cause(err)
	}
	for _, name := range names {
		if err := d.rt.RemoveAll(name); err != nil {
			return cause(err)
		}
	}
	return nil
}
