package fakeslurm

// New. Run mode's reads of the disk and their limits. The root can hold what
// the jobs wrote (job output, a model cache), and the VM has no swap, so no
// read may take a size from the disk on trust: a file that grows while it is
// read is read for at most the limit plus one byte, and anything over a limit
// is an error, never a truncated answer.

import (
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
)

// The limits of the run-mode file system. A test that stages large files
// changes them on its diskFS; the Platform's own are these.
const (
	// maxHashBytes bounds sha256sum: streamed, so it costs time, not memory,
	// and the line holds the platform's lock while it runs.
	maxHashBytes = 256 << 20
	// maxFileBytes bounds one file in Files, maxTotalBytes all of them, and
	// maxSnapshotFiles their number.
	maxFileBytes     = 16 << 20
	maxTotalBytes    = 64 << 20
	maxSnapshotFiles = 50000
)

// errTooLarge is the error of a read over a limit.
var errTooLarge = errors.New("larger than the run-mode limit")

// copyCapped copies r to w for at most limit bytes and reports errTooLarge if
// r holds more. It reads one byte past the limit and no further, so a file
// that grows during the copy cannot make it run on.
func copyCapped(w hash.Hash, r io.Reader, limit int64) error {
	n, err := io.Copy(w, io.LimitReader(r, limit+1))
	switch {
	case err != nil:
		return err
	case n > limit:
		return fmt.Errorf("%w (%d bytes)", errTooLarge, limit)
	}
	return nil
}

// readCapped reads r to its end for at most limit bytes; errTooLarge if there
// is more.
func readCapped(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	switch {
	case err != nil:
		return nil, err
	case int64(len(b)) > limit:
		return nil, fmt.Errorf("%w (%d bytes)", errTooLarge, limit)
	}
	return b, nil
}

// snapshotFiles reads every regular file under the root. A file that a job
// removes or replaces during the walk is left out; a file over fileLimit, a
// tree over totalLimit or maxFiles, and a read that fails are errors, with no
// map. Links are not followed. It reads nothing under the platform's lock:
// the disk has no state of the fake's to protect.
func (d *diskFS) snapshotFiles() (map[string]File, error) {
	out := map[string]File{}
	var total int64
	err := fs.WalkDir(d.rt.FS(), ".", func(rel string, e fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // removed during the walk
		}
		if err != nil {
			return err
		}
		if !e.Type().IsRegular() {
			return nil
		}
		if len(out) >= d.maxFiles {
			return fmt.Errorf("%w: more than %d files", errTooLarge, d.maxFiles)
		}
		f, err := d.openRegular(rel, os.O_RDONLY, 0)
		switch {
		case errors.Is(err, fs.ErrNotExist) || errors.Is(err, errNotRegular):
			return nil // replaced during the walk
		case err != nil:
			return err
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		if fi.Size() > d.fileLimit { // fail before reading a byte
			return fmt.Errorf("%s: %w (%d bytes, the limit per file is %d)", rel, errTooLarge, fi.Size(), d.fileLimit)
		}
		b, err := readCapped(f, d.fileLimit) // and again as it is read, for a file that grows
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		if total += int64(len(b)); total > d.totalLimit {
			return fmt.Errorf("%s: %w (%d bytes after %d already read, the limit in all is %d)", rel, errTooLarge, len(b), total-int64(len(b)), d.totalLimit)
		}
		out[path.Join(d.root, rel)] = File{Content: b, Mode: fi.Mode().Perm()}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fakeslurm: files under %s: %w", d.root, err)
	}
	return out, nil
}

// snapshotDirs lists every directory under the root with its mode.
func (d *diskFS) snapshotDirs() map[string]fs.FileMode {
	out := map[string]fs.FileMode{}
	_ = fs.WalkDir(d.rt.FS(), ".", func(rel string, e fs.DirEntry, err error) error {
		if err != nil || !e.IsDir() {
			return nil
		}
		if fi, ierr := e.Info(); ierr == nil {
			out[path.Join(d.root, rel)] = fi.Mode().Perm()
		}
		return nil
	})
	return out
}
