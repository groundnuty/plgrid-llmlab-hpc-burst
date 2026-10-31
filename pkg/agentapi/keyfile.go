package agentapi

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
)

// Key files. An Allocation's credential and an Instance's key have one form,
// and the files that hold them one discipline, because several components
// write and read the same files: the lifecycle controller, the provider, the
// agent API's server, the front. HBEP-7 fixes only "a mode-600 file named by
// the UID"; this file is the contract's choice for the rest.

const (
	// KeyFileMode is the mode of a key file.
	KeyFileMode os.FileMode = 0o600
	// KeyDirMode is the mode of the directories WriteKeyFile creates above one.
	KeyDirMode os.FileMode = 0o700

	// keyFormBytes is a key's length: 256 random bits as hex.
	keyFormBytes = 64
	// readLimit is the most ReadKeyFile reads: a key and one byte more, so a
	// file that is too long is seen to be.
	readLimit = keyFormBytes + 1
)

// keyForm is the form of a key and of an Allocation's credential: 256 random
// bits as 64 lowercase hex characters, with no newline (HBEP-7, Provisioning).
var keyForm = regexp.MustCompile(`^[0-9a-f]{64}$`)

// NewKey returns a new key: 32 bytes from crypto/rand as 64 lowercase hex
// characters. The agent generates an Instance's key with it at Instance start.
// The lifecycle controller generates an Allocation's credential with it too.
func NewKey() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("key: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// CheckKey returns an error unless b is a key's exact form: 64 lowercase hex
// characters and nothing else, no newline. The error never contains b.
func CheckKey(b []byte) error {
	if !keyForm.Match(b) {
		return fmt.Errorf("a key is 64 lowercase hex characters with no newline; got %d bytes of another form", len(b))
	}
	return nil
}

// WriteKeyFile writes key to path as a key file and replaces any file there. It
// refuses a key that CheckKey refuses before it touches the disk. It creates the
// missing parent directories with KeyDirMode, writes exactly the key's 64
// bytes, with no newline, to a temporary file in the same directory with
// KeyFileMode, flushes it to disk, renames it into place and flushes the
// directory, so a reader sees the old file or the whole new one and never a
// partial key. A failure leaves the temporary file removed and the old file, if
// any, in place. Errors never contain the key.
func WriteKeyFile(path, key string) (err error) {
	if err := CheckKey([]byte(key)); err != nil {
		return fmt.Errorf("key file %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, KeyDirMode); err != nil {
		return fmt.Errorf("key file %s: %w", path, err)
	}
	tmp, err := os.CreateTemp(dir, ".key-*")
	if err != nil {
		return fmt.Errorf("key file %s: %w", path, err)
	}
	renamed := false
	defer func() {
		if err != nil && !renamed {
			tmp.Close() // closed already on the later paths; that error is not the one to report
			os.Remove(tmp.Name())
		}
	}()
	// CreateTemp makes 0600 already; the mode is set explicitly so the contract
	// does not rest on its default.
	if err = tmp.Chmod(KeyFileMode); err != nil {
		return fmt.Errorf("key file %s: %w", path, err)
	}
	if _, err = tmp.WriteString(key); err != nil {
		return fmt.Errorf("key file %s: write: %w", path, err)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("key file %s: sync: %w", path, err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("key file %s: close: %w", path, err)
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("key file %s: %w", path, err)
	}
	renamed = true
	if err = syncDir(dir); err != nil {
		// The new file is in place; only its directory entry's durability is unknown.
		return fmt.Errorf("key file %s: sync directory: %w", path, err)
	}
	return nil
}

// syncDir flushes a directory's entries to disk.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	return errors.Join(syncErr, closeErr)
}

// ReadKeyFile reads a key file and returns the key. It refuses a file that is
// not a regular file (a symlink, a directory), that is not owned by the calling
// user, that has any permission bit for its group or others or is not readable
// by its owner, that is longer than a key, or whose content is not exactly a
// key (CheckKey). It reads at most 65 bytes. Errors never contain the content;
// one for a missing file wraps fs.ErrNotExist.
func ReadKeyFile(path string) (string, error) {
	fail := func(format string, args ...any) (string, error) {
		return "", fmt.Errorf("key file %s: %w", path, fmt.Errorf(format, args...))
	}
	li, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("key file: %w", err)
	}
	if err := checkKeyFileInfo(li, uint32(os.Geteuid())); err != nil {
		return fail("%w", err)
	}
	// Open without following a link, and check the file that was opened is the
	// one that was checked: nothing swapped in between.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", fmt.Errorf("key file: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("key file: %w", err)
	}
	if !os.SameFile(li, fi) {
		return fail("the file changed while it was opened")
	}
	if err := checkKeyFileInfo(fi, uint32(os.Geteuid())); err != nil {
		return fail("%w", err)
	}
	b, err := readKeyBytes(f)
	if err != nil {
		return "", fmt.Errorf("key file %s: read: %w", path, err)
	}
	if err := CheckKey(b); err != nil {
		return fail("%w", err)
	}
	return string(b), nil
}

// checkKeyFileInfo checks what ReadKeyFile requires of the file itself: a
// regular file, owned by euid, readable by its owner and by no one else.
func checkKeyFileInfo(fi fs.FileInfo, euid uint32) error {
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("is %s, not a regular file", fileKind(fi.Mode()))
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("its owner cannot be read on this platform")
	}
	if st.Uid != euid {
		return fmt.Errorf("is owned by user %d, not by the calling user %d", st.Uid, euid)
	}
	perm := fi.Mode().Perm()
	if perm&0o077 != 0 {
		return fmt.Errorf("has mode %04o; no permission bit for the group or others may be set", perm)
	}
	if perm&0o400 == 0 {
		return fmt.Errorf("has mode %04o; its owner cannot read it", perm)
	}
	return nil
}

// fileKind names a file mode's kind for an error message.
func fileKind(m fs.FileMode) string {
	switch {
	case m&fs.ModeSymlink != 0:
		return "a symbolic link"
	case m.IsDir():
		return "a directory"
	default:
		return "not a regular file (" + m.Type().String() + ")"
	}
}

// readKeyBytes reads at most readLimit bytes of r: a key's length and one byte
// more, so a longer input is seen to be longer without being read to its end.
func readKeyBytes(r io.Reader) ([]byte, error) { return io.ReadAll(io.LimitReader(r, readLimit)) }
