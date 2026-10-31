package hubfiles

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

// maxTokenFileBytes bounds the token file; a Hub token is under 100 bytes.
const maxTokenFileBytes = 4096

// ErrTokenFile is wrapped by every error about the token file: missing, too
// open, empty, or not shaped like a token.
var ErrTokenFile = errors.New("hubfiles: unusable token file")

// token is a Hub token. It prints as "[redacted]" under every fmt verb, so a
// log line or an error that formats it by mistake leaks nothing. The one
// place that needs the text calls reveal.
type token string

const redacted = "[redacted]"

func (token) String() string   { return redacted }
func (token) GoString() string { return redacted }

// Format implements fmt.Formatter, which takes precedence over String for
// every verb, including %v, %+v, %#v, %q and %x.
func (token) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redacted) }

func (t token) reveal() string { return string(t) }

// readToken reads the token from path. The mode is checked on the open file
// (fstat), so a file swapped between a check and the read cannot slip past.
// A file whose mode is not 0600 or 0400 is refused (so is one that group or
// others can access, or that is executable), and so is anything that is not a
// regular file. No error message holds file content.
func readToken(path string) (token, error) {
	// O_NONBLOCK: opening a named pipe that has no writer would block until
	// one appears, whatever the caller's context says. A regular file is not
	// affected; the check below refuses anything else.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrTokenFile, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrTokenFile, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%w: %s is not a regular file", ErrTokenFile, path)
	}
	if perm := info.Mode().Perm(); perm != 0o600 && perm != 0o400 {
		return "", fmt.Errorf("%w: %s has mode %04o; it must be 0600 or 0400 (owner only)", ErrTokenFile, path, perm)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxTokenFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("%w: reading %s: %v", ErrTokenFile, path, err)
	}
	if len(data) > maxTokenFileBytes {
		return "", fmt.Errorf("%w: %s is larger than %d bytes", ErrTokenFile, path, maxTokenFileBytes)
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		return "", fmt.Errorf("%w: %s is empty", ErrTokenFile, path)
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= 0x20 || s[i] >= 0x7f {
			// Not echoed: the text may be a secret in the wrong shape.
			return "", fmt.Errorf("%w: %s holds more than one line or characters a token cannot have", ErrTokenFile, path)
		}
	}
	return token(s), nil
}
