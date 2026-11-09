package hostdial

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The relay's named failures. "relay <channel>" reports each with a fixed
// line on its standard error (RelayMessage), which the controller's log
// carries (sshrun.Classify), so a live run can tell them apart.
var (
	// ErrNoPointer: the job's relay pointer is not in the staging directory:
	// the agent has not started its host-dialed transport yet, or ended.
	ErrNoPointer = errors.New("no relay pointer for the job")
	// ErrNoSocket: the pointer names a directory with no agent socket on this
	// node: the step may not be on the job's batch host, or the agent has not
	// bound its sockets yet.
	ErrNoSocket = errors.New("no agent socket on this node")
	// ErrNotPrivate: the pointer or the directory is not this user's own,
	// mode 600 or 700.
	ErrNotPrivate = errors.New("the relay pointer or directory is not this user's own")
)

// RelayMessage is the fixed line "relay <channel>" writes to standard error
// for err, with no path or name in it: the job ID only.
func RelayMessage(jobID string, err error) string {
	switch {
	case errors.Is(err, ErrNoPointer):
		return "hb-relay: no relay pointer for job " + jobID + ": the agent has not started its relay sockets yet"
	case errors.Is(err, ErrNoSocket):
		return "hb-relay: no agent socket for job " + jobID + " on this node: the step may not be on the job's batch host, or the agent has not bound it yet"
	case errors.Is(err, ErrNotPrivate):
		return "hb-relay: the relay pointer or directory of job " + jobID + " is not this user's own, mode 600 or 700"
	}
	return "hb-relay: the relay of job " + jobID + " failed"
}

// maxPointer bounds the pointer file's size.
const maxPointer = 4096

// MakeRelayDir makes the job's relay directory under root, with an
// unpredictable name (agentapi.RelayDirPrefix and a random rest), mode 700.
func MakeRelayDir(root, jobID string) (string, error) {
	prefix, err := agentapi.RelayDirPrefix(jobID)
	if err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp(root, prefix)
	if err != nil {
		return "", fmt.Errorf("the relay directory: %w", err)
	}
	if err := agentapi.CheckRelayDir(root, jobID, dir); err != nil {
		_ = os.Remove(dir)
		return "", err
	}
	return dir, nil
}

// PublishRelayDir writes dir's path to the job's relay pointer in the staging
// directory (agentapi.RelayPointerPath), mode 600, by a temporary file and a
// rename, making <stagingDir>/relay with mode 700 when it is missing. The
// returned remove deletes the pointer while it still names dir.
func PublishRelayDir(stagingDir, jobID, dir string) (remove func(), err error) {
	ptr, err := agentapi.RelayPointerPath(stagingDir, jobID)
	if err != nil {
		return nil, err
	}
	if err := privateDir(filepath.Dir(ptr), true); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(ptr), "."+jobID+"-*")
	if err != nil {
		return nil, fmt.Errorf("the relay pointer: %w", err)
	}
	_, werr := f.WriteString(dir + "\n")
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Chmod(f.Name(), 0o600)
	}
	if werr == nil {
		werr = os.Rename(f.Name(), ptr)
	}
	if werr != nil {
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("the relay pointer: %w", werr)
	}
	return func() {
		if b, err := os.ReadFile(ptr); err == nil && string(bytes.TrimSpace(b)) == dir {
			_ = os.Remove(ptr)
		}
	}, nil
}

// FindRelayDir is the relay's side: it reads the job's pointer, which must be
// a regular file of this user's, mode 600, and returns the directory it
// names, which must be one the job's agent would make under root
// (agentapi.CheckRelayDir) and this user's, mode 700.
func FindRelayDir(stagingDir, root, jobID string) (string, error) {
	ptr, err := agentapi.RelayPointerPath(stagingDir, jobID)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(ptr, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("%w: %v", ErrNoPointer, err)
	}
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrNotPrivate, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrNoPointer, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0o077 != 0 || !ok || int(st.Uid) != os.Geteuid() {
		return "", fmt.Errorf("%w: the pointer %s", ErrNotPrivate, ptr)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxPointer+1))
	if err != nil || len(b) > maxPointer {
		return "", fmt.Errorf("%w: the pointer cannot be read", ErrNoPointer)
	}
	dir := string(bytes.TrimSpace(b))
	if err := agentapi.CheckRelayDir(root, jobID, dir); err != nil {
		return "", fmt.Errorf("%w: %v", ErrNotPrivate, err)
	}
	if err := privateDir(dir, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("%w: %v", ErrNoSocket, err)
		}
		return "", fmt.Errorf("%w: %v", ErrNotPrivate, err)
	}
	return dir, nil
}
