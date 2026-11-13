package alpha

// The evidence a failed live run keeps (issue #503; the science agent's ruling, 13 Nov 2026). Row A's live run failed with its stack's
// logs in the test's temporary directory, which the test removed, so the question that mattered (did the controller try to release the
// Allocation, and when) could not be answered from the run. A live row now keeps, on a failure, the stack's own logs (the controller's,
// the front's, the supervisor's and the frps logs) and the agent's log of each job, in a directory of its own (EvidenceDir) of the run's
// record directory, beside the test's own log. They are never posted: they hold paths, logins and job IDs. The record directory is the
// script's private one (mode 700), and every file kept here is mode 600.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// EvidenceDir is the name of the directory, under the run's record directory, that holds a failed run's logs.
const EvidenceDir = "evidence"

// notKept are the stack's logs a failed run does not keep: k3s's own (the control plane's, large, and what the controllers saw of the
// API is in theirs) and its audit log (every API call, larger still).
var notKept = []string{"k3s.log", "audit.log"}

// KeepStackLogs copies the stack's logs, the *.log files of stateDir/logs but those in notKept, into outDir/EvidenceDir, each mode 0600
// in a directory of mode 0700. It returns the names it kept, sorted. A state directory with no logs directory keeps nothing and is no
// error: the stack may never have started. A file that cannot be copied is an error beside the others kept.
func KeepStackLogs(stateDir, outDir string) ([]string, error) {
	logs := filepath.Join(stateDir, "logs")
	entries, err := os.ReadDir(logs)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("the stack's logs directory: %w", err)
	}
	dst := filepath.Join(outDir, EvidenceDir)
	var kept []string
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".log") || slices.Contains(notKept, name) {
			continue
		}
		if err := KeepFile(filepath.Join(logs, name), filepath.Join(dst, name)); err != nil {
			errs = append(errs, fmt.Errorf("keep %s: %w", name, err))
			continue
		}
		kept = append(kept, name)
	}
	slices.Sort(kept)
	return kept, errors.Join(errs...)
}

// KeepFile copies src to dst, creating dst's directory mode 0700 and the file mode 0600, and replacing a file of that name.
func KeepFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); err == nil {
			err = cerr
		}
	}()
	// A file whose mode was wider before (the record directory is private, but a replaced file keeps its old mode).
	if err := out.Chmod(0o600); err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	return err
}

// KeepBytes writes b to outDir/EvidenceDir/name, mode 0600.
func KeepBytes(outDir, name string, b []byte) error {
	dst := filepath.Join(outDir, EvidenceDir)
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dst, name), b, 0o600)
}
