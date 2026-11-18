package agent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// CredentialFileName is the Allocation's credential in its directory
// (agentapi.StagedCredentialPath without the alloc/<tag> prefix).
const CredentialFileName = "credential"

// maxSocketPath is the longest Unix socket path the kernel accepts, less the
// terminating NUL: sockaddr_un's sun_path holds 108 bytes on Linux.
const maxSocketPath = 107

// Job is everything the agent knows at its start: agent.json, the
// Allocation's credential and the scheduler's environment.
type Job struct {
	// AllocDir is the Allocation's directory, the bootstrap's one argument.
	AllocDir string
	// Config is agent.json.
	Config agentapi.AgentConfig
	// Credential is the Allocation's credential, from its mode-600 file. It is
	// sent only in the Authorization header and never logged.
	Credential string
	// JobID is the scheduler's job ID (SLURM_JOB_ID).
	JobID string
	// RestartCount is SLURM_RESTART_COUNT, 0 when unset.
	RestartCount int32
	// TmpDir is the job's private temporary directory's parent: TMPDIR,
	// else /tmp. The agent makes its run directory under it.
	TmpDir string
}

// LoadJob reads the job's settings. getenv is os.Getenv in production. Every
// problem is returned together, and no message holds the credential.
func LoadJob(allocDir string, getenv func(string) string) (Job, error) {
	var errs []error
	j := Job{AllocDir: allocDir}
	if !filepath.IsAbs(allocDir) {
		errs = append(errs, fmt.Errorf("allocation directory %q is not an absolute path", allocDir))
	}
	// The Allocation's directory is under the staging directory, which is account data (HBEP-4, Provider settings; #757): the
	// errors below name no path, and the agent logs them on its standard error, which reaches job.err.
	configPath := filepath.Join(allocDir, agentapi.ConfigFileName)
	b, err := os.ReadFile(configPath)
	if err != nil {
		errs = append(errs, pathless(err, "the agent's configuration file", "its Allocation directory", configPath))
	} else if j.Config, err = agentapi.DecodeAgentConfig(b); err != nil {
		errs = append(errs, err)
	}
	credPath := filepath.Join(allocDir, CredentialFileName)
	if j.Credential, err = agentapi.ReadKeyFile(credPath); err != nil {
		errs = append(errs, pathless(err, "the Allocation's credential", "its Allocation directory", credPath))
	}
	j.JobID = strings.TrimSpace(getenv("SLURM_JOB_ID"))
	if j.JobID == "" {
		errs = append(errs, errors.New("SLURM_JOB_ID is not set: the agent runs inside a Slurm job"))
	}
	if rc := strings.TrimSpace(getenv("SLURM_RESTART_COUNT")); rc != "" {
		n, err := strconv.ParseInt(rc, 10, 32)
		if err != nil || n < 0 {
			errs = append(errs, fmt.Errorf("SLURM_RESTART_COUNT %q is not a count", rc))
		}
		j.RestartCount = int32(n)
	}
	j.TmpDir = getenv("TMPDIR")
	if j.TmpDir == "" {
		j.TmpDir = os.TempDir()
	}
	return j, errors.Join(errs...)
}

// MakeRunDir creates the job's private run directory under tmp, mode 0700,
// and checks that the longest socket path the agent puts in it fits a Unix
// socket address. It holds the engines' sockets, the agent's serving socket,
// the ssh forward's socket and the chat template, and is bound into the
// engine's container at agentapi.ContainerRunDir.
func MakeRunDir(tmp string) (string, error) {
	dir, err := os.MkdirTemp(tmp, "hb-")
	if err != nil {
		return "", fmt.Errorf("run directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", fmt.Errorf("run directory: %w", err)
	}
	// worker-<n>.sock with up to three digits is the longest name.
	if longest := filepath.Join(dir, "worker-999.sock"); len(longest) > maxSocketPath {
		os.Remove(dir)
		return "", fmt.Errorf("run directory %q is too long for a Unix socket path (%d bytes, at most %d): set TMPDIR to a shorter directory", dir, len(longest), maxSocketPath)
	}
	return dir, nil
}

// StagingPath returns a path under the staging directory from agent.json.
func (j Job) StagingPath(rel string) string { return filepath.Join(j.Config.StagingDir, rel) }

// pathlessError is an error whose text names no path. It stays reachable by errors.Is and errors.As.
type pathlessError struct {
	msg string
	err error
}

func (e *pathlessError) Error() string { return e.msg }
func (e *pathlessError) Unwrap() error { return e.err }

// pathless says why reading the named file failed, without its path: a fixed message when the file is missing, and otherwise the
// operating system's own words for the failure (an errno's text) after a fixed one; and any other error with the file's path
// written <file>.
func pathless(err error, what, where, path string) error {
	var en syscall.Errno
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return &pathlessError{msg: what + " is missing in " + where, err: err}
	case errors.As(err, &en):
		return &pathlessError{msg: what + " cannot be read: " + en.Error(), err: err}
	}
	return &pathlessError{msg: what + " cannot be used: " + strings.ReplaceAll(err.Error(), path, "<file>"), err: err}
}
