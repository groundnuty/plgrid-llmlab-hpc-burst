package fake

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Prerequisites, set on a Controller, make it wait, as the real provider-slurm
// does before Create (internal/providerctl, prerequisites), for what it needs
// from the control-plane host: a configured front, the Allocation's credential
// file, and an agent build with the Allocation's agentDigest. Until all three
// are there, the Allocation shows phase Submitting with a reason that names
// what is missing, and nothing is submitted: no attempt is claimed, no
// Rejected record is written, so the wait is not an attempt. A nil
// Prerequisites (the default) checks none of them, as before.
type Prerequisites struct {
	// Front says whether the launcher's front is configured (HBEP-11).
	Front bool
	// CredentialsDir is the controller's credentials directory. The
	// Allocation's credential is agentapi.AllocationCredentialPath(dir, UID):
	// a regular file of mode 600 that agentapi.ReadKeyFile reads, as the real
	// provider requires; "" means it is never there.
	CredentialsDir string
	// AgentBuildsDir holds agentapi.AgentBuildName(CPUArch), whose sha256
	// must be the Allocation's agentDigest; "" means no build is there.
	AgentBuildsDir string
	// CPUArch is the partition's CPU architecture.
	CPUArch string
}

// Reasons the wait shows; each is a prefix of the phase's reason.
const (
	PrerequisiteNoFront       = "no front is configured, so no job is submitted (HBEP-11)"
	PrerequisiteNoCredential  = "waiting for the Allocation's credential"
	PrerequisiteNoAgentBuild  = "no agent build for cpuArch"
	PrerequisiteAgentMismatch = "the agent build does not have the Allocation's agentDigest"
)

// missing returns why the Allocation cannot be submitted yet, or "".
func (p *Prerequisites) missing(a *v1alpha1.Allocation) string {
	if p == nil {
		return ""
	}
	if !p.Front {
		return PrerequisiteNoFront
	}
	if err := p.credential(a); err != nil {
		return PrerequisiteNoCredential + ": " + err.Error()
	}
	if p.AgentBuildsDir == "" {
		return PrerequisiteNoAgentBuild + " " + p.CPUArch
	}
	b, err := os.ReadFile(filepath.Join(p.AgentBuildsDir, agentapi.AgentBuildName(p.CPUArch)))
	if err != nil {
		return PrerequisiteNoAgentBuild + " " + p.CPUArch
	}
	sum := sha256.Sum256(b)
	if bp := a.Spec.ForProvider.Batch; bp == nil || hex.EncodeToString(sum[:]) != bp.AgentDigest {
		return PrerequisiteAgentMismatch
	}
	return ""
}

func (p *Prerequisites) credential(a *v1alpha1.Allocation) error {
	if p.CredentialsDir == "" {
		return errors.New("no credentials directory is configured")
	}
	path := agentapi.AllocationCredentialPath(p.CredentialsDir, string(a.UID))
	fi, err := os.Lstat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return errors.New("the file does not exist yet")
	case err != nil:
		return errors.New("the file cannot be read")
	case fi.Mode().Type() != 0 || fi.Mode().Perm() != 0o600:
		return errors.New("the file must be a regular file of this user, mode 600, not a link")
	}
	if _, err := agentapi.ReadKeyFile(path); err != nil {
		return errors.New("the file is not a key file")
	}
	return nil
}
