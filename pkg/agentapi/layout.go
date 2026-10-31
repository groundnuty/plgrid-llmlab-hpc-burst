package agentapi

import (
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// The file layout the provider, the agent and the controller share.

// ---- on the platform, relative to the profile's resolved stagingDir (HBEP-4, Submission) ----

// AgentBinaryName is the agent binary's file name wherever it is staged.
const AgentBinaryName = "hb-agent"

// StagedAgentPath is where the provider stages the agent build with this
// SHA-256 (hex): agent/<digest>/hb-agent. The bootstrap checks it against the
// Allocation's agentDigest before running it.
func StagedAgentPath(digest string) string { return path.Join("agent", digest, AgentBinaryName) }

// AllocationDir is the Allocation's directory, alloc/<tag>; the bootstrap runs
// the agent with its absolute path as the only argument.
func AllocationDir(tag string) string { return path.Join("alloc", tag) }

// StagedCredentialPath is the Allocation's credential, alloc/<tag>/credential,
// mode 600.
func StagedCredentialPath(tag string) string { return path.Join(AllocationDir(tag), "credential") }

// StagedConfigPath is the Allocation's agent.json, mode 600.
func StagedConfigPath(tag string) string { return path.Join(AllocationDir(tag), ConfigFileName) }

// ConfigFileName is agent.json's file name in the Allocation's directory.
const ConfigFileName = "agent.json"

// JobOutputName and JobErrorName are the job's standard output and error
// files in the Allocation's directory (sbatch --output, --error).
const (
	JobOutputName = "job.out"
	JobErrorName  = "job.err"
)

// ImagesDir is where the agent pulls images by digest.
const ImagesDir = "images"

// StagedImagePath is where the agent keeps an image in one format, relative to
// stagingDir: images/<64 hex>.sif, a file, for ImageFormatSIF, and
// images/<64 hex>.sandbox, a directory, for ImageFormatSandbox. digest is
// "sha256:" and 64 hex characters (Launch.ImageDigest); the prefix is dropped,
// because ":" is not safe in every path the agent passes to Apptainer. An empty
// format is ImageFormatSIF.
func StagedImagePath(digest string, format ImageFormat) string {
	hex := strings.TrimPrefix(digest, "sha256:")
	if format == ImageFormatSandbox {
		return path.Join(ImagesDir, hex+".sandbox")
	}
	return path.Join(ImagesDir, hex+".sif")
}

// StagedCheckpointDir is where the agent stages a checkpoint:
// checkpoints/<repository>/<revision>.
func StagedCheckpointDir(repository, revision string) string {
	return path.Join("checkpoints", repository, revision)
}

// DialHomeDir is the dial-home directory relative to the user's home on the
// platform: ~/.hpcburst/dial-home, outside stagingDir, on storage that is not
// purged (HBEP-4, Submission). The operator places the restricted key and the
// pinned host key there once, mode 600; agent.json carries its resolved
// absolute path.
const DialHomeDir = ".hpcburst/dial-home"

// DialHomeKeyFile and DialHomeKnownHostsFile are the two files in DialHomeDir
// that the provider checks before any submission in ssh mode
// (v1alpha1.ConditionDialHomeKeyMissing) and the agent's ssh forward uses.
const (
	DialHomeKeyFile        = "key"
	DialHomeKnownHostsFile = "known_hosts"
)

// ---- inside the engine's container (the paths a launch's Command names) ----

const (
	// ContainerModelDir is where the agent binds the staged checkpoint,
	// read-only.
	ContainerModelDir = "/hpcburst/model"
	// ContainerRunDir is where the agent binds the job's private temporary
	// directory, which holds the engines' Unix sockets.
	ContainerRunDir = "/hpcburst/run"
	// ContainerChatTemplate is where the agent writes the launch's chat
	// template, when it has one.
	ContainerChatTemplate = ContainerRunDir + "/chat-template.jinja"
)

// WorkerName returns the name of an Instance's i-th worker: worker-<i>. It
// equals v1alpha1.WorkerName, which names the worker's restart-generation
// annotation.
func WorkerName(i int) string { return "worker-" + strconv.Itoa(i) }

// ContainerEngineSocket is a worker's Unix socket inside the container:
// /hpcburst/run/<worker>.sock.
func ContainerEngineSocket(worker string) string { return path.Join(ContainerRunDir, worker+".sock") }

// ---- on the control-plane host ----

// AllocationCredentialPath is where the lifecycle controller keeps an
// Allocation's credential: <dir>/allocations/<Allocation UID>, mode 600, where
// dir is the controller's credentials directory (HBEP-7, Provisioning). The
// lifecycle controller writes it once the create has returned the UID; the
// provider waits for it before it submits (docs/development.md), reads it to
// stage it, and the agent API's server reads it to authenticate.
//
// The file holds a key's form (CheckKey) and nothing else. Every writer uses
// WriteKeyFile, which the controller calls with a key from NewKey, and every
// reader ReadKeyFile.
func AllocationCredentialPath(dir, allocationUID string) string {
	return filepath.Join(dir, "allocations", allocationUID)
}

// InstanceKeyPath is where the lifecycle controller keeps an Instance's key:
// <dir>/instances/<Instance UID>, mode 600, which the front reads (HBEP-7,
// Keys). HBEP-7 fixes nothing more than that, so the contract's choice is that
// an Instance's key has the Allocation credential's form (CheckKey), and the
// file is written with WriteKeyFile and read with ReadKeyFile:
//
//   - the agent generates the key with NewKey at Instance start and sends it as
//     InstanceReport.Key until DesiredInstance.KeyStored is true;
//   - the lifecycle controller checks it with CheckKey, writes it with
//     WriteKeyFile and only then sets KeyStored;
//   - the front reads it with ReadKeyFile and sends it to the engine's serving
//     path as "Authorization: Bearer <key>".
func InstanceKeyPath(dir, instanceUID string) string {
	return filepath.Join(dir, "instances", instanceUID)
}

// AgentBuildName is the file name of the agent build for a CPU architecture
// in the controller's agent-builds directory: hb-agent-<cpuArch>, with the
// profile's cpuArch strings (aarch64, x86_64). Its SHA-256 is the
// agentDigest the lifecycle controller sets, and the provider finds the file
// to stage by that digest.
func AgentBuildName(cpuArch string) string { return AgentBinaryName + "-" + cpuArch }

// CheckCredential returns an error unless b is an Allocation credential's exact
// form, which is a key's form (CheckKey): 256 random bits as 64 lowercase hex
// characters, with no newline (HBEP-7, Provisioning). The error never contains b.
func CheckCredential(b []byte) error { return CheckKey(b) }
