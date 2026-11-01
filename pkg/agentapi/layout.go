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

// FrpcBinaryName is frpc's file name wherever it is staged.
const FrpcBinaryName = "frpc"

// StagedFrpcPath is where the provider stages the frpc binary with this
// SHA-256 (hex), relative to the profile's resolved stagingDir:
// frpc/<digest>/frpc, next to the agent's binary (HBEP-4, Submission: Files
// first). frpc is a pinned release binary for the partition's cpuArch, not part
// of the agent's build, so it has its own digest (AgentConfig.FrpcDigest),
// which the agent checks before it runs frpc. AgentConfig.FrpcPath joins it to
// the staging directory.
func StagedFrpcPath(digest string) string { return path.Join("frpc", digest, FrpcBinaryName) }

// AllocationDir is the Allocation's directory, alloc/<tag>; the bootstrap runs
// the agent with its absolute path as the only argument.
func AllocationDir(tag string) string { return path.Join("alloc", tag) }

// StagedCredentialPath is the Allocation's credential, alloc/<tag>/credential,
// mode 600.
func StagedCredentialPath(tag string) string { return path.Join(AllocationDir(tag), "credential") }

// StagedDialHomeCertName is the file name of the launcher's dial-home
// certificate in the Allocation's directory. It is public, and frpc verifies
// the front with it (HBEP-4, Submission: Files first; HBEP-6, frpc in the job).
const StagedDialHomeCertName = "dial-home.crt"

// StagedDialHomeCertPath is the Allocation's copy of the launcher's dial-home
// certificate, alloc/<tag>/dial-home.crt. It is the same certificate as
// Host.AgentCertFile's, the one whose pin agent.json carries
// (AgentConfig.CertSPKISHA256): the agent checks that pin against this file
// before it runs frpc, and refuses to run frpc without the file (HBEP-6,
// frpc in the job).
func StagedDialHomeCertPath(tag string) string {
	return path.Join(AllocationDir(tag), StagedDialHomeCertName)
}

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
	// ContainerCacheDir is where the agent binds the Allocation's cache
	// directory (AllocationCacheDirName); the engine's cache variables point
	// under it.
	ContainerCacheDir = "/hpcburst/cache"
)

// AllocationCacheDirName is the engines' cache directory inside the
// Allocation's directory: compile and JIT caches that worker restarts within
// the Allocation reuse, and that its teardown, which removes the Allocation's
// directory, removes with it (science-agent ruling, 1 Nov 2026, issue #125).
const AllocationCacheDirName = "cache"

// WorkerName returns the name of an Instance's i-th worker: worker-<i>. It
// equals v1alpha1.WorkerName, which names the worker's restart-generation
// annotation.
func WorkerName(i int) string { return "worker-" + strconv.Itoa(i) }

// ContainerEngineSocket is a worker's Unix socket inside the container:
// /hpcburst/run/<worker>.sock.
func ContainerEngineSocket(worker string) string { return path.Join(ContainerRunDir, worker+".sock") }

// ---- on the control-plane host ----

// Host is the control-plane host's runtime layout: the paths and the port that
// several components share. The launcher (hpcburst up) owns the state
// directory and the loopback port block, and passes both to the controller
// binary and the front, which each fill a Host from them (docs/development.md,
// Runtime conventions). agent.json follows from it: Mode and Endpoint are the
// front the launcher recorded, SSH.User is the user who runs the controller,
// SSH.AgentPort is AgentEndpointPort, and CertSPKISHA256 is PinOf the
// certificate at AgentCertFile.
type Host struct {
	// StateDir is the launcher's state directory.
	StateDir string
	// PortBase is the start of the launcher's loopback port block.
	PortBase int
}

// Names under the launcher's state directory, and the other components' places
// in the port block. The launcher binds base, base+1, base+2 and reserves
// base+5 (internal/launcher, PortsFromBase); base+3 is the agent endpoint's,
// base+7 the dial-home listener's, and base+8 to base+11 belong to the frp
// server (HBEP-6): the lifecycle controller binds base+8, and frps, which the
// launcher starts, binds base+9 to base+11.
const (
	// CredentialsDirName is the controller's credentials directory, mode
	// 0700 (KeyDirMode), which holds allocations/ and instances/.
	CredentialsDirName = "credentials"
	// AgentBuildsDirName holds the agent builds, AgentBuildName files.
	AgentBuildsDirName = "agent-builds"
	// AgentEndpointDirName holds the agent endpoint's certificate and key,
	// which the launcher creates.
	AgentEndpointDirName = "agent-endpoint"
	// AgentCertFileName and AgentKeyFileName are the certificate (PEM) and
	// its private key (PEM, mode 0600).
	AgentCertFileName = "tls.crt"
	AgentKeyFileName  = "tls.key"
	// AgentEndpointPortOffset is the agent endpoint's port, relative to the
	// launcher's port base.
	AgentEndpointPortOffset = 3
	// DialHomePortOffset is the front's dial-home listener's default port in
	// ssh dial-home mode, relative to the launcher's port base
	// (`@hpc-burst-science-agent`, 31 Oct 2026). Each platform's restricted
	// VM key permits forwards to exactly this port and the agent endpoint's,
	// so a launcher started with another port base needs new keys. In direct
	// mode the dial-home listener's address stays required and explicit,
	// because compute nodes must reach it, and this default does not apply.
	DialHomePortOffset = 7
	// FrpsPluginPortOffset is the port of frps's server plugin, plain HTTP on
	// loopback, relative to the launcher's port base (HBEP-6, Loopback ports;
	// `@hpc-burst-science-agent`, 31 Oct 2026). The lifecycle controller binds
	// it, in its own process, and frps posts its Login, NewProxy and CloseProxy
	// calls to it. It is not the agent endpoint's port, because the restricted
	// key opens that one to every job.
	FrpsPluginPortOffset = 8
	// FrpsBindPortOffset is frps's bind port, where the front's dial-home
	// listener passes frpc's websocket (GET /~!frp).
	FrpsBindPortOffset = 9
	// FrpsTCPMuxPortOffset is frps's tcpmux CONNECT port, where the front
	// sends CONNECT <Instance UID>:80 to reach an Instance.
	FrpsTCPMuxPortOffset = 10
	// FrpsWebPortOffset is frps's web server, whose API lists the proxies
	// (GET /api/proxy/tcpmux). The lifecycle controller reads it with
	// FrpsWebUser and the password in Host.FrpsWebPasswordFile; the launcher's
	// health check uses its unauthenticated /healthz.
	FrpsWebPortOffset = 11
	// ControllerMetricsPortOffset is the lifecycle controller's /metrics,
	// served on loopback by controller-runtime's metrics server, relative to
	// the launcher's port base (HBEP-8, Metrics; HBEP-11). It makes the
	// launcher's block 13 ports, base to base+12.
	ControllerMetricsPortOffset = 12

	// FrpsDirName is the launcher's frps directory under the state directory,
	// which holds the web password file.
	FrpsDirName = "frps"
	// FrpsWebPasswordFileName is the web password file's name in FrpsDirName.
	FrpsWebPasswordFileName = "web-password"
	// FrpsWebUser is the web server's user name (frps's webServer.user); the
	// launcher writes it into frps's configuration and the lifecycle
	// controller sends it as the basic-auth user.
	FrpsWebUser = "hpcburst"
	// FrpsPluginPath is the plugin endpoint's path (frps's httpPlugins path),
	// which the launcher writes into frps's configuration and the lifecycle
	// controller serves.
	FrpsPluginPath = "/frp/v1/handler"
)

// CredentialsDir is <state>/credentials, the dir of AllocationCredentialPath
// and InstanceKeyPath.
func (h Host) CredentialsDir() string { return filepath.Join(h.StateDir, CredentialsDirName) }

// AgentBuildsDir is <state>/agent-builds.
func (h Host) AgentBuildsDir() string { return filepath.Join(h.StateDir, AgentBuildsDirName) }

// AgentCertFile is <state>/agent-endpoint/tls.crt. The agent API's server
// presents it, and so does the front's dial-home listener in direct mode.
func (h Host) AgentCertFile() string {
	return filepath.Join(h.StateDir, AgentEndpointDirName, AgentCertFileName)
}

// AgentKeyFile is <state>/agent-endpoint/tls.key.
func (h Host) AgentKeyFile() string {
	return filepath.Join(h.StateDir, AgentEndpointDirName, AgentKeyFileName)
}

// AgentEndpointPort is the agent endpoint's loopback port: the port base plus
// AgentEndpointPortOffset.
func (h Host) AgentEndpointPort() int { return h.PortBase + AgentEndpointPortOffset }

// AgentEndpointAddr is "127.0.0.1:<AgentEndpointPort>", where the agent API's
// server listens and the front's direct-mode passthrough connects.
func (h Host) AgentEndpointAddr() string {
	return "127.0.0.1:" + strconv.Itoa(h.AgentEndpointPort())
}

// DialHomePort is the dial-home listener's default loopback port in ssh mode:
// the port base plus DialHomePortOffset.
func (h Host) DialHomePort() int { return h.PortBase + DialHomePortOffset }

// DialHomeAddr is "127.0.0.1:<DialHomePort>", the front's default dial-home
// listener in ssh mode, which a job's second ssh forward reaches.
func (h Host) DialHomeAddr() string {
	return "127.0.0.1:" + strconv.Itoa(h.DialHomePort())
}

// FrpsPluginPort is the frps server plugin's loopback port: the port base plus
// FrpsPluginPortOffset.
func (h Host) FrpsPluginPort() int { return h.PortBase + FrpsPluginPortOffset }

// FrpsPluginAddr is "127.0.0.1:<FrpsPluginPort>", where the lifecycle
// controller serves the plugin and frps's httpPlugins addr points.
func (h Host) FrpsPluginAddr() string { return loopback(h.FrpsPluginPort()) }

// FrpsBindPort is frps's bind port: the port base plus FrpsBindPortOffset.
func (h Host) FrpsBindPort() int { return h.PortBase + FrpsBindPortOffset }

// FrpsBindAddr is "127.0.0.1:<FrpsBindPort>", the front's target for frpc's
// websocket.
func (h Host) FrpsBindAddr() string { return loopback(h.FrpsBindPort()) }

// FrpsTCPMuxPort is frps's tcpmux CONNECT port: the port base plus
// FrpsTCPMuxPortOffset.
func (h Host) FrpsTCPMuxPort() int { return h.PortBase + FrpsTCPMuxPortOffset }

// FrpsTCPMuxAddr is "127.0.0.1:<FrpsTCPMuxPort>", the transport address the
// front dials for an Instance's UID.
func (h Host) FrpsTCPMuxAddr() string { return loopback(h.FrpsTCPMuxPort()) }

// FrpsWebPort is frps's web server port: the port base plus FrpsWebPortOffset.
func (h Host) FrpsWebPort() int { return h.PortBase + FrpsWebPortOffset }

// FrpsWebAddr is "127.0.0.1:<FrpsWebPort>", the IPv4 loopback address of frps's
// web server (HBEP-6, Path state).
func (h Host) FrpsWebAddr() string { return loopback(h.FrpsWebPort()) }

// ControllerMetricsPort is the lifecycle controller's /metrics port: the port
// base plus ControllerMetricsPortOffset.
func (h Host) ControllerMetricsPort() int { return h.PortBase + ControllerMetricsPortOffset }

// ControllerMetricsAddr is "127.0.0.1:<ControllerMetricsPort>", the bind
// address of the lifecycle controller's metrics server.
func (h Host) ControllerMetricsAddr() string { return loopback(h.ControllerMetricsPort()) }

// FrpsWebPasswordFile is <state>/frps/web-password, mode 600, which holds the
// password of frps's web server (HBEP-6, frps on the control-plane host, which
// proposes this path). The launcher writes it, with the password in frps's
// configuration file; the lifecycle controller reads it to list the proxies.
//
// HBEP-6 fixes only "random, in a mode-600 file". The contract's choice is the
// form of a key file (CheckKey: 64 lowercase hex characters, no newline), so the
// launcher writes it with NewKey and WriteKeyFile and the controller reads it
// with ReadKeyFile, which refuses another form, mode, owner or a symlink. A
// trailing newline from a shell would otherwise make the controller's password
// differ from frps's.
func (h Host) FrpsWebPasswordFile() string {
	return filepath.Join(h.StateDir, FrpsDirName, FrpsWebPasswordFileName)
}

// loopback is "127.0.0.1:<port>".
func loopback(port int) string { return "127.0.0.1:" + strconv.Itoa(port) }

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
