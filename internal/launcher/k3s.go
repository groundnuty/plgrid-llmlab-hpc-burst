package launcher

// Adapted from k3sHost in spikes/s3/cmd/s3bench/hosts.go (this repository, PR #36): the flag
// set and the isolation that S3 proved on a machine that also runs a system k3s.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// launchSpec is everything one k3s start needs.
type launchSpec struct {
	l     layout
	bin   Binary
	ports Ports
	nice  int
	// extraArgs are appended to k3s's arguments. Only tests set them (to start a host whose
	// garbage collector is off); no command-line flag reaches them.
	extraArgs []string
	// controllers and frontRun are the supervision the host is meant to have. startAndWait puts
	// them in the first durable record, so a launcher that dies while it waits for k3s leaves a
	// record whose status says the supervisor is missing, not a healthy host that needs none.
	controllers *ControllerConfig
	frontRun    *FrontRunConfig
	frps        *FrpsRunConfig
	// eventTTL and omitEventTTL are set by tests only. eventTTL replaces DefaultEventTTL, so that a
	// test can see the API server expire an Event within minutes; omitEventTTL leaves the argument
	// out, as the releases before it was added did, to start the host that an upgrade must bring up
	// to date. Neither is reachable from the command line.
	eventTTL     string
	omitEventTTL bool
}

// niceBin is nice(1), named by absolute path so the caller's PATH cannot choose it.
const niceBin = "/usr/bin/nice"

// binFD is the descriptor number the verified k3s binary gets in the child (ExtraFiles[0]). The
// child executes /proc/self/fd/3, so it runs the very file whose bytes the launcher hashed,
// whatever happens to the path in between.
const binFD = 3

// auditPolicy has the API server log every request at the Metadata level (HBEP-11): who did what
// to which object, and never a request or response body, so a Secret's content is never logged.
const auditPolicy = `# Written by hpcburst at every start (HBEP-11): metadata only, never a request or response body.
apiVersion: audit.k8s.io/v1
kind: Policy
omitStages:
  - RequestReceived
rules:
  - level: Metadata
`

// env is the whole environment of the k3s process: nothing is inherited. It is what
// "env -i NAME=VALUE ..." gives, set directly on the child so the launcher's PID is k3s's PID.
// HOME, XDG_RUNTIME_DIR and KUBECONFIG are inside the state directory, so k3s never reads or
// writes the invoking user's files, and K3S_CONFIG_FILE is an empty file, so it never reads a
// system config such as /etc/rancher/k3s/config.yaml. TMPDIR keeps k3s's temporary files (its
// TLS listener writes certificates to a temporary file first) inside the state directory too.
func (s launchSpec) env() []string {
	return []string{
		"PATH=/usr/bin:/bin",
		"HOME=" + s.l.home(),
		"XDG_RUNTIME_DIR=" + s.l.runDir(),
		"KUBECONFIG=" + s.l.home() + "/kubeconfig",
		"K3S_CONFIG_FILE=" + s.l.emptyConfig(),
		"TMPDIR=" + s.l.tmpDir(),
	}
}

// envKeys are the variable names env() sets, for tests and for status output.
var envKeys = []string{"PATH", "HOME", "XDG_RUNTIME_DIR", "KUBECONFIG", "K3S_CONFIG_FILE", "TMPDIR"}

// k3sArgs is the argument list after the binary. The components S3 disabled stay disabled:
// the scheduler, cloud controller manager, helm controller, kube-proxy, network policy
// controller and the packaged add-ons. A host without nodes has no use for them, and HBEP-11
// needs none of them (the garbage collector and namespace controllers live in the embedded
// controller manager, which stays on).
func (s launchSpec) k3sArgs() []string {
	a := []string{
		"server", "--rootless", "--disable-agent",
		"--data-dir", s.l.dataDir(),
		"--write-kubeconfig", s.l.adminKubeconfig(), "--write-kubeconfig-mode", "0600",
		"--private-registry", s.l.emptyRegistry(),
		"--bind-address", "127.0.0.1",
		"--https-listen-port", strconv.Itoa(s.ports.HTTPS),
		"--apiserver-port", strconv.Itoa(s.ports.API),
		"--lb-server-port", strconv.Itoa(s.ports.LB),
		"--kube-controller-manager-arg=secure-port=" + strconv.Itoa(s.ports.KCM),
		"--disable-scheduler", "--disable-cloud-controller", "--disable-kube-proxy", "--disable-network-policy",
		"--disable-helm-controller",
		"--disable=coredns", "--disable=servicelb", "--disable=traefik",
		"--disable=local-storage", "--disable=metrics-server", "--disable=runtimes",
		// The audit log (HBEP-11), bounded: at most four files of 100 MB.
		"--kube-apiserver-arg=audit-policy-file=" + s.l.auditPolicy(),
		"--kube-apiserver-arg=audit-log-path=" + s.l.auditLog(),
		"--kube-apiserver-arg=audit-log-maxsize=100",
		"--kube-apiserver-arg=audit-log-maxbackup=3",
	}
	if ttl := s.eventTTLValue(); ttl != "" {
		// Events are kept seven days, not the API server's hour (HBEP-8, Retention). Every start builds
		// its arguments afresh, so a host that was started without this one has it after the next
		// restart, an upgrade included.
		a = append(a, "--kube-apiserver-arg="+eventTTLKey+"="+ttl)
	}
	return append(a, s.extraArgs...)
}

// execPath is what the child executes: the descriptor of the verified binary.
func execPath() string { return fmt.Sprintf("/proc/self/fd/%d", binFD) }

// argv is the full command. With a nice value it is wrapped in nice(1), which execs into k3s,
// so the PID is still k3s's.
func (s launchSpec) argv() []string {
	a := append([]string{execPath()}, s.k3sArgs()...)
	if s.nice != 0 {
		a = append([]string{niceBin, "-n", strconv.Itoa(s.nice)}, a...)
	}
	return a
}

// prepareIsolation creates the directories and the empty files k3s is pointed at.
func prepareIsolation(l layout) error {
	for _, d := range []string{l.dir, l.home(), l.runDir(), l.dataDir(), l.logDir(), l.tmpDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	for _, f := range []string{l.emptyConfig(), l.emptyRegistry()} {
		// Truncate on every start: the files must stay empty, whatever happened to them.
		if err := os.WriteFile(f, nil, 0o600); err != nil {
			return err
		}
	}
	// Rewritten on every start, so an edited policy never outlives a restart.
	return writeFileAtomic(l.auditPolicy(), []byte(auditPolicy), 0o600)
}

// launch starts k3s detached from the launcher (own session, no stdin, output to the log), and
// returns a handle on it (see startProc for how the identity is pinned). It does not wait for
// readiness. The caller records the identity before anything else, so that a crash from here on
// can still be cleaned up by down.
func launch(s launchSpec) (*procHandle, error) {
	if s.bin.file == nil {
		return nil, errors.New("the k3s binary was not opened and verified")
	}
	if s.nice != 0 {
		if _, err := os.Stat(niceBin); err != nil {
			return nil, fmt.Errorf("--nice needs %s: %w", niceBin, err)
		}
	}
	if err := prepareIsolation(s.l); err != nil {
		return nil, err
	}
	// A kubeconfig from an earlier run would make a not-yet-ready host look ready.
	for _, f := range []string{s.l.adminKubeconfig(), s.l.kubeconfig()} {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	lf, err := os.OpenFile(s.l.logFile(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer lf.Close()
	argv := s.argv()
	cmd := &exec.Cmd{Path: argv[0], Args: argv, Env: s.env(), Dir: s.l.dir, Stdout: lf, Stderr: lf,
		ExtraFiles: []*os.File{s.bin.file}}
	if s.nice == 0 {
		cmd.Args[0] = s.bin.Path // what ps shows; the file executed is still the descriptor
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	h, err := startProc("k3s", cmd)
	if err != nil {
		return nil, err
	}
	// Reap the child if it exits while this process lives (a library caller or a test); the
	// command line launcher exits first and k3s is then reparented.
	go func() { _ = cmd.Wait() }()
	return h, nil
}
