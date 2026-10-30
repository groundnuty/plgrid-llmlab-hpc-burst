package launcher

// Adapted from k3sHost in spikes/s3/cmd/s3bench/hosts.go (this repository, PR #36): the flag
// set and the isolation that S3 proved on a machine that also runs a system k3s.

import (
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
}

// env is the whole environment of the k3s process: nothing is inherited. It is what
// "env -i NAME=VALUE ..." gives, set directly on the child so the launcher's PID is k3s's PID.
// HOME, XDG_RUNTIME_DIR and KUBECONFIG are inside the state directory, so k3s never reads or
// writes the invoking user's files, and K3S_CONFIG_FILE is an empty file, so it never reads a
// system config such as /etc/rancher/k3s/config.yaml.
func (s launchSpec) env() []string {
	return []string{
		"PATH=/usr/bin:/bin",
		"HOME=" + s.l.home(),
		"XDG_RUNTIME_DIR=" + s.l.runDir(),
		"KUBECONFIG=" + s.l.home() + "/kubeconfig",
		"K3S_CONFIG_FILE=" + s.l.emptyConfig(),
	}
}

// envKeys are the variable names env() sets, for tests and for status output.
var envKeys = []string{"PATH", "HOME", "XDG_RUNTIME_DIR", "KUBECONFIG", "K3S_CONFIG_FILE"}

// k3sArgs is the argument list after the binary. The components S3 disabled stay disabled:
// the scheduler, cloud controller manager, helm controller, kube-proxy, network policy
// controller and the packaged add-ons. A host without nodes has no use for them, and HBEP-11
// needs none of them (the garbage collector and namespace controllers live in the embedded
// controller manager, which stays on).
func (s launchSpec) k3sArgs() []string {
	return []string{
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
	}
}

// argv is the full command. With a nice value it is wrapped in nice(1), which execs into k3s,
// so the PID is still k3s's.
func (s launchSpec) argv() []string {
	a := append([]string{s.bin.Path}, s.k3sArgs()...)
	if s.nice != 0 {
		a = append([]string{"nice", "-n", strconv.Itoa(s.nice)}, a...)
	}
	return a
}

// prepareIsolation creates the directories and the empty files k3s is pointed at.
func prepareIsolation(l layout) error {
	for _, d := range []string{l.dir, l.home(), l.runDir(), l.dataDir(), l.logDir()} {
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
	return nil
}

// launch starts k3s detached from the launcher (own session, no stdin, output to the log), and
// returns its identity. It does not wait for readiness. The caller records the identity
// before anything else, so that a crash from here on can still be cleaned up by down.
func launch(s launchSpec) (ProcRec, error) {
	if err := prepareIsolation(s.l); err != nil {
		return ProcRec{}, err
	}
	// A kubeconfig from an earlier run would make a not-yet-ready host look ready.
	for _, f := range []string{s.l.adminKubeconfig(), s.l.kubeconfig()} {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			return ProcRec{}, err
		}
	}
	lf, err := os.OpenFile(s.l.logFile(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return ProcRec{}, err
	}
	defer lf.Close()
	argv := s.argv()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = s.env()
	cmd.Dir = s.l.dir
	cmd.Stdout, cmd.Stderr = lf, lf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return ProcRec{}, fmt.Errorf("start k3s: %w", err)
	}
	// Reap the child if it exits while this process lives (a library caller or a test); the
	// command line launcher exits first and k3s is then reparented.
	go func() { _ = cmd.Wait() }()
	rec, err := newRec("k3s", cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		return ProcRec{}, err
	}
	return rec, nil
}
