package launcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// ControllerBinName is the controllers' binary, looked for beside the running hpcburst unless
// --controller-bin names another.
const ControllerBinName = "hpcburst-controller"

// noControllerWarning is printed when up finds no controller binary: the host starts, and nothing
// runs on it.
const noControllerWarning = "warning: no controller binary found (" + ControllerBinName + " beside hpcburst, or --controller-bin): no controllers run"

// superviseStartTimeout bounds the wait for the supervisor's first child.
const superviseStartTimeout = 30 * time.Second

// superviseSettle is how long a child must stay up, once it runs, for the start to count: a child
// that crashes at once (a bad flag, a missing file) is running for a few milliseconds, and the
// record shows that. Without the wait, up could report success for a controller that never ran.
const superviseSettle = 500 * time.Millisecond

// resolveControllerBin finds the controller binary before anything is started. An explicit path
// must be an executable regular file, and is made absolute; the default is ControllerBinName in
// the directory of the running executable, and its absence is not an error: it returns "".
func resolveControllerBin(explicit string) (string, error) {
	path, isExplicit := explicit, explicit != ""
	if !isExplicit {
		self, err := os.Executable()
		if err != nil {
			return "", nil
		}
		path = filepath.Join(filepath.Dir(self), ControllerBinName)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(abs)
	switch {
	case errors.Is(err, os.ErrNotExist) && !isExplicit:
		return "", nil
	case err != nil:
		return "", fmt.Errorf("controller binary: %w", err)
	case !fi.Mode().IsRegular():
		return "", fmt.Errorf("controller binary %s is not a regular file", abs)
	case fi.Mode().Perm()&0o111 == 0:
		return "", fmt.Errorf("controller binary %s is not executable", abs)
	}
	return abs, nil
}

// ensureControllerNamespace creates v1alpha1.ControllerNamespace, where the controllers' Leases
// live. A namespace that exists is fine.
func ensureControllerNamespace(ctx context.Context, cfg *rest.Config) error {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   v1alpha1.ControllerNamespace,
		Labels: map[string]string{"app.kubernetes.io/managed-by": fieldManager},
	}}
	if _, err := cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{FieldManager: fieldManager}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", v1alpha1.ControllerNamespace, err)
	}
	return nil
}

// prepareControllerDirs creates the controllers' directory and the host layout's credentials
// directory with mode 0700 (set explicitly, so a umask or an older directory cannot loosen it), and
// the agent builds directory with mode 0700 when it is absent, so an operator can drop builds
// there; an existing one keeps its mode and contents. It removes the previous supervisor's
// record: a file left by an earlier run must never look like this start's.
func prepareControllerDirs(l layout, ports Ports) error {
	host := l.host(ports)
	for _, d := range []string{l.controllerDir(), host.CredentialsDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(d, 0o700); err != nil {
			return err
		}
	}
	if _, err := os.Stat(host.AgentBuildsDir()); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(host.AgentBuildsDir(), 0o700); err != nil {
			return err
		}
		if err := os.Chmod(host.AgentBuildsDir(), 0o700); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err := os.Remove(l.supervisorFile()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// supervisorArgv is the supervisor's command line: this executable's hidden "supervise"
// subcommand. Tests give their own.
func supervisorArgv(l layout) ([]string, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("find the hpcburst executable to start its supervisor: %w", err)
	}
	return []string{self, "supervise", "--state-dir", l.dir}, nil
}

// startSupervisor starts the supervisor detached (own session, stdin /dev/null, output to
// logs/supervisor.log), records it in state.json at once, and waits until it reports every child
// started. st must carry Controllers. On failure the supervisor, if it was started, is recorded in
// st, so the caller's stop reaches it.
func startSupervisor(ctx context.Context, l layout, st *State, argv []string, timeout time.Duration, log io.Writer) error {
	if st.Controllers == nil {
		return errors.New("no controllers are configured")
	}
	// The supervisor reads its configuration from state.json.
	if err := l.saveState(st); err != nil {
		return err
	}
	if err := prepareControllerDirs(l, st.Ports); err != nil {
		return err
	}
	lf, err := os.OpenFile(l.supervisorLog(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	cmd := &exec.Cmd{Path: argv[0], Args: argv, Env: os.Environ(), Dir: l.dir, Stdout: lf, Stderr: lf}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	h, err := startProc(supervisorName, cmd)
	if err != nil {
		return err
	}
	defer h.Close()
	go func() { _ = cmd.Wait() }() // reaps it if it exits while this process lives; otherwise it is reparented
	st.Procs = append(st.Procs, h.rec)
	// Recorded before waiting, as k3s is: an interrupted or crashed start is still cleaned up by down.
	if err := l.saveState(st); err != nil {
		_ = pidfds.signal(h.fd, syscall.SIGKILL)
		st.Procs = st.Procs[:len(st.Procs)-1]
		return err
	}
	logf(log, "started the supervisor as pid %d; waiting for %s to start", h.rec.PID, ControllerBinName)
	tail := func(cause error) error { return fmt.Errorf("%w%s", cause, logTail(l.supervisorLog(), 15)) }
	deadline := time.Now().Add(timeout)
	settled := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if gone, err := h.exited(); err != nil {
			return err
		} else if gone {
			return tail(errors.New("the supervisor exited before its processes started"))
		}
		if ready, err := childrenStarted(l, h.rec); err != nil {
			return tail(err)
		} else if ready {
			if settled {
				return nil
			}
			// Everything runs. Wait, then look again: the checks above then also catch a child
			// that exited meanwhile.
			settled = true
			select {
			case <-time.After(superviseSettle):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		if time.Now().After(deadline) {
			return tail(fmt.Errorf("the supervisor did not report its processes started within %s", timeout))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// childrenStarted reports whether supervisor.json, written by the supervisor recorded as sup, shows
// every process running. A file that names another supervisor is an error: it is not this start's.
// So is a process that has already exited: one that cannot stay up through its own start (a bad
// flag, a missing file) makes the start fail, instead of being restarted behind a successful up.
func childrenStarted(l layout, sup ProcRec) (bool, error) {
	f, err := readSupervisorFile(l)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if f.Supervisor != sup {
		return false, fmt.Errorf("%s was written by another process (pid %d), not by the supervisor started here (pid %d)", l.supervisorFile(), f.Supervisor.PID, sup.PID)
	}
	if len(f.Entries) == 0 {
		return false, nil
	}
	ready := true
	for _, e := range f.Entries {
		if e.LastExit != nil {
			return false, fmt.Errorf("%s exited while it was being started: %s", e.Name, e.LastExit.Detail)
		}
		if e.State != entryRunning || e.Proc == nil {
			ready = false
		}
	}
	return ready, nil
}
