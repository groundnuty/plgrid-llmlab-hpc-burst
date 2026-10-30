package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

// ---- fake Slurm ----

type fakeSlurm struct {
	dir, state, log string
	env             []string // FAKE_SLURM_* settings shared by every caller
}

func newFakeSlurm(t *testing.T, extra ...string) *fakeSlurm {
	t.Helper()
	dir := filepath.Join(h.work, sanitize(t.Name()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f := &fakeSlurm{dir: dir, state: filepath.Join(dir, "slurm-state.json"), log: filepath.Join(dir, "slurm-calls.jsonl")}
	f.env = append([]string{
		"FAKE_SLURM_STATE=" + f.state,
		"FAKE_SLURM_LOG=" + f.log,
		fmt.Sprintf("FAKE_SLURM_MIN_JOB_AGE=%d", int(minJobAge/time.Second)),
	}, extra...)
	return f
}

func (f *fakeSlurm) cmdEnv(caller string, extra ...string) []string {
	env := append(os.Environ(), "PATH="+h.slurmBin+":"+os.Getenv("PATH"), "FAKE_SLURM_CALLER="+caller)
	env = append(env, f.env...)
	return append(env, extra...)
}

func (f *fakeSlurm) ctl(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command(filepath.Join(h.slurmBin, "fakeslurmctl"), args...)
	cmd.Env = f.cmdEnv("test")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fakeslurmctl %v: %v: %s", args, err, out)
	}
	return string(out)
}

// squeueNames returns the job IDs the fake's squeue lists for a name. The
// test decodes the JSON itself rather than with the provider's parser.
func (f *fakeSlurm) squeueIDs(t *testing.T, name string) []int64 {
	t.Helper()
	cmd := exec.Command(filepath.Join(h.slurmBin, "squeue"), "--me", "--json", "--name="+name)
	cmd.Env = f.cmdEnv("test")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("squeue: %v", err)
	}
	var r struct {
		Jobs []struct {
			JobID int64  `json:"job_id"`
			Name  string `json:"name"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatalf("squeue json: %v", err)
	}
	var ids []int64
	for _, j := range r.Jobs {
		if j.Name == name {
			ids = append(ids, j.JobID)
		}
	}
	return ids
}

type logEntry struct {
	TS     time.Time `json:"ts"`
	Event  string    `json:"event"`
	Cmd    string    `json:"cmd"`
	Argv   []string  `json:"argv"`
	Caller string    `json:"caller"`
	PID    int       `json:"pid"`
	JobID  int64     `json:"job_id"`
	Exit   int       `json:"exit"`
}

func (f *fakeSlurm) entries(t *testing.T) []logEntry {
	t.Helper()
	fh, err := os.Open(f.log)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	var out []logEntry
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var e logEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("fake log line %q: %v", sc.Text(), err)
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// jobName extracts the --job-name value from an sbatch argv.
func jobName(argv []string) string {
	for i, a := range argv {
		switch {
		case strings.HasPrefix(a, "--job-name="):
			return strings.TrimPrefix(a, "--job-name=")
		case (a == "--job-name" || a == "-J") && i+1 < len(argv):
			return argv[i+1]
		}
	}
	return ""
}

// sbatch returns the log events of one kind (sbatch_start or
// sbatch_submitted) for one tag, or for every job when tag is "". Each test
// has its own fake Slurm.
func (f *fakeSlurm) sbatch(t *testing.T, event, tag string) []logEntry {
	var out []logEntry
	for _, e := range f.entries(t) {
		if e.Event == event && (tag == "" || jobName(e.Argv) == tag) {
			out = append(out, e)
		}
	}
	return out
}

func (f *fakeSlurm) calls(t *testing.T, cmd, caller string, after time.Time) int {
	n := 0
	for _, e := range f.entries(t) {
		if e.Event == cmd && (caller == "" || e.Caller == caller) && e.TS.After(after) {
			n++
		}
	}
	return n
}

// ---- provider processes ----

type proc struct {
	name, logPath string
	cmd           *exec.Cmd
	done          chan struct{} // closed when the process has exited
}

type provCfg struct {
	name string
	args []string
	env  []string
}

func startProvider(t *testing.T, ns string, f *fakeSlurm, pc provCfg) *proc {
	t.Helper()
	logPath := filepath.Join(f.dir, pc.name+".log")
	lf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{
		"--kubeconfig=" + h.kubeconfig, "--namespace=" + ns, "--identity=" + pc.name,
		"--poll=" + pollInterval.String(), "--timeout=" + reconcileTO.String(),
		"--inflight-window=" + inflightWindow.String(), "--creation-grace=2s", "--max-backoff=1s",
	}, pc.args...)
	cmd := exec.Command(h.provider, args...)
	cmd.Env = f.cmdEnv(pc.name, pc.env...)
	cmd.Stdout, cmd.Stderr = lf, lf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{name: pc.name, logPath: logPath, cmd: cmd, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); lf.Close(); close(p.done) }()
	t.Logf("started %s pid %d args %v", pc.name, cmd.Process.Pid, pc.args)
	t.Cleanup(func() { p.kill(t) })
	// Ready: workers started, or (with leader election) waiting for the Lease.
	eventually(t, 20*time.Second, func() (bool, string) {
		select {
		case <-p.done:
			b, _ := os.ReadFile(logPath)
			t.Fatalf("%s exited during start-up:\n%s", pc.name, b)
		default:
		}
		return p.logContains(t, "Starting workers") || p.logContains(t, "Attempting to acquire leader lease"), pc.name + " not ready"
	})
	return p
}

// kill SIGKILLs the provider's whole process group (the provider and any
// sbatch it is running) and waits for it to exit.
func (p *proc) kill(t *testing.T) {
	select {
	case <-p.done:
		return
	default:
	}
	_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Errorf("%s (pid %d) did not exit after SIGKILL", p.name, p.cmd.Process.Pid)
	}
	t.Logf("killed %s pid %d", p.name, p.cmd.Process.Pid)
}

func (p *proc) logContains(t *testing.T, s string) bool {
	b, err := os.ReadFile(p.logPath)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Contains(string(b), s)
}

func (p *proc) waitLog(t *testing.T, s string, d time.Duration) {
	t.Helper()
	eventually(t, d, func() (bool, string) { return p.logContains(t, s), p.name + " log lacks " + s })
}

// ---- Kubernetes objects ----

func newNamespace(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(sanitize(t.Name()))
	if len(name) > 50 {
		name = name[:50]
	}
	name = strings.Trim(name, "-") + fmt.Sprintf("-%d", time.Now().UnixNano()%100000)
	if err := h.k8s.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatal(err)
	}
	return name
}

func createAllocation(t *testing.T, ns, name string) client.ObjectKey {
	t.Helper()
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.AllocationSpec{ForProvider: v1alpha1.AllocationParameters{
			Partition: "gpu", TimeLimit: "01:00:00", Wrap: "serve-model",
		}},
	}
	if err := h.k8s.Create(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	return client.ObjectKeyFromObject(a)
}

func getAlloc(t *testing.T, key client.ObjectKey) *v1alpha1.Allocation {
	t.Helper()
	a := &v1alpha1.Allocation{}
	if err := h.k8s.Get(context.Background(), key, a); err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return a
}

func allocGone(t *testing.T, key client.ObjectKey) bool {
	err := h.k8s.Get(context.Background(), key, &v1alpha1.Allocation{})
	if kerrors.IsNotFound(err) {
		return true
	}
	if err != nil {
		t.Fatal(err)
	}
	return false
}

// waitAlloc polls the Allocation until cond holds.
func waitAlloc(t *testing.T, key client.ObjectKey, d time.Duration, what string, cond func(*v1alpha1.Allocation) bool) *v1alpha1.Allocation {
	t.Helper()
	var a *v1alpha1.Allocation
	eventually(t, d, func() (bool, string) {
		a = getAlloc(t, key)
		return cond(a), fmt.Sprintf("%s: status %+v annotations %v", what, a.Status.AtProvider, a.GetAnnotations())
	})
	return a
}

// expectedTag recomputes the tag from the object (namespace/name/uid); the
// provider writes it into the external-name annotation.
func externalName(a *v1alpha1.Allocation) string { return meta.GetExternalName(a) }

func hasAnno(a *v1alpha1.Allocation, k string) bool { _, ok := a.GetAnnotations()[k]; return ok }

func eventually(t *testing.T, d time.Duration, f func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(d)
	var msg string
	for time.Now().Before(deadline) {
		var ok bool
		if ok, msg = f(); ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s: %s", d, msg)
}

type marker struct {
	PID   int   `json:"pid"`
	JobID int64 `json:"job_id"`
}

func waitMarker(t *testing.T, path string, d time.Duration) marker {
	t.Helper()
	var m marker
	eventually(t, d, func() (bool, string) {
		b, err := os.ReadFile(path)
		if err != nil {
			return false, "no marker " + path
		}
		return json.Unmarshal(b, &m) == nil, "bad marker"
	})
	return m
}

func sanitize(s string) string {
	r := strings.NewReplacer("/", "-", "_", "-", ".", "-", " ", "-")
	return r.Replace(s)
}
