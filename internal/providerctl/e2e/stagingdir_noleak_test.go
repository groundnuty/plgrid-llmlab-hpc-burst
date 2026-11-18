package e2e

// A Provider's stagingDir never appears in a status, an event or a log (HBEP-4, Provider settings; #757, #735). A literal
// cluster path carries a user name, so, like the account, it is account data: it lives in the Provider's spec, which only our
// own control plane holds. The rows drive the controller over the real API server, the real ProfileReader, the real Batch and
// the fake Slurm, through a reconcile to Running, a refused submission and an unreachable login host, and a refused Provider.
// Then they scan what the controller left behind: the Provider's status and metadata, the Allocations whole, the Events the API
// holds, the durable log's messages, and every line the controller logged at any verbosity. The path is distinctive and has a
// unique middle segment, so a truncated or joined form in a message is found too. The scan is shown able to fail: a mutant that
// logs the resolved staging directory once, or lets a Slurm error text through unredacted, fails these rows.

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	eventsv1 "k8s.io/api/events/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

const (
	// leakDir is the Provider's stagingDir in these rows: an invented path of the literal form, with a middle segment no other
	// string holds. leakSegment is not hex, so no UID, hash or generated name can contain it.
	leakDir     = "/invented-stage/zq7f3akx/home/someone"
	leakSegment = "zq7f3akx"
	// leakDotDir is the same with a ".." segment, which ReadProfile refuses.
	leakDotDir = "/invented-stage/zq7f3akx/../home/someone"
)

// leakNeedles are what the rows look for: the whole path and its unique segment.
var leakNeedles = []string{leakDir, leakSegment}

// leakedIn says whether text holds any needle: the whole path or a part of it that no other string holds.
func leakedIn(text string, needles ...string) bool {
	if len(needles) == 0 {
		needles = leakNeedles
	}
	for _, n := range needles {
		if strings.Contains(text, n) {
			return true
		}
	}
	return false
}

// The scan's own controls: it finds the whole path, the segment alone, and a joined form, and passes a clean text.
func TestTheLeakScanFindsWhatItLooksFor(t *testing.T) {
	for _, text := range []string{
		"x " + leakDir + " y", "the file " + leakDir + "/alloc/hb-x/job.out", "only the middle: …/" + leakSegment + "/…",
		"joined: invented-stage" + leakSegment, "truncated: " + leakDir[:20] + leakSegment,
	} {
		if !leakedIn(text) {
			t.Errorf("the scan misses %q", text)
		}
	}
	for _, text := range []string{"", "/invented-stage/other/home/someone", "provider.spec.stagingDir: must not contain a '..' segment"} {
		if leakedIn(text) {
			t.Errorf("the scan flags the clean text %q", text)
		}
	}
}

// logVerbosity is the deepest level the sink keeps: the controller's own (Info and Debug), controller-runtime's (up to 5) and the
// status writer's. Above it is client-go's trace of every HTTP request and response body, which prints each object whole, a
// Provider's spec with its account, grant and stagingDir among them, when an operator runs the controller at -v=8: that is the
// API's object, not a line of ours, and it is no more a leak of the stagingDir than of the account.
const logVerbosity = 5

// logSink keeps every line the controller logs up to logVerbosity.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *logSink) logr() logr.Logger {
	return funcr.New(func(prefix, args string) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.lines = append(s.lines, strings.TrimSpace(prefix+" "+args))
	}, funcr.Options{Verbosity: logVerbosity})
}

func (s *logSink) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

// echoSbatch answers an sbatch with a refusal whose text carries the job's output path, as Slurm's own text can, while armed.
type echoSbatch struct {
	inner slurm.Runner
	armed atomic.Bool
}

var outputWord = regexp.MustCompile(`--output=([^\s'"]+)`)

func (e *echoSbatch) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	if m := outputWord.FindStringSubmatch(cmd.Line); m != nil && strings.HasPrefix(cmd.Line, "sbatch ") && e.armed.Load() {
		return slurm.Result{ExitCode: 1, Stderr: []byte("sbatch: error: Batch job submission failed: Unable to open file '" + m[1] + "'\n")}, nil
	}
	return e.inner.Run(ctx, cmd)
}

func (e *echoSbatch) Generation() uint64 { return e.inner.Generation() }

// withStagingDir gives the rig's Provider a stagingDir of its own, against the profile's "$SCRATCH/stage".
func (r *evRig) withStagingDir(dir string) {
	r.t.Helper()
	var p v1alpha1.Provider
	if err := c.Get(r.ctx, client.ObjectKey{Name: r.prov}, &p); err != nil {
		r.t.Fatal(err)
	}
	p.Spec.StagingDir = dir
	if err := c.Update(r.ctx, &p); err != nil {
		r.t.Fatal(err)
	}
}

// capturing rebuilds the rig's controller with a log that keeps every line (also the one in the context of a pass), over a
// Batch whose runner answers an sbatch with Slurm's text of a refusal while the returned runner is armed.
func (r *evRig) capturing(mods ...func(*providerctl.Options)) (*logSink, *echoSbatch) {
	r.t.Helper()
	sink, echo := &logSink{}, &echoSbatch{inner: r.faults}
	o := r.options()
	o.Log = logging.NewLogrLogger(sink.logr())
	o.NewPlatform = func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
		return slurm.NewBatch(echo, v.Profile, v.Settings, slurm.Options{Clock: r.clock, PollInterval: 100 * time.Millisecond, BusyInterval: 100 * time.Millisecond}), io.NopCloser(nil), nil
	}
	for _, m := range mods {
		m(&o)
	}
	ctl, err := providerctl.New(c, c, scheme(), o)
	if err != nil {
		r.t.Fatal(err)
	}
	r.ctl = ctl
	r.ctx = logr.NewContext(r.ctx, sink.logr())
	return sink, echo
}

// surfaces is everything the controller leaves for a reader: name to text. The Provider's own spec is left out, since it holds
// the path; its status and metadata are in.
func (r *evRig) surfaces(sink *logSink, allocs ...string) map[string]string {
	r.t.Helper()
	out := map[string]string{}
	var p v1alpha1.Provider
	if err := c.Get(r.ctx, client.ObjectKey{Name: r.prov}, &p); err != nil {
		r.t.Fatal(err)
	}
	for name, v := range map[string]any{"the Provider's status": p.Status, "the Provider's metadata": p.ObjectMeta} {
		b, err := yaml.Marshal(v)
		if err != nil {
			r.t.Fatal(err)
		}
		out[name] = string(b)
	}
	for _, n := range allocs {
		a, gone := r.get(n)
		if gone {
			continue
		}
		b, err := yaml.Marshal(a)
		if err != nil {
			r.t.Fatal(err)
		}
		out["the Allocation "+n] = string(b)
	}
	var evs eventsv1.EventList
	if err := c.List(r.ctx, &evs); err != nil {
		r.t.Fatal(err)
	}
	for _, e := range evs.Items {
		if e.Regarding.Name == r.prov || e.Regarding.Namespace == r.ns {
			b, err := yaml.Marshal(e)
			if err != nil {
				r.t.Fatal(err)
			}
			out[fmt.Sprintf("the Event %s/%s", e.Regarding.Name, e.Reason)] += string(b)
		}
	}
	r.idle()
	for _, m := range r.logAll() {
		out["the durable log's "+m.ID] = string(m.Payload)
	}
	out["the controller's log"] = strings.Join(sink.all(), "\n")
	return out
}

func (r *evRig) requireNoLeak(sink *logSink, allocs ...string) {
	r.requireNoNeedle(sink, leakNeedles, allocs...)
}

// requireNoNeedle is requireNoLeak for a path other than leakDir: the needles are its whole and its parts.
func (r *evRig) requireNoNeedle(sink *logSink, needles []string, allocs ...string) {
	r.t.Helper()
	for name, text := range r.surfaces(sink, allocs...) {
		if !leakedIn(text, needles...) {
			continue
		}
		var hits []string
		for _, line := range strings.Split(text, "\n") {
			if leakedIn(line, needles...) && len(hits) < 3 {
				hits = append(hits, line[:min(len(line), 300)])
			}
		}
		r.t.Errorf("%s holds the Provider's stagingDir or its unique segment, in: %q", name, hits)
	}
}

// sbatchWords is the fake Slurm's call words of every sbatch the controller made.
func (r *evRig) sbatchWords() []string {
	r.t.Helper()
	var out []string
	for _, call := range r.sl.Calls() {
		if call.Words[0] == "sbatch" {
			out = append(out, strings.Join(call.Words, " "))
		}
	}
	return out
}

// The Provider's stagingDir is used (the fake Slurm's call log and file system hold it, so the scan below looks for a path that
// is really in the system), and appears in nothing the controller leaves: through a pass to Running, a submission Slurm
// refuses with a text that names the output path, and an unreachable login host.
func TestTheProvidersStagingDirNeverLeaks(t *testing.T) {
	r := newEvRig(t, rigOpts{})
	r.withStagingDir(leakDir)
	sink, echo := r.capturing()

	// A pass to Running, with the files staged under the Provider's directory.
	r.newAlloc("a", "3h")
	a := r.until("a", "Pending", 12, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationPending })
	if err := r.sl.Start(r.jobID(a)); err != nil {
		t.Fatal(err)
	}
	a = r.until("a", "Running", 5, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationRunning })
	words := r.sbatchWords()
	if len(words) != 1 || !strings.Contains(words[0], "--output="+leakDir+"/alloc/") {
		t.Fatalf("the sbatch words %q do not name the Provider's directory: the path is not in the system, and the scan would prove nothing", words)
	}
	staged := false
	for p := range r.sl.Files() {
		staged = staged || strings.HasPrefix(p, leakDir+"/agent/")
	}
	if !staged {
		t.Fatal("no file was staged under the Provider's directory: the scan would prove nothing")
	}

	// A submission Slurm refuses, with its text naming the output path.
	echo.armed.Store(true)
	r.newAlloc("b", "3h")
	b := r.until("b", "a refusal in the status", 12, func(a *v1alpha1.Allocation) bool {
		return strings.Contains(a.Status.AtProvider.Reason, "Batch job submission failed")
	})
	echo.armed.Store(false)

	// The login host cannot be reached: every command fails before it starts.
	r.faults.fail("", -1, fmt.Errorf("%w: the connection was refused", slurm.ErrNotStarted))
	r.passes("a", 3)
	r.passes("b", 2)
	if err := r.ctl.SyncStatus(r.ctx); err != nil {
		t.Logf("SyncStatus with the host unreachable: %v", err)
	}
	r.faults.clear()

	// Positive controls: the failures reached what the scan reads, the log has the reconcile's own lines, and the API holds the
	// failure's events.
	if lines := sink.all(); len(lines) == 0 {
		t.Fatal("the controller logged nothing at all: the log capture is not the real stream")
	}
	// Slurm's text named the output path, and the status holds it redacted: the path reached the code that must keep it out.
	if reason := b.Status.AtProvider.Reason; !strings.Contains(reason, "Unable to open file '<path>'") {
		t.Fatalf("status.atProvider.reason %q is not Slurm's refusal with its path redacted: the injected text did not reach the code, and the scan would prove nothing", reason)
	}
	r.waitEvents(a, want{"started", "JobSubmitted"}, want{"ready", "JobStarted"})
	r.requireNoLeak(sink, "a", "b")
}

// dotDotProvider is a client.Reader that serves the Provider with a stagingDir holding a ".." segment. The API server refuses
// such a value at admission (the CEL rule of providers:$.spec.stagingDir#0), so the row reads the Provider through this wrapper,
// as ReadProfile would read a stored object that predates the rule: it is never trusted blindly.
type dotDotProvider struct {
	client.Reader
	dir string
}

func (d dotDotProvider) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := d.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if p, ok := obj.(*v1alpha1.Provider); ok {
		p.Spec.StagingDir = d.dir
	}
	return nil
}

// A Provider whose stagingDir ReadProfile refuses (a ".." segment, which admission refuses first and which a stored object may
// still hold) has its reason in the Allocation's status and its event, and that text names the field and never the value.
func TestTheRefusalOfAStagingDirNeverEchoesIt(t *testing.T) {
	r := newEvRig(t, rigOpts{})
	sink, _ := r.capturing(func(o *providerctl.Options) {
		o.Profiles = &slurm.ProfileReader{Reader: dotDotProvider{Reader: c, dir: leakDotDir}, Home: r.home}
	})
	r.newAlloc("a", "3h")
	a := r.until("a", "Refused", 4, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationRefused })
	if reason := a.Status.AtProvider.Reason; !strings.Contains(reason, "provider.spec.stagingDir") {
		t.Fatalf("status.atProvider.reason %q does not name provider.spec.stagingDir: the refusal did not reach the status, and the scan would prove nothing", reason)
	}
	r.waitEvents(a, want{"failing", "AllocationRefused"})
	r.requireNoLeak(sink, "a")
}

// A staging directory with a colon (the pattern allows ":", "=" and ","), under a submission Slurm refuses with a text that
// echoes the output path: the status holds no part of the path, neither before the colon nor after it (#762, Astra's review).
// The text reaches the lifecycle controller's SubmissionRejected event from this status, so the Allocation's status is the
// source the row checks, with the events the API holds about the Allocation and the Provider.
func TestAStagingDirWithAColonNeverLeaksThroughSlurmText(t *testing.T) {
	const colonDir = "/invented-stage/zq7f3akx:alicexw8m4/run"
	needles := []string{colonDir, "zq7f3akx", "alicexw8m4"}
	r := newEvRig(t, rigOpts{})
	r.withStagingDir(colonDir)
	sink, echo := r.capturing()
	echo.armed.Store(true)
	r.newAlloc("b", "3h")
	b := r.until("b", "a refusal in the status", 12, func(a *v1alpha1.Allocation) bool {
		return strings.Contains(a.Status.AtProvider.Reason, "Batch job submission failed")
	})
	// Slurm's text named the output path, and the status holds it redacted: the injected text reached the code that must keep it out.
	if reason := b.Status.AtProvider.Reason; !strings.Contains(reason, "Unable to open file '<path>'") {
		t.Fatalf("status.atProvider.reason %q is not Slurm's refusal with its path redacted: the injected text did not reach the code, and the scan would prove nothing", reason)
	}
	r.requireNoNeedle(sink, needles, "b")
}
