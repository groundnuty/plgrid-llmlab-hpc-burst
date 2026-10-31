package fake_test

import (
	"context"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

const (
	allocUID  = "alloc-uid-1"
	imageSHA  = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	socketOf0 = "/tmp/fake-job/worker-0.sock"
)

var (
	t0      = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	hex32   = regexp.MustCompile(`^[0-9a-f]{32}$`)
	hex64   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	bgndCtx = context.Background()
)

// clock is the test's time: the agent reads it, the test moves it.
type clock struct{ t time.Time }

func (c *clock) Now() time.Time          { return c.t }
func (c *clock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func caller(id string) agentapi.Caller {
	return agentapi.Caller{
		Namespace:       "ns-example",
		Allocation:      "alloc-example",
		AllocationUID:   allocUID,
		Tag:             "tag-example",
		JobID:           "job-1",
		AgentInstanceID: id,
	}
}

// instance returns a fresh desired Instance with one worker, worker-0, at
// generation 0, and Instance generation 1. mods adjust it.
func instance(mods ...func(*agentapi.DesiredInstance)) agentapi.DesiredInstance {
	i := agentapi.DesiredInstance{
		Name:         "inst-example",
		UID:          "inst-uid-1",
		DeploymentID: "dep-example",
		Launch: agentapi.Launch{
			Image:       "registry.example/engine",
			ImageDigest: imageSHA,
			Command:     []string{"engine", "--model", agentapi.ContainerModelDir},
			Checkpoint:  agentapi.Checkpoint{Repository: "org-example/model-example", Revision: "0123456789abcdef0123456789abcdef01234567"},
			GPUs:        1,
		},
		Generation: 1,
		Workers:    []agentapi.DesiredWorker{worker(0, 0)},
	}
	for _, m := range mods {
		m(&i)
	}
	return i
}

func worker(index int, generation int64) agentapi.DesiredWorker {
	name := agentapi.WorkerName(index)
	return agentapi.DesiredWorker{Name: name, Generation: generation, EngineSocket: agentapi.ContainerEngineSocket(name)}
}

func withGeneration(g int64) func(*agentapi.DesiredInstance) {
	return func(i *agentapi.DesiredInstance) { i.Generation = g }
}

func withWorkers(gens ...int64) func(*agentapi.DesiredInstance) {
	return func(i *agentapi.DesiredInstance) {
		i.Workers = nil
		for n, g := range gens {
			i.Workers = append(i.Workers, worker(n, g))
		}
	}
}

func withChecks(c agentapi.Checks) func(*agentapi.DesiredInstance) {
	return func(i *agentapi.DesiredInstance) { i.Checks = &c }
}

func withKeyStored(i *agentapi.DesiredInstance) { i.KeyStored = true }

func run(i agentapi.DesiredInstance) agentapi.DesiredState {
	return agentapi.DesiredState{Action: agentapi.ActionRun, Instance: &i}
}

// rig is an Agent on a Backend with a clock the test moves.
type rig struct {
	t   *testing.T
	b   *fake.Backend
	a   *fake.Agent
	clk *clock
}

func newRig(t *testing.T, e fake.Engine) *rig {
	t.Helper()
	return newRigOn(t, e, func(b *fake.Backend) agentapi.Backend { return b })
}

// newRigOn puts the Agent on a wrapper of the Backend.
func newRigOn(t *testing.T, e fake.Engine, wrap func(*fake.Backend) agentapi.Backend) *rig {
	t.Helper()
	clk := &clock{t: t0}
	b := fake.NewBackend()
	return &rig{t: t, b: b, clk: clk, a: fake.NewAgent(wrap(b), caller(""), clk.Now, e)}
}

func (r *rig) set(d agentapi.DesiredState) { r.b.SetDesired(allocUID, d) }

func (r *rig) step() {
	r.t.Helper()
	if err := r.a.Step(bgndCtx); err != nil {
		r.t.Fatalf("Step: %v", err)
	}
}

func (r *rig) steps(n int) {
	r.t.Helper()
	for range n {
		r.step()
	}
}

func (r *rig) reports() []agentapi.Report { return r.b.Reports(allocUID) }

// last is the Backend's latest report: what the controller sees.
func (r *rig) last() agentapi.Report {
	r.t.Helper()
	rep, ok := r.b.LastReport(allocUID)
	if !ok {
		r.t.Fatal("the Backend has received no report")
	}
	return rep
}

func (r *rig) lastInstance() agentapi.InstanceReport {
	r.t.Helper()
	rep := r.last()
	if rep.Instance == nil {
		r.t.Fatal("the last report has no Instance")
	}
	return *rep.Instance
}

// workerOf returns the named worker in the Backend's last report.
func (r *rig) workerOf(name string) agentapi.WorkerReport {
	r.t.Helper()
	for _, w := range r.lastInstance().Workers {
		if w.Name == name {
			return w
		}
	}
	r.t.Fatalf("the last report has no worker %q", name)
	return agentapi.WorkerReport{}
}

func (r *rig) calls() (register, desired, report int) {
	return r.b.Calls(fake.MethodRegister), r.b.Calls(fake.MethodDesired), r.b.Calls(fake.MethodReport)
}

func eq[T any](t *testing.T, what string, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %+v, want %+v", what, got, want)
	}
}

func intp(v int) *int { return &v }
