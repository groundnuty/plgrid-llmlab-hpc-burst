package alpha

// The stand-in proof of the live steps (issue #245): the LIVE steps (liveRun),
// over the real stack on this host (hpcburst up: k3s, the controller, frps and
// the front, the real hb-agent in the job with the engine double as its
// engine), against a stand-in cluster: the fake Slurm in run mode behind the
// rehearsal's sshd, reached by the system ssh. Each row's own verdict judges
// what the steps saw, through Run, on a scaled clock (Row.Timing) so that a
// row ends within a 20-minute hold. It proves the wiring, not a cluster.
//
// It runs only with HPCBURST_ALPHA_STANDIN=1, so go test ./... skips it; with
// that set, a missing piece fails the test. It needs what the rehearsal needs
// (hack/rehearsal.sh): the four binaries in HPCBURST_REHEARSAL_BIN, k3s in
// HPCBURST_BIN_DIR, the pinned frp (HPCBURST_FRP_DIR), and OpenSSH. Run each
// test under launcher.lock and a memory cap (docs/development.md). Ports:
// issue #245's block, 29700-29719 (the launcher's 29700-29712, the client
// 29713, the dial-home listener 29714).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// envStandIn is the stand-in proof's gate.
const envStandIn = "HPCBURST_ALPHA_STANDIN"

// standInPorts are issue #245's block.
var standInPorts = stack.Ports{Base: 29700, ClientAddr: "127.0.0.1:29713", DialHomeListen: "127.0.0.1:29714"}

// The stand-in's clocks. The handover and the no-overlap rows: the
// rehearsal's 7-minute lease, a 90 s deadline and a 30 s drainMargin (a 2-minute
// drain window, the profile's 10-minute drainWindow being longer), a 30 s
// handoverMargin and a 4-minute health.startTimeout: a lead of 6 min 30 s. The
// no-overlap row's idle limit is 10 s, and up to a minute counts as short.
var standInPair = Timing{MaxRequestDuration: 90 * time.Second, DrainMargin: 30 * time.Second, HandoverMargin: 30 * time.Second,
	StartTimeout: 4 * time.Minute, IdleLimit: 10 * time.Second, MaxIdleLimit: time.Minute}

// The tunnel row: a 75 s deadline, a 5-minute hour (traffic for 5 min 5 s),
// requests of about a minute near it, a 30 s idle stretch, a look at least
// every 30 s, and a 12-minute lease, so that the hour ends before the drain
// start, which the one-job end puts a minute early (LatestReady: 12 min less
// 1 min 45 s less 5 min 5 s, over 5 minutes). The large requests (issue #224)
// are prompts of 128 KiB from one sender that pauses 250 ms after each reply,
// to keep the stand-in's load on this host light.
var standInTunnel = Timing{MaxRequestDuration: 75 * time.Second, DrainMargin: 30 * time.Second, HandoverMargin: 30 * time.Second,
	StartTimeout: 4 * time.Minute, TunnelHour: 5*time.Minute + 5*time.Second, Hour: 5 * time.Minute, NearDeadline: time.Minute,
	MinIdleGap: 30 * time.Second, MaxSampleGap: 30 * time.Second,
	UploadBody: 128 << 10, UploadStreams: 1, UploadPause: 250 * time.Millisecond}

// standInFrontLabel says where the stand-in's front ran (issue #224): this
// development host, its note saying the run reached no cluster.
const standInFrontLabel = "the development host, outside Cyfronet's network (stand-in run, no cluster)"

// standInTokenDelay is the engine double's time per token: 50 tokens a second
// a stream, above the 20 the tunnel row's streams need.
const standInTokenDelay = 20 * time.Millisecond

// standInQOS is the no-overlap row's Provider QOS on the stand-in cluster, one
// GPU per user as Helios's now; partQOS is the partition's own, 2,000 jobs
// per user as Helios's plgrid (the shape of PR #247's rehearsal part 3).
const (
	standInQOS     = "example-now"
	standInPartQOS = "example-part"
)

// standInRow is the row on the stand-in: the scaled clock and lease, and the
// stand-in cluster's partition and QOS in place of the real ones, which its
// verdict checks the cluster's record against.
func standInRow(t *testing.T, name string) Row {
	r, ok := RowByName(name)
	if !ok {
		t.Fatalf("no row %s", name)
	}
	// A row under a QOS (Helios's now) runs under the stand-in's one-GPU QOS.
	qos := r.QOS
	r.Partition, r.QOS = stack.Partition, ""
	if qos != "" {
		r.QOS = standInQOS
	}
	switch {
	case strings.HasPrefix(name, "tunnel-hour-"):
		r.Timing, r.Lease = standInTunnel, 12*time.Minute
	default:
		r.Timing, r.Lease = standInPair, 7*time.Minute
	}
	return r
}

// standInBinaries are what the stand-in runs, as the rehearsal's harness finds them.
func standInBinaries(t *testing.T) stack.Binaries {
	t.Helper()
	dir := os.Getenv("HPCBURST_REHEARSAL_BIN")
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	b := stack.Binaries{HPCBurst: filepath.Join(dir, "hpcburst"), Controller: filepath.Join(dir, "hpcburst-controller"),
		Front: filepath.Join(dir, "hpcburst-front"), Agent: filepath.Join(dir, "hb-agent"), K3sDir: os.Getenv("HPCBURST_BIN_DIR"), Repo: repo}
	b.Frps, b.Frpc = testenv.FrpBinaries(t)
	if m := b.Missing(); len(m) > 0 {
		t.Fatalf("%s=1 but these binaries are missing (HPCBURST_REHEARSAL_BIN, HPCBURST_BIN_DIR, HPCBURST_FRP_DIR): %v", envStandIn, m)
	}
	return b
}

// standInWorld is the stand-in cluster and the stack's objects for a row.
func standInWorld(t *testing.T, row Row) world {
	t.Helper()
	w, _ := standInWorldShaped(t, row, nil)
	return w
}

// standInDirect is the stand-in's dial-home in direct mode: the front's
// dial-home listener on loopback, which the fake job reaches as a node would.
var standInDirect = stack.DialHome{Mode: "direct", Addr: standInPorts.DialHomeListen}

// standInHostDialed is the stand-in's dial-home in host-dialed mode (HBEP-6,
// Dial-home modes): no address, no key; the controller's relays reach the job
// through the stand-in's login node, over the fake Slurm's srun -u --overlap
// steps, as alpha's host reaches Helios and Athena.
var standInHostDialed = stack.DialHome{Mode: "host-dialed"}

// inMode is a stand-in world in another dial-home mode: host-dialed has no
// dial-home listener for the nodes, so the front's stays at its loopback
// default, base+7.
func inMode(w world, dh stack.DialHome) world {
	w.dh = dh
	if dh.Mode != "direct" {
		w.ports.DialHomeListen = ""
	}
	return w
}

// standInWorldShaped is standInWorld with a change to the cluster's shape, and the cluster.
func standInWorldShaped(t *testing.T, row Row, reshape func(*stack.ClusterShape)) (world, *stack.Cluster) {
	t.Helper()
	b := standInBinaries(t)
	rehearsal.RequireTools(t)
	dir := stack.ShortDir(t)
	tm := row.Clock()
	stm := stack.Timing{Lease: string(*minutes(row.Lease)), MaxRequest: tm.MaxRequestDuration.String(), DrainMargin: tm.DrainMargin.String(),
		DrainWindow: "10m", HandoverMargin: tm.HandoverMargin.String(), StopGrace: "5s", AgentTimeout: "20s", StartTimeout: tm.StartTimeout.String()}
	// Job IDs of this process's own: the worker fault finds the job's processes by
	// SLURM_JOB_ID, and other agents' fake clusters on this host number from 1001.
	shape := stack.ClusterShape{FirstJobID: 10_000_000 + 8*os.Getpid()}
	arch := stack.CPUArch(t)
	prof := stack.BatchProfile(arch, stm)
	prov := stack.BatchProvider("")
	if row.QOS != "" {
		shape.Partitions = []fakeslurm.Partition{{Name: stack.Partition, GPUOnly: true, QOS: standInPartQOS}}
		shape.QOS = []fakeslurm.QOS{{Name: standInPartQOS, MaxSubmitPerUser: fakeslurm.Cap(2000)},
			{Name: row.QOS, MaxSubmitPerUser: fakeslurm.Cap(1), MaxGPUsPerUser: fakeslurm.Cap(1)}}
		prof.Spec.Partitions[0].QOS = standInPartQOS
		prof.Spec.QOS = []v1alpha1.QOSProfile{{Name: standInPartQOS, MaxSubmitPerUser: stack.Ptr(int32(2000))},
			{Name: row.QOS, MaxSubmitPerUser: stack.Ptr(int32(1)), MaxGPUsPerUser: stack.Ptr(int32(1))}}
		prov.Spec.QOS = row.QOS
	}
	if reshape != nil {
		reshape(&shape)
	}
	cl := stack.StartCluster(t, dir, shape, enginedouble.Install, nil)
	creds := filepath.Join(dir, "provider-ssh")
	if err := cl.Login.WriteCredentials(creds); err != nil {
		t.Fatal(err)
	}
	prov.Spec.CredentialsRef = &v1alpha1.CredentialsReference{File: creds}
	ctl := filepath.Join(dir, "ctl")
	if err := os.Mkdir(ctl, 0o700); err != nil {
		t.Fatal(err)
	}
	hub := stack.StartHub(t)
	md := stack.ServedDeployment(stm, v1alpha1.EnvVar{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + standInTokenDelay.String()})
	if strings.HasPrefix(row.Name, "tunnel-hour-") {
		// The live deployment's context (steps_live_test.go), so that a large body (MinLargeBody) fits under it.
		for i, f := range md.Spec.Source.Served.Flags {
			if f.Name == "max-model-len" {
				md.Spec.Source.Served.Flags[i].Value = "32768"
			}
		}
	}
	return world{
		name: "stand-in", bins: b, place: func(h *stack.Host) { h.PlaceAgentBuild(arch) },
		dh: standInDirect, ports: standInPorts,
		// The controller lists the checkpoint from the Hub double through HF_ENDPOINT, which up passes on to it.
		env:     stack.CleanEnv("HF_ENDPOINT=" + hub.URL),
		objects: []client.Object{prof, prov}, provider: stack.ProviderName, cluster: row.Cluster, qos: row.QOS, md: md, profile: prof.Spec,
		controlDir: ctl, hostLock: filepath.Join(dir, "host.lock"), matchEngine: standInEngineMatch(t),
	}, cl
}

// standInEngineMatch tells the stand-in's engine among the job's processes: the fake Apptainer runs the engine
// double as itself, this test binary (os.Executable, through the symlink enginedouble.Install made), with the
// launch's arguments, each bound path rewritten to the host's. Only here may an argument differ from the launch's,
// and only a path for a path with the same text before its first "/" (a flag's name); the program must be this
// binary, and the count of arguments the launch's. A cluster's engine is matched by MatchesCommand alone.
func standInEngineMatch(t *testing.T) func(argv, command []string) bool {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if exe, err = filepath.EvalSymlinks(exe); err != nil {
		t.Fatal(err)
	}
	return func(argv, command []string) bool {
		if len(command) < 2 || len(argv) != len(command) || argv[0] != exe {
			return false
		}
		for i := 1; i < len(command); i++ {
			got, want := argv[i], command[i]
			if got == want {
				continue
			}
			gi, wi := strings.Index(got, "/"), strings.Index(want, "/")
			if gi < 0 || wi < 0 || got[:gi] != want[:wi] {
				return false
			}
		}
		return true
	}
}

// runStandIn runs one row's LIVE steps over the stand-in, through Run, and
// requires its verdict to pass.
func runStandIn(t *testing.T, name string) { runStandInIn(t, name, standInDirect) }

// runStandInIn is runStandIn in the dial-home mode dh.
func runStandInIn(t *testing.T, name string, dh stack.DialHome) {
	if os.Getenv(envStandIn) != "1" {
		t.Skipf("the stand-in proof of the live steps runs only with %s=1 (internal/alpha, standin_test.go)", envStandIn)
	}
	row := standInRow(t, name)
	id, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Row: row, StandIn: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: 2 * time.Minute, RunID: id, FrontLabel: standInFrontLabel,
		DialHomeMode: dh.Mode, DialHomeAddr: dh.Addr}
	t.Logf("%s: %s, dial-home mode %s", row.Name, cfg.Mode(), dh.Mode)
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	var st Steps = newLiveRun(t, cfg, inMode(standInWorld(t, row), dh))
	ctx, cancel := context.WithTimeout(context.Background(), cfg.PendingMax+20*time.Minute)
	defer cancel()
	failures, err := Run(ctx, cfg, st, rec, t.Logf)
	if err != nil {
		t.Fatalf("%s did not finish: %v", row.Name, err)
	}
	post, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
	t.Logf("%s's text to post:\n%s", row.Name, post)
	if want := watchResetsLine(st.WatchResets()); !strings.Contains(string(post), want) {
		t.Errorf("the text to post lacks %q", want)
	}
	if len(failures) > 0 {
		t.Fatalf("%s failed its verdict over the stand-in: %s", row.Name, Summary(failures))
	}
	record, err := os.ReadFile(filepath.Join(cfg.Out, RecordFile))
	if err != nil || !strings.Contains(string(record), "STAND-IN RUN") || strings.Contains(string(record), "LIVE RUN") {
		t.Errorf("the record does not say the run was a stand-in one (%v)", err)
	}
}

// holdSecondJob puts the run's second job on an admin hold as soon as the cluster shows it pending, until stop closes, so that it never
// starts: not when its predecessor's job ends either. The hold is what the scheduler can do to a job any day, and it is the one thing
// faked: everything else is the live steps over the real stack.
func holdSecondJob(t *testing.T, plat *fakeslurm.Platform, prefix string, stop <-chan struct{}) {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
			var ours []fakeslurm.Job
			for _, j := range plat.Jobs() {
				if strings.HasPrefix(j.Name, prefix) {
					ours = append(ours, j)
				}
			}
			if len(ours) >= 2 && ours[1].State == fakeslurm.Pending {
				if err := plat.HoldAdmin(ours[1].ID); err != nil {
					t.Errorf("holding the second job: %v", err)
				}
				return
			}
		}
	}()
}

// secondHeldRun is what one run of a two-job row's LIVE steps over the stand-in, its second job held in the queue, ended with.
type secondHeldRun struct {
	row      Row
	cfg      Config
	cl       *stack.Cluster
	prefix   string
	failures []string
	err      error
}

// standInSecondHeld runs a two-job row's LIVE steps over the stand-in with its second job held in the queue: the first job runs through
// the real stack, the second never starts. wrapAPI, when not nil, wraps the run's API client (liveRun.wrapAPI).
func standInSecondHeld(t *testing.T, name, what string, reshape func(*stack.ClusterShape), wrapAPI func(client.Client) client.Client) secondHeldRun {
	t.Helper()
	if os.Getenv(envStandIn) != "1" {
		t.Skipf("the stand-in proof of the live steps runs only with %s=1 (internal/alpha, standin_test.go)", envStandIn)
	}
	row := standInRow(t, name)
	id, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Row: row, StandIn: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: 2 * time.Minute, RunID: id, FrontLabel: standInFrontLabel}
	t.Logf("%s: %s, its second job held in the queue%s", row.Name, cfg.Mode(), what)
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	w, cl := standInWorldShaped(t, row, reshape)
	stop := make(chan struct{})
	holdSecondJob(t, cl.Plat, TagPrefix(id), stop)
	defer close(stop)
	lr := newLiveRun(t, cfg, w)
	lr.wrapAPI = wrapAPI
	var st Steps = lr
	ctx, cancel := context.WithTimeout(context.Background(), cfg.PendingMax+20*time.Minute)
	defer cancel()
	failures, err := Run(ctx, cfg, st, rec, t.Logf)
	return secondHeldRun{row: row, cfg: cfg, cl: cl, prefix: TagPrefix(id), failures: failures, err: err}
}

// oneRanOneNever checks that one job of the run ran and one never started.
func (s secondHeldRun) oneRanOneNever(t *testing.T) {
	t.Helper()
	var ran, never int
	for _, j := range s.cl.Plat.Jobs() {
		if !strings.HasPrefix(j.Name, s.prefix) {
			continue
		}
		if j.Start.IsZero() {
			never++
		} else {
			ran++
		}
	}
	if ran != 1 || never != 1 {
		t.Errorf("%d jobs of the run ran and %d never started, want one of each", ran, never)
	}
}

// wroteNothing checks that the run wrote no record and no text to post.
func (s secondHeldRun) wroteNothing(t *testing.T, why string) {
	t.Helper()
	for _, f := range []string{PostsFile, RecordFile} {
		if _, err := os.Stat(filepath.Join(s.cfg.Out, f)); err == nil {
			t.Errorf("%s wrote %s", why, f)
		}
	}
}

// runStandInSecondHeld runs a two-job row's LIVE steps over the stand-in with its second job held in the queue (the delta review of
// PR 284, L1, L2 and L3): the first job runs through the real stack, the second never starts, and the row must end NOT RUN, with
// its line saying what was read of ReplacementLate, no record and no text to post.
func runStandInSecondHeld(t *testing.T, name, wantLate string, reshape func(*stack.ClusterShape)) {
	s := standInSecondHeld(t, name, "", reshape, nil)
	if len(s.failures) != 0 || !errors.Is(s.err, ErrSecondNotStarted) || !NotRun(s.row, s.err) {
		t.Fatalf("failures %v, err %v; want the NOT RUN of a second job that never started", s.failures, s.err)
	}
	note := NotRunNote(s.row, s.cfg.PendingMax, s.err)
	t.Logf("the marker would say: %s", strings.TrimSpace(note))
	if want := "ReplacementLate for its deployment: " + wantLate + ";"; !strings.Contains(note, want) {
		t.Errorf("note %q lacks %q", note, want)
	}
	s.wroteNothing(t, "a NOT RUN")
	s.oneRanOneNever(t)
}

// errAllocationList is the failure allocationListFails gives every LIST of Allocations.
var errAllocationList = errors.New("the API server is unavailable (the stand-in's injected failure of a LIST of Allocations)")

// allocationListFails is the run's API client with every LIST of Allocations failing, as a call to the API can fail. On the two-job rows'
// path only their result catch-ups list Allocations (stack.Recorder.CatchUp): the watch has a client of its own, the worker restart's
// catch-up covers the Instances alone, and the rest are reads of one object, of Instances or of events. Everything else passes through.
type allocationListFails struct {
	client.Client
	failed *atomic.Int32
}

func (c allocationListFails) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.AllocationList); ok {
		c.failed.Add(1)
		return errAllocationList
	}
	return c.Client.List(ctx, list, opts...)
}

// runStandInSecondHeldCatchUpFails is runStandInSecondHeld with the row's result catch-up failing, on demand, in the real waiter
// (issue #303): its LIST of Allocations fails, so it cannot tell whether the watch caught up. A run whose second job never started still
// judges its first job's part from the watch, so that is a failure of the run naming the catch-up, with what was read of the second job
// beside it, and never NOT RUN (the science agent, on PR 284). Run ends it before any record or text to post.
func runStandInSecondHeldCatchUpFails(t *testing.T, name string, reshape func(*stack.ClusterShape)) {
	failed := new(atomic.Int32)
	s := standInSecondHeld(t, name, ", and its result catch-up's LIST of Allocations failing", reshape, func(c client.Client) client.Client {
		return allocationListFails{Client: c, failed: failed}
	})
	if failed.Load() == 0 {
		t.Fatalf("no LIST of Allocations was made, so no catch-up failed (failures %v, err %v)", s.failures, s.err)
	}
	if got := rowOutcome(s.row, s.failures, s.err, nil); got != "FAIL" || len(s.failures) != 0 || s.err == nil {
		t.Fatalf("the row ends %s (failures %v, err %v); want a FAIL of the run itself, from its result catch-up", got, s.failures, s.err)
	}
	for _, want := range []string{"result: whether the watch has caught up with the API is not known", "the API could not be listed", errAllocationList.Error(), ErrSecondNotStarted.Error()} {
		if !strings.Contains(s.err.Error(), want) {
			t.Errorf("err = %v, which lacks %q", s.err, want)
		}
	}
	s.wroteNothing(t, "a run that failed its catch-up")
	s.oneRanOneNever(t)
}

// TestStandInHandoverSecondHeld is alpha-athena-handover's live steps with its successor held in the queue, behind the one slot its
// predecessor takes: the predecessor's streams still open, across its drain start, and the controller raises ReplacementLate.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInHandoverSecondHeld(t *testing.T) {
	runStandInSecondHeld(t, "alpha-athena-handover", LateRaised, func(s *stack.ClusterShape) {
		s.Partitions = []fakeslurm.Partition{{Name: stack.Partition, GPUOnly: true, Slots: 1}}
	})
}

// TestStandInNoOverlapSecondHeld is alpha-helios-nooverlap's live steps with its successor held in the queue, past its bound from the
// predecessor's lease end. In no-overlap mode the gap is planned, so the controller does not raise ReplacementLate.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInNoOverlapSecondHeld(t *testing.T) {
	runStandInSecondHeld(t, "alpha-helios-nooverlap", LateNotRaised, nil)
}

// TestStandInHandoverSecondHeldCatchUpFails is TestStandInHandoverSecondHeld with the handover's result catch-up failing: a FAIL of the
// run, where the same run with the catch-up working is NOT RUN (issue #303).
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInHandoverSecondHeldCatchUpFails(t *testing.T) {
	runStandInSecondHeldCatchUpFails(t, "alpha-athena-handover", func(s *stack.ClusterShape) {
		s.Partitions = []fakeslurm.Partition{{Name: stack.Partition, GPUOnly: true, Slots: 1}}
	})
}

// TestStandInNoOverlapSecondHeldCatchUpFails is TestStandInNoOverlapSecondHeld with the no-overlap row's result catch-up failing: a FAIL
// of the run, where the same run with the catch-up working is NOT RUN (issue #303).
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInNoOverlapSecondHeldCatchUpFails(t *testing.T) {
	runStandInSecondHeldCatchUpFails(t, "alpha-helios-nooverlap", nil)
}

// TestStandInHandover is alpha-athena-handover's live steps over the stand-in.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInHandover(t *testing.T) { runStandIn(t, "alpha-athena-handover") }

// TestStandInNoOverlap is alpha-helios-nooverlap's live steps over the stand-in.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInNoOverlap(t *testing.T) { runStandIn(t, "alpha-helios-nooverlap") }

// TestStandInTunnelHour is tunnel-hour-athena's live steps over the stand-in.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInTunnelHour(t *testing.T) { runStandIn(t, "tunnel-hour-athena") }

// TestStandInTunnelHourHelios is tunnel-hour-helios's live steps over the
// stand-in, under its one-GPU QOS: one job in the slot, no replacement.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInTunnelHourHelios(t *testing.T) { runStandIn(t, "tunnel-hour-helios") }

// TestStandInHandoverHostDialed is alpha-athena-handover's live steps over the
// stand-in in host-dialed mode (HBEP-6, Dial-home modes): both jobs' agents
// reach this host only over the controller's relays, so the handover's
// streams and the successor's start cross them.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInHandoverHostDialed(t *testing.T) {
	runStandInIn(t, "alpha-athena-handover", standInHostDialed)
}

// TestStandInNoOverlapHostDialed is alpha-helios-nooverlap's live steps over
// the stand-in in host-dialed mode.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInNoOverlapHostDialed(t *testing.T) {
	runStandInIn(t, "alpha-helios-nooverlap", standInHostDialed)
}

// TestStandInTunnelHourHostDialed is tunnel-hour-athena's live steps over the
// stand-in in host-dialed mode: S1's hour, its path rate measured over the
// serving relay.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInTunnelHourHostDialed(t *testing.T) {
	runStandInIn(t, "tunnel-hour-athena", standInHostDialed)
}
