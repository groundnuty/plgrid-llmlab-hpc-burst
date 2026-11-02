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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
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
// every 30 s, and an 11-minute lease, so that the hour ends before the drain
// start (LatestReady: 11 min less 1 min 45 s less 5 min 5 s, over 4 minutes).
var standInTunnel = Timing{MaxRequestDuration: 75 * time.Second, DrainMargin: 30 * time.Second, HandoverMargin: 30 * time.Second,
	StartTimeout: 4 * time.Minute, TunnelHour: 5*time.Minute + 5*time.Second, Hour: 5 * time.Minute, NearDeadline: time.Minute,
	MinIdleGap: 30 * time.Second, MaxSampleGap: 30 * time.Second}

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
	r.Partition, r.QOS = stack.Partition, ""
	switch {
	case strings.HasPrefix(name, "tunnel-hour-"):
		r.Timing, r.Lease = standInTunnel, 11*time.Minute
	default:
		r.Timing, r.Lease = standInPair, 7*time.Minute
	}
	if name == "alpha-helios-nooverlap" {
		r.QOS = standInQOS
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
	b := standInBinaries(t)
	rehearsal.RequireTools(t)
	dir := stack.ShortDir(t)
	tm := row.Clock()
	stm := stack.Timing{Lease: string(*minutes(row.Lease)), MaxRequest: tm.MaxRequestDuration.String(), DrainMargin: tm.DrainMargin.String(),
		DrainWindow: "10m", HandoverMargin: tm.HandoverMargin.String(), StopGrace: "5s", AgentTimeout: "20s", StartTimeout: tm.StartTimeout.String()}
	// Job IDs of this process's own: the worker fault finds the job's processes by
	// SLURM_JOB_ID, and other agents' fake clusters on this host number from 1001.
	shape := stack.ClusterShape{FirstJobID: 10_000_000 + 8*os.Getpid()}
	arch := stack.CPUArch()
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
	cl := stack.StartCluster(t, dir, shape, nil)
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
	return world{
		name: "stand-in", bins: b, place: func(h *stack.Host) { h.PlaceAgentBuild(arch) },
		dh: stack.DialHome{Mode: "direct", Addr: standInPorts.DialHomeListen}, ports: standInPorts,
		// The controller lists the checkpoint from the Hub double through HF_ENDPOINT, which up passes on to it.
		env:     stack.CleanEnv("HF_ENDPOINT=" + hub.URL),
		objects: []client.Object{prof, prov}, provider: stack.ProviderName, cluster: row.Cluster, md: md, profile: prof.Spec,
		controlDir: ctl, hostLock: filepath.Join(dir, "host.lock"),
	}
}

// runStandIn runs one row's LIVE steps over the stand-in, through Run, and
// requires its verdict to pass.
func runStandIn(t *testing.T, name string) {
	if os.Getenv(envStandIn) != "1" {
		t.Skipf("the stand-in proof of the live steps runs only with %s=1 (internal/alpha, standin_test.go)", envStandIn)
	}
	row := standInRow(t, name)
	cfg := Config{Row: row, StandIn: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: 2 * time.Minute}
	t.Logf("%s: %s", row.Name, cfg.Mode())
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	var st Steps = newLiveRun(t, cfg, standInWorld(t, row))
	ctx, cancel := context.WithTimeout(context.Background(), cfg.PendingMax+20*time.Minute)
	defer cancel()
	failures, err := Run(ctx, cfg, st, rec, t.Logf)
	if err != nil {
		t.Fatalf("%s did not finish: %v", row.Name, err)
	}
	post, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
	t.Logf("%s's text to post:\n%s", row.Name, post)
	if len(failures) > 0 {
		t.Fatalf("%s failed its verdict over the stand-in: %s", row.Name, Summary(failures))
	}
	record, err := os.ReadFile(filepath.Join(cfg.Out, RecordFile))
	if err != nil || !strings.Contains(string(record), "STAND-IN RUN") || strings.Contains(string(record), "LIVE RUN") {
		t.Errorf("the record does not say the run was a stand-in one (%v)", err)
	}
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
