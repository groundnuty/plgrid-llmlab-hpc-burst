package scenario_test

// The one-host rehearsal of issue #146: the real hpcburst up (k3s, the
// controller, frps and the front, supervised) manages a batch Provider whose
// cluster is the fake Slurm in run mode, reached over real ssh and sshd. The
// real hb-agent starts inside the fake job, with the engine double as its
// engine. Everything is asserted from the API, events, the front's usage
// files and /metrics, never from log lines.
//
// It runs only with HPCBURST_REHEARSAL=1 (hack/rehearsal.sh), so the ordinary
// go test ./... skips it; with that set, a missing piece fails the test.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

//livecheck:none the package's TestMain: it runs the tests, it is not one
func TestMain(m *testing.M) {
	enginedouble.Dispatch() // the fake apptainer and engine roles
	rehearsal.Dispatch()    // the login node's forced command
	start := time.Now()
	code := m.Run()
	if os.Getenv("HPCBURST_REHEARSAL") == "1" {
		// Issue #146, criterion 4: the rehearsal should take under 15 minutes.
		// It is a measured target, not a bound: hack/rehearsal.sh sets
		// -test.timeout well above it, so that a slow run still cleans up.
		took := time.Since(start).Round(time.Second)
		fmt.Printf("the rehearsal's tests took %s (issue #146's target: under %s)\n", took, rehearsalTarget)
		if took > rehearsalTarget {
			fmt.Printf("FINDING: the rehearsal took %s, above issue #146's target of %s\n", took, rehearsalTarget)
		}
	}
	os.Exit(code)
}

// rehearsalTarget is issue #146's time for the whole rehearsal.
const rehearsalTarget = 15 * time.Minute

// ports146: issue #146's block is 29500-29539. hpcburst up takes launcher
// base 29520 (29520-29532); the front's client and dial-home listeners take
// 29500 and 29501.
var ports146 = stack.Ports{Base: 29520, ClientAddr: "127.0.0.1:29500", DialHomeListen: "127.0.0.1:29501"}

// requireRehearsal finds what the rehearsal runs: the four binaries of this
// repo, built by hack/rehearsal.sh into $HPCBURST_REHEARSAL_BIN, k3s from
// $HPCBURST_BIN_DIR, and the pinned frps and frpc from $HPCBURST_FRP_DIR.
func requireRehearsal(t *testing.T) stack.Binaries {
	t.Helper()
	if os.Getenv("HPCBURST_REHEARSAL") != "1" {
		t.Skip("the one-host rehearsal runs the real stack, and only with HPCBURST_REHEARSAL=1 (hack/rehearsal.sh)")
	}
	dir := os.Getenv("HPCBURST_REHEARSAL_BIN")
	b := stack.Binaries{
		HPCBurst: filepath.Join(dir, "hpcburst"), Controller: filepath.Join(dir, "hpcburst-controller"),
		Front: filepath.Join(dir, "hpcburst-front"), Agent: filepath.Join(dir, "hb-agent"),
		K3sDir: os.Getenv("HPCBURST_BIN_DIR"),
	}
	if missing := b.MissingBuilds(); len(missing) > 0 {
		t.Fatalf("HPCBURST_REHEARSAL=1 but these binaries are missing (HPCBURST_REHEARSAL_BIN, HPCBURST_BIN_DIR): %v", missing)
	}
	b.Frps, b.Frpc = testenv.FrpBinaries(t)
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	b.Repo = repo
	return b
}

// cluster is the fake Slurm in run mode and its login node.
type cluster struct {
	plat    *fakeslurm.Platform
	login   *rehearsal.LoginNode
	root    string // RunMode.Root: the cluster's shared file system
	home    string // the cluster user's HOME
	scratch string
	fakes   string // the engine double's fake apptainer
	// cmds records every command the controller ran on the cluster, with
	// the slot holders just before it (cmdlog_test.go).
	cmds *cmdLog
	// gate holds engine starts while armed; nil unless the shape asks for
	// it (enginegate_test.go).
	gate *engineGate
}

// clusterUser is the fake cluster's own user, never this host's.
const clusterUser = "example-user"

// clusterShape is the fake cluster's partitions and QOSes; the zero value is
// #146's: one GPU partition, no QOS.
type clusterShape struct {
	partitions []fakeslurm.Partition
	qos        []fakeslurm.QOS
	// engineGate puts the engine gate (enginegate_test.go) in front of the
	// jobs' apptainer; it holds nothing until armed.
	engineGate bool
}

func (s clusterShape) partitionsOrDefault() []fakeslurm.Partition {
	if len(s.partitions) > 0 {
		return s.partitions
	}
	return []fakeslurm.Partition{{Name: stack.Partition, GPUOnly: true}}
}

func startCluster(t *testing.T, dir string, jobEnv map[string]string, shape clusterShape) *cluster {
	t.Helper()
	c := &cluster{root: filepath.Join(dir, "c"), fakes: filepath.Join(dir, "fk")}
	c.home, c.scratch = filepath.Join(c.root, "home", clusterUser), filepath.Join(c.root, "scratch", clusterUser)
	for _, d := range []string{c.home, c.scratch, c.fakes} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// The login node's directory holds the relay socket, which runs commands
	// on the fake cluster: mode 700.
	if err := os.Mkdir(filepath.Join(dir, "ln"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := enginedouble.Install(c.fakes); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"HPCBURST_APPTAINER": filepath.Join(c.fakes, "apptainer"),
		// No GPU on this host: the agent's GPU probe only warns without one.
		"HPCBURST_NVIDIA_SMI": filepath.Join(c.fakes, "no-nvidia-smi"),
	}
	if shape.engineGate {
		var wrapper string
		c.gate, wrapper = installEngineGate(t, dir, c.fakes)
		env["HPCBURST_APPTAINER"] = wrapper
	}
	for k, v := range jobEnv {
		env[k] = v
	}
	c.plat = fakeslurm.New(slurm.RealClock, fakeslurm.Config{
		Partitions: shape.partitionsOrDefault(), QOS: shape.qos,
		// KillWait 20 s: longer than the agent's own shutdown (the profile's
		// 5 s stopGrace, its 10 s margin and a 5 s last report), shorter than
		// Slurm's 30 s default, which several scancels would spend from the
		// 15 min budget. Slots stay unlimited, so no ended job's slot goes to
		// a phantom job.
		Run: &fakeslurm.RunMode{Root: c.root, JobDir: filepath.Join(dir, "j"), Env: env, KillWait: 20 * time.Second},
	})
	t.Cleanup(func() {
		if err := c.plat.Close(); err != nil {
			t.Errorf("the fake cluster's Close: %v", err)
		}
	})
	c.cmds = &cmdLog{plat: c.plat}
	login, err := rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(dir, "ln"), Platform: c.plat, Around: c.cmds.around,
		Login: fakeslurm.Login{User: clusterUser, Env: map[string]string{"HOME": c.home, "SCRATCH": c.scratch}},
	})
	if err != nil {
		t.Fatalf("the login node: %v", err)
	}
	t.Cleanup(func() {
		if err := login.Close(); err != nil {
			t.Errorf("the login node's Close: %v", err)
		}
	})
	c.login = login
	return c
}
