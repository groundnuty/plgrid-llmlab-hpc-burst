package front

import (
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A frps that an earlier test binary left running holds the rig's ports and answers /healthz, and
// the next rig on them fails naming the wrong cause: on 12 Nov 2026 every later real-frp rig failed
// with "frps-bulk never posted CloseProxy" (#484). These tests run the front's rigs in a child test
// binary (this binary, on one test, as TestEnvoyBinaryProbe does), so that what the rig does at its
// start is what the test sees, and not a copy of it.

// frpRigChildBase is the base the single-frps rig's child takes: the first of frpRigBases, a block
// this package's rigs own, held by no other test while these run (a package's tests run one after
// another, and the other packages use other blocks).
const frpRigChildBase = 29432

// rigChildBaseEnv names the port base the child's rig takes, and rigChildKindEnv which rig it is.
const (
	rigChildBaseEnv = "HPCBURST_FRONT_RIG_CHILD_BASE"
	rigChildKindEnv = "HPCBURST_FRONT_RIG_CHILD_KIND"
	rigChildTest    = "TestFrontRigChild"
)

// frontRigRecord is what the child writes for its parent: the launcher's record of each frps it
// started (name, PID, start time, boot ID), and the identity of the frpc it started, when it started
// one.
type frontRigRecord struct {
	Frps []launcher.ProcRec `json:"frps"`
	Frpc *testenv.ProcID    `json:"frpc,omitempty"`
}

// TestFrontRigChild is run only as a child of the tests below, and returns at once otherwise. It
// starts the rig named by its kind ("frp": the single frps, and a frpc that logs in through it;
// "bulk": the two frps of the bulk path), writes the records of what it started for its parent, and
// waits to be killed. The 2 minutes only end a child that nobody kills.
func TestFrontRigChild(t *testing.T) {
	if _, ok := testenv.ChildRecordPath(); !ok {
		return
	}
	var rec frontRigRecord
	switch kind := os.Getenv(rigChildKindEnv); kind {
	case "frp":
		base, err := strconv.Atoi(os.Getenv(rigChildBaseEnv))
		if err != nil {
			t.Fatalf("%s: %v", rigChildBaseEnv, err)
		}
		// The rig takes its base from this pool: leave it only the one the parent holds a port of.
		for len(frpRigBases) > 0 {
			<-frpRigBases
		}
		frpRigBases <- base
		r := newFrpRig(t)
		rec.Frps = []launcher.ProcRec{r.proc.Rec()}
		dial, _ := r.serveDialHome(ModeDirect)
		_, sock := r.engine(newKey(t))
		p := r.startFrpc("00000000-0000-4000-8000-000000000484", sock, dial)
		id, err := testenv.IdentityOf(p.cmd.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		rec.Frpc = &id
	case "bulk":
		r := newBulkRig(t)
		for _, p := range r.procs {
			rec.Frps = append(rec.Frps, p.Rec())
		}
	default:
		t.Fatalf("%s is %q, want frp or bulk", rigChildKindEnv, kind)
	}
	testenv.WriteChildRecord(t, rec)
	<-time.After(2 * time.Minute)
	t.Fatal("the child was not killed")
}

// A rig refuses a port that something holds, whichever of its ports it is, before it starts frps,
// and fails naming the port: not with the /healthz of whatever answers there. The holder here
// answers nothing: the case a check of /healthz cannot see. Each case holds one port and runs the
// real rig in a child; the rig's start must fail with that port in its message and no sign that it
// went on to start frps.
func TestFrontRigsRefuseAPortSomethingHolds(t *testing.T) {
	testenv.FrpBinaries(t) // the child needs them; without them this test skips as the rigs do
	one := agentapi.Host{PortBase: frpRigChildBase}
	bulk := agentapi.Host{PortBase: bulkRigBase}
	for _, c := range []struct{ name, kind, base, addr string }{
		{"the frp rig, the plugin's port", "frp", strconv.Itoa(frpRigChildBase), one.FrpsPluginAddr()},
		{"the frp rig, frps's bind port", "frp", strconv.Itoa(frpRigChildBase), one.FrpsBindAddr()},
		{"the frp rig, frps's tcpmux port", "frp", strconv.Itoa(frpRigChildBase), one.FrpsTCPMuxAddr()},
		{"the frp rig, frps's web port", "frp", strconv.Itoa(frpRigChildBase), one.FrpsWebAddr()},
		{"the bulk rig, the first frps's bind port", "bulk", strconv.Itoa(bulkRigBase), bulk.FrpsBindAddr()},
		{"the bulk rig, the bulk frps's bind port", "bulk", strconv.Itoa(bulkRigBase), bulk.FrpsBulkBindAddr()},
		{"the bulk rig, the bulk frps's web port", "bulk", strconv.Itoa(bulkRigBase), bulk.FrpsBulkWebAddr()},
	} {
		t.Run(c.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", c.addr)
			if err != nil {
				t.Fatalf("this test cannot hold %s: %v", c.addr, err)
			}
			defer ln.Close()
			child := testenv.StartChild(t, rigChildTest, rigChildKindEnv+"="+c.kind, rigChildBaseEnv+"="+c.base)
			code, out := child.Wait(2 * time.Minute)
			_, port, _ := net.SplitHostPort(c.addr)
			if code == 0 {
				t.Fatalf("the rig started with %s held:\n%s", c.addr, out)
			}
			if !strings.Contains(out, c.addr) || !strings.Contains(out, "port "+port) {
				t.Errorf("the failure does not name %s and its port %s:\n%s", c.addr, port, out)
			}
			if strings.Contains(out, "never answered /healthz") || strings.Contains(out, "never posted") {
				t.Errorf("the rig went on to start frps, and failed on that:\n%s", out)
			}
		})
	}
}

// The leak itself: a test binary that is killed, as `go test -timeout` and an outer time limit kill
// one, runs no t.Cleanup, and the frps and the frpc it started were left running on the rig's ports.
// Each case runs a real rig in a child, waits for the child's records of what it started (the
// launcher's own for each frps: PID, start time, boot ID; the same three values for a frpc), takes
// each process by a pidfd, which proves it is running and is the recorded one, kills the child
// through ITS pidfd, and requires every process to exit: by the pidfd's readiness, not by a sleep,
// and by the record, never by a name. A process that does not exit is killed through its verified
// pidfd, so a failing run does not leave it for the next one.
func TestAKilledTestBinaryLeavesNoFrontRigProcess(t *testing.T) {
	testenv.FrpBinaries(t)
	for _, c := range []struct {
		name, kind, base string
		frps, frpc       int
	}{
		{"the frp rig: its frps and a frpc", "frp", strconv.Itoa(frpRigChildBase), 1, 1},
		{"the bulk rig: its two frps", "bulk", strconv.Itoa(bulkRigBase), 2, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			child := testenv.StartChild(t, rigChildTest, rigChildKindEnv+"="+c.kind, rigChildBaseEnv+"="+c.base)
			var rec frontRigRecord
			child.WaitRecord(2*time.Minute, &rec)
			ids := map[string]testenv.ProcID{}
			for i, r := range rec.Frps {
				ids["frps "+strconv.Itoa(i+1)] = testenv.ProcID{PID: r.PID, StartTicks: r.StartTicks, BootID: r.BootID}
			}
			if rec.Frpc != nil {
				ids["frpc"] = *rec.Frpc
			}
			wantFrpc := 0
			if rec.Frpc != nil {
				wantFrpc = 1
			}
			if len(rec.Frps) != c.frps || wantFrpc != c.frpc {
				t.Fatalf("the child recorded %d frps and %d frpc, want %d and %d: %+v", len(rec.Frps), wantFrpc, c.frps, c.frpc, rec)
			}
			held := map[string]*testenv.Proc{}
			for name, id := range ids {
				p, err := testenv.OpenProc(id)
				if err != nil {
					t.Fatalf("the child's %s %+v is not running before the kill, so this check could not see a leak: %v", name, id, err)
				}
				defer p.Close()
				if p.WaitExit(0) {
					t.Fatalf("the child's %s %+v had already exited before the kill", name, id)
				}
				held[name] = p
			}
			child.Kill()
			for name, p := range held {
				if !p.WaitExit(20 * time.Second) {
					_ = p.Kill()
					t.Errorf("%s %+v outlived the test binary that started it and was killed", name, ids[name])
				}
			}
		})
	}
}
