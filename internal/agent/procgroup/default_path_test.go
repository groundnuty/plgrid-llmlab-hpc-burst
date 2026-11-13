package procgroup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// TestTheDefaultCommandIsUnchanged pins what every production caller gets (#514: the token and bulk
// frpc, the forwards, the workers, Envoy and staging; none sets ExitWithParent): the process built
// is the leader of a new process group and nothing else, with no Pdeathsig, and the 5 s WaitDelay,
// as before ExitWithParent existed. The field changes nothing in what is built: Pdeathsig is added
// only on startDyingWithParent's path. TestExitWithParentKillsTheProcessWithItsStarter's control
// shows the same from outside: a process started without the field outlives its starter.
func TestTheDefaultCommandIsUnchanged(t *testing.T) {
	for _, guard := range []bool{false, true} {
		cmd := Spec{Path: "/bin/true", Args: []string{"x"}, Argv0: "shown", Dir: "/", ExitWithParent: guard}.command()
		if want := (&syscall.SysProcAttr{Setpgid: true}); !reflect.DeepEqual(cmd.SysProcAttr, want) {
			t.Errorf("ExitWithParent %v: SysProcAttr %+v, want only Setpgid", guard, cmd.SysProcAttr)
		}
		if cmd.WaitDelay != 5*time.Second {
			t.Errorf("ExitWithParent %v: WaitDelay %s, want 5s", guard, cmd.WaitDelay)
		}
		if !reflect.DeepEqual(cmd.Args, []string{"shown", "x"}) || cmd.Path != "/bin/true" || cmd.Dir != "/" {
			t.Errorf("ExitWithParent %v: Path %q Args %q Dir %q", guard, cmd.Path, cmd.Args, cmd.Dir)
		}
	}
}

// probeOutEnv is where TestStartedProcessProbe, run as the process procgroup started, writes what it
// reads of itself.
const probeOutEnv = "HPCBURST_PROCGROUP_PROBE_OUT"

// probe is what a started process reads of itself: its parent-death signal (prctl
// PR_GET_PDEATHSIG; 0 when none), its PID, its process group and its session.
type probe struct {
	Pdeathsig int `json:"pdeathsig"`
	PID       int `json:"pid"`
	Pgid      int `json:"pgid"`
	Sid       int `json:"sid"`
}

// TestStartedProcessProbe is run only as the process TestTheStartedProcessHasTheDefaultAttributes
// starts through procgroup, and returns at once otherwise. It writes what it reads of itself.
func TestStartedProcessProbe(t *testing.T) {
	out := os.Getenv(probeOutEnv)
	if out == "" {
		return
	}
	var sig int32
	if _, _, e := unix.Syscall(unix.SYS_PRCTL, unix.PR_GET_PDEATHSIG, uintptr(unsafe.Pointer(&sig)), 0); e != 0 {
		t.Fatal(e)
	}
	pgid, err := unix.Getpgid(0)
	if err != nil {
		t.Fatal(err)
	}
	sid, err := unix.Getsid(0)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(probe{Pdeathsig: int(sig), PID: os.Getpid(), Pgid: pgid, Sid: sid})
	if err := os.WriteFile(out, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestTheStartedProcessHasTheDefaultAttributes reads the attributes from the started process itself
// (#514): procgroup starts this test binary, which reports its parent-death signal, process group
// and session. Without ExitWithParent, as every production caller starts its process: no
// parent-death signal, the leader of a process group of its own (Setpgid), in its starter's session
// (no Setsid), as before the field existed. With it: SIGKILL, and the same group and session.
func TestTheStartedProcessHasTheDefaultAttributes(t *testing.T) {
	ourSid, err := unix.Getsid(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		guard bool
		want  int
	}{{false, 0}, {true, int(syscall.SIGKILL)}} {
		out := filepath.Join(t.TempDir(), "probe.json")
		g, err := Start(Spec{Path: os.Args[0], Args: []string{"-test.run=^TestStartedProcessProbe$", "-test.count=1"},
			Env: append(os.Environ(), probeOutEnv+"="+out), ExitWithParent: c.guard}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-g.Done():
		case <-time.After(time.Minute):
			t.Fatalf("ExitWithParent %v: the probe did not end", c.guard)
		}
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("ExitWithParent %v: the probe wrote nothing (exit %d): %v", c.guard, g.ExitCode(), err)
		}
		var p probe
		if err := json.Unmarshal(b, &p); err != nil {
			t.Fatal(err)
		}
		if p.Pdeathsig != c.want {
			t.Errorf("ExitWithParent %v: the started process's parent-death signal is %d, want %d", c.guard, p.Pdeathsig, c.want)
		}
		if p.Pgid != p.PID || p.PID != g.PID() {
			t.Errorf("ExitWithParent %v: pid %d (procgroup says %d), process group %d: want the leader of its own group", c.guard, p.PID, g.PID(), p.Pgid)
		}
		if p.Sid != ourSid {
			t.Errorf("ExitWithParent %v: session %d, want its starter's %d (no Setsid)", c.guard, p.Sid, ourSid)
		}
	}
}
