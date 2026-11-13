package procgroup

import (
	"reflect"
	"syscall"
	"testing"
	"time"
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
