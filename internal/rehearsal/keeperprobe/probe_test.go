package keeperprobe

// What the probe reads off a process and a report, with no stack: a relay's ssh command line as /proc carries it, and the keeper's report as
// the controller's collector reads it.

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// startWithLastArg starts a real process, bash with a trailing command so that it stays bash, whose last argument is line: the command line
// of a relay's ssh as HBEP-6 gives it (the remote command is the last argument).
func startWithLastArg(t *testing.T, line string) int {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("no bash")
	}
	cmd := exec.Command(bash, "-c", "sleep 60; true", "_", line)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	// Start returns once the exec has begun, and /proc/<pid>/cmdline reads empty until the new image is in place: wait for the line itself.
	stack.WaitFor(t, 10*time.Second, "the process's command line to show its last argument", func() bool {
		b, err := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/cmdline")
		return err == nil && strings.HasSuffix(strings.TrimRight(string(b), "\x00"), line)
	})
	return cmd.Process.Pid
}

func TestRelayLineOfReadsAnSshsRemoteCommand(t *testing.T) {
	base := "srun -u --overlap --jobid=424242 --ntasks=1 hb-agent " + agentapi.RelaySubcommand + " " + string(agentapi.RelayServing)
	for _, c := range []struct {
		name  string
		line  string
		ok    bool
		epoch int64
		seq   int64
	}{
		{"with its fence", base + " --epoch 2147483700 --seq 3", true, 2147483700, 3},
		{"without its fence", base, true, -1, -1},
		{"with the epoch alone", base + " --epoch 9", true, 9, -1},
		{"not srun", "sleep -u --overlap --jobid=424242 --ntasks=1 hb-agent " + agentapi.RelaySubcommand + " " + string(agentapi.RelayServing), false, 0, 0},
		{"another subcommand", "srun -u --overlap --jobid=424242 --ntasks=1 hb-agent not-a-relay serving", false, 0, 0},
		{"too short", "srun -u --overlap --jobid=424242", false, 0, 0},
		{"one word short of a channel", "srun -u --overlap --jobid=424242 --ntasks=1 hb-agent " + agentapi.RelaySubcommand, false, 0, 0},
		{"a dangling flag", base + " --epoch", true, -1, -1},
		{"another ntasks", "srun -u --overlap --jobid=424242 --ntasks=2 hb-agent " + agentapi.RelaySubcommand + " serving", false, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			l, ok := RelayLineOf(startWithLastArg(t, c.line))
			if ok != c.ok {
				t.Fatalf("RelayLineOf ok = %t, want %t (%+v)", ok, c.ok, l)
			}
			if !ok {
				return
			}
			if l.Job != "424242" || l.Ch != agentapi.RelayServing || l.Epoch != c.epoch || l.Seq != c.seq {
				t.Errorf("RelayLineOf = %+v, want job 424242, channel serving, epoch %d, seq %d", l, c.epoch, c.seq)
			}
		})
	}
	if _, ok := RelayLineOf(1 << 30); ok {
		t.Error("RelayLineOf read a process that does not exist")
	}
	if _, ok := RelayLineOf(os.Getpid()); ok {
		t.Error("RelayLineOf read this test as a relay")
	}
}

func TestRelayChildrenAreTheRelaysSshOfAProcess(t *testing.T) {
	// A parent that has two children, one a relay's ssh and one another process.
	line := "srun -u --overlap --jobid=7 --ntasks=1 hb-agent " + agentapi.RelaySubcommand + " " + string(agentapi.RelayControl) + " --epoch 5 --seq 1"
	relay := startWithLastArg(t, line)
	other := startWithLastArg(t, "an ordinary argument")
	kids := RelayChildren(os.Getpid())
	if l, ok := kids[relay]; !ok || l.Job != "7" || l.Ch != agentapi.RelayControl || l.Epoch != 5 {
		t.Errorf("the relay %d is not among this process's relay children: %+v", relay, kids)
	}
	if _, ok := kids[other]; ok {
		t.Errorf("a child that is no relay (%d) is listed as one: %+v", other, kids)
	}
	for pid := range kids {
		if pid == os.Getpid() || strconv.Itoa(pid) == "" {
			t.Errorf("an impossible child %d", pid)
		}
	}
}

func TestProviderPartAndUpRelaysReadTheKeepersReport(t *testing.T) {
	up := func(job string, ch agentapi.RelayChannel, pid int) wire.RelayState {
		return wire.RelayState{JobID: job, Channel: ch, State: wire.StateUp, PID: pid, Epoch: 5, Seq: 2}
	}
	rep := wire.StatusReport{Providers: []wire.ProviderStatus{
		{Provider: "someone-else", Relays: []wire.RelayState{up("1", agentapi.RelayServing, 11)}},
		{Provider: stack.ProviderName, Relays: []wire.RelayState{
			up("1", agentapi.RelayControl, 21), up("1", agentapi.RelayServing, 22), up("2", agentapi.RelayServing, 23),
			{JobID: "1", Channel: agentapi.RelayServing, State: wire.StateUp, PID: 0}, // up with no process: not up
			{JobID: "1", Channel: agentapi.RelayControl, State: "down", PID: 24},      // not up
		}},
	}}
	if got := ProviderPart(rep).Provider; got != stack.ProviderName {
		t.Fatalf("ProviderPart is %q's, want the rehearsal Provider's", got)
	}
	if got := ProviderPart(wire.StatusReport{}); got.Provider != "" {
		t.Errorf("ProviderPart of a report with none = %+v", got)
	}
	states, n := UpRelays(rep, "1")
	if len(states) != 2 || states[agentapi.RelayControl].PID != 21 || states[agentapi.RelayServing].PID != 22 || n[agentapi.RelayControl] != 1 || n[agentapi.RelayServing] != 1 {
		t.Errorf("UpRelays(job 1) = %+v, %+v; want the control and the serving relay, one each, the others not counted", states, n)
	}
	// Two relays up for one channel are counted twice: the rig fails a job with more than one.
	rep.Providers[1].Relays = append(rep.Providers[1].Relays, up("1", agentapi.RelayServing, 25))
	if _, n := UpRelays(rep, "1"); n[agentapi.RelayServing] != 2 {
		t.Errorf("two serving relays up counted %d", n[agentapi.RelayServing])
	}
	if states, _ := UpRelays(rep, "3"); len(states) != 0 {
		t.Errorf("UpRelays of a job with none = %+v", states)
	}
}
