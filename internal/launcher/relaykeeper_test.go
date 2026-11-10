package launcher

// The relay keeper under the supervisor (relaykeeper.go; HBEP-11, Single-user host; HBEP-6, The
// relay keeper; issue #343, PR 5). The keeper these tests supervise is the real one: the fake
// controller's relaykeeper role runs relaykeeper.Run, with its state file, process lock and socket.
// No relay runs here, so nothing in this file is a relay or stream test: what restarts here is
// the keeper's process and its E, never a claim that a stream survives (HBEP-6: a keeper restart
// cuts every host-dialed stream).

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const firstKeeperE = int64(1)<<31 + 1

// keeperFixture is a ctlFixture in host-dialed mode whose record runs the relay keeper.
func keeperFixture(t *testing.T) *ctlFixture {
	t.Helper()
	f := newCtlFixture(t, Front{Mode: string(agentapi.DialHomeHostDialed)})
	f.st.RelayKeeper = true
	if err := f.l.saveState(f.st); err != nil {
		t.Fatal(err)
	}
	if n := len(f.l.host(f.ports).RelayKeeperSocket()); n > agentapi.MaxRelaySocketPath {
		t.Fatalf("the keeper's socket path is %d bytes, over %d", n, agentapi.MaxRelaySocketPath)
	}
	return f
}

// keeperUp waits for the keeper's entry to run and returns it.
func (f *ctlFixture) keeperUp() EntryRecord {
	f.t.Helper()
	return f.awaitEntry(keeperEntryName, "the keeper to run", func(e EntryRecord) bool { return e.State == entryRunning && e.Proc != nil })
}

// keeperHealth checks the keeper's entry as status does, polling while the keeper is still
// binding its socket, and returns its health once the ping is answered, or the last problems.
func (f *ctlFixture) keeperHealth(e EntryRecord) (EntryHealth, []string) {
	f.t.Helper()
	sup := f.supervisorHandle()
	var eh EntryHealth
	var problems []string
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if eh, problems = checkEntry(sup, sup.rec.PID, e); len(problems) == 0 {
			break
		}
	}
	return eh, problems
}

// sessionOf reads pid's process group and session from /proc while holding a verified handle on
// the recorded process, so a reused PID cannot answer for it.
func sessionOf(t *testing.T, rec ProcRec) (pgrp, sid int) {
	t.Helper()
	h, v, d := openRec(rec)
	if v != Ours {
		t.Fatalf("pid %d: %v: %s", rec.PID, v, d)
	}
	defer h.Close()
	b, err := os.ReadFile("/proc/" + strconv.Itoa(rec.PID) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	if len(fields) < 4 {
		t.Fatalf("/proc/%d/stat: %q", rec.PID, s)
	}
	pgrp, _ = strconv.Atoi(fields[2])
	sid, _ = strconv.Atoi(fields[3])
	if v, d := h.verify(); v != Ours {
		t.Fatalf("pid %d changed while its stat was read: %s", rec.PID, d)
	}
	return pgrp, sid
}

// Pin: without --relay-keeper the supervised set is alpha's, the controller's command line ending
// where it ended before PR 5, and no keeper entry. The positive control is the same record with
// RelayKeeper: the keeper's entry is appended, and the controller's line gains the socket at its
// end.
func TestTheKeeperEntryOnlyWithRelayKeeper(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	ports, err := PortsFromBase(30020)
	if err != nil {
		t.Fatal(err)
	}
	st := &State{Ports: ports, Front: Front{Mode: string(agentapi.DialHomeHostDialed)}, Controllers: &ControllerConfig{Binary: "/opt/example/hpcburst-controller"}}
	alpha, err := st.supervisedSet(l)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--state-dir", l.dir, "--port-base", "30020", "--health-addr", ports.ControllerAddr(), "--front-mode", "host-dialed"}
	if len(alpha) != 1 || !slices.Equal(alpha[0].Args, want) {
		t.Fatalf("the supervised set without the keeper: %+v; want the controller alone, with %q", alpha, want)
	}
	st.RelayKeeper = true
	beta, err := st.supervisedSet(l)
	if err != nil {
		t.Fatal(err)
	}
	sock := l.host(ports).RelayKeeperSocket()
	if len(beta) != 2 || !slices.Equal(beta[0].Args, append(slices.Clone(want), "--relay-keeper-socket", sock)) {
		t.Fatalf("the supervised set with the keeper: %+v; want the controller with the socket, then the keeper", beta)
	}
	k := beta[1]
	if k.Name != keeperEntryName || k.Path != st.Controllers.Binary || !slices.Equal(k.Args, []string{"relaykeeper", "--state-dir", l.dir, "--port-base", "30020"}) ||
		k.HealthSocket != sock || k.Health != "" || !k.Setsid || !k.WaitReady || len(k.Ports) != 0 {
		t.Errorf("the keeper's entry: %+v", k)
	}
	if alpha[0].Setsid || beta[0].Setsid {
		t.Error("the controller runs in a session of its own; only the keeper does")
	}
}

// The supervisor runs the keeper beside the controller:
//   - the controller's command line names the keeper's socket, and the keeper's is the
//     subcommand's;
//   - the keeper is healthy as status checks it: a child of the supervisor, answering a ping with
//     pong{E} within 1 s, E = 2^31 + 1 on its first start, and listening on no port;
//   - it runs in a session and process group of its own (setsid), unlike the controller, which
//     shares the supervisor's;
//   - stopping the supervisor stops it, removes its socket and keeps its state file; supervised
//     again, it answers with the next E (a restart of the keeper's process, with no relay: it
//     shows the record and the health check, never a stream's continuity).
func TestTheSupervisorRunsTheKeeperBesideTheController(t *testing.T) {
	f := keeperFixture(t)
	stop := f.supervise(SuperviseOptions{})
	ctl := f.awaitEntry(controllerEntryName, "the controller to run", func(e EntryRecord) bool { return e.State == entryRunning && e.Proc != nil })
	k := f.keeperUp()
	h := f.l.host(f.ports)
	if got := f.argvOf(ctl.Proc.PID); !slices.Contains(got, "--relay-keeper-socket") || got[len(got)-1] != h.RelayKeeperSocket() {
		t.Errorf("the controller's command line %q: want --relay-keeper-socket %s", got, h.RelayKeeperSocket())
	}
	if got, want := f.argvOf(k.Proc.PID), []string{"relaykeeper", "--state-dir", f.l.dir, "--port-base", strconv.Itoa(f.base)}; !slices.Equal(got, want) {
		t.Errorf("the keeper's command line %q, want %q", got, want)
	}
	eh, problems := f.keeperHealth(k)
	if len(problems) != 0 || !eh.Running || eh.PingOK == nil || !*eh.PingOK || eh.KeeperEpoch != firstKeeperE || eh.HealthSocket != h.RelayKeeperSocket() {
		t.Fatalf("the keeper's health: %+v, problems %v; want running, the ping answered with E %d", eh, problems, firstKeeperE)
	}
	pgrp, sid := sessionOf(t, *k.Proc)
	if sid != k.Proc.PID || pgrp != k.Proc.PID {
		t.Errorf("the keeper pid %d is in process group %d and session %d, want its own", k.Proc.PID, pgrp, sid)
	}
	if cpg, csid := sessionOf(t, *ctl.Proc); csid == ctl.Proc.PID || cpg == ctl.Proc.PID {
		t.Errorf("the controller pid %d leads its own group %d or session %d: only the keeper does", ctl.Proc.PID, cpg, csid)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(h.RelayKeeperSocket()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the keeper's socket after the stop: %v, want it removed", err)
	}
	ks, err := relaykeeper.LoadState(relaykeeper.StatePath(f.l.dir))
	if err != nil || ks.Starts != 1 {
		t.Fatalf("the keeper's state file after the stop: %+v, %v; want it kept, with one start", ks, err)
	}
	f.supervise(SuperviseOptions{})
	k2 := f.awaitEntry(keeperEntryName, "the keeper to run again", func(e EntryRecord) bool {
		return e.State == entryRunning && e.Proc != nil && e.Proc.PID != k.Proc.PID
	})
	if eh, problems := f.keeperHealth(k2); len(problems) != 0 || eh.KeeperEpoch != firstKeeperE+1 {
		t.Errorf("the keeper supervised again: %+v, problems %v; want E %d", eh, problems, firstKeeperE+1)
	}
}

// The keeper's health check fails a keeper that does not answer, and one that binds a port: the
// positive controls of the check above. The fake's relaykeeper role, steered, runs no keeper that
// answers (keeper-no-answer), or listens on a TCP port beside the real keeper (keeper-listen-tcp).
func TestTheKeepersHealthFailsANoAnswerAndAPort(t *testing.T) {
	t.Run("no answer", func(t *testing.T) {
		f := keeperFixture(t)
		f.steer("keeper-no-answer", "")
		f.supervise(SuperviseOptions{})
		k := f.keeperUp()
		sup := f.supervisorHandle()
		eh, problems := checkEntry(sup, sup.rec.PID, k)
		if !eh.Running || eh.PingOK == nil || *eh.PingOK || !strings.Contains(strings.Join(problems, "; "), "ping on ") {
			t.Errorf("a keeper that does not answer: %+v, problems %v; want it running, its ping failed", eh, problems)
		}
	})
	t.Run("a port", func(t *testing.T) {
		f := keeperFixture(t)
		port := freePort(t)
		f.steer("keeper-listen-tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		f.supervise(SuperviseOptions{})
		k := f.keeperUp()
		sup := f.supervisorHandle()
		var eh EntryHealth
		var problems []string
		for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if eh, problems = checkEntry(sup, sup.rec.PID, k); strings.Contains(strings.Join(problems, "; "), "binds a port") {
				break
			}
		}
		if !strings.Contains(strings.Join(problems, "; "), "127.0.0.1:"+strconv.Itoa(port)) {
			t.Errorf("a keeper that listens on %d: %+v, problems %v; want the port reported", port, eh, problems)
		}
	})
}

// up's start waits for the keeper as for every child that must be ready: the supervisor's start
// fails while the keeper does not answer (entriesNotReady), and passes once it does.
func TestTheStartWaitsForTheKeepersAnswer(t *testing.T) {
	f := keeperFixture(t)
	f.steer("keeper-no-answer", "")
	f.supervise(SuperviseOptions{})
	f.keeperUp()
	sup := f.supervisorHandle()
	why, err := entriesNotReady(f.l, sup, map[string]bool{keeperEntryName: true})
	if err != nil || !strings.HasPrefix(why, keeperEntryName+": ") {
		t.Errorf("the start with a keeper that does not answer: %q, %v; want it not ready", why, err)
	}
}
