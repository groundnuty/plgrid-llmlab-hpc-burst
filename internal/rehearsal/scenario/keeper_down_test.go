package scenario_test

// Three relay keeper items of HBEP-6's Test plan on the whole stack (issue
// #343, PR 5), on one keeper-mode host whose real controller runs real
// host-dialed Allocations:
//   - status taken while the keeper's relays run says the keeper is healthy: its
//     ping answered by the keeper itself, with its E, and no port bound by the
//     keeper process, while that process holds its own dials to the dial-home
//     listener and its relays' ssh children run (a reviewer's point on PR 361:
//     the no-port check's scope is the keeper's own listeners);
//   - "down leaves no keeper and no relay ssh behind, and keeps the state file";
//   - "after down, up without --relay-keeper refuses while host-dialed
//     Allocations run", naming them (HBEP-6, Compatibility and upgrades).

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
)

// TestRehearsalKeeperStatusWhileRelaysRunThenDownAndTheSwitchBack: a status
// taken while relays run and a stream crosses finds the keeper healthy, the
// stream whole; a down with a stream in flight cuts it, leaves none of the
// keeper, the supervisor and every relay ssh, pinned before, and keeps the
// keeper's state file; and up without --relay-keeper then refuses, naming the
// running Allocations, and leaves the host stopped and the keeper unstarted.
//
//livecheck:none the keeper rehearsal; hack/rehearsal.sh runs it, with REHEARSAL_RUN naming it
func TestRehearsalKeeperStatusWhileRelaysRunThenDownAndTheSwitchBack(t *testing.T) {
	// No watch: down stops the API it would follow.
	r := startKeeperRig(t, rigSetup{noWatch: true})
	in, _ := r.keeperStart()
	key := r.fc.KeyFor(in)
	v := r.keeperRelays("after the start", in.Status.JobID)
	ref := r.quietReference("status while relays run", in, key, func() { r.keeperStatusWhileRelaysRun(v) })
	running := r.runningAllocationNames()
	r.keeperDown(in, key, ref, v)
	r.keeperSwitchBackRefused(running, in.Status.JobID)
}

// tcpSock is a TCP socket of a process: its state as the kernel writes it
// (01 established, 0A listening) and its ports.
type tcpSock struct {
	state         string
	local, remote string
	lport, rport  int
}

// processTCP lists the TCP sockets the process pid itself holds: its
// descriptors' socket inodes, found in its network namespace's tcp and tcp6
// tables. An unreadable descriptor is an error, never an empty answer.
func processTCP(pid int) ([]tcpSock, error) {
	dir := "/proc/" + strconv.Itoa(pid)
	fds, err := os.ReadDir(dir + "/fd")
	if err != nil {
		return nil, err
	}
	inodes := map[string]bool{}
	for _, fd := range fds {
		l, err := os.Readlink(dir + "/fd/" + fd.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue // closed since the listing
		}
		if err != nil {
			return nil, err
		}
		if ino, ok := strings.CutPrefix(l, "socket:["); ok {
			inodes[strings.TrimSuffix(ino, "]")] = true
		}
	}
	var out []tcpSock
	for _, table := range []string{"/net/tcp", "/net/tcp6"} {
		b, err := os.ReadFile(dir + table)
		if table == "/net/tcp6" && errors.Is(err, os.ErrNotExist) {
			continue // no IPv6 in this kernel
		}
		if err != nil {
			return nil, err
		}
		for i, l := range strings.Split(string(b), "\n") {
			f := strings.Fields(l)
			if i == 0 || len(f) < 10 || !inodes[f[9]] {
				continue
			}
			out = append(out, tcpSock{state: f[3], local: f[1], remote: f[2], lport: hexPort(f[1]), rport: hexPort(f[2])})
		}
	}
	return out, nil
}

// hexPort is the port of a /proc/net/tcp address, ADDR:PORT in hex.
func hexPort(a string) int {
	i := strings.LastIndexByte(a, ':')
	n, _ := strconv.ParseInt(a[i+1:], 16, 32)
	return int(n)
}

// keeperStatusWhileRelaysRun is status while the keeper's relays run and a
// stream crosses them. First the reading that the check must tell apart: the
// keeper process holds established TCP connections, its own dials to the
// front's dial-home listener (base+7) for frpc's session inside the serving
// relay, and listens on none. Then hpcburst status, with its relay ssh pins
// alive before and after it.
func (r *rig) keeperStatusWhileRelaysRun(v keeperView) {
	r.t.Helper()
	socks, err := processTCP(v.keeper.pid)
	if err != nil {
		r.t.Fatalf("the keeper's sockets: %v", err)
	}
	var dials, listens []tcpSock
	for _, s := range socks {
		switch {
		case s.state == "01" && s.rport == r.ports.Base+7:
			dials = append(dials, s)
		case s.state == "0A":
			listens = append(listens, s)
		}
	}
	if len(dials) == 0 {
		r.t.Fatalf("the keeper %d holds no established connection to the dial-home listener (base+7): the status check would have nothing to tell from a bound port: %+v", v.keeper.pid, socks)
	}
	if len(listens) != 0 {
		r.t.Errorf("the keeper %d listens on TCP: %+v", v.keeper.pid, listens)
	}
	for ch, p := range v.relays {
		if err := p.stillChild(v.keeper); err != nil {
			r.t.Fatalf("the %s relay before status: %v", ch, err)
		}
	}
	st, out, err := r.statusJSON()
	if err != nil || st.Health != "healthy" || len(st.Reasons) != 0 {
		r.t.Errorf("status while the keeper's relays run: %v, %s %q:\n%.2000s", err, st.Health, st.Reasons, out)
	}
	if !keeperAtE(st, v.keeper.pid, v.epoch) {
		r.t.Errorf("status while the keeper's relays run does not show the keeper %d answering its ping at E %d: %+v", v.keeper.pid, v.epoch, st.Controllers)
	}
	for ch, p := range v.relays {
		if err := p.stillChild(v.keeper); err != nil {
			r.t.Errorf("the %s relay across status: %v", ch, err)
		}
	}
	r.t.Logf("status while relays run: %s; the keeper %d held %d dials to the dial-home listener and %d TCP sockets in all", st.Health, v.keeper.pid, len(dials), len(socks))
}

// downLead is how many tokens of the stream the engine generates before down.
const downLead = 300

// generatedTokens is the Instance's engine's generated tokens as the serving
// controller exports them from its agent's latest report
// (hpcburst_instance_generation_tokens_total), and whether it has one.
func (r *rig) generatedTokens(in *v1alpha1.Instance) (float64, bool) {
	ss, err := scrape(r.ports.URL(12, "/metrics"))
	if err != nil {
		return 0, false
	}
	got := find(ss, "hpcburst_instance_generation_tokens_total", map[string]string{"instance_uid": string(in.UID)})
	return sum(got), len(got) > 0
}

// keeperDown runs down with a stream in flight: the stream is cut, what
// arrived of it is in order, and the keeper, the supervisor, the controller
// and every relay ssh of the keeper, pinned before, are gone; the keeper's
// state file is kept, and no retired marker is written.
func (r *rig) keeperDown(in *v1alpha1.Instance, key, ref string, v keeperView) {
	r.t.Helper()
	defer r.stepTimer("keeper down")()
	pins := []*stackPin{v.keeper, v.sup}
	for pid := range relayChildren(v.keeper.pid) {
		p, err := pinChild(r.t, fmt.Sprintf("relay ssh %d", pid), pid, v.keeper, 0)
		if err != nil {
			r.t.Fatalf("pin the keeper's relay ssh %d: %v", pid, err)
		}
		pins = append(pins, p)
	}
	if len(pins) < 4 {
		r.t.Fatalf("the keeper runs %d relays before down, want both of each running job's", len(pins)-2)
	}
	ctl, _, err := pinEntry(r.t, r.h.State, "controller", v.sup)
	if err != nil {
		r.t.Fatalf("pin the controller: %v", err)
	}
	pins = append(pins, ctl)
	g0, _ := r.generatedTokens(in)
	first, done := r.fc.StartStream(keeperStreamBody(), key)
	r.firstToken("down", first)
	// A stretch of the stream first, so what arrives before the cut is more
	// than its first tokens: the engine's own count, as the agent reports it.
	stack.WaitState(r.t, stack.WaitDeadline, "the engine to generate "+strconv.Itoa(downLead)+" tokens of the stream", func() (bool, string) {
		g, ok := r.generatedTokens(in)
		return ok && g >= g0+downLead, fmt.Sprintf("%g generated, %g before the stream (read %v)", g, g0, ok)
	})
	start := time.Now()
	if err := r.h.DownErr(); err != nil {
		r.t.Fatalf("down: %v", err)
	}
	took := time.Since(start)
	s := r.keeperStream("down", done)
	got := contentOf(s.Body)
	if s.Err == nil && len(streamProblems(sseParse(s.Body), keeperStreamTokens)) == 0 {
		r.t.Errorf("the stream arrived whole across a down that took %s: down left something serving", took.Round(100*time.Millisecond))
	}
	if got == "" || !strings.HasPrefix(ref, got) {
		r.t.Errorf("what arrived of the stream before down cut it, %d bytes, is not the start of the request's quiet reply: %.120q", len(got), got)
	}
	r.t.Logf("down took %s; the stream received %d of %d tokens, in order, and ended %s after down began (%v)",
		took.Round(100*time.Millisecond), len(strings.Fields(got)), keeperStreamTokens, s.EndAt.Sub(start).Round(100*time.Millisecond), s.Err)
	for _, p := range pins {
		p.waitExited(r.t, 30*time.Second)
	}
	path := relaykeeper.StatePath(r.h.State)
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		r.t.Fatalf("the keeper's state file after down: %v %v, want it kept, mode 600", fi, err)
	}
	if n := r.keeperStarts("after down"); n != 1 {
		r.t.Errorf("the keeper's state file after down records %d starts, want 1", n)
	}
	if _, err := os.Stat(r.retiredMarker()); !errors.Is(err, os.ErrNotExist) {
		r.t.Errorf("a keeper retired marker after down: %v", err)
	}
}

// keeperSwitchBackRefused runs up without --relay-keeper while the
// Allocations named running, which the keeper's relays reached, are Running
// in the store and the primary's job, jobID, still runs in the fake cluster:
// it refuses, naming each, leaves no host running and starts no keeper.
func (r *rig) keeperSwitchBackRefused(running []string, jobID string) {
	r.t.Helper()
	defer r.stepTimer("keeper switch back")()
	if len(running) == 0 {
		r.t.Fatal("no Allocation is Running before down: the switch back's refusal would show nothing")
	}
	id, _ := strconv.Atoi(jobID)
	if j, ok := r.cl.Plat.Job(id); !ok || j.State != "RUNNING" {
		r.t.Fatalf("job %s is %+v after down, not RUNNING: the switch back's refusal would show nothing", jobID, j)
	}
	out, err := r.h.UpAgain()
	if err == nil {
		r.t.Fatalf("up without --relay-keeper passed while %v run:\n%s", running, out)
	}
	if !strings.Contains(out, "relay keeper ran on this host") {
		r.t.Errorf("up without --relay-keeper refused (%v), but not for the keeper:\n%s", err, out)
	}
	for _, name := range running {
		if !strings.Contains(out, name) {
			r.t.Errorf("up's refusal does not name the running Allocation %s:\n%s", name, out)
		}
	}
	if _, err := os.Stat(filepath.Join(r.h.State, "state.json")); !errors.Is(err, os.ErrNotExist) {
		r.t.Errorf("the launcher's record after the refused up: %v, want the host stopped", err)
	}
	if n := r.keeperStarts("after the refused up"); n != 1 {
		r.t.Errorf("the keeper's state file after the refused up records %d starts, want 1: no keeper starts", n)
	}
	if _, err := os.Stat(r.retiredMarker()); !errors.Is(err, os.ErrNotExist) {
		r.t.Errorf("a keeper retired marker after the refused up: %v", err)
	}
}
