package scenario_test

// HBEP-11's beta criterion "an upgrade with the controllers running is tested" (issue #552), on the
// whole stack: an operator's upgrade. The host runs on k3s 1.35.9 with the three up-time flags (the relay
// keeper, the bulk path, the instance router), a deployment ready and a stream in flight through the front.
// The controller's and the front's files at their recorded paths are replaced by a second build of the
// commit, and that build's hpcburst runs upgrade --to 1.36.5. Every check reads the stack, not the
// launcher's report.

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// portsUpgrade is the upgrade test's block, 30440-30459 (IMPL-COMMON's table). The client listens at
// base+16, since the bulk path's second frps takes base+13 to base+15.
var portsUpgrade = stack.Ports{Base: 30440, ClientAddr: "127.0.0.1:30456"}

// upgradeEntries are the supervised processes of a host with the three up-time flags.
var upgradeEntries = []string{"controller", "front", "frps", "frps-bulk", "relaykeeper"}

// upgradeRestartReasons are the events that would show the upgrade disturbed the Instance: a restart rung,
// or a handover.
var upgradeRestartReasons = []string{v1alpha1.ReasonWorkerRestart, v1alpha1.ReasonInstanceRestart, v1alpha1.ReasonRouterRestart, v1alpha1.EventReasonHandedOver}

// portsAnswered lists the ports of the block (base to base+16, the client's included) that accept a TCP
// connection before the stack starts: something else on this host takes them, though no listener of
// ours exists yet, as a Kubernetes NodePort (30000-32767) of the host's own k3s does, below the socket
// layer where a free-port check by listeners cannot see it.
func portsAnswered(p stack.Ports) []int {
	var out []int
	for n := p.Base; n <= p.Base+16; n++ {
		c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(n)), 300*time.Millisecond)
		if err == nil {
			c.Close()
			out = append(out, n)
		}
	}
	return out
}

// startUpgradeRig is the keeper rig, on the upgrade test's ports, with the bulk path and the instance router
// too, on k3s 1.35.9, running its own copies of the controller and the front.
func startUpgradeRig(t *testing.T) *rig {
	t.Helper()
	s := rigSetup{
		mode: string(agentapi.DialHomeHostDialed), ports: portsUpgrade, relayKeeper: true, bulkPath: true, instanceRouter: true,
		k3sVersion: keeperOlderK3s, noWatch: true,
		objects: func(_ *rig, arch, creds string) []client.Object {
			return []client.Object{stack.BatchProfile(arch, keeperTiming), stack.BatchProvider(creds)}
		},
		bins: func(b stack.Binaries, dir string) stack.Binaries { return ownCopies(t, b, dir) },
	}
	r := startRigWith(t, s)
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + tokenDelay.String()}}
	return r
}

// instanceNames are the deployment's Instances' names.
func instanceNames(r *rig) []string {
	var out []string
	for _, in := range r.instances() {
		out = append(out, in.Name)
	}
	slices.Sort(out)
	return out
}

// disturbances counts the events that would show a restart rung or a handover of an Instance.
func disturbances(r *rig, name string) int {
	evs := r.eventReasons(name)
	n := 0
	for _, reason := range upgradeRestartReasons {
		n += countReason(evs, reason)
	}
	return n
}

// cmdline is a process's command line, its arguments.
func cmdline(pid int) []string {
	b, _ := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	return strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
}

// frontCutStream checks a stream the front itself cut on its stop (HBEP-5): status 200, ended by no client,
// every event an unfinished chunk but the last, stream_cut with detail shutdown, no [DONE], and its usage
// record cut by front_shutdown.
func (r *rig) frontCutStream(s *openStream) {
	r.t.Helper()
	status, endErr, byClient := s.state()
	s.mu.Lock()
	events := append([]string(nil), s.events...)
	s.mu.Unlock()
	if byClient || status != http.StatusOK {
		r.t.Fatalf("%s: status %d, ended %v, by its client %v; want a 200 stream the front ended", s.what, status, endErr, byClient)
	}
	if p := openEventProblems(events, true); len(p) != 0 {
		r.t.Errorf("%s: its events: %v", s.what, p)
	}
	var last struct {
		Error *stack.FrontError `json:"error"`
	}
	if len(events) == 0 || json.Unmarshal([]byte(events[len(events)-1]), &last) != nil || last.Error == nil ||
		last.Error.Code != "stream_cut" || last.Error.Detail == nil || *last.Error.Detail != "shutdown" {
		tail := ""
		if len(events) > 0 {
			tail = events[len(events)-1]
		}
		r.t.Errorf("%s: its last event %.200s; want stream_cut with detail shutdown, the front's own cut", s.what, tail)
	}
	if row := r.fc.RowOf(s.id); row.CutBy == nil || *row.CutBy != "front_shutdown" || row.Finish == nil || *row.Finish != "cut" {
		r.t.Errorf("%s: its usage record: finish %s, cut by %s; want cut by front_shutdown", s.what, strOrNil(row.Finish), strOrNil(row.CutBy))
	}
}

// TestRehearsalUpgradeWithTheControllersRunning: hpcburst upgrade keeps a serving deployment serving while
// the launcher replaces the controllers and the front.
//
//livecheck:none the keeper job's rehearsal; hack/rehearsal.sh runs it, with REHEARSAL_RUN naming it
func TestRehearsalUpgradeWithTheControllersRunning(t *testing.T) {
	b := requireRehearsal(t)
	if fi, err := os.Stat(filepath.Join(b.K3sDir, "k3s-"+keeperOlderK3s, "k3s")); err != nil || fi.Mode()&0o111 == 0 {
		t.Fatalf("the upgrade test starts on k3s %s, missing from HPCBURST_BIN_DIR (hack/fetch-k3s.sh DIR %s %s): %v", keeperOlderK3s, keeperOlderK3s, keeperNewerK3s, err)
	}
	second := secondBuild(t)
	if taken := portsAnswered(portsUpgrade); len(taken) > 0 {
		t.Fatalf("ports %v of the upgrade test's block answer before its stack starts: something else on this host takes them (a NodePort of the host's k3s, say); the block must move", taken)
	}
	r := startUpgradeRig(t)
	in, repl := r.keeperStart()
	r.beforeDrain("upgrade", in)
	key := r.fc.KeyFor(in)
	state := r.h.State

	// Before: the objects, the Lease, the Instances, the processes, the store's writer, the stream.
	uids := frameworkUIDs(t, r.c)
	lease0, ok := lifecycleLease(t, r.c)
	if !ok || lease0.Holder == "" {
		t.Fatalf("the lifecycle Lease has no holder: %+v", lease0)
	}
	names0 := instanceNames(r)
	restarts0 := map[string]int{}
	dist0 := map[string]int{}
	for _, x := range []*v1alpha1.Instance{in, repl} {
		now, _ := r.instance(x.Name)
		restarts0[x.Name], dist0[x.Name] = len(now.Status.Restarts), disturbances(r, x.Name)
	}
	pins := pinHost(t, state, upgradeEntries...)
	backups0 := backupNames(state)
	writer := storeVersion(t, state)
	s := r.startOpenStream("the stream through the upgrade", key)
	r.waitFirst(s)
	r.waitTokens(s, marginTokens, time.Time{}, "before the upgrade")

	// The operator's upgrade: the second build at the recorded paths, then its hpcburst's upgrade.
	ctlID, err := replaceAtomically(second.Controller, r.b.Controller)
	if err != nil {
		t.Fatalf("replace the controller: %v", err)
	}
	frontID, err := replaceAtomically(second.Front, r.b.Front)
	if err != nil {
		t.Fatalf("replace the front: %v", err)
	}
	launcherID, err := fileIDOf(second.HPCBurst)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	backup, log, err := r.h.UpgradeWith(second.HPCBurst, keeperNewerK3s)
	if err != nil {
		t.Fatalf("upgrade to %s: %v\n%s", keeperNewerK3s, err, log)
	}
	t.Logf("the upgrade took %s; its report names the backup %s", time.Since(start).Round(100*time.Millisecond), backup)
	r.c = stack.KubeClient(t, state)

	// The stream: the front, stopped first (HBEP-11, #553), cut it itself after its grace (HBEP-5).
	r.waitEnd(s, stack.WaitDeadline)
	r.frontCutStream(s)

	// The processes: every old one exited, new ones run, the controller and the front from the replaced
	// files and the supervisor from the second hpcburst.
	stack.WaitFor(t, stack.WaitDeadline, "every supervised process running again", func() bool {
		rec, err := readSupervisor(state)
		if err != nil {
			return false
		}
		for _, n := range upgradeEntries {
			if _, ok := rec.EntryOf(n); !ok {
				return false
			}
		}
		return true
	})
	fresh := pinHost(t, state, upgradeEntries...)
	if p := restartProblems(pins, fresh); len(p) != 0 {
		t.Errorf("the processes across the upgrade: %v", p)
	}
	for _, c := range []struct {
		name string
		want fileID
	}{{"controller", ctlID}, {"front", frontID}, {"supervisor", launcherID}} {
		if got, err := exeIDOf(fresh[c.name].PID); err != nil || got != c.want {
			t.Errorf("the new %s (pid %d) runs %v (%v), want the second build's file %v", c.name, fresh[c.name].PID, got, err, c.want)
		}
	}

	// The backup, from the disk.
	dir, err := newBackup(state, backups0)
	if err != nil {
		t.Fatal(err)
	}
	if dir != backup {
		t.Errorf("the new backup directory is %s, the report names %s", dir, backup)
	}
	if p := backupProblems(dir, writer, []string{string(in.UID), string(repl.UID)}); len(p) != 0 {
		t.Errorf("the backup %s: %v", dir, p)
	}

	// The Lease re-acquired by the new controller.
	var lease1 leaseState
	stack.WaitFor(t, stack.WaitDeadline, "the new lifecycle controller leading its Lease", func() bool {
		lease1, _ = lifecycleLease(t, r.c)
		return lease1.Holder != "" && lease1.Holder != lease0.Holder
	})
	if lease1.Transitions != lease0.Transitions+1 {
		t.Errorf("the lifecycle Lease's transitions went %d -> %d, want one more", lease0.Transitions, lease1.Transitions)
	}

	// Every object kept its UID.
	if p := uidProblems(uids, frameworkUIDs(t, r.c)); len(p) != 0 {
		t.Errorf("objects across the upgrade: %v", p)
	}

	// The up-time flags, from the new processes: the controller's --instance-router, the bulk frps and the
	// relay keeper running (pinned above).
	if args := cmdline(fresh["controller"].PID); !slices.Contains(args, "--instance-router=envoy") && !hasFlag(args, "--instance-router", "envoy") {
		t.Errorf("the new controller's command line %q has no --instance-router envoy", args)
	}

	// A new request after the upgrade is served by the same Instance.
	r.tunnelBack("after the upgrade", in)
	rp := r.fc.SendCtx(context.Background(), "/v1/chat/completions", stack.ChatBody(stack.Model, 4, 0, false, ""), key)
	if row := r.fc.RowOf(rp.ID); rp.Status != http.StatusOK || row.InstanceUID != string(in.UID) {
		t.Errorf("the request after the upgrade: %d, served by %s (%s); want 200 from %s", rp.Status, row.Instance, row.InstanceUID, in.Name)
	}

	// The Instances stayed ready: the same ones, no restart rung, no handover.
	if got := instanceNames(r); !slices.Equal(got, names0) {
		t.Errorf("the Instances after the upgrade %v, before %v: one was replaced", got, names0)
	}
	for _, x := range []*v1alpha1.Instance{in, repl} {
		now, ok := r.instance(x.Name)
		switch {
		case !ok || now.UID != x.UID:
			t.Errorf("Instance %s is gone or recreated", x.Name)
		case now.Status.Phase != v1alpha1.InstanceReady:
			t.Errorf("Instance %s is %s after the upgrade, want ready", x.Name, now.Status.Phase)
		case len(now.Status.Restarts) != restarts0[x.Name]:
			t.Errorf("Instance %s has %d restarts after the upgrade, %d before", x.Name, len(now.Status.Restarts), restarts0[x.Name])
		case disturbances(r, x.Name) != dist0[x.Name]:
			t.Errorf("Instance %s has restart or handover events after the upgrade (%v)", x.Name, r.eventReasons(x.Name))
		}
	}
}

// hasFlag reports whether args hold flag followed by value.
func hasFlag(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}
