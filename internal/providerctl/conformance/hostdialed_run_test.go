package conformance

// The reach-host-dialed rows (issue #305; HBEP-6, Dial-home modes, host-dialed mode; HBEP-4, open question 16): one short
// job of the real agent in host-dialed mode with no Instance, on a compute node of each alpha platform, and the two relays
// the batch Provider's controller starts to it from the control-plane host through the login node. The reach rows probe
// the other direction, a compute node dialing out, which host-dialed mode does not have.
//
// The harness is this package's world, the lightest that runs the real relay code on both ends: the real Provider
// controller over the real transport (providerctl.New with Options.Relays, so internal/hostdial's Owner, told by each
// observation what runs, as in production), the real agent binary as the job, and each relay a real ssh to the login
// node running srun -u --overlap with the agent's own "relay <channel>" as the step's program, so every byte crosses the
// step's standard input and output. The control port is the real agent API server, so the agent's registration is the
// control session's proof; no k3s, lifecycle controller or front is needed for it. The serving session carries one
// exchange of the 256 byte values each way, between a short step of the job (testdata/serving-probe.sh, standing where
// frpc would, which the agent starts only for an Instance) and the responder on the serving port.
//
// The job starts under the row's one-job slot and start wait, like the reach rows: past the bound it is cancelled by its
// tag and the row is NOT RUN (startwait_test.go). A run that started is judged by judgeHostDialed, a check per line of
// the text to post; frps and frpc are not run (see hostDialedPosts).

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

//go:embed testdata/serving-probe.sh
var servingProbeScript string

// The row's timings. The job's time limit is five minutes: a run that goes well ends in about one, and a job that stalls
// holds the owner's one-job slot that long at most. Each wait below is bounded, named here, and added up by the row's
// timeout test (budget_test.go); the session wait is derived from the relay's own worst case (hostDialedSessionWait's test).
const (
	hostDialedTimeLimit = "5m"
	// hostDialedEpoch is the fence's epoch E the relays carry, the Lease's leaseTransitions in production (HBEP-6,
	// Fencing). The row has no Lease: it is a fixed value, and the stale relay's is one below it.
	hostDialedEpoch int64 = 7
	// hostDialedSessionWait bounds the wait, once the job runs, for both sessions to come up and the agent to register.
	hostDialedSessionWait = 3 * time.Minute
	// hostDialedProbeWait bounds the serving probe's step: a connection that finds no session waits for one up to
	// hostdial.DefaultWait in the agent, then the probe's read of the host's answer has probeReadSeconds.
	hostDialedProbeWait = 2 * time.Minute
	// probeReadSeconds is how long the probe waits for the host's 256 bytes on the node.
	probeReadSeconds = 60
	// hostDialedFenceWait bounds the stale relay: its ssh connect (sshrun.RelayConnectTimeout), its srun step and the
	// agent's refusal.
	hostDialedFenceWait = 1 * time.Minute
	// hostDialedEndWait bounds the wait for the job's end once the agent is told to stop.
	hostDialedEndWait = 2 * time.Minute
)

// bound is a wait of the body: the live bound, or a dry run's shorter one (world.shorten).
func (w *world) bound(d time.Duration) time.Duration {
	if w.shorten != nil {
		return w.shorten(d)
	}
	return d
}

// untilOr reconciles until cond holds, and reports whether it did within timeout. Unlike until it does not stop the
// test: a session that does not come up is a check the verdict names.
func (w *world) untilOr(name, what string, timeout time.Duration, cond func(*v1alpha1.Allocation) bool) bool {
	w.t.Helper()
	pace := w.pace
	if pace <= 0 {
		pace = 3 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		w.reconcile(name)
		if a := w.get(name); a != nil && cond(a) {
			return true
		}
		if time.Now().After(deadline) {
			w.p.log("%s: not within %s: %s", name, timeout, what)
			return false
		}
		time.Sleep(pace)
	}
}

// relaysOf is the owner's relays of the tag, by channel.
func (w *world) relaysOf(tag string) map[agentapi.RelayChannel]hostdial.RelayState {
	out := map[agentapi.RelayChannel]hostdial.RelayState{}
	for _, r := range w.ctl.Relays() {
		if r.Tag == tag {
			out[r.Channel] = r
		}
	}
	return out
}

// registered counts the agent's registrations for the Allocation that presented its recorded job.
func (g *hostDialedRig) registered(uid, jobID string) int {
	n := 0
	for _, r := range g.backend.Registrations(uid) {
		if r.Caller.JobID == jobID && jobID != "" {
			n++
		}
	}
	return n
}

// ended reports a job that is no longer running.
func ended(a *v1alpha1.Allocation) bool {
	switch a.Status.AtProvider.Phase {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
		return true
	}
	return false
}

// runProbe runs the serving probe as a step of the job, through the controller's own transport.
func (w *world) runProbe(ctx context.Context, jobID, allocDir string) (*probeReport, string) {
	line, err := slurm.Line("srun", "--overlap", "--jobid="+jobID, "--ntasks=1", "bash", "-s", "--", allocDir)
	if err != nil {
		return nil, err.Error()
	}
	ctx, cancel := context.WithTimeout(ctx, hostDialedProbeWait)
	defer cancel()
	read := probeReadSeconds
	if w.probeRead > 0 {
		read = w.probeRead
	}
	script := strings.ReplaceAll(servingProbeScript, "@READ_SECONDS@", strconv.Itoa(read))
	res, err := w.cap.Run(ctx, slurm.Command{Line: line, Stdin: []byte(script), Timeout: hostDialedProbeWait})
	if err != nil || res.ExitCode != 0 {
		// The error and the step's standard error can name the host and paths: they go to the private log only.
		w.p.log("probe: exit %d, err %v, stderr %q", res.ExitCode, err, res.Stderr)
		if err != nil {
			return nil, "the probe's step did not run to its end (a transport error; the private log says which)"
		}
		return nil, fmt.Sprintf("the probe's step exited %d", res.ExitCode)
	}
	p, err := parseProbe(res.Stdout)
	if err != nil {
		return nil, err.Error()
	}
	w.p.log("probe: tmpdir-step %v tmpdir %q socket %q mode %s agent log %v agent tmpdir %q port %v connected %v got %d %s",
		p.TmpDirSet, p.TmpDir, p.Socket, p.SocketDirMode, p.AgentLogOK, p.AgentTmpDir, p.PortFound, p.Connected, p.GotBytes, p.GotSHA256)
	return &p, ""
}

// staleRelays runs the relay pair of the epoch below the Lease's, E-1, as the old leader would after an unfenced
// hand-over (HBEP-6, Fencing), and returns each channel's relay once both have ended, or at the bound. It is hostdial's own
// Owner, the production lifecycle: each relay's ssh is started from the owner's locked thread with a parent-death signal,
// so it dies with this process whatever stops it, and is stopped by its PID (the review of PR 306, B3). hostdial numbers
// every relay start of the process, so the pair's sequences come after the kept relays': only the epoch can make the agent
// refuse it (B1). The ssh is the rig's stale wrapper, which writes down the remote lines apart from the kept relays'.
func (w *world) staleRelays(ctx context.Context, g *hostDialedRig, target hostdial.Target) map[agentapi.RelayChannel]hostdial.RelayState {
	rel, err := sshrun.NewRelay(sshrun.RelayOptions{Name: "conformance-stale", Config: w.sshConfig, ControlDir: g.settings.ControlDir, Binary: g.staleWrapper})
	if err != nil {
		w.p.log("stale relays: %v", err)
		return nil
	}
	defer rel.Close()
	owner, err := hostdial.NewOwner(hostdial.OwnerOptions{
		Ports: g.settings.Ports, Epoch: func(context.Context) (int64, error) { return hostDialedEpoch - 1, nil },
		Slots: hostdial.NewSlots(0), MinBackoff: g.settings.MinBackoff, MaxBackoff: g.settings.MaxBackoff, StopGrace: g.settings.StopGrace,
		Logger: slog.New(slog.NewTextHandler(w.p.f, nil)),
	})
	if err != nil {
		w.p.log("stale relays: %v", err)
		return nil
	}
	target.Login, target.Command, target.Gen = rel.Login(), rel.Command, "stale"
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = owner.Start(ctx); close(done) }()
	owner.Want(target)
	both := func() map[agentapi.RelayChannel]hostdial.RelayState {
		out := map[agentapi.RelayChannel]hostdial.RelayState{}
		for _, r := range owner.Relays() {
			out[r.Channel] = r
		}
		return out
	}
	// The wait has a deadline of its own, apart from the owner's run: the last read of the pair comes before the owner
	// stops, which forgets its relays.
	deadline := time.Now().Add(w.bound(hostDialedFenceWait))
	var last map[agentapi.RelayChannel]hostdial.RelayState
	for {
		last = both()
		if (last[agentapi.RelayControl].Final && last[agentapi.RelayServing].Final) || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done // the owner stops every relay of its own by PID
	for ch, r := range last {
		w.p.log("stale relay %s: epoch %d seq %d starts %d sessions %d final %v %q last end %q refusal %q", ch, r.Epoch, r.Seq, r.Starts, r.Sessions, r.Final, r.FinalCause, r.LastEnd, r.LastRefusal)
	}
	return last
}

// agentTail reads the last bytes of the agent's output, job.err in the Allocation's directory, keeps them in the private
// record, and returns them; "" when they cannot be read. It is read whatever the run came to, before the clean-up removes
// the staging: a job whose agent stopped at its start leaves nothing else (the review of PR 306, B5).
func (w *world) agentTail(ctx context.Context, allocDir string) string {
	line, err := slurm.Line("tail", "-c", strconv.Itoa(agentTailBytes), path.Join(allocDir, agentapi.JobErrorName))
	if err != nil {
		return ""
	}
	res, err := w.cap.Run(ctx, slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		w.p.log("the agent's output: exit %d, err %v", res.ExitCode, err)
		return ""
	}
	w.p.save("job-err-tail.txt", res.Stdout)
	return string(res.Stdout)
}

// agentTailBytes is how much of job.err the row keeps: the agent's last lines, not its whole output.
const agentTailBytes = 4096

// hostDialedBody is the run, live or dry: the job, its two sessions, the probe, the stale relays, the stop, the verdict.
func hostDialedBody(t testing.TB, w *world, g *hostDialedRig, rowName string) {
	t.Helper()
	ctx := context.Background()
	row, ok := alpha.RowByName(rowName)
	if !ok || !strings.HasPrefix(rowName, "reach-host-dialed-") {
		t.Fatalf("%s is not a reach-host-dialed row", rowName)
	}
	name := "reach-host-dialed"
	w.timeLimit = hostDialedTimeLimit
	a := w.newAlloc(name)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	uid := string(a.UID)
	// The first clock: the row's start wait, past which the job is cancelled by its tag and the row is NOT RUN. A job that
	// ended Failed has started: its agent stopped at its start, which the verdict names, with the agent's last error (B5).
	started := func(a *v1alpha1.Allocation) bool {
		if a.Status.AtProvider.Phase == v1alpha1.AllocationFailed {
			return true
		}
		return accountingStarted(t, a)
	}
	w.failedIsStart = true
	a = w.untilRuns("the job", name, "the job runs", w.at(), "", started)
	o := hostDialedObs{Row: row, Epoch: hostDialedEpoch, JobID: a.Status.AtProvider.JobID, UID: uid, Tag: tag}
	paths, err := w.batch.Paths(ctx)
	if err != nil {
		t.Fatalf("the staging directory could not be resolved: %v", err)
	}
	o.AgentPath = path.Join(paths.Staging, agentapi.StagedAgentPath(w.digest))
	allocDir := path.Join(paths.Staging, agentapi.AllocationDir(tag))

	// Both sessions and the registration, while the job runs.
	w.untilOr(name, "both sessions are up and the agent registered", w.bound(hostDialedSessionWait), func(a *v1alpha1.Allocation) bool {
		rs := w.relaysOf(tag)
		up := rs[agentapi.RelayControl].Sessions >= 1 && rs[agentapi.RelayServing].Sessions >= 1 && g.registered(uid, o.JobID) >= 1
		return up || ended(a)
	})
	alive := func() bool { cur := w.get(name); return cur != nil && !ended(cur) }
	// The probe runs whether the serving session came up or not: the rendezvous is read either way, and with no session
	// its exchange gets nothing back, which the 256-byte check names.
	if alive() {
		o.Probe, o.ProbeErr = w.runProbe(ctx, o.JobID, allocDir)
	}
	if kept := w.relaysOf(tag); alive() && kept[agentapi.RelayControl].Sessions >= 1 {
		o.Kept = kept
		o.Stale = w.staleRelays(ctx, g, hostdial.Target{Tag: tag, JobID: o.JobID, AgentPath: o.AgentPath, AllocDir: allocDir, UID: uid,
			Jobs: w.batch, Stderr: sshrun.Stderr, Describe: sshrun.Classify})
	}
	o.Relays = w.relaysOf(tag)
	for ch, r := range o.Relays {
		w.p.log("relay %s: epoch %d seq %d starts %d sessions %d streams %d joined %d pid %d last end %q refusal %q final %v %q", ch, r.Epoch, r.Seq, r.Starts, r.Sessions, r.Streams, r.Joined, r.PID, r.LastEnd, r.LastRefusal, r.Final, r.FinalCause)
	}
	o.Registered = g.registered(uid, o.JobID)
	o.JobEnded = !alive()
	o.Exchanges = g.serving.exchanges()

	// The stop, over the control session: the agent API serves it to the agent's own poll, the agent sends its last
	// report, exits 0, and the job ends COMPLETED (B2). Each is read, never taken from the job's end alone.
	if !o.JobEnded {
		if w.beforeStop != nil {
			w.beforeStop(o.JobID)
		}
		g.stop.arm(uid)
		g.backend.SetDesired(uid, agentapi.DesiredState{Action: agentapi.ActionStop})
		w.untilOr(name, "the job ends once the agent is told to stop", w.bound(hostDialedEndWait), ended)
		o.StopServed, o.StopAcked = g.stop.seen()
	}
	o.EndState = w.endState(ctx, o.JobID)
	o.AgentTail = w.agentTail(ctx, allocDir)
	if lines, err := relayLines(g.record); err != nil {
		w.p.log("the relays' record: %v", err)
		o.LinesErr = "the wrapper's record of them is there but unreadable"
	} else {
		o.Lines = lines
	}
	for _, l := range o.Lines {
		w.p.log("relay line: %s", l)
	}

	checks := judgeHostDialed(o)
	qos := qosUsed(w.s.qos, w.s.part.QOS)
	posts := hostDialedPosts(o, checks, qos, w.s.gpus, w.s.cpus, w.s.memory.String())
	w.p.save(alpha.PostsFile, []byte(posts))
	failed := failedChecks(checks)
	verdict := "PASS"
	if len(failed) > 0 {
		verdict = "FAIL"
	}
	w.p.save(alpha.RecordFile, []byte(fmt.Sprintf("{\"row\": %q, \"verdict\": %q, \"failed\": %q, \"partition\": %q, \"qos\": %q, \"tmpdir\": %q, \"socket\": %q, \"agentTmpdir\": %q, \"endState\": %q}\n",
		row.Name, verdict, strings.Join(failed, "; "), row.Partition, qos, probeField(o.Probe, true), probeField(o.Probe, false), agentTmpField(o.Probe), o.EndState)))
	if len(failed) > 0 {
		t.Fatalf("%s failed: %s", row.Name, strings.Join(failed, "; "))
	}
	t.Logf("%s passed; the text to post is in %s", row.Name, alpha.PostsFile)
}

// endState is the job's end state as the scheduler records it (COMPLETED for an exit 0), "" when it cannot be read or
// the job has none yet.
func (w *world) endState(ctx context.Context, jobID string) string {
	if jobID == "" {
		return ""
	}
	j, found, err := w.batch.Observe(ctx, jobID)
	if err != nil || !found || len(j.State) == 0 || j.EndTime == nil {
		w.p.log("the job's end state: found %v, err %v, state %v", found, err, j.State)
		return ""
	}
	return j.State[0]
}

// agentTmpField is the agent's TMPDIR as the probe read it, for the private record.
func agentTmpField(p *probeReport) string {
	if p == nil {
		return ""
	}
	return p.AgentTmpDir
}

// probeField is the probe's step TMPDIR (tmp) or socket path for the private record, or "".
func probeField(p *probeReport, tmp bool) string {
	switch {
	case p == nil:
		return ""
	case tmp:
		return p.TmpDir
	}
	return p.Socket
}
