package launcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// statusOf judges the supervised processes as status does (checkSupervision), with this test process
// as the supervisor, and returns what it reported.
func (f *ctlFixture) statusOf() (rep *StatusReport, health string) {
	f.t.Helper()
	f.ctlUp()
	awaitHealthy(f.t, f.ports.ControllerAddr()) // the controller binds its health port a moment after it starts
	self, err := newRec(supervisorName, os.Getpid())
	if err != nil {
		f.t.Fatal(err)
	}
	st := *f.st
	st.Procs = []ProcRec{f.st.Procs[0], self}
	sup, v, d := openRec(self)
	if v != Ours {
		f.t.Fatalf("this process is %v: %s", v, d)
	}
	defer sup.Close()
	rep = &StatusReport{Health: Healthy}
	checkSupervision(f.l, &st, sup, rep, time.Now, func(format string, a ...any) {
		rep.Health = Unhealthy
		rep.Reasons = append(rep.Reasons, fmt.Sprintf(format, a...))
	})
	return rep, rep.Health
}

func (r *StatusReport) entry(name string) (EntryHealth, bool) {
	for _, c := range r.Controllers {
		if c.Name == name {
			return c, true
		}
	}
	return EntryHealth{}, false
}

func startTimeOf(t *testing.T, e EntryRecord) time.Time {
	t.Helper()
	if e.StartedAt == nil {
		t.Fatalf("entry %s has no start time: %+v", e.Name, e)
	}
	return *e.StartedAt
}

// The log starts first: the controller is not started until the log answers /healthz, and the fake
// controller, which asks once at its own start, records the answer. A supervisor that started the
// entries together would have the controller ask a log that is not up, and the record says so.
func TestTheLogStartsBeforeTheControllersAndIsHealthyWhenTheyStart(t *testing.T) {
	f := eventLogFixture(t, plentyConfig())
	f.supervise(SuperviseOptions{disk: f.diskFor(100*gb, 0, 0)})
	ctl := f.ctlUp()
	log := f.logUp()

	probe := f.awaitFileText(fmt.Sprintf("eventlog-probe-%d.txt", ctl.Proc.PID))
	if probe != "200" {
		t.Fatalf("the controller's own look at the log's /healthz when it started: %q, want 200: the log was not up before it", probe)
	}
	if !startTimeOf(t, log).Before(startTimeOf(t, ctl)) {
		t.Errorf("the log started at %v, not before the controller at %v", startTimeOf(t, log), startTimeOf(t, ctl))
	}
	// The controller's command line gets the token file, as the launcher's record says, and no more of the log.
	argv := f.argvOf(ctl.Proc.PID)
	token := f.l.host(f.ports).EventLogTokenFile()
	if i := slices.Index(argv, "--event-log-token-file"); i < 0 || argv[i+1] != token {
		t.Errorf("the controller's command line %v lacks --event-log-token-file %s", argv, token)
	}
	// The log's own: the subcommand, the two files, no token on it.
	logArgv := f.argvOf(log.Proc.PID)
	dir := f.l.eventLogDir()
	if want := []string{"eventlog", "--config", filepath.Join(dir, "nats.conf"), "--caps-file", filepath.Join(dir, "caps.json")}; !slices.Equal(logArgv, want) {
		t.Errorf("the log's command line %v, want %v", logArgv, want)
	}
	if strings.Contains(strings.Join(logArgv, " "), f.logToken()) {
		t.Error("the token is on the log's command line")
	}
	// The log is its own session leader, and a child of the supervisor.
	if pgrp, sid := sessionOf(t, *log.Proc); sid != log.Proc.PID || pgrp != log.Proc.PID {
		t.Errorf("the log is in process group %d, session %d, want its own (%d)", pgrp, sid, log.Proc.PID)
	}
	if ppidOf(t, log.Proc.PID) != os.Getpid() {
		t.Errorf("the log's parent is pid %d, want the supervisor's", ppidOf(t, log.Proc.PID))
	}
	// Status judges the log healthy, with the two streams and their caps.
	rep, health := f.statusOf()
	if health != Healthy {
		t.Fatalf("status: %s %v", health, rep.Reasons)
	}
	eh, ok := rep.entry(eventLogEntryName)
	if !ok || !eh.Running || eh.HealthzOK == nil || !*eh.HealthzOK || eh.Degraded != "" {
		t.Fatalf("the log's health: %+v", eh)
	}
	if len(eh.Streams) != 2 || eh.Streams[0].Name != "HB_EVENTS" || eh.Streams[1].Name != "HB_USAGE" || eh.Streams[0].MaxBytes != 64<<20 || eh.Streams[0].Messages != 0 {
		t.Errorf("streams %+v, want HB_EVENTS and HB_USAGE, capped at 64 MiB, empty", eh.Streams)
	}
}

// On stop the front goes first, then the others, and the log last, and waited for: the controller
// takes 3 s to shut down, far longer than the log needs, so a log stopped at the same time would be gone
// before it, and each exit is read from the supervisor's own log lines.
func TestTheLogStopsLast(t *testing.T) {
	f := eventLogFixture(t, plentyConfig())
	f.steer("slow-term", "3000")
	log := &stampLog{}
	stop := f.supervise(SuperviseOptions{disk: f.diskFor(100*gb, 0, 0), Log: log})
	ctl := f.ctlUp()
	lg := f.logUp()
	// Both are past their start: the controller serves (its signal handling is in place, so its slow
	// stop is the slow one this test needs), and the log has made its streams.
	awaitHealthy(t, f.ports.ControllerAddr())
	f.logReady()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	ctlExit := log.when(t, fmt.Sprintf("controller: pid %d exited", ctl.Proc.PID))
	logExit := log.when(t, fmt.Sprintf("eventlog: pid %d exited", lg.Proc.PID))
	if !logExit.After(ctlExit) {
		t.Errorf("the log exited at %v, not after the controller at %v", logExit, ctlExit)
	}
	t.Logf("controller exited at %v, the log at %v", ctlExit.Format("05.000"), logExit.Format("05.000"))
	stage1 := log.when(t, "stopping the tunnel processes and the controllers")
	stage2 := log.when(t, "stopping the event log last")
	if !stage2.After(stage1) || stage2.Before(ctlExit) {
		t.Errorf("the log's stage began at %v: want it after the others' stage (%v) and after the controller's exit (%v)", stage2, stage1, ctlExit)
	}
	// The log was told to stop by SIGTERM and left cleanly: exit 0.
	final := f.record()
	for _, e := range final.Entries {
		if e.Name == eventLogEntryName && (e.State != entryStopped || e.LastExit == nil || e.LastExit.Code != 0) {
			t.Errorf("the log's final record %+v, want stopped after exit 0", e)
		}
	}
	nothingListens := func(addr string) {
		if _, err := healthzCode(addr); err == nil {
			t.Errorf("something still answers on %s", addr)
		}
	}
	nothingListens(f.ports.EventLogMonitorAddr())
}

// The gate, through the supervisor: with 23.5 GB free the log is not started, its entry is gated with
// the reason, the controller runs anyway, and the supervisor starts the log by itself once the free
// space reads 23.6 GB (HBEP-8, Test plan, The caps fit the disk). Cap 2 GiB each, floor 15 GB.
func TestAGatedLogIsStartedByTheSupervisorOnceTheSpaceFits(t *testing.T) {
	cfg := EventLogConfig{Caps: eventlog.Caps{Events: 2 * gib, Usage: 2 * gib}, Floor: floor15gb}
	f := eventLogFixture(t, cfg)
	disk := f.diskFor(23_500_000_000, 0, 0)
	f.supervise(SuperviseOptions{disk: disk, GatePoll: 20 * time.Millisecond})
	f.ctlUp() // the controller starts: nothing waits on a log that is held back
	g := f.awaitEntry(eventLogEntryName, "the log to be gated", func(e EntryRecord) bool { return e.State == entryGated })
	if g.Proc != nil || g.StartedAt != nil || g.Gate == "" {
		t.Fatalf("a gated entry: %+v", g)
	}
	for _, want := range []string{"23500 MB free on the store's file system", "caps 2 GiB+2 GiB", "floor 15 GB"} {
		if !strings.Contains(g.Gate, want) {
			t.Errorf("the gate's reason %q does not contain %q", g.Gate, want)
		}
	}
	if fileOf(filepath.Join(f.ctl, "eventlog-starts")) != "" {
		t.Fatal("the log was started although the gate was closed")
	}
	// Status: degraded with the reason, healthy, nothing blamed.
	rep, health := f.statusOf()
	eh, _ := rep.entry(eventLogEntryName)
	if health != Healthy || !strings.HasPrefix(eh.Degraded, "event log not started: 23500 MB free on the store's file system") || eh.Running {
		t.Fatalf("status of a gated log: %s %v, entry %+v", health, rep.Reasons, eh)
	}
	// The space comes back: no new up, the supervisor starts it.
	disk.set(map[string]int64{f.l.eventLogDir(): 23_600_000_000}, nil)
	up := f.logUp()
	f.logReady()
	if up.Gate != "" || up.Restarts != 0 {
		t.Errorf("the entry after the gate opened: %+v", up)
	}
	if code, err := healthzCode(f.ports.EventLogMonitorAddr()); err != nil || code != 200 {
		t.Errorf("GET /healthz on the log: %d, %v", code, err)
	}
	// Every read was of the store's own path (the double knows no other).
	for _, a := range disk.askedPaths() {
		if a != "Free "+f.l.eventLogDir() && a != "Size "+f.l.eventLogDir() && a != "Size "+f.l.eventLogCopyDir() {
			t.Errorf("the supervisor read %q", a)
		}
	}
	if rep, health := f.statusOf(); health != Healthy {
		t.Errorf("status once it runs: %s %v", health, rep.Reasons)
	} else if eh, _ := rep.entry(eventLogEntryName); eh.Degraded != "" || !eh.Running {
		t.Errorf("status once it runs: %+v", eh)
	}
}

// The other bounds of the same row, through the supervisor: 1 GiB in the store, or a 1 GiB copy,
// make the bound 15e9 + 7,516,192,768 = 22,516,192,768, so 22.6 GB starts the log and 22.5 GB does not.
func TestTheGateBoundMovesWithTheStoreAndTheCopy(t *testing.T) {
	for name, c := range map[string]struct{ store, copySize int64 }{"a store of 1 GiB": {gib, 0}, "a copy of 1 GiB": {0, gib}} {
		t.Run(name, func(t *testing.T) {
			cfg := EventLogConfig{Caps: eventlog.Caps{Events: 2 * gib, Usage: 2 * gib}, Floor: floor15gb}
			f := eventLogFixture(t, cfg)
			disk := f.diskFor(22_500_000_000, c.store, c.copySize)
			f.supervise(SuperviseOptions{disk: disk, GatePoll: 20 * time.Millisecond})
			g := f.awaitEntry(eventLogEntryName, "the log to be gated", func(e EntryRecord) bool { return e.State == entryGated })
			if !strings.Contains(g.Gate, "22500 MB free") {
				t.Errorf("reason %q", g.Gate)
			}
			disk.set(map[string]int64{f.l.eventLogDir(): 22_600_000_000}, nil)
			f.logUp()
		})
	}
}

// The gate is consulted before each start, the restarts too: a log killed while the space has gone
// is not started again until it comes back, and then it is, on the same ports and the same store.
// The kill is SIGKILL through a pidfd opened on the verified recorded process (never a recorded PID
// signalled by number, and no group signal), and the recorded exit says the signal.
func TestAKilledLogComesBackOnTheSameStoreAndTheGateIsAskedAgain(t *testing.T) {
	f := eventLogFixture(t, plentyConfig())
	disk := f.diskFor(100*gb, 0, 0)
	f.supervise(SuperviseOptions{disk: disk, GatePoll: 20 * time.Millisecond, BackoffMin: 20 * time.Millisecond, BackoffMax: 50 * time.Millisecond})
	first := f.logUp()
	_, js := f.logClient()
	f.publishEvents(js, 5)
	if st := f.streamState(js, "HB_EVENTS"); st.Msgs != 5 {
		t.Fatalf("the events stream holds %d messages, want 5", st.Msgs)
	}

	disk.set(map[string]int64{f.l.eventLogDir(): 1 * gb}, nil) // the space goes: below caps and floor
	h, v, d := openRec(*first.Proc)
	if v != Ours {
		t.Fatalf("the log's record: %v: %s", v, d)
	}
	sent, err := signalHandle(h, syscall.SIGKILL)
	h.Close()
	if err != nil || !sent {
		t.Fatalf("SIGKILL through the pidfd: sent %v, %v", sent, err)
	}
	g := f.awaitEntry(eventLogEntryName, "the log to be gated after the kill", func(e EntryRecord) bool { return e.State == entryGated })
	if g.LastExit == nil || g.LastExit.Detail != "signal: killed" {
		t.Fatalf("the last exit %+v, want signal: killed (the wait status says SIGKILL)", g.LastExit)
	}
	if got := strings.Count(fileOf(filepath.Join(f.ctl, "eventlog-starts")), "\n"); got != 1 {
		t.Fatalf("the log started %d times while the gate was closed, want 1", got)
	}

	disk.set(map[string]int64{f.l.eventLogDir(): 100 * gb}, nil)
	second := f.awaitEntry(eventLogEntryName, "the log to run again", func(e EntryRecord) bool {
		return e.State == entryRunning && e.Proc != nil && e.Proc.PID != first.Proc.PID
	})
	if second.Restarts != 1 {
		t.Errorf("restarts %d, want 1", second.Restarts)
	}
	_, js = f.logClient() // the same address and the same token
	if st := f.streamState(js, "HB_EVENTS"); st.Msgs != 5 || st.FirstSeq != 1 || st.LastSeq != 5 {
		t.Errorf("after the kill and the restart the events stream is %d messages, sequences %d to %d, want 5, 1 to 5", st.Msgs, st.FirstSeq, st.LastSeq)
	}
}

// A log that exits refusing its settings is unhealthy in status, naming them. The configuration is
// corrupted after up wrote it (a misspelt sync_interval), the supervisor starts the log as it is, the
// log exits with status 3 at each start, and status reads the refusal it wrote.
func TestALogThatRefusesItsSettingsIsUnhealthyByName(t *testing.T) {
	f := eventLogFixture(t, plentyConfig())
	conf := filepath.Join(f.l.eventLogDir(), eventlog.ConfFile)
	text, err := os.ReadFile(conf)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(conf, []byte(strings.Replace(string(text), "sync_interval: always", "sync_intervall: always", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	f.supervise(SuperviseOptions{disk: f.diskFor(100*gb, 0, 0), BackoffMin: 20 * time.Millisecond, BackoffMax: 50 * time.Millisecond, EventLogStartWait: 200 * time.Millisecond})
	e := f.awaitEntry(eventLogEntryName, "the log to exit refusing", func(e EntryRecord) bool {
		return e.State == entryBackoff && e.LastExit != nil && e.LastExit.Code == eventlog.ExitRefused
	})
	if e.Proc != nil {
		t.Errorf("a log in backoff has a process: %+v", e.Proc)
	}
	f.ctlUp() // nothing waits on a log that does not come
	rep, health := f.statusOf()
	if health != Unhealthy {
		t.Fatalf("status of a log that refuses: %s %v, want unhealthy", health, rep.Reasons)
	}
	if reasons := strings.Join(rep.Reasons, "\n"); !strings.Contains(reasons, "the event log refuses to serve") || !strings.Contains(reasons, "sync_intervall") {
		t.Errorf("the reasons do not name the setting:\n%s", reasons)
	}
	// Control: the same fixture with the configuration as up wrote it is healthy (the other tests).
}

// A full events stream is degraded, with its remedy, and the log keeps serving: status stays healthy
// and exits 0. A stream with room for one more message of the size it holds is not full. The cap is
// three messages of 183 bytes (worked out in internal/eventlog/node: 22 + 17 + 4 + 32 + 100 + 8), so
// 549 bytes: two messages leave room for a third; three leave none.
func TestAFullEventsStreamIsDegradedWithItsRemedy(t *testing.T) {
	cfg := EventLogConfig{Caps: eventlog.Caps{Events: 3 * 183, Usage: 1 << 20}, Floor: gb}
	f := eventLogFixture(t, cfg)
	f.supervise(SuperviseOptions{disk: f.diskFor(100*gb, 0, 0)})
	f.logUp()
	_, js := f.logClient()
	degraded := func() (EntryHealth, string) {
		rep, health := f.statusOf()
		eh, _ := rep.entry(eventLogEntryName)
		if health != Healthy {
			t.Logf("status reasons: %v", rep.Reasons)
		}
		return eh, health
	}
	if eh, health := degraded(); health != Healthy || eh.Degraded != "" {
		t.Fatalf("an empty log: %s %+v", health, eh)
	}
	f.publishEvents(js, 2)
	if eh, health := degraded(); health != Healthy || eh.Degraded != "" || eh.Streams[0].Messages != 2 || eh.Streams[0].Bytes != 2*183 {
		t.Fatalf("two of three messages: %s %+v", health, eh)
	}
	f.publishEvents(js, 3) // the first two are repeats, within the duplicate window; the third is stored
	if st := f.streamState(js, "HB_EVENTS"); st.Msgs != 3 || st.Bytes != 3*183 {
		t.Fatalf("the stream holds %d messages, %d bytes, want 3 and 549", st.Msgs, st.Bytes)
	}
	eh, health := degraded()
	if health != Healthy {
		t.Fatalf("a full events stream is no fault: %s", health)
	}
	for _, want := range []string{"event log full", "HB_EVENTS", "--event-log-events-cap", "hpcburst down, then hpcburst up --event-log", "prune"} {
		if !strings.Contains(eh.Degraded, want) {
			t.Errorf("the degraded text %q does not contain %q", eh.Degraded, want)
		}
	}
	if !eh.Running || eh.HealthzOK == nil || !*eh.HealthzOK {
		t.Errorf("a full log keeps serving: %+v", eh)
	}
}

// The stop in three stages (HBEP-11, #553 and this log): the front first, then the others together,
// and the log last, told only once every other runner has returned. A gate holds the controller's stop,
// so a log told at the same time as the others would show itself in the order of the "told" lines.
func TestTheSupervisorStopsTheLogAfterEveryOtherProcessHasStopped(t *testing.T) {
	specs := []supervised{{Name: controllerEntryName}, {Name: eventLogEntryName}, {Name: frontEntryName}, {Name: keeperEntryName}}
	var mu sync.Mutex
	var told []string
	gate := make(chan struct{})
	controllerTold := make(chan struct{})
	run := func(ctx context.Context, sp supervised) {
		<-ctx.Done()
		mu.Lock()
		told = append(told, sp.Name)
		mu.Unlock()
		if sp.Name == controllerEntryName {
			close(controllerTold)
			<-gate
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var stages []string
	done := make(chan struct{})
	go func() {
		runInStopOrder(ctx, specs, run, func(s string) { mu.Lock(); stages = append(stages, s); mu.Unlock() })
		close(done)
	}()
	cancel()
	select {
	case <-controllerTold:
	case <-time.After(30 * time.Second):
		t.Fatal("the controller was never told to stop")
	}
	// While the controller's stop is held, the log must not be told: in the right order this cannot
	// happen, so the wait only gives an all-at-once stop time to show itself.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	if slices.Contains(told, eventLogEntryName) {
		t.Errorf("the log was told to stop while the controller's stop was still running: told %v", told)
	}
	mu.Unlock()
	close(gate)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runInStopOrder did not return")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(told) != 4 || told[0] != frontEntryName || told[3] != eventLogEntryName {
		t.Errorf("told %v, want the front first and the log last", told)
	}
	if want := []string{"the front first", "the tunnel processes and the controllers", "the event log last"}; !slices.Equal(stages, want) {
		t.Errorf("stages %v, want %v", stages, want)
	}
}

// The log's stage comes after the front's and the others': the least grace with a front is still
// SupervisorGraceWithFront and DefaultGrace is not raised for the log, whose stop is short and loses
// nothing acknowledged under sync_interval always. The arithmetic: the front's 30 s, the controller's
// 34 s and the log's 4 s are 68 s, under DefaultGrace's 70 s, so even a stop that uses every grace in
// full ends inside it.
func TestTheLogsStopStageFitsTheMargin(t *testing.T) {
	if sum := FrontTermGrace + ControllerTermGrace + EventLogTermGrace; sum >= DefaultGrace {
		t.Errorf("the front (%s), the controller (%s) and the log (%s) in a row are %s, not under DefaultGrace (%s)", FrontTermGrace, ControllerTermGrace, EventLogTermGrace, sum, DefaultGrace)
	}
	if EventLogTermGrace <= 0 || EventLogTermGrace >= DefaultTermGrace {
		t.Errorf("the log's kill grace %s: want a short one under the supervisor's general %s", EventLogTermGrace, DefaultTermGrace)
	}
}
