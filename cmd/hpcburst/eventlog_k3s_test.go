package main

// The durable log through the CLI, on a real k3s (#625, PR E1; HBEP-8, The durable log; HBEP-11, Single-user
// host and its Test plan rows "The durable log (beta)" and "Upgrade with the log (beta)"). The log is the
// real one: the fake controller's eventlog role runs node.Run, an embedded nats-server with its streams,
// its token and its caps. Nothing here publishes events or usage: that is PR E2's and E3's, so the one
// client is the test's own.
//
// The launcher's block here is base 28480, so 28480-28497 (IMPL-COMMON, #625's block). The test floor and
// caps are small (1 GB, 64 MiB) so that a hosted runner's disk passes the free-space check.
//
// CI's launcher-k3s job is where these run (hack/test-launcher.sh); they are not run on the development VM.

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const eventLogK3sBase = 28480

// eventLogUpArgs is up with the log on, the test floor and the test caps, and the fake controller.
func eventLogUpArgs(h *host, bin string, extra ...string) []string {
	return h.upArgs(eventLogK3sBase, append([]string{"--json", "--controller-bin", bin, "--event-log",
		"--event-log-floor", "1GB", "--event-log-events-cap", "64MiB", "--event-log-usage-cap", "64MiB"}, extra...)...)
}

// logEntry is one entry of supervisor.json, with the fields these tests read.
type logEntry struct {
	Name      string     `json:"name"`
	State     string     `json:"state"`
	Gate      string     `json:"gate"`
	Proc      *stateProc `json:"proc"`
	StartedAt time.Time  `json:"started_at"`
	Restarts  int        `json:"restarts"`
	LastExit  *struct {
		Code   int    `json:"code"`
		Detail string `json:"detail"`
	} `json:"last_exit"`
}

func supervisorEntries(t *testing.T, dir string) []logEntry {
	t.Helper()
	var r struct {
		Entries []logEntry `json:"entries"`
	}
	if b := fileOf(filepath.Join(dir, "controller", "supervisor.json")); b != "" {
		parseJSON(t, b, &r)
	}
	return r.Entries
}

func entryByName(entries []logEntry, name string) *logEntry {
	for i := range entries {
		if entries[i].Name == name {
			return &entries[i]
		}
	}
	return nil
}

// eventLogStatus is status --json, with the parts of the log's entry these tests judge.
type eventLogStatus struct {
	Health      string   `json:"health"`
	Reasons     []string `json:"reasons"`
	Controllers []struct {
		Name      string `json:"name"`
		Running   bool   `json:"running"`
		PID       int    `json:"pid"`
		State     string `json:"state"`
		Restarts  int    `json:"restarts"`
		HealthzOK *bool  `json:"healthz_ok"`
		Degraded  string `json:"degraded"`
		Gate      string `json:"gate"`
		Streams   []struct {
			Name     string `json:"name"`
			Messages uint64 `json:"messages"`
			Bytes    uint64 `json:"bytes"`
			MaxBytes int64  `json:"max_bytes"`
		} `json:"streams"`
	} `json:"controllers"`
}

func (s eventLogStatus) log() (idx int) {
	for i, c := range s.Controllers {
		if c.Name == "eventlog" {
			return i
		}
	}
	return -1
}

func logStatusOf(t *testing.T, h *host) (eventLogStatus, int) {
	t.Helper()
	r := cli(t, h.args("status", "--json", "--no-gc-probe")...)
	var s eventLogStatus
	parseJSON(t, r.out, &s)
	return s, r.code
}

// logToken reads the token the way a client does.
func logToken(t *testing.T, dir string) string {
	t.Helper()
	return strings.TrimSpace(fileOf(agentapi.Host{StateDir: dir, PortBase: eventLogK3sBase}.EventLogTokenFile()))
}

func logClient(t *testing.T, dir string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect("nats://"+agentapi.Host{PortBase: eventLogK3sBase}.EventLogAddr(), nats.Token(logToken(t, dir)), nats.MaxReconnects(0))
	if err != nil {
		t.Fatalf("connect to the event log: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func noListener(t *testing.T, port int) {
	t.Helper()
	if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", itoa(port)), time.Second); err == nil {
		c.Close()
		t.Errorf("something still listens on port %d", port)
	}
}

// With up --event-log: the log starts before the controllers (the fake controller asks the log's
// /healthz when it starts, and the supervisor's record orders the starts), holds exactly its two
// loopback ports, takes its token from files (never a command line), and shows in status with its
// streams. A SIGKILL through a pidfd opened on the verified recorded process brings it back on the same
// ports, the wait status saying so. And down stops it last, after the controllers have gone.
func TestEventLogStartsFirstComesBackAfterAKillAndStopsLast(t *testing.T) {
	h := newHost(t)
	bin, ctlDir := fakeController(t)
	r := cli(t, eventLogUpArgs(h, bin)...)
	if r.code != exitOK {
		t.Fatalf("up --event-log exited %d: %s", r.code, r.err)
	}
	var up struct {
		EventLogAddr       string `json:"event_log_addr"`
		EventLogNotStarted string `json:"event_log_not_started"`
	}
	parseJSON(t, r.out, &up)
	if up.EventLogAddr != "127.0.0.1:28496" || up.EventLogNotStarted != "" {
		t.Errorf("up reports the log at %q, not started: %q; want 127.0.0.1:28496 and started", up.EventLogAddr, up.EventLogNotStarted)
	}

	entries := supervisorEntries(t, h.dir)
	ctl, lg := entryByName(entries, "controller"), entryByName(entries, "eventlog")
	if ctl == nil || lg == nil || ctl.Proc == nil || lg.Proc == nil || lg.State != "running" {
		t.Fatalf("supervisor.json: %+v", entries)
	}
	if !lg.StartedAt.Before(ctl.StartedAt) {
		t.Errorf("the log started at %v, not before the controller at %v", lg.StartedAt, ctl.StartedAt)
	}
	waitFor(t, 20*time.Second, "the fake controller's record of the log's /healthz at its start", func() bool {
		return fileOf(filepath.Join(ctlDir, "eventlog-probe-"+itoa(ctl.Proc.PID)+".txt")) != ""
	})
	if got := strings.TrimSpace(fileOf(filepath.Join(ctlDir, "eventlog-probe-"+itoa(ctl.Proc.PID)+".txt"))); got != "200" {
		t.Errorf("the log's /healthz when the controller started answered %q, want 200: the log was not up before it", got)
	}
	// The controller's command line has the token file and nothing of the token.
	var argv []string
	parseJSON(t, fileOf(filepath.Join(ctlDir, "argv-"+itoa(ctl.Proc.PID)+".json")), &argv)
	tokenFile := filepath.Join(h.dir, "eventlog", "token")
	if i := slices.Index(argv, "--event-log-token-file"); i < 0 || argv[i+1] != tokenFile {
		t.Errorf("the controller's command line %q lacks --event-log-token-file %s", argv, tokenFile)
	}

	// The files: a directory of mode 700, the rest 600, a token of 32 random bytes in hex.
	for path, mode := range map[string]os.FileMode{filepath.Join(h.dir, "eventlog"): 0o700, tokenFile: 0o600,
		filepath.Join(h.dir, "eventlog", "nats.conf"): 0o600, filepath.Join(h.dir, "eventlog", "caps.json"): 0o600} {
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != mode {
			t.Errorf("%s: %v, %v; want mode %v", path, fi, err, mode)
		}
	}
	token := logToken(t, h.dir)
	if !eventlog.ValidToken(token) {
		t.Fatalf("the token file holds %q, want 64 lower-case hex digits", token)
	}
	for _, pid := range []int{lg.Proc.PID, ctl.Proc.PID, recordedProcs(t, h.dir)[1].PID} {
		if strings.Contains(fileOf("/proc/"+itoa(pid)+"/cmdline"), token) {
			t.Errorf("the token is on the command line of pid %d", pid)
		}
	}
	// Exactly its two loopback ports.
	if got := listenersOf(t, lg.Proc.PID); len(got) != 2 || !slices.Contains(got, "127.0.0.1:28496") || !slices.Contains(got, "127.0.0.1:28497") {
		t.Errorf("the log listens on %v, want exactly 127.0.0.1:28496 and 127.0.0.1:28497", got)
	}

	// A client without the token is refused; with it, the two streams are there with the test caps.
	if nc, err := nats.Connect("nats://"+up.EventLogAddr, nats.MaxReconnects(0)); err == nil {
		nc.Close()
		t.Error("a client without the token connected")
	}
	waitFor(t, 30*time.Second, "status to report the host and the log healthy", func() bool {
		s, code := logStatusOf(t, h)
		return code == exitOK && s.Health == "healthy" && s.log() >= 0 && len(s.Controllers[s.log()].Streams) == 2
	})
	s, _ := logStatusOf(t, h)
	ls := s.Controllers[s.log()]
	if !ls.Running || ls.HealthzOK == nil || !*ls.HealthzOK || ls.Degraded != "" || ls.Streams[0].Name != "HB_EVENTS" || ls.Streams[0].MaxBytes != 64<<20 || ls.Streams[1].MaxBytes != 64<<20 {
		t.Errorf("status of the log: %+v", ls)
	}
	js := logClient(t, h.dir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 1; i <= 3; i++ {
		if _, err := js.PublishMsg(ctx, &nats.Msg{Subject: "hb.ev.instance.u1", Data: []byte("event " + itoa(i))}, jetstream.WithMsgID("e"+itoa(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// SIGKILL through a pidfd opened on the verified recorded process, never a recorded PID signalled
	// by number: the entry records the wait status, and the log comes back on the same ports.
	pf, err := unix.PidfdOpen(lg.Proc.PID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if startTicks(t, lg.Proc.PID) != lg.Proc.StartTicks {
		unix.Close(pf)
		t.Fatalf("pid %d is not the recorded log", lg.Proc.PID)
	}
	if err := unix.PidfdSendSignal(pf, syscall.SIGKILL, nil, 0); err != nil {
		unix.Close(pf)
		t.Fatal(err)
	}
	unix.Close(pf)
	waitFor(t, 60*time.Second, "the supervisor to start the killed log again", func() bool {
		e := entryByName(supervisorEntries(t, h.dir), "eventlog")
		return e != nil && e.State == "running" && e.Proc != nil && e.Proc.PID != lg.Proc.PID && e.Restarts == 1
	})
	restarted := entryByName(supervisorEntries(t, h.dir), "eventlog")
	if restarted.LastExit == nil || restarted.LastExit.Detail != "signal: killed" {
		t.Errorf("the log's last exit %+v, want signal: killed", restarted.LastExit)
	}
	waitFor(t, 30*time.Second, "status to report the log healthy again, with its three events", func() bool {
		s, code := logStatusOf(t, h)
		i := s.log()
		return code == exitOK && i >= 0 && len(s.Controllers[i].Streams) == 2 && s.Controllers[i].Streams[0].Messages == 3
	})

	// down: the log stops last. The supervisor's log says which stage came when, and each process's
	// exit.
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	// The log was SIGKILLed and restarted above, so its first exit line is the killed process's:
	// the order is read from the restarted log's own exit, by its PID, and from the stop's last stage
	// lines, which down writes once.
	log := fileOf(filepath.Join(h.dir, "logs", "supervisor.log"))
	idx := func(sub string) int { return strings.LastIndex(log, sub) }
	stopCtl, stopLog := idx("stopping the tunnel processes and the controllers"), idx("stopping the event log last")
	ctlExit := idx("controller: pid " + itoa(ctl.Proc.PID) + " exited")
	logExit := idx("eventlog: pid " + itoa(restarted.Proc.PID) + " exited")
	order := fmt.Sprintf("controllers' stage at byte %d, controller %d's exit at %d, the log's stage at %d, log %d's exit at %d",
		stopCtl, ctl.Proc.PID, ctlExit, stopLog, restarted.Proc.PID, logExit)
	if stopCtl < 0 || ctlExit < 0 || stopLog < 0 || logExit < 0 {
		t.Fatalf("the supervisor's log lacks a line of the stop (%s):\n%s", order, log)
	}
	if !(stopCtl < ctlExit && ctlExit < stopLog && stopLog < logExit) {
		t.Errorf("the log did not stop last (%s):\n%s", order, log)
	}
	noListener(t, 28496)
	noListener(t, 28497)
}

// A log that the caps' bound keeps out (here a floor no disk reaches, 100 TB): up still exits 0, with a
// warning that says why, the controller runs, status says "degraded: event log not started" with exit 0,
// the token is written all the same, and upgrade refuses before anything stops, naming the sizes.
// An upgrade that passes its floor can still leave the restarted log held back for space: the
// upgrade's floor counts the store and the previous copy, the start gate also what the streams can
// still grow. Upgrade then warns as up does, rather than exiting 0 saying nothing (Fable's review of
// #639). Caps of 1 TiB each keep the log gated on any runner, while a 1 GB floor lets the upgrade,
// with an empty store and no previous copy, go ahead.
func TestEventLogGatedAfterAnUpgradeIsWarnedAbout(t *testing.T) {
	h := newHost(t)
	bin, _ := fakeController(t)
	r := cli(t, h.upArgs(eventLogK3sBase, "--controller-bin", bin, "--version", "1.35.9", "--event-log",
		"--event-log-floor", "1GB", "--event-log-events-cap", "1TiB", "--event-log-usage-cap", "1TiB")...)
	if r.code != exitOK || !strings.Contains(r.err, "warning: the event log was not started") {
		t.Fatalf("up exited %d, stderr %q; want 0 and the log held back for space", r.code, r.err)
	}
	u := cli(t, h.args("upgrade", "--bin-dir", h.binDir, "--to", "1.36.5")...)
	if u.code != exitOK {
		t.Fatalf("upgrade exited %d: %s; want it to pass its floor and run", u.code, u.err)
	}
	if !strings.Contains(u.err, "warning: the event log was not started") || !strings.Contains(u.err, "floor 1 GB") {
		t.Errorf("upgrade's stderr %q; want up's warning, naming the floor", u.err)
	}
	if readState(t, h.dir).Version != "v1.36.5+k3s1" {
		t.Error("the upgrade did not record the new release")
	}
	if lg := entryByName(supervisorEntries(t, h.dir), "eventlog"); lg == nil || lg.State != "gated" || lg.Proc != nil {
		t.Errorf("the log's entry after the upgrade: %+v; want it gated, not running", lg)
	}
	noListener(t, 28496)
}

func TestEventLogRefusedForSpaceStartsTheHostWithoutItAndUpgradeRefuses(t *testing.T) {
	h := newHost(t)
	bin, _ := fakeController(t)
	r := cli(t, h.upArgs(eventLogK3sBase, "--json", "--controller-bin", bin, "--version", "1.35.9", "--event-log",
		"--event-log-floor", "100TB", "--event-log-events-cap", "64MiB", "--event-log-usage-cap", "64MiB")...)
	if r.code != exitOK {
		t.Fatalf("up with a log the disk cannot hold exited %d, want 0: %s", r.code, r.err)
	}
	var up struct {
		EventLogNotStarted string `json:"event_log_not_started"`
	}
	parseJSON(t, r.out, &up)
	if !strings.Contains(r.err, "warning: the event log was not started") || !strings.Contains(up.EventLogNotStarted, "floor 100 TB") {
		t.Errorf("up's warning and report: %q, %q; want both to name the floor", r.err, up.EventLogNotStarted)
	}
	if !eventlog.ValidToken(logToken(t, h.dir)) {
		t.Error("the token was not written although the log is not started")
	}
	entries := supervisorEntries(t, h.dir)
	if lg, ctl := entryByName(entries, "eventlog"), entryByName(entries, "controller"); lg == nil || lg.State != "gated" || lg.Proc != nil || ctl == nil || ctl.Proc == nil {
		t.Fatalf("the entries: %+v", entries)
	}
	waitFor(t, 30*time.Second, "status to report the host healthy with the log degraded", func() bool {
		s, code := logStatusOf(t, h)
		i := s.log()
		return code == exitOK && s.Health == "healthy" && i >= 0 && strings.HasPrefix(s.Controllers[i].Degraded, "event log not started: ")
	})
	if st := cli(t, h.args("status", "--no-gc-probe")...); st.code != exitOK || !strings.Contains(st.out, "degraded: event log not started: ") || !strings.Contains(st.out, "floor 100 TB") {
		t.Errorf("the text status exited %d:\n%s", st.code, st.out)
	}
	noListener(t, 28496)

	// upgrade: refused at the floor, before anything stops. The host keeps running its release.
	pid := recordedProcs(t, h.dir)[0].PID
	u := cli(t, h.args("upgrade", "--bin-dir", h.binDir, "--to", "1.36.5")...)
	if u.code != exitFail || !strings.Contains(u.err, "refusing to upgrade") || !strings.Contains(u.err, "floor of 100 TB") || !strings.Contains(u.err, "nothing was stopped") {
		t.Fatalf("upgrade exited %d, stderr %q; want it refused at the floor, naming the sizes", u.code, u.err)
	}
	if !running(pid) || readState(t, h.dir).Version != "v1.35.9+k3s1" {
		t.Error("the upgrade that was refused changed the host")
	}
}

// Upgrade with the log (HBEP-11, Test plan): the store is copied beside the SQLite backup once the host
// has stopped, copy.json names that backup, the log comes back with its events, and restore puts the
// copy back with that backup and says so.
func TestEventLogUpgradeCopiesItsStoreBesideTheBackupAndRestorePutsBothBack(t *testing.T) {
	h := newHost(t)
	bin, _ := fakeController(t)
	if r := cli(t, eventLogUpArgs(h, bin, "--version", "1.35.9")...); r.code != exitOK {
		t.Fatalf("up exited %d: %s", r.code, r.err)
	}
	var js jetstream.JetStream
	waitFor(t, 30*time.Second, "the log to hold both streams", func() bool {
		s, code := logStatusOf(t, h)
		i := s.log()
		return code == exitOK && i >= 0 && len(s.Controllers[i].Streams) == 2
	})
	js = logClient(t, h.dir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 1; i <= 3; i++ {
		if _, err := js.PublishMsg(ctx, &nats.Msg{Subject: "hb.ev.instance.u1", Data: []byte("event " + itoa(i))}, jetstream.WithMsgID("e"+itoa(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	r := cli(t, h.args("upgrade", "--bin-dir", h.binDir, "--to", "1.36.5", "--json")...)
	if r.code != exitOK {
		t.Fatalf("upgrade exited %d: %s", r.code, r.err)
	}
	var up struct {
		Backup string `json:"backup"`
	}
	parseJSON(t, r.out, &up)
	var info struct {
		Backup  string `json:"backup"`
		Version string `json:"version"`
	}
	parseJSON(t, fileOf(filepath.Join(h.dir, "backups", "eventlog", "copy.json")), &info)
	if info.Backup != filepath.Base(up.Backup) || info.Version != "v1.35.9+k3s1" {
		t.Errorf("copy.json %+v, want the backup %s and the old release", info, filepath.Base(up.Backup))
	}
	ents, _ := os.ReadDir(filepath.Join(h.dir, "backups"))
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	// Compared as sorted sets: the listing's order is the file system's.
	want := []string{"eventlog", filepath.Base(up.Backup)}
	slices.Sort(names)
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Errorf("backups/ holds %v, want exactly the SQLite backup and one log copy: %v", names, want)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "backups", "eventlog", "jetstream")); err != nil {
		t.Errorf("the copy holds no store: %v", err)
	}
	// The host is back, with the log and the three events: upgrade kept the choice and the store.
	waitFor(t, 60*time.Second, "status to report the upgraded host with the log's three events", func() bool {
		s, code := logStatusOf(t, h)
		i := s.log()
		return code == exitOK && i >= 0 && len(s.Controllers[i].Streams) == 2 && s.Controllers[i].Streams[0].Messages == 3
	})
	var rec struct {
		EventLog *struct {
			Caps  struct{ Events, Usage int64 } `json:"caps"`
			Floor int64                         `json:"floor"`
		} `json:"event_log"`
	}
	parseJSON(t, fileOf(filepath.Join(h.dir, "state.json")), &rec)
	if rec.EventLog == nil || rec.EventLog.Caps.Events != 64<<20 || rec.EventLog.Floor != 1_000_000_000 {
		t.Errorf("state.json after the upgrade: %+v, want the log kept with its caps and floor", rec.EventLog)
	}

	// restore: the host down first. The copy sits beside this backup, so it goes back.
	if d := cli(t, h.args("down")...); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	rr := cli(t, h.args("restore", "--backup", up.Backup, "--json")...)
	if rr.code != exitOK {
		t.Fatalf("restore exited %d: %s", rr.code, rr.err)
	}
	var restored struct {
		EventLog string `json:"event_log"`
	}
	parseJSON(t, rr.out, &restored)
	if !strings.HasPrefix(restored.EventLog, "restored the event log's store") {
		t.Errorf("restore reports the log as %q, want it restored with its paired backup", restored.EventLog)
	}
	copied := fileOf(filepath.Join(h.dir, "backups", "eventlog", "jetstream", "$G", "streams", "HB_EVENTS", "msgs", "1.blk"))
	live := fileOf(filepath.Join(h.dir, "eventlog", "jetstream", "$G", "streams", "HB_EVENTS", "msgs", "1.blk"))
	if copied == "" || copied != live {
		t.Errorf("the restored events block (%d bytes) is not the copy's (%d bytes)", len(live), len(copied))
	}
	// The restored host starts the old release again, and its log holds the three events.
	if r := cli(t, eventLogUpArgs(h, bin)...); r.code != exitOK {
		t.Fatalf("up after the restore exited %d: %s", r.code, r.err)
	}
	waitFor(t, 60*time.Second, "the restored log to hold its three events", func() bool {
		s, code := logStatusOf(t, h)
		i := s.log()
		return code == exitOK && i >= 0 && len(s.Controllers[i].Streams) == 2 && s.Controllers[i].Streams[0].Messages == 3
	})
}
