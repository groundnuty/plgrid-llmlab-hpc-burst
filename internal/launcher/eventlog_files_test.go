package launcher

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The block with the log: base 28480 is 28480-28497 (the 13 ports, base+13 to base+15 the bulk path's
// whether or not it runs, base+16 and base+17 the log's), and the block grows only when asked.
func TestPortsWithTheEventLog(t *testing.T) {
	p, err := PortsFromBase(28480)
	if err != nil {
		t.Fatal(err)
	}
	seq := func(from, to int) []int {
		var out []int
		for i := from; i <= to; i++ {
			out = append(out, i)
		}
		return out
	}
	if got := p.All(); !slices.Equal(got, seq(28480, 28492)) {
		t.Errorf("the block without the log: %v, want 28480-28492 (unchanged)", got)
	}
	bulk, _ := p.WithBulkPath()
	if got := bulk.All(); !slices.Equal(got, seq(28480, 28495)) {
		t.Errorf("the block with the bulk path: %v, want 28480-28495 (unchanged)", got)
	}
	withLog, err := p.WithEventLog()
	if err != nil {
		t.Fatal(err)
	}
	if got := withLog.All(); !slices.Equal(got, seq(28480, 28497)) {
		t.Errorf("the block with the log: %v, want 28480-28497: the log's two ports and, since base+13 to base+15 are the bulk path's, those too", got)
	}
	both, err := bulk.WithEventLog()
	if err != nil || !slices.Equal(both.All(), seq(28480, 28497)) || !both.BulkPath || !both.EventLog {
		t.Errorf("the block with both: %v, %v", both.All(), err)
	}
	if withLog.EventLogAddr() != "127.0.0.1:28496" || withLog.EventLogMonitorAddr() != "127.0.0.1:28497" {
		t.Errorf("the log's addresses %s and %s, want 127.0.0.1:28496 and 127.0.0.1:28497", withLog.EventLogAddr(), withLog.EventLogMonitorAddr())
	}
	// Expected (k3s's three) is unchanged: the log's ports are judged per supervised process.
	if !slices.Equal(withLog.Expected(), p.Expected()) {
		t.Errorf("Expected changed: %v", withLog.Expected())
	}
	if _, err := p2(t, 65520).WithEventLog(); err == nil || !strings.Contains(err.Error(), "65518") {
		t.Errorf("a base that cannot hold the block: %v, want an error naming the highest base, 65518", err)
	}
	if _, err := p2(t, 65518).WithEventLog(); err != nil {
		t.Errorf("the highest base was refused: %v", err)
	}
}

func p2(t *testing.T, base int) Ports {
	t.Helper()
	p, err := PortsFromBase(base)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// The record: the choice, the caps and the floor are state, and survive its JSON round trip, the
// launcher's own copy of them (upgrade reads them back from it).
func TestStateKeepsTheEventLogChoice(t *testing.T) {
	l := layout{dir: filepath.Join(t.TempDir(), "st")}
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	self, err := newRec("k3s", os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	ports, _ := PortsFromBase(28480)
	ports, _ = ports.WithEventLog()
	want := &State{Ports: ports, Procs: []ProcRec{self},
		EventLog: &EventLogConfig{Caps: eventlog.Caps{Events: 2 << 30, Usage: 64 << 20}, Floor: 15_000_000_000}}
	if err := l.saveState(want); err != nil {
		t.Fatal(err)
	}
	got, err := l.loadState()
	if err != nil {
		t.Fatal(err)
	}
	if got.EventLog == nil || *got.EventLog != *want.EventLog || !got.Ports.EventLog {
		t.Errorf("the record reads back %+v, ports %+v", got.EventLog, got.Ports)
	}
	// The JSON names are the record's contract; written out by hand.
	b, _ := json.Marshal(want.EventLog)
	if string(b) != `{"caps":{"events":2147483648,"usage":67108864},"floor":15000000000}` {
		t.Errorf("the event log's record is %s", b)
	}
	// Without the log, the record is as it was: no field.
	noLog, _ := PortsFromBase(28480)
	pb, _ := json.Marshal(&State{Ports: noLog, Procs: []ProcRec{self}})
	if strings.Contains(string(pb), "event_log") {
		t.Errorf("a record with no log names one: %s", pb)
	}
	if l.eventLogDir() != (agentapi.Host{StateDir: l.dir}).EventLogDir() || l.eventLogDir() != filepath.Join(l.dir, "eventlog") {
		t.Errorf("the launcher and the shared layout disagree about the store: %s, %s", l.eventLogDir(), agentapi.Host{StateDir: l.dir}.EventLogDir())
	}
}

// The supervised set: the log is the last entry (the order it starts and stops in is the supervisor's
// own, not the slice's), the controller and the front get the token file only with the log on, and
// without it every command line is as before.
func TestSupervisedSetWithTheEventLog(t *testing.T) {
	l := layout{dir: "/run/st"}
	ports, _ := PortsFromBase(28480)
	ports, _ = ports.WithEventLog()
	st := &State{Ports: ports, Controllers: &ControllerConfig{Binary: "/opt/hpcburst-controller"},
		FrontRun: &FrontRunConfig{Binary: "/opt/hpcburst-front", Config: "/etc/front.yaml"}, Front: Front{Mode: "host-dialed"},
		EventLog: &EventLogConfig{Caps: eventlog.Caps{Events: 1, Usage: 1}, Floor: 1}}
	set, err := st.supervisedSet(l)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range set {
		names = append(names, s.Name)
	}
	if !slices.Equal(names, []string{"controller", "front", "eventlog"}) {
		t.Fatalf("the supervised set %v, want the controller, the front, then the log", names)
	}
	token := "/run/st/eventlog/token"
	for _, s := range set[:2] {
		if i := slices.Index(s.Args, "--event-log-token-file"); i < 0 || s.Args[i+1] != token {
			t.Errorf("%s's arguments %v lack --event-log-token-file %s", s.Name, s.Args, token)
		}
	}
	log := set[2]
	want := supervised{Name: "eventlog", Path: "/opt/hpcburst-controller",
		Args:   []string{"eventlog", "--config", "/run/st/eventlog/nats.conf", "--caps-file", "/run/st/eventlog/caps.json"},
		Health: "127.0.0.1:28497", Ports: []int{28496, 28497}, Setsid: true, WaitReady: true, TermGrace: EventLogTermGrace}
	if log.Name != want.Name || log.Path != want.Path || !slices.Equal(log.Args, want.Args) || log.Health != want.Health || !slices.Equal(log.Ports, want.Ports) ||
		!log.Setsid || !log.WaitReady || log.TermGrace != want.TermGrace || log.HealthSocket != "" {
		t.Errorf("the log's entry %+v, want %+v", log, want)
	}
	if strings.Contains(strings.Join(log.Args, " "), "token") && strings.Contains(strings.Join(log.Args, " "), "--event-log-token") {
		t.Error("the log's command line carries a token flag")
	}

	// Off: the same record without the log gives the command lines as before, byte for byte.
	st.EventLog = nil
	st.Ports.EventLog = false
	off, err := st.supervisedSet(l)
	if err != nil {
		t.Fatal(err)
	}
	if len(off) != 2 {
		t.Fatalf("without the log the set is %d entries", len(off))
	}
	for _, s := range off {
		if slices.Contains(s.Args, "--event-log-token-file") {
			t.Errorf("%s gets the token file without the log: %v", s.Name, s.Args)
		}
	}
}

// up's files for the log (HBEP-8, Where it runs): a directory of mode 700 with nats.conf, token and
// caps.json at mode 600, the token 32 random bytes in hex and the same in the configuration, kept
// when it is well formed, and the stale refusal removed. The configuration's text is written out here.
func TestPrepareEventLogFiles(t *testing.T) {
	host := agentapi.Host{StateDir: filepath.Join(t.TempDir(), "st"), PortBase: 28480}
	if err := os.MkdirAll(host.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := EventLogConfig{Caps: eventlog.Caps{Events: 2 << 30, Usage: 64 << 20}, Floor: 15_000_000_000}
	if err := os.MkdirAll(host.EventLogDir(), 0o755); err != nil { // an older directory, too open
		t.Fatal(err)
	}
	stale := filepath.Join(host.EventLogDir(), eventlog.RefusalFile)
	if err := os.WriteFile(stale, []byte("sync_interval\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepareEventLogFiles(host, cfg); err != nil {
		t.Fatal(err)
	}
	dir := host.EventLogDir()
	for path, mode := range map[string]os.FileMode{dir: 0o700, host.EventLogTokenFile(): 0o600, filepath.Join(dir, "nats.conf"): 0o600, filepath.Join(dir, "caps.json"): 0o600} {
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != mode {
			t.Errorf("%s: %v, %v; want mode %v", path, fi, err, mode)
		}
	}
	if _, err := os.Stat(stale); err == nil {
		t.Error("the stale refusal was kept")
	}
	tokenBytes, _ := os.ReadFile(host.EventLogTokenFile())
	token := string(tokenBytes)
	if len(token) != 64 || !eventlog.ValidToken(token) {
		t.Fatalf("the token file holds %q, want 64 hex digits", token)
	}
	conf, _ := os.ReadFile(filepath.Join(dir, "nats.conf"))
	wantConf := "listen: 127.0.0.1:28496\nhttp: 127.0.0.1:28497\nauthorization {\n  token: \"" + token + "\"\n}\njetstream {\n  store_dir: \"" + dir + "\"\n  sync_interval: always\n}\n"
	if string(conf) != wantConf {
		t.Errorf("nats.conf:\n%s\nwant:\n%s", conf, wantConf)
	}
	caps, _ := os.ReadFile(filepath.Join(dir, "caps.json"))
	if string(caps) != `{"events":2147483648,"usage":67108864}`+"\n" {
		t.Errorf("caps.json: %s", caps)
	}

	// A second start keeps the token, and the caps follow the record.
	cfg.Caps.Events = 1 << 30
	if err := prepareEventLogFiles(host, cfg); err != nil {
		t.Fatal(err)
	}
	if again, _ := os.ReadFile(host.EventLogTokenFile()); string(again) != token {
		t.Error("a second start changed a well-formed token")
	}
	if caps, _ := os.ReadFile(filepath.Join(dir, "caps.json")); !strings.Contains(string(caps), `"events":1073741824`) {
		t.Errorf("caps.json after the change: %s", caps)
	}
	// A token that is not well formed, or loosely held, is made right: replaced, and mode 600.
	if err := os.WriteFile(host.EventLogTokenFile(), []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := prepareEventLogFiles(host, cfg); err != nil {
		t.Fatal(err)
	}
	if fresh, _ := os.ReadFile(host.EventLogTokenFile()); !eventlog.ValidToken(string(fresh)) || string(fresh) == "short" {
		t.Errorf("the token after a bad one: %q", fresh)
	}
	if fi, _ := os.Stat(host.EventLogTokenFile()); fi.Mode().Perm() != 0o600 {
		t.Errorf("the token's mode %v", fi.Mode().Perm())
	}
	// A record that names no caps is refused before anything is written.
	if err := prepareEventLogFiles(host, EventLogConfig{}); err == nil {
		t.Error("an empty record was accepted")
	}
}

func TestEventLogConfigDefaultsAndValidation(t *testing.T) {
	d := EventLogConfig{}.withDefaults()
	if d.Caps.Events != 2147483648 || d.Caps.Usage != 2147483648 || d.Floor != 15_000_000_000 {
		t.Errorf("the defaults %+v, want 2 GiB, 2 GiB and 15 GB", d)
	}
	if err := d.Validate(); err != nil {
		t.Error(err)
	}
	given := EventLogConfig{Caps: eventlog.Caps{Events: 64 << 20}, Floor: 1_000_000_000}.withDefaults()
	if given.Caps.Events != 64<<20 || given.Caps.Usage != 2147483648 || given.Floor != 1_000_000_000 {
		t.Errorf("a given size was overwritten: %+v", given)
	}
	if err := (EventLogConfig{Caps: eventlog.Caps{Events: 1, Usage: 1}}).Validate(); err == nil {
		t.Error("a zero floor was valid")
	}
}
