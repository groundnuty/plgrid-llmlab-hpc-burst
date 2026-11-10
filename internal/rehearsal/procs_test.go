package rehearsal

// New. Issue #308: the guards between the sshd sweep and a process the test
// did not start, each tested alone. No test here sends a real signal: every
// send and kill goes to a sink that records it, through the procAPI fields
// the pool and signalProcs call in production. Each guard is also shown alone
// at the pool, with the other two replaced through the pool's seams (walk and
// signal) by stand-ins that do no checking.

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// ownRow is this test process's row, the root below takes for "this test".
func ownRow() procRow {
	r, ok := readProcRow(os.Getpid())
	if !ok {
		panic("rehearsal: this test process's own /proc row cannot be read")
	}
	return r
}

// sink records what the pool would have signalled. Its fake pidfds are the
// PID plus fdBase, so a send names the process it would have reached.
type sink struct {
	mu     sync.Mutex
	sent   []int // PIDs, through send or unchecked
	killed []int // PIDs, through kill
	// events are "open <pid>" and, from a fakeHost, "row <pid>", in order.
	events []string
	// check, when set, is called with each PID sent, and an error it returns
	// is kept in bad.
	check func(pid int) error
	bad   []error
}

const fdBase = 1 << 24

// fake replaces every field of api that could send a signal with the sink's,
// keeping the reads.
func (s *sink) fake(api *procAPI) *procAPI {
	api.send = func(fd int, sig syscall.Signal) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		pid := fd - fdBase
		s.sent = append(s.sent, pid)
		if s.check != nil {
			if err := s.check(pid); err != nil {
				s.bad = append(s.bad, err)
			}
		}
		return nil
	}
	api.kill = func(p *os.Process) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.killed = append(s.killed, p.Pid)
		return nil
	}
	api.probe = func(int) error { return syscall.ESRCH }
	api.open = func(pid int) (int, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.events = append(s.events, "open "+strconv.Itoa(pid))
		return pid + fdBase, nil
	}
	api.closeFd = func(int) {}
	return api
}

// unchecked stands in for signalProcs with guard 3 gone: every row it is
// given counts as signalled.
func (s *sink) unchecked(_ *procAPI, rows []procRow, _ syscall.Signal) ([]procRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range rows {
		s.sent = append(s.sent, r.pid)
	}
	return rows, nil
}

// hostWide stands in for below with guard 2 gone: the walk from any root
// lists every live process, as the walk from PID 0 did.
func hostWide(procRow) []procRow { return liveProcs() }

func (s *sink) snapshot() (sent, killed []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sent), slices.Clone(s.killed)
}

// Guard 1 alone. An sshd that has exited when its row is read (a client gone
// before auth) is killed through its os.Process and reaped, and never stored,
// so no walk can start from its empty row. Guards 2 and 3 are replaced by
// stand-ins that walk the whole host and signal whatever they are given, so
// nothing but guard 1 stands in the way. A child that exits at once stands in
// for sshd, and the test waits until it is a zombie before its row is read:
// the state the issue names, exited and unreaped, built for real.
func TestAnExitedSSHDIsKilledThroughItsProcessAndNeverRecorded(t *testing.T) {
	bin, err := exec.LookPath("true")
	if err != nil {
		t.Fatalf("the stand-in for an sshd that exits at once: %v", err)
	}
	log, err := os.Create(filepath.Join(t.TempDir(), "sshd.log"))
	if err != nil {
		t.Fatal(err)
	}
	var (
		rec     sink
		mu      sync.Mutex
		pid     int
		zombie  bool
		hookErr error
	)
	api := rec.fake(hostProcs())
	api.started = func(p int) {
		deadline := time.Now().Add(waitDeadline)
		for {
			_, live, err := statRow(p)
			if err == nil && !live {
				break
			}
			if time.Now().After(deadline) {
				mu.Lock()
				hookErr = errors.New("the stand-in sshd never became a zombie")
				mu.Unlock()
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		mu.Lock()
		pid, zombie = p, hookErr == nil
		mu.Unlock()
	}
	pool := &sshdPool{bin: bin, conf: os.DevNull, log: log, api: api, walk: hostWide, signal: rec.unchecked, wait: 100 * time.Millisecond}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() }) // a second Close is harmless
	pool.wg.Add(1)
	go pool.serve(ln)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// spawn has ended once the sshd is killed, or stored.
	deadline := time.Now().Add(waitDeadline)
	for {
		_, killed := rec.snapshot()
		pool.mu.Lock()
		stored := len(pool.procs)
		pool.mu.Unlock()
		if len(killed) > 0 || stored > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("spawn neither killed nor stored the sshd")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ln.Close()
	stopErr := pool.stop(nil)
	log.Close()

	mu.Lock()
	defer mu.Unlock()
	if hookErr != nil || !zombie {
		t.Fatalf("the exited sshd was not built: %v", hookErr)
	}
	if n := len(pool.procs); n != 0 {
		t.Errorf("the pool stored %d sshds whose row it could not read; want none (row %+v)", n, pool.procs[0].self)
	}
	sent, killed := rec.snapshot()
	if !slices.Equal(killed, []int{pid}) {
		t.Errorf("killed through os.Process: %v, want the exited sshd %d alone", killed, pid)
	}
	if len(sent) != 0 {
		t.Errorf("with guards 2 and 3 gone, the stop signalled %d processes; want none", len(sent))
	}
	if stopErr != nil {
		t.Errorf("stop: %v, want nil", stopErr)
	}
	// Reaped by spawn: the test has no such child to wait for. WNOWAIT leaves
	// any child that has the number now unreaped.
	var info unix.Siginfo
	if err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil); !errors.Is(err, syscall.ECHILD) {
		t.Errorf("waitid(%d) after stop: %v; want ECHILD, the sshd reaped by spawn", pid, err)
	}
	if b, err := os.ReadFile(log.Name()); err != nil || !strings.Contains(string(b), "sshd "+strconv.Itoa(pid)+" had no live row") {
		t.Errorf("sshd.log does not say the exited sshd was killed and not recorded: %q, %v", b, err)
	}
}

// The issue's case end to end, with every guard in place: a stored zero row
// (what spawn stored before guard 1) and the stop of the pool signal nothing,
// over the host's real /proc. Guards 2 and 3 each stop it, so this test kills
// no mutant of one guard; the tests of each guard alone do.
func TestAZeroRowSignalsNothing(t *testing.T) {
	var rec sink
	pool := &sshdPool{api: rec.fake(hostProcs()), wait: 100 * time.Millisecond}
	p := &sshdProc{done: make(chan struct{}), below: map[int]procRow{}}
	pool.procs = []*sshdProc{p}
	if err := pool.stop(nil); err != nil {
		t.Errorf("stop: %v, want nil", err)
	}
	if sent, killed := rec.snapshot(); len(sent)+len(killed) != 0 {
		t.Errorf("a zero row signalled %v and killed %v; want nothing", sent, killed)
	}
	if rows := p.seen(); len(rows) != 0 {
		t.Errorf("recorded below a zero row: %d processes, want none", len(rows))
	}
}

// Guard 2. below walks nothing from PID 1 or less, from a row with no start
// time, nor from a root whose start time no longer matches. The listing is a
// host's shape: PID 1 and kthreadd with parent 0, everything else below them.
func TestBelowWalksNothingFromPID1OrLessOrFromAnotherProcess(t *testing.T) {
	host := []procRow{
		{pid: 1, ppid: 0, ticks: 10}, {pid: 2, ppid: 0, ticks: 10},
		{pid: 40, ppid: 2, ticks: 0}, {pid: 41, ppid: 40, ticks: 12}, {pid: 300, ppid: 1, ticks: 20},
		{pid: 700, ppid: 300, ticks: 40}, {pid: 701, ppid: 700, ticks: 41}, {pid: 702, ppid: 701, ticks: 42},
	}
	pids := func(rows []procRow) []int {
		var out []int
		for _, r := range rows {
			out = append(out, r.pid)
		}
		slices.Sort(out)
		return out
	}
	// The control: a root that is in the listing with its start time has its
	// descendants, so a nil below is the guard's.
	if got := pids(belowIn(host, procRow{pid: 700, ppid: 300, ticks: 40})); !slices.Equal(got, []int{701, 702}) {
		t.Fatalf("below the root 700: %v, want [701 702]", got)
	}
	for _, c := range []struct {
		name string
		root procRow
	}{
		{"a zero row", procRow{}},
		{"PID 0 with a start time", procRow{pid: 0, ticks: 10}},
		{"PID 1 as listed", procRow{pid: 1, ppid: 0, ticks: 10}},
		{"a negative PID", procRow{pid: -1}},
		{"a row with no start time, as listed", procRow{pid: 40, ppid: 2, ticks: 0}},
		{"a root whose PID names another process now", procRow{pid: 700, ppid: 300, ticks: 39}},
		{"a root that is gone", procRow{pid: 999, ppid: 1, ticks: 50}},
	} {
		if got := belowIn(host, c.root); got != nil {
			t.Errorf("%s: below walked %v, want nothing", c.name, pids(got))
		}
	}
	// The host's own /proc: PID 1 is always listed, with its start time.
	pid1, ok := readProcRow(1)
	if !ok {
		t.Fatal("PID 1's row cannot be read")
	}
	for _, root := range []procRow{{}, pid1} {
		if got := below(root); got != nil {
			t.Errorf("below PID %d on this host: %d processes, want none", root.pid, len(got))
		}
	}
}

// Guard 2 alone, at the pool: guard 1 is passed by storing the rows it would
// refuse, and guard 3 is replaced by a stand-in that signals whatever it is
// given. A zero row, PID 1's row and a stale root (this test's own PID with
// another start time) record nothing below them, so the stop passes on the
// stored rows themselves and nothing else.
func TestGuard2AloneWalksNothingFromAZeroOrAStaleRoot(t *testing.T) {
	pid1, ok := readProcRow(1)
	if !ok {
		t.Fatal("PID 1's row cannot be read")
	}
	stale := ownRow()
	stale.ticks++
	var rec sink
	pool := &sshdPool{api: rec.fake(hostProcs()), walk: below, signal: rec.unchecked, wait: 100 * time.Millisecond}
	for _, self := range []procRow{{}, pid1, stale} {
		pool.procs = append(pool.procs, &sshdProc{self: self, done: make(chan struct{}), below: map[int]procRow{}})
	}
	if err := pool.stop(nil); err != nil {
		t.Errorf("stop: %v, want nil", err)
	}
	for _, p := range pool.procs {
		if rows := p.seen(); len(rows) != 0 {
			t.Errorf("recorded %d processes below the root %+v; want none", len(rows), p.self)
		}
	}
	sent, _ := rec.snapshot()
	slices.Sort(sent)
	if want := []int{1, stale.pid}; !slices.Equal(sent, want) {
		t.Errorf("passed on %v, want the stored rows alone %v", sent, want)
	}
}

// A refusal reaches the caller: Close returns it, twice.
func TestARefusalIsTheErrorOfClose(t *testing.T) {
	var rec sink
	log, err := os.Create(filepath.Join(t.TempDir(), "sshd.log"))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pool := &sshdPool{log: log, api: rec.fake(hostProcs()), wait: 100 * time.Millisecond}
	// A row spawn can no longer store: PID 1, as if it had been read for an sshd.
	pid1, ok := readProcRow(1)
	if !ok {
		t.Fatal("PID 1's row cannot be read")
	}
	pool.procs = []*sshdProc{{self: pid1, done: make(chan struct{}), below: map[int]procRow{}}}
	g := &Gateway{srv: &sshdServer{ln: ln, pool: pool, log: log}}
	first := g.Close()
	if first == nil || !strings.Contains(first.Error(), "refused to send terminated to process 1: ") {
		t.Errorf("Close: %v, want PID 1 refused", first)
	}
	if again := g.Close(); again != first {
		t.Errorf("the second Close: %v, want the first one's error", again)
	}
	if sent, _ := rec.snapshot(); len(sent) != 0 {
		t.Errorf("signalled %v, want nothing", sent)
	}
}

// withRelayEnv selects nothing for a path that is not absolute: an empty one
// would match any process whose variable is set and empty.
func TestWithRelayEnvSelectsNothingForAnEmptyPath(t *testing.T) {
	for _, p := range []string{"", "relay.sock"} {
		if got := withRelayEnv(p); got != nil {
			t.Errorf("withRelayEnv(%q): %d processes, want none", p, len(got))
		}
	}
}
