package rehearsal

// New. Issue #308: the three guards between the sshd sweep and a process the
// test did not start, each tested alone. No test here sends a real signal:
// every send and kill goes to a sink that records it, through the procAPI
// fields the pool and signalProcs call in production.

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
	sent   []int // PIDs, through send
	killed []int // PIDs, through kill
	// events are "open <pid>" and, from a fakeHost, "row <pid>", in order.
	events []string
}

const fdBase = 1 << 24

// fake replaces every field of api that could send a signal with the sink's,
// keeping the reads.
func (s *sink) fake(api *procAPI) *procAPI {
	api.send = func(fd int, sig syscall.Signal) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.sent = append(s.sent, fd-fdBase)
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

func (s *sink) snapshot() (sent, killed []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sent), slices.Clone(s.killed)
}

// Guard 1. An sshd that has exited when its row is read (a client gone before
// auth) is killed through its os.Process and reaped, and never stored, so no
// walk can start from its empty row. A child that exits at once stands in for
// sshd, and the test waits until it is a zombie before its row is read: the
// state the issue names, exited and unreaped, built for real.
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
	pool := &sshdPool{bin: bin, conf: os.DevNull, log: log, api: api}
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
		t.Errorf("signals sent: %v, want none", sent)
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

// The issue's case end to end: a stored zero row (what spawn stored before
// guard 1) and the stop of the pool signal nothing, over the host's real
// /proc. Guards 2 and 3 each stop it, so this test kills no mutant of one
// guard alone; the tests of each guard do.
func TestAZeroRowSignalsNothing(t *testing.T) {
	var rec sink
	pool := &sshdPool{api: rec.fake(hostProcs())}
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

// Guard 2. below walks nothing from PID 1 or less, nor from a root whose
// start time no longer matches. The listing is a host's shape: PID 1 and
// kthreadd with parent 0, everything else below them.
func TestBelowWalksNothingFromPID1OrLessOrFromAnotherProcess(t *testing.T) {
	host := []procRow{
		{pid: 1, ppid: 0, ticks: 10}, {pid: 2, ppid: 0, ticks: 10},
		{pid: 40, ppid: 2, ticks: 11}, {pid: 300, ppid: 1, ticks: 20},
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

// fakeHost is a process table for signalProcs: rows by PID and cgroups by PID.
type fakeHost struct {
	rows    map[int]procRow
	cgroups map[int]string
}

func (h fakeHost) api(self int, rec *sink) *procAPI {
	api := rec.fake(&procAPI{self: self})
	api.row = func(pid int) (procRow, bool, error) {
		rec.mu.Lock()
		rec.events = append(rec.events, "row "+strconv.Itoa(pid))
		rec.mu.Unlock()
		r, ok := h.rows[pid]
		return r, ok, nil
	}
	api.cgroup = func(pid int) (string, error) {
		if cg, ok := h.cgroups[pid]; ok {
			return cg, nil
		}
		return "", os.ErrNotExist
	}
	open := api.open
	api.open = func(pid int) (int, error) {
		if pid <= 0 {
			return 0, syscall.EINVAL // as pidfd_open answers
		}
		if _, ok := h.rows[pid]; !ok {
			return 0, syscall.ESRCH
		}
		return open(pid)
	}
	return api
}

// Guard 3. signalProcs refuses PID 1 and below, the process itself, its
// ancestors, and a process neither below it nor in its cgroup, and says so in
// its error, while what is the test's is still signalled. The table puts each
// refused process where only its own check refuses it: PID 1 and the ancestor
// in the test's cgroup (a container, a systemd-run scope that also holds
// timeout and go test), the stranger in another cgroup and outside the tree.
func TestSignalProcsRefusesWhatIsNotTheTests(t *testing.T) {
	const (
		self      = 5000
		testCg    = "0::/user.slice/example.slice/run-example.scope\n"
		managerCg = "0::/user.slice/example.slice/user@1000.service/init.scope\n"
	)
	h := fakeHost{
		rows: map[int]procRow{
			1:    {pid: 1, ppid: 0, ticks: 1},
			3000: {pid: 3000, ppid: 1, ticks: 30},    // go test
			4000: {pid: 4000, ppid: 3000, ticks: 40}, // timeout
			self: {pid: self, ppid: 4000, ticks: 50},
			6000: {pid: 6000, ppid: 1, ticks: 60},    // the user manager
			6100: {pid: 6100, ppid: self, ticks: 61}, // an sshd, below the test
			6110: {pid: 6110, ppid: 6100, ticks: 62}, // its session, in another cgroup
			6200: {pid: 6200, ppid: 6000, ticks: 63}, // a forced command reparented to the manager
			6300: {pid: 6300, ppid: self, ticks: 99}, // listed with 64: recycled since
		},
		cgroups: map[int]string{
			1: testCg, 3000: testCg, 4000: testCg, self: testCg,
			6000: managerCg, 6100: testCg, 6110: "0::/elsewhere.scope\n", 6200: testCg, 6300: testCg,
		},
	}
	var rec sink
	listed := []procRow{
		{pid: 0}, {pid: -1}, {pid: 1, ticks: 1}, {pid: self, ticks: 50}, {pid: 4000, ticks: 40}, {pid: 6000, ticks: 60},
		{pid: 6100, ticks: 61}, {pid: 6110, ticks: 62}, {pid: 6200, ticks: 63}, {pid: 6300, ticks: 64}, {pid: 6400, ticks: 65},
	}
	kept, err := signalProcs(h.api(self, &rec), listed, syscall.SIGTERM)
	sent, killed := rec.snapshot()
	slices.Sort(sent)
	if !slices.Equal(sent, []int{6100, 6110, 6200}) {
		t.Errorf("signalled %v, want [6100 6110 6200]: the sshd, its session and the reparented forced command", sent)
	}
	if len(killed) != 0 {
		t.Errorf("killed through os.Process: %v, want none", killed)
	}
	if err == nil {
		t.Fatal("no error for the refusals")
	}
	for pid, why := range map[int]string{
		0: "PID 1 and below", -1: "PID 1 and below", 1: "PID 1 and below", self: "the process that sends it",
		4000: "an ancestor", 6000: "neither below",
	} {
		if want := "process " + strconv.Itoa(pid) + ": "; !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), why) {
			t.Errorf("the error does not refuse process %d (%s): %v", pid, why, err)
		}
	}
	var keptPIDs []int
	for _, r := range kept {
		keptPIDs = append(keptPIDs, r.pid)
	}
	slices.Sort(keptPIDs)
	if !slices.Equal(keptPIDs, []int{6100, 6110, 6200, 6300, 6400}) {
		t.Errorf("kept %v, want the signalled, the recycled and the gone ones", keptPIDs)
	}
	// Each process is pinned with a pidfd before its row is checked: none of
	// these is read in the walk up the test's ancestry, so the first read of
	// its row is its check, and it comes after its open.
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, pid := range []int{6100, 6110, 6200, 6300} {
		o := slices.Index(rec.events, "open "+strconv.Itoa(pid))
		r := slices.Index(rec.events, "row "+strconv.Itoa(pid))
		if o < 0 || r < o {
			t.Errorf("process %d: pinned at event %d, its row read at event %d; want the pin first (%v)", pid, o, r, rec.events)
		}
	}
}

// A refusal costs no wait and is not retried with SIGKILL, and a check that
// cannot be made refuses: an ancestry that cannot be read refuses everything.
func TestSignalProcsRefusesEverythingWhenItsAncestryCannotBeRead(t *testing.T) {
	var rec sink
	api := fakeHost{rows: map[int]procRow{6100: {pid: 6100, ppid: 5000, ticks: 61}}}.api(5000, &rec)
	kept, err := signalProcs(api, []procRow{{pid: 6100, ticks: 61}}, syscall.SIGTERM)
	if err == nil || !strings.Contains(err.Error(), "the ancestry of process 5000") {
		t.Errorf("error %v, want the unreadable ancestry", err)
	}
	if sent, _ := rec.snapshot(); len(sent)+len(kept) != 0 {
		t.Errorf("signalled %v and kept %v with no ancestry; want neither", sent, kept)
	}
}

// Guard 3 over this host's real /proc, as the walk from PID 0 listed it:
// every live process. PID 1, the test process and its parent are never
// signalled, and the refusals are an error.
func TestSignalProcsOverTheWholeHostSparesInitItselfAndItsParent(t *testing.T) {
	var rec sink
	api := rec.fake(hostProcs())
	all := liveProcs()
	if len(all) < 3 {
		t.Fatalf("listed %d processes; want the host's", len(all))
	}
	_, err := signalProcs(api, all, syscall.SIGTERM)
	sent, killed := rec.snapshot()
	for _, pid := range []int{1, os.Getpid(), os.Getppid()} {
		if slices.Contains(sent, pid) {
			t.Errorf("signalled process %d", pid)
		}
	}
	if len(killed) != 0 {
		t.Errorf("killed through os.Process: %v, want none", killed)
	}
	if err == nil || !strings.Contains(err.Error(), "process 1: ") {
		t.Errorf("error %v, want PID 1 refused", err)
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
	pool := &sshdPool{log: log, api: rec.fake(hostProcs())}
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
