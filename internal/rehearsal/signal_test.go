package rehearsal

// New. Issue #308, guard 3: signalProcs signals only what is strictly below
// the test process, by a walk whose hops are checked by start-time order; it
// refuses everything else as an error, and reports what is outside the
// test's tree as a leak. Every send goes to a sink.

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeHost is a process table for signalProcs.
type fakeHost struct {
	rows    map[int]procRow
	rowErr  map[int]error // a row that cannot be read
	openErr map[int]error // a pidfd that cannot be taken
	// dieAfter is how many reads of a PID's row find it live; it is gone
	// after them.
	dieAfter map[int]int
	reads    map[int]int // guarded by the sink's mu
}

func (h *fakeHost) api(self int, rec *sink) *procAPI {
	api := rec.fake(&procAPI{self: self})
	h.reads = map[int]int{}
	api.row = func(pid int) (procRow, bool, error) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.events = append(rec.events, "row "+strconv.Itoa(pid))
		if err := h.rowErr[pid]; err != nil {
			return procRow{}, false, err
		}
		h.reads[pid]++
		if n, ok := h.dieAfter[pid]; ok && h.reads[pid] > n {
			return procRow{}, false, nil
		}
		r, ok := h.rows[pid]
		return r, ok, nil
	}
	api.comm = func(pid int) string { return "fake-" + strconv.Itoa(pid) }
	open := api.open
	api.open = func(pid int) (int, error) {
		switch _, ok := h.rows[pid]; {
		case pid <= 0:
			return 0, syscall.EINVAL // as pidfd_open answers
		case h.openErr[pid] != nil:
			return 0, h.openErr[pid]
		case !ok:
			return 0, syscall.ESRCH
		}
		return open(pid)
	}
	return api
}

// row is a process's row.
func row(pid, ppid int, ticks uint64) procRow { return procRow{pid: pid, ppid: ppid, ticks: ticks} }

// refusedFor is the line of a joined error that names pid, or "".
func refusedFor(err error, pid int) string {
	if err == nil {
		return ""
	}
	for _, l := range strings.Split(err.Error(), "\n") {
		if strings.Contains(l, "process "+strconv.Itoa(pid)+" ") || strings.Contains(l, "process "+strconv.Itoa(pid)+": ") {
			return l
		}
	}
	return ""
}

// Guard 3, over a scripted process table. It refuses PID 1 and below, the
// test itself, its ancestor and a row with no start time. It reports as a
// leak, not signalled, each process that is not strictly below the test: a
// stranger, a sibling (another child of go test), a process whose "parent"
// started after it (a reused PID whose new owner is below the test), one whose
// parent or grandparent has exited, and one below the test that started
// before it. A parent
// that cannot be read, and a pidfd that cannot be taken (ENOSYS, EPERM), are
// errors. It signals the test's child and grandchild, leaves a recycled PID
// and a gone one alone, and pins each process before it checks it.
func TestSignalProcsRefusesWhatIsNotTheTests(t *testing.T) {
	const self = 5000
	h := &fakeHost{
		rows: map[int]procRow{
			1:    row(1, 0, 1),
			3000: row(3000, 1, 30),    // go test
			4000: row(4000, 3000, 40), // timeout
			self: row(self, 4000, 50),
			6000: row(6000, 1, 60),    // the user manager
			6100: row(6100, self, 61), // an sshd, below the test
			6110: row(6110, 6100, 62), // its session process
			6300: row(6300, self, 99), // listed with 64: recycled since
			6500: row(6500, 3000, 65), // a sibling: another child of go test
			6600: row(6600, self, 66), // listed with no start time
			6700: row(6700, self, 67), // its pidfd: ENOSYS
			6710: row(6710, self, 67), // its pidfd: EPERM
			6800: row(6800, 6810, 68), // its "parent" 6810 started after it: a reused PID
			6810: row(6810, self, 90),
			6900: row(6900, 6910, 69), // its parent 6910 cannot be read
			7000: row(7000, 7010, 70), // its parent 7010 has exited
			7100: row(7100, self, 40), // below the test, but started before it
			7200: row(7200, 7210, 72), // its grandparent 7220 has exited: its parent 7210 left the tree with it
			7210: row(7210, 7220, 71),
		},
		rowErr:  map[int]error{6910: syscall.EACCES},
		openErr: map[int]error{6700: syscall.ENOSYS, 6710: syscall.EPERM},
	}
	var rec sink
	listed := []procRow{
		{pid: 0}, {pid: -1}, {pid: 1, ticks: 1}, {pid: self, ticks: 50}, {pid: 4000, ticks: 40}, {pid: 6000, ticks: 60},
		{pid: 6100, ticks: 61}, {pid: 6110, ticks: 62}, {pid: 6300, ticks: 64}, {pid: 6400, ticks: 65},
		{pid: 6500, ticks: 65}, {pid: 6600, ticks: 0}, {pid: 6700, ticks: 67}, {pid: 6710, ticks: 67}, {pid: 6800, ticks: 68},
		{pid: 6900, ticks: 69}, {pid: 7000, ticks: 70}, {pid: 7100, ticks: 40}, {pid: 7200, ticks: 72},
	}
	kept, err := signalProcs(h.api(self, &rec), "login node", listed, syscall.SIGTERM)
	sent, killed := rec.snapshot()
	slices.Sort(sent)
	if want := []int{6100, 6110}; !slices.Equal(sent, want) {
		t.Errorf("signalled %v, want %v: the sshd and its session process", sent, want)
	}
	if len(killed) != 0 {
		t.Errorf("killed through os.Process: %v, want none", killed)
	}
	if err == nil {
		t.Fatal("no error for the refusals")
	}
	const leak = "outside the test's tree"
	for pid, why := range map[int][]string{
		0: {"PID 1 and below"}, -1: {"PID 1 and below"}, 1: {"PID 1 and below"}, self: {"the process that sends it"},
		4000: {"an ancestor"}, 6600: {"no start time"},
		6000: {leak, "fake-6000", "tick 60", "login node", "reaches PID 1"},
		6500: {leak, "reaches PID 1"},
		6800: {leak, "a reused PID"},
		7000: {leak, "ancestor 7010 has exited"},
		7100: {leak, "started before the test process"},
		7200: {leak, "ancestor 7220 has exited"},
		6700: {"function not implemented"}, 6710: {"operation not permitted"},
		6900: {"its ancestry cannot be read"},
	} {
		l := refusedFor(err, pid)
		for _, w := range why {
			if !strings.Contains(l, w) {
				t.Errorf("process %d: the error says %q, want it to say %q", pid, l, w)
			}
		}
	}
	if n := len(leakErrors(err)); n != 6 {
		t.Errorf("%d processes outside the test's tree, want 6: 6000, 6500, 6800, 7000, 7100 and 7200", n)
	}
	var keptPIDs []int
	for _, r := range kept {
		keptPIDs = append(keptPIDs, r.pid)
	}
	slices.Sort(keptPIDs)
	if want := []int{6100, 6110, 6300, 6400}; !slices.Equal(keptPIDs, want) {
		t.Errorf("kept %v, want the signalled, the recycled and the gone ones %v", keptPIDs, want)
	}
	// Each process is pinned with a pidfd before its row is checked: the
	// first read of its row comes after its open.
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, pid := range []int{6100, 6110, 6300, 6800, 7000} {
		o := slices.Index(rec.events, "open "+strconv.Itoa(pid))
		r := slices.Index(rec.events, "row "+strconv.Itoa(pid))
		if o < 0 || r < o {
			t.Errorf("process %d: pinned at event %d, its row read at event %d; want the pin first", pid, o, r)
		}
	}
}

// leakErrors are the processes outside the test's tree in a signal pass's
// error.
func leakErrors(err error) []*leakError {
	leaks, _ := splitLeaks(err)
	return leaks
}

// A process found outside the test's tree is judged at the stop's last
// deadline: one that has exited by then, as a forced command does between its
// reparenting and its parent-death SIGKILL, is not a leak, and one still alive
// is. The start times are far above any real process's.
func TestStopReportsOnlyWhatIsStillOutsideTheTreeAtItsDeadline(t *testing.T) {
	const far = uint64(1) << 50
	for _, c := range []struct {
		name  string
		dies  bool
		leaks int64
	}{{"dying", true, 0}, {"still alive", false, 1}} {
		h := &fakeHost{rows: map[int]procRow{
			5000: row(5000, 1, far),
			6100: row(6100, 5000, far+1), // the sshd
			7000: row(7000, 7010, far+2), // its forced command, reparented: 7010 is gone
		}}
		if c.dies {
			h.dieAfter = map[int]int{7000: 1} // live when it is checked, gone by the deadline
		}
		var rec sink
		pool := &sshdPool{name: "login node", api: h.api(5000, &rec), wait: 100 * time.Millisecond}
		done := make(chan struct{})
		close(done)
		pool.procs = []*sshdProc{{self: h.rows[6100], done: done, below: map[int]procRow{7000: h.rows[7000]}}}
		err := pool.stop(nil)
		if n := rec.leaks.Load(); n != c.leaks {
			t.Errorf("%s: counted %d leaks, want %d", c.name, n, c.leaks)
		}
		if got := refusedFor(err, 7000) != ""; got != !c.dies {
			t.Errorf("%s: stop: %v; want process 7000 reported %v", c.name, err, !c.dies)
		}
		if sent, _ := rec.snapshot(); slices.Contains(sent, 7000) {
			t.Errorf("%s: signalled process 7000, which is outside the test's tree", c.name)
		}
	}
}

// A check that cannot be made refuses: an ancestry that cannot be read
// refuses everything.
func TestSignalProcsRefusesEverythingWhenItsAncestryCannotBeRead(t *testing.T) {
	var rec sink
	h := &fakeHost{rows: map[int]procRow{6100: row(6100, 5000, 61)}}
	kept, err := signalProcs(h.api(5000, &rec), "login node", []procRow{{pid: 6100, ticks: 61}}, syscall.SIGTERM)
	if err == nil || !strings.Contains(err.Error(), "the ancestry of process 5000") {
		t.Errorf("error %v, want the unreadable ancestry", err)
	}
	if sent, _ := rec.snapshot(); len(sent)+len(kept) != 0 {
		t.Errorf("signalled %v and kept %v with no ancestry; want neither", sent, kept)
	}
}

// kernelBelow is every process below pid by the kernel's own children lists
// (/proc/<pid>/task/<tid>/children): an account of "below" that shares no code
// with signalProcs' walk up the parents.
func kernelBelow(pid int) map[int]bool {
	out := map[int]bool{}
	var down func(int)
	down = func(p int) {
		tasks, _ := os.ReadDir(fmt.Sprintf("/proc/%d/task", p))
		for _, tk := range tasks {
			b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", p, tk.Name()))
			for _, f := range strings.Fields(string(b)) {
				if c, err := strconv.Atoi(f); err == nil && !out[c] {
					out[c] = true
					down(c)
				}
			}
		}
	}
	down(pid)
	return out
}

// sameUIDSystemd reports whether pid is a systemd of this uid, the user
// manager's class, which took the host down on 10 Nov 2026.
func sameUIDSystemd(pid int) bool {
	comm, _ := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
	var st syscall.Stat_t
	if syscall.Stat("/proc/"+strconv.Itoa(pid), &st) != nil {
		return false
	}
	return strings.TrimSpace(string(comm)) == "systemd" && int(st.Uid) == os.Getuid()
}

// Guard 3 alone, at the pool, over this host's real /proc: guard 1 is passed
// by storing a zero row, and guard 2 is replaced by a walk that lists every
// live process, as the walk from PID 0 did. Every process it signals is below
// the test by the kernel's own children lists, read at the send; it never
// signals PID 1, the test, its parent or a systemd of this uid; the rest is
// refused or reported as leaks, which is an error.
func TestSignalProcsAloneOverTheWholeHostSignalsOnlyBelowTheTest(t *testing.T) {
	self, parent := os.Getpid(), os.Getppid()
	rec := sink{check: func(pid int) error {
		switch {
		case pid == 1 || pid == self || pid == parent:
			return fmt.Errorf("signalled process %d: PID 1, the test or its parent", pid)
		case sameUIDSystemd(pid):
			return fmt.Errorf("signalled process %d: a systemd of this uid", pid)
		case !kernelBelow(self)[pid]:
			return fmt.Errorf("signalled process %d, which the kernel does not list below the test", pid)
		}
		return nil
	}}
	pool := &sshdPool{api: rec.fake(hostProcs()), wait: 100 * time.Millisecond}
	// spawn gives each sshd the pool's walk; a stored row carries its own.
	p := &sshdProc{done: make(chan struct{}), walk: hostWide, below: map[int]procRow{}}
	pool.procs = []*sshdProc{p}
	err := pool.stop(nil)
	if n := len(p.seen()); n < 3 {
		t.Fatalf("the stand-in walk recorded %d processes; want the host's", n)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, b := range rec.bad {
		t.Error(b)
	}
	if len(rec.killed) != 0 {
		t.Errorf("killed through os.Process: %v, want none", rec.killed)
	}
	if refusedFor(err, 1) == "" {
		t.Errorf("error %v, want PID 1 refused", err)
	}
	if rec.leaks.Load() == 0 {
		t.Error("no leak reported, though the host's processes are not below the test")
	}
}

// ReportLeaks fails a passing run that left any process outside the test's
// tree, keeps a failing run's own code, and passes a run with none. Its
// verdict is tested without its line, which only the run's TestMain prints.
func TestReportLeaksFailsARunThatLeftAProcess(t *testing.T) {
	for _, c := range []struct {
		leaks      int64
		code, want int
	}{{0, 0, 0}, {1, 0, 1}, {2, 3, 3}} {
		if got := leakVerdict(c.leaks, c.code); got != c.want {
			t.Errorf("%d leaks, code %d: %d, want %d", c.leaks, c.code, got, c.want)
		}
	}
}
