package rehearsal

// New. Issue #308, guard 3: signalProcs signals only what is strictly below
// the test process, by a walk whose hops are checked by start-time order, and
// refuses everything else as an error. Every send goes to a sink.

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

// fakeHost is a process table for signalProcs. A PID's rows are what its
// successive reads return (the last one repeats), so a parent that changes
// between two reads can be scripted.
type fakeHost struct {
	rows    map[int][]procRow
	rowErr  map[int]error // a row that cannot be read
	openErr map[int]error // a pidfd that cannot be taken
	reads   map[int]int   // guarded by the sink's mu
}

func (h *fakeHost) api(self int, rec *sink) *procAPI {
	h.reads = map[int]int{}
	api := rec.fake(&procAPI{self: self})
	api.row = func(pid int) (procRow, bool, error) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.events = append(rec.events, "row "+strconv.Itoa(pid))
		if err := h.rowErr[pid]; err != nil {
			return procRow{}, false, err
		}
		seq, ok := h.rows[pid]
		if !ok {
			return procRow{}, false, nil
		}
		n := min(h.reads[pid], len(seq)-1)
		h.reads[pid]++
		return seq[n], true, nil
	}
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
	api.reap = func(int) error { return nil }
	return api
}

// one is a PID's single row.
func one(pid, ppid int, ticks uint64) []procRow {
	return []procRow{{pid: pid, ppid: ppid, ticks: ticks}}
}

// refusedFor is the line of a joined error that names pid, or "".
func refusedFor(err error, pid int) string {
	if err == nil {
		return ""
	}
	for _, l := range strings.Split(err.Error(), "\n") {
		if strings.Contains(l, "process "+strconv.Itoa(pid)+": ") {
			return l
		}
	}
	return ""
}

// Guard 3. Over a scripted process table it refuses, each for its own reason:
// PID 1 and below, the test itself, its ancestor, a row with no start time, a
// stranger and a sibling (a child of go test) that are not below it, a
// process whose "parent" started after it (a reused PID whose new owner is
// below the test), a process whose parent cannot be read, a process below the
// test that started before it, and a process that cannot be pinned (ENOSYS,
// EPERM). It signals the test's child, grandchild and an orphan reparented to
// the test, also when the reparenting happens during the walk; it leaves a
// recycled PID and a gone one alone without error; and it pins each process
// before it checks it.
func TestSignalProcsRefusesWhatIsNotTheTests(t *testing.T) {
	const self = 5000
	h := &fakeHost{
		rows: map[int][]procRow{
			1:    one(1, 0, 1),
			3000: one(3000, 1, 30),    // go test
			4000: one(4000, 3000, 40), // timeout
			self: one(self, 4000, 50),
			6000: one(6000, 1, 60),    // the user manager
			6100: one(6100, self, 61), // an sshd, below the test
			6110: one(6110, 6100, 62), // its session
			6200: one(6200, self, 63), // a forced command reparented to the test, a subreaper
			6300: one(6300, self, 99), // listed with 64: recycled since
			6500: one(6500, 3000, 65), // a sibling: another child of go test
			6600: one(6600, self, 66), // listed with no start time
			6700: one(6700, self, 67), // its pidfd: ENOSYS
			6710: one(6710, self, 67), // its pidfd: EPERM
			6800: one(6800, 6810, 68), // its "parent" 6810 started after it: a reused PID
			6810: one(6810, self, 90),
			6900: one(6900, 6910, 69), // its parent 6910 cannot be read
			// Its parent 7010 ends during the walk, and it is reparented to the test.
			7000: {{pid: 7000, ppid: 7010, ticks: 70}, {pid: 7000, ppid: self, ticks: 70}},
			7100: one(7100, self, 40), // below the test, but started before it
		},
		rowErr:  map[int]error{6910: syscall.EACCES},
		openErr: map[int]error{6700: syscall.ENOSYS, 6710: syscall.EPERM},
	}
	var rec sink
	listed := []procRow{
		{pid: 0}, {pid: -1}, {pid: 1, ticks: 1}, {pid: self, ticks: 50}, {pid: 4000, ticks: 40}, {pid: 6000, ticks: 60},
		{pid: 6100, ticks: 61}, {pid: 6110, ticks: 62}, {pid: 6200, ticks: 63}, {pid: 6300, ticks: 64}, {pid: 6400, ticks: 65},
		{pid: 6500, ticks: 65}, {pid: 6600, ticks: 0}, {pid: 6700, ticks: 67}, {pid: 6710, ticks: 67}, {pid: 6800, ticks: 68},
		{pid: 6900, ticks: 69}, {pid: 7000, ticks: 70}, {pid: 7100, ticks: 40},
	}
	kept, err := signalProcs(h.api(self, &rec), listed, syscall.SIGTERM)
	sent, killed := rec.snapshot()
	slices.Sort(sent)
	if want := []int{6100, 6110, 6200, 7000}; !slices.Equal(sent, want) {
		t.Errorf("signalled %v, want %v: the sshd, its session, the reparented forced command, the one reparented mid-walk", sent, want)
	}
	if len(killed) != 0 {
		t.Errorf("killed through os.Process: %v, want none", killed)
	}
	if err == nil {
		t.Fatal("no error for the refusals")
	}
	for pid, why := range map[int]string{
		0: "PID 1 and below", -1: "PID 1 and below", 1: "PID 1 and below", self: "the process that sends it",
		4000: "an ancestor", 6000: "not below", 6500: "not below", 6600: "no start time",
		6700: "function not implemented", 6710: "operation not permitted", 6800: "not below",
		6900: "its ancestry cannot be read", 7100: "not below",
	} {
		if l := refusedFor(err, pid); !strings.Contains(l, why) {
			t.Errorf("process %d: the error says %q, want it refused with %q", pid, l, why)
		}
	}
	var keptPIDs []int
	for _, r := range kept {
		keptPIDs = append(keptPIDs, r.pid)
	}
	slices.Sort(keptPIDs)
	if want := []int{6100, 6110, 6200, 6300, 6400, 7000}; !slices.Equal(keptPIDs, want) {
		t.Errorf("kept %v, want the signalled, the recycled and the gone ones %v", keptPIDs, want)
	}
	// Each process is pinned with a pidfd before its row is checked: the
	// first read of its row comes after its open.
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, pid := range []int{6100, 6110, 6200, 6300, 6800, 7000} {
		o := slices.Index(rec.events, "open "+strconv.Itoa(pid))
		r := slices.Index(rec.events, "row "+strconv.Itoa(pid))
		if o < 0 || r < o {
			t.Errorf("process %d: pinned at event %d, its row read at event %d; want the pin first", pid, o, r)
		}
	}
}

// A check that cannot be made refuses: an ancestry that cannot be read
// refuses everything.
func TestSignalProcsRefusesEverythingWhenItsAncestryCannotBeRead(t *testing.T) {
	var rec sink
	h := &fakeHost{rows: map[int][]procRow{6100: one(6100, 5000, 61)}}
	kept, err := signalProcs(h.api(5000, &rec), []procRow{{pid: 6100, ticks: 61}}, syscall.SIGTERM)
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
// signals PID 1, the test, its parent or a systemd of this uid; and the
// refusals are an error.
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
	pool := &sshdPool{api: rec.fake(hostProcs()), walk: hostWide, wait: 100 * time.Millisecond}
	p := &sshdProc{done: make(chan struct{}), below: map[int]procRow{}}
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
}
