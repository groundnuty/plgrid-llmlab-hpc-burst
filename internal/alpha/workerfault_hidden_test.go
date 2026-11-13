package alpha

// The engine whose Apptainer parent is hidden from the job's user (issue #488; the science agent's ruling, 12 Nov 2026). On Athena
// the worker fault found no engine: Apptainer's setuid starter runs as another user, so it is not in the listing (this user's
// processes only, ListScript) and, with hidepid, its /proc entry cannot be read either. The one process of the job that runs the
// launch's command had that parent. PickEngine now takes a process whose parent's PID is absent from the listing AND whose /proc
// entry the listing could not read as under Apptainer (condition 2), when it is of the job (1), runs the launch's command (3), and is
// the only such process (4). A parent that can be read must still be Apptainer's, as before.
//
// The tests, every expectation a literal:
//   - the listing script, over a /proc of its own, marks a parent it cannot read (absent, or not readable), and not one it can;
//   - the incident's listing, 8 processes of the job as the live run's redacted shape showed them, with its engine's parent hidden:
//     today's rule refuses it (the same listing without the word), the fix picks the one engine, and the shape says "parent:hidden";
//   - two candidates are refused, a visible parent that is not Apptainer's is refused, and so is a hidden parent of a process that
//     is not of the job or runs another command or sits under a shell.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The incident's listing (12 Nov 2026, #145 comment 6828368141, #457's redacted shape): the 8 processes of the run's job on its node.
// Each root's parent is slurmstepd's or the starter's, another user's, which hidepid hides: parent-hidden on each. The engine is
// the python at depth 0 whose command line is the launch's; the python -c under it and the vllm EngineCore are its children.
func incidentListing(hidden bool) string {
	mark := ""
	if hidden {
		mark = " parent-hidden"
	}
	const job = "7654321"
	return strings.Join([]string{
		"self 4100001",
		"proc 3456701 3456600 925087416 1 1 j " + b64("/stage/hb-agent", "--job="+job) + mark,
		"proc 3456702 3456701 925087417 1 1 j " + b64("/bin/bash", "/stage/job_"+job+".sh"),
		"proc 3456703 3456600 925087418 1 1 j " + b64("sleep", "300") + mark,
		"proc 3456704 3456600 925087419 1 1 j " + b64("/bin/bash", "-c", "wait") + mark,
		"proc 3456705 3456600 925087420 0 1 j " + b64("/usr/bin/squashfuse_ll", "img.sif", "/var/mnt") + mark,
		"proc 3456706 3456600 925087421 0 1 j " + b64("/usr/bin/python3", "/usr/local/bin/vllm", "serve", "/hpcburst/model", "--port=29455") + mark,
		"proc 3456707 3456706 925087422 0 1 j " + b64("/usr/bin/python3", "-c", "import time; time.sleep(300)"),
		"proc 3456708 3456706 925087423 0 1 j " + b64("VLLM::EngineCore_DP0"),
		"listed", "",
	}, "\n")
}

var incidentCommand = []string{"vllm", "serve", "/hpcburst/model", "--port=29455"}

func incidentProcs(t *testing.T, hidden bool) []NodeProc {
	t.Helper()
	procs, err := ParseListing([]byte(incidentListing(hidden)))
	if err != nil {
		t.Fatal(err)
	}
	return procs
}

// The incident: today's rule finds no engine in the listing, since the engine's parent is neither listed nor Apptainer's by what the
// listing shows; the same listing with the node's word that the parent could not be read has exactly one, the python at depth 0, and
// its shape says the parent is hidden.
func TestAnEngineWhoseApptainerParentIsHiddenIsPicked(t *testing.T) {
	// Without the word, the listing of the old script: refused, count 0, as the live run was.
	_, err := PickEngine(incidentProcs(t, false), incidentCommand, MatchesCommand)
	var ec ErrEngineCount
	if !errors.As(err, &ec) || ec.N != 0 {
		t.Fatalf("PickEngine on the listing without the word = %v, want the count of 0 of the live run", err)
	}
	procs := incidentProcs(t, true)
	eng, err := PickEngine(procs, incidentCommand, MatchesCommand)
	if err != nil || eng.PID != 3456706 {
		t.Fatalf("PickEngine on the listing with the word = %+v, %v; want the python at depth 0, PID 3456706", eng, err)
	}
	shape := Shape(procs, incidentCommand, MatchesCommand)
	if !strings.Contains(shape, "8 processes listed, 8 in the job's tree, 1 meeting all three conditions") {
		t.Errorf("the shape's counts are not 8, 8 and 1:\n%s", shape)
	}
	if !strings.Contains(shape, "\n  d0 python in-job:cgroup env:no cgroup:job_<n> argv:python-script meets:job+apptainer+command parent:hidden") {
		t.Errorf("the shape does not show the engine with its parent hidden:\n%s", shape)
	}
	// The listing's other roots say their parents are hidden too, and are not engines: their commands are not the launch's.
	if n := strings.Count(shape, "parent:hidden"); n != 5 {
		t.Errorf("the shape says parent:hidden %d times, want the 5 roots:\n%s", n, shape)
	}
	// Without the word the same roots say unlisted: the column's other value is kept.
	if old := Shape(incidentProcs(t, false), incidentCommand, MatchesCommand); strings.Contains(old, "parent:hidden") || strings.Count(old, "parent:unlisted") != 5 {
		t.Errorf("a listing without the word does not say parent:unlisted for its 5 roots:\n%s", old)
	}
}

// Condition 4 stands: two processes that meet the three conditions are refused, with the count and the shape, and nothing is picked.
func TestTwoCandidatesWithAHiddenParentAreRefused(t *testing.T) {
	procs := incidentProcs(t, true)
	twin := procs[len(procs)-1]
	for _, p := range procs {
		if p.PID == 3456706 {
			twin = p
		}
	}
	twin.PID, twin.PPID, twin.Start = 3456799, 3456600, "925087999"
	procs = append(procs, twin)
	eng, err := PickEngine(procs, incidentCommand, MatchesCommand)
	var ec ErrEngineCount
	if !errors.As(err, &ec) || ec.N != 2 || eng.PID != 0 {
		t.Fatalf("PickEngine = %+v, %v; want the count of 2 and no process", eng, err)
	}
	if !strings.Contains(err.Error(), "2 meeting all three conditions") || !strings.Contains(err.Error(), "parent:hidden") {
		t.Errorf("the refusal does not carry the shape with its two candidates:\n%v", err)
	}
}

// A parent that is not hidden is still Apptainer's to be, as before, and the other conditions stand.
func TestAParentThatIsNotHiddenOrAProcessThatIsNotTheEngineIsStillRefused(t *testing.T) {
	engineIdx := func(procs []NodeProc) int {
		for i, p := range procs {
			if p.PID == 3456706 {
				return i
			}
		}
		t.Fatal("no engine in the listing")
		return -1
	}
	hiddenParent := func(procs []NodeProc) []NodeProc { return procs }
	for _, c := range []struct {
		name   string
		mutate func(procs []NodeProc) []NodeProc
		want   int // the count PickEngine reports
	}{
		{"the control: the engine's parent hidden", hiddenParent, 1},
		// The parent is absent from the listing but the node could read it: what the old listing said, a visible parent that is not
		// Apptainer's, since a readable parent can be told.
		{"the parent absent but readable", func(procs []NodeProc) []NodeProc {
			procs[engineIdx(procs)].ParentHidden = false
			return procs
		}, 0},
		// The word, but the parent is in the listing: a listed parent is judged by what it is, here a program of the job that is not
		// Apptainer's (the word on the engine is then of no use).
		{"the parent listed, not Apptainer's, with the word", func(procs []NodeProc) []NodeProc {
			procs[engineIdx(procs)].PPID = 3456703
			return procs
		}, 0},
		// Condition 1: the process with the hidden parent is of no job.
		{"a hidden parent, the process of no job", func(procs []NodeProc) []NodeProc {
			p := &procs[engineIdx(procs)]
			p.InJob, p.Env, p.CgroupJob, p.CgroupJobForm = false, EnvOther, false, false
			return procs
		}, 0},
		// Condition 3: the process runs another command.
		{"a hidden parent, another command", func(procs []NodeProc) []NodeProc {
			procs[engineIdx(procs)].Argv = []string{"/usr/bin/python3", "/usr/local/bin/vllm", "serve", "/hpcburst/other-model", "--port=29455"}
			return procs
		}, 0},
		// The hidden parent counts at the engine's own parent, not above a shell: a shell of the job between them is not enough,
		// since the engine's parent is then a visible shell, which is judged as it was.
		{"a shell between the engine and the hidden parent", func(procs []NodeProc) []NodeProc {
			procs[engineIdx(procs)].PPID = 3456704
			for i := range procs {
				if procs[i].PID == 3456704 {
					procs[i].ParentHidden = true
				}
			}
			return procs
		}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			procs := c.mutate(incidentProcs(t, true))
			eng, err := PickEngine(procs, incidentCommand, MatchesCommand)
			if c.want == 1 {
				if err != nil || eng.PID != 3456706 {
					t.Fatalf("PickEngine = %+v, %v; want PID 3456706", eng, err)
				}
				return
			}
			var ec ErrEngineCount
			if !errors.As(err, &ec) || ec.N != c.want {
				t.Errorf("PickEngine = %+v, %v; want the refusal with a count of %d", eng, err, c.want)
			}
		})
	}
}

// ParseListing reads the word after a process's command line and no other: the ninth word is parent-hidden or the listing is
// malformed, and a tenth is malformed.
func TestParseListingReadsTheParentHiddenWord(t *testing.T) {
	line := func(rows ...string) []byte {
		return []byte("self 7\n" + strings.Join(rows, "\n") + "\nlisted\n")
	}
	procs, err := ParseListing(line("proc 10 1 500 1 0 o "+b64("a")+" parent-hidden", "proc 11 1 501 1 0 o "+b64("b")))
	if err != nil || len(procs) != 2 || !procs[0].ParentHidden || procs[1].ParentHidden {
		t.Fatalf("ParseListing = %+v, %v; want the first process marked and the second not", procs, err)
	}
	for name, bad := range map[string][]byte{
		"another ninth word": line("proc 10 1 500 1 0 o " + b64("a") + " parent-visible"),
		"a tenth word":       line("proc 10 1 500 1 0 o " + b64("a") + " parent-hidden extra"),
		"the word alone":     line("proc 10 1 500 1 0 parent-hidden"),
	} {
		if _, err := ParseListing(bad); !errors.Is(err, ErrListing) {
			t.Errorf("%s: ParseListing returned %v, want ErrListing", name, err)
		}
	}
}

// The listing script, over a /proc of its own, marks a process whose parent's /proc entry it cannot read, and no other. The parents:
//   - 900 is absent (hidepid=2: another user's process is not there at all): hidden;
//   - 901 is there but its stat cannot be read (hidepid=1): hidden;
//   - 902 is another owner's directory with a readable stat, so it is not listed and can be read: not hidden. It is the directory of
//     the stat program's own file, which no one here owns and everyone reads (a stand-in for a process of another user on a host
//     with no hidepid);
//   - 205 is listed, this user's: not hidden;
//   - 0 is no process: not hidden.
func TestTheListingMarksAParentItCannotRead(t *testing.T) {
	statPath, err := exec.LookPath("stat")
	if err != nil {
		t.Skip("no stat program on this host to stand for another owner's readable entry")
	}
	readable := filepath.Dir(statPath)
	fi, err := os.Stat(readable)
	if err != nil {
		t.Skip("cannot read the stat program's directory")
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); !ok || int(st.Uid) == os.Getuid() {
		t.Skip("this user owns the stat program's directory, so it is not another owner's")
	}
	root := t.TempDir()
	stat := func(pid, ppid int) string {
		return fmt.Sprintf("%d (x) S %d %s 4242\n", pid, ppid, strings.TrimSpace(strings.Repeat("0 ", 17)))
	}
	write := func(pid, ppid int) {
		d := filepath.Join(root, strconv.Itoa(pid))
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
		for f, b := range map[string]string{"stat": stat(pid, ppid), "environ": "SLURM_JOB_ID=4321\x00", "cgroup": "0::/user.slice\n", "cmdline": "/bin/x\x00"} {
			if err := os.WriteFile(filepath.Join(d, f), []byte(b), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(201, 900)
	write(202, 901)
	write(203, 902)
	write(204, 205)
	write(205, 0)
	// 901: its entry is there, its stat is not readable.
	if err := os.Mkdir(filepath.Join(root, "901"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "901", "stat"), []byte(stat(901, 1)), 0); err != nil {
		t.Fatal(err)
	}
	// 902: a directory no one here owns, with a readable file named stat.
	if err := os.Symlink(readable, filepath.Join(root, "902")); err != nil {
		t.Fatal(err)
	}
	if f, err := os.Open(filepath.Join(root, "901", "stat")); err == nil {
		f.Close()
		t.Skip("this user can read a mode-0 file (running as root?), so a hidden entry cannot be made")
	}
	procs, err := ParseListing(runScript(t, listScript(root, "4321")))
	if err != nil {
		t.Fatal(err)
	}
	got := map[int]bool{}
	for _, p := range procs {
		got[p.PID] = p.ParentHidden
	}
	want := map[int]bool{201: true, 202: true, 203: false, 204: false, 205: false}
	if len(got) != len(want) {
		t.Fatalf("the listing holds %v, want the five processes %v (900, 901 and 902 are not listed: absent, unreadable, another owner's)", got, want)
	}
	for pid, w := range want {
		if g, ok := got[pid]; !ok || g != w {
			t.Errorf("PID %d: parent hidden = %t (listed: %t), want %t", pid, g, ok, w)
		}
	}
}

// The same picks over REAL processes. Each tree is a parent that starts the engine double as its child (the fake Slurm's doubles:
// outsideDouble, which gives the engine a parent-death signal so that it ends with its parent, and engineDouble, which writes its own
// PID to a file: that file, not the code under test, is the expected pick). The trees carry SLURM_JOB_ID=hiddenJob, so they are of
// the job. The listing is the real ListScript over a /proc of this test's own, made of symlinks to the real /proc/<pid> of the
// processes it lists: a parent left out of it is what Apptainer's setuid starter is to the job's user, absent from the listing and
// not there to read (hidepid=2), and the same parent put in is a visible one. No process of another user exists here to hide for real.
const hiddenJob = "4321"

// startEngineTree starts a parent titled title with the engine double as its child, and returns their PIDs. Its cleanup ends the
// parent through its pidfd (os.Process: never a group), and the engine ends with it.
func startEngineTree(t *testing.T, name, title string) (parent, engine int) {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("the engine doubles need python3: %v", err)
	}
	if py, err = filepath.Abs(py); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for f, c := range map[string]struct {
		body string
		mode os.FileMode
	}{
		"vllm":      {strings.NewReplacer("{py}", py).Replace(engineDouble), 0o755},
		"mode":      {"\n", 0o644},
		"parent.py": {outsideDouble, 0o644},
	} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(c.body), c.mode); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(py, append([]string{filepath.Join(dir, "parent.py")}, faultLaunch[1:]...)...)
	cmd.Args[0] = title
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "SLURM_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "SLURM_JOB_ID="+hiddenJob)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if engine > 0 {
			waitGone(t, engine, "the engine of the tree "+name)
		}
	})
	deadline := time.Now().Add(60 * time.Second)
	for engine == 0 {
		b, err := os.ReadFile(filepath.Join(dir, "engine.pid"))
		if n, perr := strconv.Atoi(string(b)); err == nil && perr == nil && n > 0 {
			engine = n
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the engine double of %s wrote no PID within 60 s", name)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cmd.Process.Pid, engine
}

// listOf is the real listing script's output, parsed, over a /proc made of symlinks to the real entries of pids.
func listOf(t *testing.T, pids ...int) []NodeProc {
	t.Helper()
	root := t.TempDir()
	for _, pid := range pids {
		if err := os.Symlink(fmt.Sprintf("/proc/%d", pid), filepath.Join(root, strconv.Itoa(pid))); err != nil {
			t.Fatal(err)
		}
	}
	procs, err := ParseListing(runScript(t, listScript(root, hiddenJob)))
	if err != nil {
		t.Fatal(err)
	}
	return procs
}

// One engine whose Apptainer parent is hidden is picked, and killed through its pidfd; the listing without the word (the old
// script's) finds none. The parent is titled as Apptainer's runtime parent, and it is not in the listing.
func TestTheEngineOfARealTreeWithAHiddenParentIsPickedAndKilled(t *testing.T) {
	parent, engine := startEngineTree(t, "hidden", "Apptainer runtime parent")
	procs := listOf(t, engine)
	if len(procs) != 1 || procs[0].PID != engine || procs[0].PPID != parent || !procs[0].ParentHidden || !procs[0].InJob {
		t.Fatalf("the listing = %+v, want the engine alone, of the job, its parent %d hidden", procs, parent)
	}
	old := append([]NodeProc(nil), procs...)
	old[0].ParentHidden = false
	if _, err := PickEngine(old, faultLaunch, MatchesCommand); err == nil {
		t.Fatal("the old listing's rule picked an engine whose parent is not in the listing")
	}
	eng, err := PickEngine(procs, faultLaunch, MatchesCommand)
	if err != nil || eng.PID != engine {
		t.Fatalf("PickEngine = %+v, %v; want the engine double, PID %d (the double's own file)", eng, err, engine)
	}
	if out := runScript(t, KillScript(eng)); !Killed(out, eng) {
		t.Fatalf("the kill said %q, want killed %d", out, engine)
	}
	waitGone(t, engine, "the engine")
}

// Two such engines are refused, and neither is killed: nothing is signalled on a count that is not one.
func TestTwoRealEnginesWithHiddenParentsAreRefusedAndLeftAlone(t *testing.T) {
	_, e1 := startEngineTree(t, "one", "Apptainer runtime parent")
	_, e2 := startEngineTree(t, "two", "Apptainer runtime parent")
	procs := listOf(t, e1, e2)
	if len(procs) != 2 || !procs[0].ParentHidden || !procs[1].ParentHidden {
		t.Fatalf("the listing = %+v, want the two engines, each with its parent hidden", procs)
	}
	_, err := PickEngine(procs, faultLaunch, MatchesCommand)
	var ec ErrEngineCount
	if !errors.As(err, &ec) || ec.N != 2 {
		t.Fatalf("PickEngine returned %v, want the count of 2", err)
	}
	if !alive(e1) || !alive(e2) {
		t.Errorf("an engine is gone after a refusal: first alive %t, second alive %t", alive(e1), alive(e2))
	}
}

// A visible parent is still Apptainer's to be: the same tree with its parent in the listing, titled as something else, is refused,
// and titled as Apptainer's runtime parent is picked, as before the fix and with no word in the listing for the engine.
func TestARealEngineWithAVisibleParentIsJudgedByIt(t *testing.T) {
	for _, c := range []struct {
		name, title string
		wantEngine  bool
	}{
		{"a visible launcher", "launcher", false},
		{"a visible Apptainer runtime parent", "Apptainer runtime parent", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			parent, engine := startEngineTree(t, "visible", c.title)
			procs := listOf(t, parent, engine)
			if len(procs) != 2 {
				t.Fatalf("the listing = %+v, want the parent and the engine", procs)
			}
			for _, p := range procs {
				if p.PID == engine && p.ParentHidden {
					t.Fatalf("the engine's parent is in the listing, and the listing says it is hidden: %+v", p)
				}
			}
			eng, err := PickEngine(procs, faultLaunch, MatchesCommand)
			if c.wantEngine {
				if err != nil || eng.PID != engine {
					t.Fatalf("PickEngine = %+v, %v; want the engine %d", eng, err, engine)
				}
				return
			}
			var ec ErrEngineCount
			if !errors.As(err, &ec) || ec.N != 0 {
				t.Errorf("PickEngine = %+v, %v; want the refusal with a count of 0", eng, err)
			}
		})
	}
}
