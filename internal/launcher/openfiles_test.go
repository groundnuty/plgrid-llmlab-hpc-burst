package launcher

// Tests of the tunnel processes' open files (HBEP-11, Single-user host; issue #431). They read the
// real parser and the real reader: a fake /proc tree holds a limits file in the kernel's own layout
// and an fd directory of real-shaped entries, and the numbers below are typed here as literals, never
// computed by the code under test. One test reads this process's own /proc/self through the same
// reader, as the positive control that the parser fits what the kernel writes.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// realLimits is /proc/<pid>/limits as the kernel wrote it, captured from a process whose soft limit
// on open files was 1024 and whose hard limit was 1048576. Its other rows carry other numbers and
// several "unlimited" columns, so a parser that took another row, or the wrong column, reads a
// different number.
func realLimits(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "proc-limits.txt"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// limitsWith is realLimits with only the open-files row's two limits changed, each given as the text
// the kernel would print ("unlimited" or digits), in the kernel's column layout for that row: the
// name in 25 columns, the soft and hard limits in 20 each, the unit in 10, one space between them
// (fs/proc/base.c, proc_pid_limits: "%-25s %-20s %-20s %-10s\n").
func limitsWith(t *testing.T, soft, hard string) string {
	t.Helper()
	lines := strings.Split(realLimits(t), "\n")
	for i, l := range lines {
		if strings.HasPrefix(l, "Max open files") {
			lines[i] = fmt.Sprintf("%-25s %-20s %-20s %-10s", "Max open files", soft, hard, "files")
			return strings.Join(lines, "\n")
		}
	}
	t.Fatal("the captured limits file has no open files row")
	return ""
}

// TestLimitsWithReproducesTheCapture is the check on the helper above: with the captured numbers it
// writes the captured bytes, so its column layout is the kernel's and not a guess of the test's.
func TestLimitsWithReproducesTheCapture(t *testing.T) {
	if got, want := limitsWith(t, "1024", "1048576"), realLimits(t); got != want {
		t.Fatalf("the helper's row differs from the capture:\n%q\n%q", got, want)
	}
}

func TestParseOpenFilesLimits(t *testing.T) {
	t.Run("the kernel's own file", func(t *testing.T) {
		soft, hard, err := parseOpenFilesLimits(realLimits(t))
		if err != nil || soft != (Limit{Value: 1024}) || hard != (Limit{Value: 1048576}) {
			t.Fatalf("soft %+v, hard %+v, err %v; want 1024 and 1048576", soft, hard, err)
		}
	})
	for _, c := range []struct {
		name, soft, hard string
		wantSoft         Limit
		wantHard         Limit
	}{
		{"an unlimited hard limit", "4096", "unlimited", Limit{Value: 4096}, Limit{Unlimited: true}},
		{"an unlimited soft limit", "unlimited", "unlimited", Limit{Unlimited: true}, Limit{Unlimited: true}},
		{"a soft limit of 0 is a number, not unlimited", "0", "0", Limit{Value: 0}, Limit{Value: 0}},
		{"the widest number the column holds", "18446744073709551614", "18446744073709551614", Limit{Value: 18446744073709551614}, Limit{Value: 18446744073709551614}},
	} {
		t.Run(c.name, func(t *testing.T) {
			soft, hard, err := parseOpenFilesLimits(limitsWith(t, c.soft, c.hard))
			if err != nil || soft != c.wantSoft || hard != c.wantHard {
				t.Fatalf("soft %+v, hard %+v, err %v; want %+v and %+v", soft, hard, err, c.wantSoft, c.wantHard)
			}
		})
	}
	for _, c := range []struct {
		name, content, want string
	}{
		{"an empty file", "", `no "Max open files" row`},
		{"a file without the row", "Limit                     Soft Limit           Hard Limit           Units     \nMax processes             128232               128232               processes \n", `no "Max open files" row`},
		{"a row named like it but not it", "Max open filesystems    1                    2                    files     \n", `no "Max open files" row`},
		{"only a soft limit", "Max open files            1024\n", "want a soft limit, a hard limit and the unit files"},
		{"another unit", "Max open files            1024                 4096                 bytes     \n", "want a soft limit, a hard limit and the unit files"},
		{"a soft limit that is no number", "Max open files            lots                 4096                 files     \n", `the soft limit of the "Max open files" row: "lots" is neither a number nor unlimited`},
		{"a negative hard limit", "Max open files            1024                 -1                   files     \n", `the hard limit of the "Max open files" row: "-1" is neither a number nor unlimited`},
	} {
		t.Run(c.name, func(t *testing.T) {
			soft, hard, err := parseOpenFilesLimits(c.content)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %+v, %+v, %v; want an error containing %q", soft, hard, err, c.want)
			}
		})
	}
}

// TestLimitJSON: a limit is written as a number or the string "unlimited", and read back as the
// same limit. The unlimited limit never becomes 0.
func TestLimitJSON(t *testing.T) {
	for _, c := range []struct {
		l    Limit
		text string
	}{
		{Limit{Value: 1048576}, `1048576`},
		{Limit{Value: 0}, `0`},
		{Limit{Unlimited: true}, `"unlimited"`},
	} {
		b, err := json.Marshal(c.l)
		if err != nil || string(b) != c.text {
			t.Errorf("%+v marshals to %s (%v), want %s", c.l, b, err, c.text)
		}
		var back Limit
		if err := json.Unmarshal([]byte(c.text), &back); err != nil || back != c.l {
			t.Errorf("%s unmarshals to %+v (%v), want %+v", c.text, back, err, c.l)
		}
	}
	var l Limit
	if err := json.Unmarshal([]byte(`"lots"`), &l); err == nil {
		t.Errorf(`"lots" unmarshals to %+v without an error`, l)
	}
}

// fdTargets are what a Go process's descriptors point at in /proc/<pid>/fd: sockets, pipes, the
// epoll instance, a pidfd, a file. They are links, as in the kernel's directory.
var fdTargets = []string{"socket:[40113]", "pipe:[40201]", "anon_inode:[eventpoll]", "anon_inode:[pidfd]", "/dev/null"}

// writeFakeLimits writes /proc/<pid>/limits on the fake tree.
func writeFakeLimits(t *testing.T, root string, pid int, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, strconv.Itoa(pid), "limits"), []byte(content), 0o444); err != nil {
		t.Fatal(err)
	}
}

// fillFakeFds makes the fake /proc/<pid>/fd hold exactly total entries: it adds numbered links of
// real shapes to those already there (a fake host's process holds its listener's), and checks the
// directory by listing it.
func fillFakeFds(t *testing.T, root string, pid, total int) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid), "fd")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) > total {
		t.Fatalf("the fake fd directory of pid %d already holds %d entries, more than %d", pid, len(ents), total)
	}
	for n, made := 0, len(ents); made < total; n++ {
		p := filepath.Join(dir, strconv.Itoa(n))
		if _, err := os.Lstat(p); err == nil {
			continue
		}
		if err := os.Symlink(fdTargets[n%len(fdTargets)], p); err != nil {
			t.Fatal(err)
		}
		made++
	}
	if ents, err = os.ReadDir(dir); err != nil || len(ents) != total {
		t.Fatalf("the fake fd directory of pid %d holds %d entries (%v), want %d", pid, len(ents), err, total)
	}
}

// asTunnel records the fake host's supervised child as the named entry, as TestStatusFrontUsageDegraded
// records "front".
func (s *supervisedFakeHost) asTunnel(name string) {
	s.t.Helper()
	s.writeSupervisorFile(s.supRec, EntryRecord{Name: name, Health: s.health, State: entryRunning, Proc: &s.childRec})
}

// TestStatusReportsOpenFilesAgainstTheSoftLimit is #431's threshold test. A fake process's limits
// file and fd directory are read through the real reader, and the report says what is in the table,
// all of it typed here: 79% and 80% of the soft limit are not degraded, 81% is, and the same use is
// not degraded when the soft limit is far above it, whatever the hard limit is. The host stays
// healthy with no reason in every case, which is what makes status exit 0.
//
// The soft limit is below the hard one in the cases that separate them: a check made against the
// hard limit instead of the soft one reads 790, 800 and 810 of 100000 as 1% and never degraded.
func TestStatusReportsOpenFilesAgainstTheSoftLimit(t *testing.T) {
	for _, name := range []string{"front", "frps"} {
		for _, c := range []struct {
			name       string
			soft, hard string // as /proc writes them
			inUse      int
			degraded   bool
		}{
			{"79% of the soft limit", "1000", "100000", 790, false},
			{"exactly 80% of the soft limit is not past it", "1000", "100000", 800, false},
			{"81% of the soft limit", "1000", "100000", 810, true},
			{"81% with the soft limit at the hard one", "1000", "1000", 810, true},
			{"81% of a soft limit that is not a round number", "1023", "65536", 829, true},
			{"80% of a soft limit that is not a round number", "1023", "65536", 818, false},
			{"far below the soft limit", "100000", "100000", 810, false},
			{"an unlimited soft limit is never passed", "unlimited", "unlimited", 3000, false},
		} {
			t.Run(name+", "+c.name, func(t *testing.T) {
				s := newSupervisedFakeHost(t)
				s.asTunnel(name)
				writeFakeLimits(t, s.root, s.childRec.PID, limitsWith(t, c.soft, c.hard))
				fillFakeFds(t, s.root, s.childRec.PID, c.inUse)
				rep := s.status(true)
				if rep.Health != Healthy || len(rep.Reasons) != 0 {
					t.Fatalf("health %s, reasons %v: open files are never a health problem", rep.Health, rep.Reasons)
				}
				if len(rep.Controllers) != 1 {
					t.Fatalf("entries %+v", rep.Controllers)
				}
				eh := rep.Controllers[0]
				if eh.Name != name || !eh.Running || eh.OpenFiles == nil || eh.OpenFilesUnknown != "" {
					t.Fatalf("the entry is %+v", eh)
				}
				if eh.OpenFiles.InUse != c.inUse || eh.OpenFiles.Soft.String() != c.soft || eh.OpenFiles.Hard.String() != c.hard {
					t.Errorf("read %d in use, soft %s, hard %s; the process has %d descriptors, soft %s, hard %s",
						eh.OpenFiles.InUse, eh.OpenFiles.Soft, eh.OpenFiles.Hard, c.inUse, c.soft, c.hard)
				}
				if got, want := eh.Degraded == DegradedOpenFiles, c.degraded; got != want || (!c.degraded && eh.Degraded != "") {
					t.Errorf("degraded %q, want degraded: %v", eh.Degraded, c.degraded)
				}
			})
		}
	}
}

// TestDegradedOpenFilesText: what the report carries is the words HBEP-11 gives, and the JSON of the
// entry names the reading under open_files, a number for each limit or "unlimited".
func TestDegradedOpenFilesText(t *testing.T) {
	if DegradedOpenFiles != "open files" {
		t.Errorf("DegradedOpenFiles = %q; status prints it as \"degraded: open files\"", DegradedOpenFiles)
	}
	s := newSupervisedFakeHost(t)
	s.asTunnel("front")
	writeFakeLimits(t, s.root, s.childRec.PID, limitsWith(t, "1000", "unlimited"))
	fillFakeFds(t, s.root, s.childRec.PID, 810)
	b, err := json.Marshal(s.status(true).Controllers[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"open_files":{"in_use":810,"soft":1000,"hard":"unlimited"}`, `"degraded":"open files"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the entry's JSON lacks %s:\n%s", want, b)
		}
	}
}

// TestOpenFilesAndUsageAreBothNamed: a front that has lost usage records and is low on descriptors
// says both; neither cause hides the other.
func TestOpenFilesAndUsageAreBothNamed(t *testing.T) {
	const failing = `{"synced":true,"usage":{"state":"failing","lostRecords":3}}`
	const ok = `{"synced":true,"usage":{"state":"ok","lostRecords":0}}`
	for _, c := range []struct {
		name  string
		body  string
		inUse int
		want  string
	}{
		{"both", failing, 810, "usage writes failing; open files"},
		{"usage only", failing, 790, "usage writes failing"},
		{"open files only", ok, 810, "open files"},
		{"neither", ok, 790, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newSupervisedFakeHost(t)
			*s.body = c.body
			s.asTunnel("front")
			writeFakeLimits(t, s.root, s.childRec.PID, limitsWith(t, "1000", "100000"))
			fillFakeFds(t, s.root, s.childRec.PID, c.inUse)
			rep := s.status(true)
			if rep.Health != Healthy || len(rep.Reasons) != 0 {
				t.Fatalf("health %s, reasons %v", rep.Health, rep.Reasons)
			}
			if got := rep.Controllers[0].Degraded; got != c.want {
				t.Errorf("degraded %q, want %q", got, c.want)
			}
		})
	}
}

// TestOpenFilesOfOtherProcessesAreNotAsked: only frps and the front are tunnel processes. The
// controller's descriptors are not read, and a low or unreadable limit of it says nothing.
func TestOpenFilesOfOtherProcessesAreNotAsked(t *testing.T) {
	s := newSupervisedFakeHost(t) // its entry is the controller
	writeFakeLimits(t, s.root, s.childRec.PID, limitsWith(t, "1000", "1000"))
	fillFakeFds(t, s.root, s.childRec.PID, 990)
	rep := s.status(true)
	if eh := rep.Controllers[0]; eh.Name != "controller" || eh.OpenFiles != nil || eh.OpenFilesUnknown != "" || eh.Degraded != "" {
		t.Errorf("the controller is reported as %+v; want no open files said of it", eh)
	}
}

// TestStatusSaysOpenFilesAreUnknownWhenTheyCannotBeRead: a limits file or an fd directory that
// cannot be read gives an unknown, with the cause, and never a number: not 0 in use, not healthy
// by default, and not degraded. It does not make the host unhealthy either, as for the usage sink's
// "usage state unknown". The failures are made by what the kernel would do: no file (a process
// that is gone, or a kernel without the file), a refused read (EISDIR stands in for EACCES, which a
// test running as root would not see), an fd path that is no directory, and a row that is not the
// kernel's.
func TestStatusSaysOpenFilesAreUnknownWhenTheyCannotBeRead(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(t *testing.T, root string, pid int)
		want  string // in the cause
	}{
		{"no limits file", func(t *testing.T, root string, pid int) {}, "/limits"},
		{"a limits file that cannot be read", func(t *testing.T, root string, pid int) {
			if err := os.MkdirAll(filepath.Join(root, strconv.Itoa(pid), "limits"), 0o755); err != nil {
				t.Fatal(err)
			}
		}, "/limits"},
		{"a limits file without the row", func(t *testing.T, root string, pid int) {
			writeFakeLimits(t, root, pid, "Limit                     Soft Limit           Hard Limit           Units     \nMax processes             128232               128232               processes \n")
		}, `no "Max open files" row`},
		{"a row that is not the kernel's", func(t *testing.T, root string, pid int) {
			writeFakeLimits(t, root, pid, "Max open files            lots                 4096                 files     \n")
		}, "neither a number nor unlimited"},
		{"an fd path that is no directory", func(t *testing.T, root string, pid int) {
			writeFakeLimits(t, root, pid, limitsWith(t, "1000", "1000"))
			if err := os.RemoveAll(filepath.Join(root, strconv.Itoa(pid), "fd")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, strconv.Itoa(pid), "fd"), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}, "/fd"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newSupervisedFakeHost(t)
			s.asTunnel("front")
			c.setup(t, s.root, s.childRec.PID)
			rep := s.status(true)
			if len(rep.Controllers) != 1 {
				t.Fatalf("entries %+v", rep.Controllers)
			}
			eh := rep.Controllers[0]
			if eh.OpenFiles != nil || !strings.Contains(eh.OpenFilesUnknown, c.want) || eh.Degraded != "" {
				t.Fatalf("the entry is %+v; want no reading, an unknown naming %q, and nothing degraded", eh, c.want)
			}
			// The fd path that is no directory also costs the listener check its socket, so the
			// host's health is asked only for the cases whose process still holds what it must.
			if c.name != "an fd path that is no directory" && (rep.Health != Healthy || len(rep.Reasons) != 0) {
				t.Errorf("health %s, reasons %v: an unknown is not a health problem", rep.Health, rep.Reasons)
			}
			b, err := json.Marshal(eh)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(b), `"open_files":`) || !strings.Contains(string(b), `"open_files_unknown":`) {
				t.Errorf("the JSON shows a reading, or no unknown:\n%s", b)
			}
		})
	}
}

// checkTunnelChild is checkChild with the entry named as a tunnel process.
func (s *supervisedFakeHost) checkTunnelChild(name string) (EntryHealth, []string) {
	s.t.Helper()
	sup, v, d := openRec(s.supRec)
	if v != Ours {
		s.t.Fatalf("the fake supervisor is %v: %s", v, d)
	}
	defer sup.Close()
	e := EntryRecord{Name: name, Health: s.health, Ports: s.ports, State: entryRunning, Proc: &s.childRec}
	return checkEntry(sup, s.supRec.PID, e)
}

// TestOpenFilesAreNeverReadFromAProcessThatExited: the pidfd fires at each look in turn, as in
// TestCheckEntryProcessExitsDuringTheCheck, and at every one the answer is one of two: a reading of
// the process's own 120 descriptors, taken while it lived, or no reading. Never a number for a
// process that was gone when its files were read, and never silence for one that is reported as
// running. At least one look must be the one right after the files were read, where the exit is
// found by the verification that follows them.
func TestOpenFilesAreNeverReadFromAProcessThatExited(t *testing.T) {
	fixture := func(t *testing.T) *supervisedFakeHost {
		s := newSupervisedFakeHost(t)
		writeFakeLimits(t, s.root, s.childRec.PID, limitsWith(t, "1000", "100000"))
		fillFakeFds(t, s.root, s.childRec.PID, 120)
		return s
	}
	s := fixture(t)
	looks := s.exitsAtLook(0)
	if eh, problems := s.checkTunnelChild("front"); len(problems) != 0 || eh.OpenFiles == nil || eh.OpenFiles.InUse != 120 {
		t.Fatalf("the control, a process that lives: %+v, problems %v", eh, problems)
	}
	total := looks()
	if total < 4 {
		t.Fatalf("only %d looks at the pidfd in a whole check", total)
	}
	foundAfterTheRead := false
	for k := 1; k <= total; k++ {
		t.Run(fmt.Sprintf("exits at look %d", k), func(t *testing.T) {
			s := fixture(t)
			s.exitsAtLook(k)
			eh, problems := s.checkTunnelChild("front")
			switch {
			case eh.OpenFiles != nil:
				if eh.OpenFiles.InUse != 120 || eh.OpenFiles.Soft.String() != "1000" || eh.OpenFiles.Hard.String() != "100000" || eh.OpenFilesUnknown != "" {
					t.Errorf("a reading of %+v (unknown %q); the process has 120 descriptors, soft 1000, hard 100000", eh.OpenFiles, eh.OpenFilesUnknown)
				}
			case eh.OpenFilesUnknown != "":
				if strings.Contains(eh.OpenFilesUnknown, "no longer the recorded one") || strings.Contains(eh.OpenFilesUnknown, "changed while") {
					foundAfterTheRead = true
				}
				if eh.Degraded != "" {
					t.Errorf("degraded %q on an unknown", eh.Degraded)
				}
			case eh.Running:
				t.Errorf("a running process with neither a reading nor an unknown: %+v, problems %v", eh, problems)
			}
		})
	}
	if !foundAfterTheRead {
		t.Error("no look was the one that finds the exit after the files were read: the test does not reach the case it is for")
	}
}

// TestOpenFilesWarningsAtUp: up warns when a tunnel process's HARD limit is below 5,400, HBEP-6's
// figure, naming the limit and the figure, and says nothing for one at or above it. The processes
// are the fake supervisor's two entries, read through the real reader and the real decision; only
// k3s is not started. 4,096 warns and 65,536 does not (#431); 5,399 warns and 5,400 does not, the
// edge of the figure; an unlimited limit does not. A limit that cannot be read is said not to have
// been checked, and a process that is not a tunnel process is not asked.
func TestOpenFilesWarningsAtUp(t *testing.T) {
	const frontPID, frpsPID = 4351, 4352
	for _, c := range []struct {
		name         string
		front, frps  string // the hard limit as /proc writes it; "-" writes no limits file
		controller   string // the controller's hard limit; it is not a tunnel process
		wantWarnings []string
	}{
		{"4,096 warns, 65,536 does not", "4096", "65536", "100", []string{"front (pid 4351) has a hard limit of 4096 open files, below the 5400 that HBEP-6 gives"}},
		{"65,536 does not warn, 4,096 does", "65536", "4096", "100", []string{"frps (pid 4352) has a hard limit of 4096 open files, below the 5400 that HBEP-6 gives"}},
		{"5,399 warns and 5,400 does not", "5399", "5400", "100", []string{"front (pid 4351) has a hard limit of 5399 open files, below the 5400 that HBEP-6 gives"}},
		{"both below", "1024", "4096", "100", []string{"front (pid 4351) has a hard limit of 1024 open files", "frps (pid 4352) has a hard limit of 4096 open files"}},
		{"an unlimited hard limit does not", "unlimited", "1048576", "100", nil},
		{"both high", "65536", "1048576", "100", nil},
		{"a limit that cannot be read is not a pass", "-", "65536", "100", []string{"the open-files limit of front (pid 4351) was not checked: pid 4351: /proc/4351/limits"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newSupervisedFakeHost(t)
			recs := map[string]*ProcRec{}
			for name, pid := range map[string]int{"front": frontPID, "frps": frpsPID} {
				writeFakeProc(t, s.root, pid, "S", s.supRec.PID, uint64(4000+pid))
				s.f.setAlive(pid, true)
				recs[name] = &ProcRec{Name: name, PID: pid, StartTicks: uint64(4000 + pid), BootID: "boot-A"}
			}
			for name, hard := range map[string]string{"front": c.front, "frps": c.frps} {
				if hard != "-" {
					writeFakeLimits(t, s.root, recs[name].PID, limitsWith(t, "1000", hard))
				}
			}
			writeFakeLimits(t, s.root, s.childRec.PID, limitsWith(t, "1000", c.controller))
			r := &reporter{l: s.l, f: SupervisorFile{Schema: supervisorSchema, Supervisor: s.supRec, Entries: []EntryRecord{
				{Name: "controller", Health: s.health, State: entryRunning, Proc: &s.childRec},
				{Name: "front", Health: s.health, State: entryRunning, Proc: recs["front"]},
				{Name: "frps", Health: s.health, State: entryRunning, Proc: recs["frps"]},
			}}}
			r.writeLocked()

			got := openFilesWarnings(s.l)
			if len(got) != len(c.wantWarnings) {
				t.Fatalf("warnings %q, want %d: %q", got, len(c.wantWarnings), c.wantWarnings)
			}
			for i, w := range c.wantWarnings {
				if !strings.HasPrefix(got[i], "warning: ") || !strings.Contains(got[i], w) {
					t.Errorf("warning %d is %q, want it to start with \"warning: \" and contain %q", i, got[i], w)
				}
			}
		})
	}
}

// TestOpenFilesWarningsAtUpNeedTheRecord: with no record of the supervisor's processes the check
// says it could not look; it does not pass. With the record but no tunnel process in it (a host
// with no frps and no front) there is nothing to check and nothing is said.
func TestOpenFilesWarningsAtUpNeedTheRecord(t *testing.T) {
	s := newSupervisedFakeHost(t)
	if got := openFilesWarnings(s.l); len(got) != 0 {
		t.Errorf("a record with only the controller: %q, want none", got)
	}
	if err := os.Remove(s.l.supervisorFile()); err != nil {
		t.Fatal(err)
	}
	got := openFilesWarnings(s.l)
	if len(got) != 1 || !strings.Contains(got[0], "were not checked: cannot read the supervisor's record") {
		t.Errorf("no record: %q, want one warning that the limits were not checked", got)
	}
}

// TestOpenFilesDegradedDoesNotOverflow: the comparison is the exact integer one at the widest limits
// /proc can state, where a product of the limit and 80 would wrap.
func TestOpenFilesDegradedDoesNotOverflow(t *testing.T) {
	for _, c := range []struct {
		soft     uint64
		inUse    int
		degraded bool
	}{
		{18446744073709551614, 1, false},
		{18446744073709551614, 1 << 62, false},
		{10000000000000000000, 9223372036854775807, true},
		{10000000000000000000, 7999999999999999999, false},
		{100, 80, false},
		{100, 81, true},
		{1, 0, false},
		{1, 1, true},
		{0, 1, true},
		{0, 0, false},
	} {
		if got := (OpenFiles{InUse: c.inUse, Soft: Limit{Value: c.soft}}).degraded(); got != c.degraded {
			t.Errorf("%d descriptors of a soft limit of %d: degraded %v, want %v", c.inUse, c.soft, got, c.degraded)
		}
	}
}

// limitOf is a limit as getrlimit(2) and prlimit(2) state it: RLIM_INFINITY is "unlimited".
func limitOf(v uint64) Limit {
	if v == unix.RLIM_INFINITY {
		return Limit{Unlimited: true}
	}
	return Limit{Value: v}
}

// stableFdCount lists /proc/<pid>/fd the moment before and the moment after read runs, and returns
// what read returned together with the listing, for a snapshot that counts only when the two
// listings agree. A descriptor that another goroutine or process opened between them is retried,
// never excused, and no descriptor set is assumed: the count is whatever the process holds, listed
// by the test itself in the same moment.
func stableFdCount(t *testing.T, pid int, read func() (OpenFiles, error)) OpenFiles {
	t.Helper()
	count := func() int {
		ents, err := os.ReadDir("/proc/" + strconv.Itoa(pid) + "/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(ents)
	}
	var last string
	for attempt := 0; attempt < 50; attempt++ {
		before := count()
		got, err := read()
		after := count()
		if err != nil {
			t.Fatal(err)
		}
		if before == after && got.InUse == before {
			return got
		}
		last = fmt.Sprintf("listed %d descriptors before, the reader found %d, listed %d after", before, got.InUse, after)
	}
	t.Fatalf("no stable snapshot in 50 attempts; the last: %s", last)
	return OpenFiles{}
}

// TestReadOpenFilesOfThisProcessFitsTheKernel is the positive control for the parser: it reads this
// test process's own /proc/self through the same reader, with a pidfd and the record of this
// process, and compares the reading with what the process itself knows: its limits from
// getrlimit(2), which the Go runtime has already raised, and its descriptors, counted by the test
// in the same moment (stableFdCount).
func TestReadOpenFilesOfThisProcessFitsTheKernel(t *testing.T) {
	if procRoot != "/proc" {
		t.Fatalf("procRoot is %q: this control reads the real /proc", procRoot)
	}
	rec, err := newRec("self", os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	h, v, d := openRec(rec)
	if v != Ours {
		t.Fatalf("this process is %v: %s", v, d)
	}
	defer h.Close()
	var rl unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rl); err != nil {
		t.Fatal(err)
	}
	got := stableFdCount(t, os.Getpid(), h.readOpenFiles)
	if want := limitOf(rl.Cur); got.Soft != want {
		t.Errorf("read soft limit %s; getrlimit says %s", got.Soft, want)
	}
	if want := limitOf(rl.Max); got.Hard != want {
		t.Errorf("read hard limit %s; getrlimit says %s", got.Hard, want)
	}
}

// TestReadOpenFilesOfAnotherProcessFitsTheKernel is the same control on a live process that is not
// this one, the shape of a supervised child: a shell that lowers its own soft limit to 777 and
// becomes sleep, so the soft limit differs from the hard one and from this process's. The oracle for
// its limits is prlimit(2) on its PID, which does not read /proc/<pid>/limits, and for its
// descriptors the test's own listing in the same moment. The child's descriptor set is not assumed.
func TestReadOpenFilesOfAnotherProcessFitsTheKernel(t *testing.T) {
	cmd := startBystander(t, "bash", "-c", "ulimit -Sn 777 && exec sleep 300")
	pid := cmd.Process.Pid
	waitExecOfSleep(t, pid)
	rec, err := newRec("bystander", pid)
	if err != nil {
		t.Fatal(err)
	}
	h, v, d := openRec(rec)
	if v != Ours {
		t.Fatalf("the bystander is %v: %s", v, d)
	}
	defer h.Close()
	var rl unix.Rlimit
	if err := unix.Prlimit(pid, unix.RLIMIT_NOFILE, nil, &rl); err != nil {
		t.Fatal(err)
	}
	if rl.Cur != 777 || rl.Max == 777 {
		t.Fatalf("prlimit says the bystander's limits are %d and %d: the setup did not give it a soft limit of 777 below its hard one", rl.Cur, rl.Max)
	}
	got := stableFdCount(t, pid, h.readOpenFiles)
	if want := limitOf(rl.Cur); got.Soft != want || got.Soft.Value != 777 {
		t.Errorf("read soft limit %s; prlimit says %s", got.Soft, want)
	}
	if want := limitOf(rl.Max); got.Hard != want {
		t.Errorf("read hard limit %s; prlimit says %s", got.Hard, want)
	}
}
