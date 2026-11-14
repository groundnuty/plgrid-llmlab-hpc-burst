package livechecks

// Every row that starts the stack checks LIVE_BIN_DIR's layout BEFORE the slot is read and before any build (issue #444; the
// science agent's ruling on #145, 12 Nov 2026). The live tests take the front's frps and frpc from the TOP of the directory and
// hpcburst up takes k3s from k3s-1.36.5/ in it; a directory laid out the launcher's way (frp under frp-0.71.1/) once failed
// alpha-athena-now-serve 0.08 s into its test, after the slot had been read. The script now refuses such a row with exit 3 and
// names the setting, never its value.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// stackRowCount is how many rows of alphaShapes start the stack, written out: the handover, the two no-overlaps, the two tunnel
// hours, row A, the router and keeper rows (issue #521) and the Helios router row (issue #566). A row added later must be added to this
// check, not left out of it.
const stackRowCount = 9

func stackRows(t *testing.T) []alphaShape {
	t.Helper()
	var out []alphaShape
	for _, s := range alphaShapes {
		if s.needsStack {
			out = append(out, s)
		}
	}
	if len(out) != stackRowCount {
		t.Fatalf("%d rows start the stack, the table below was written for %d: add the new row's case", len(out), stackRowCount)
	}
	return out
}

// The wrong layouts, each worked out from what the script must find: k3s-1.36.5/k3s, frps and frpc, each an executable file, the
// last two at the top.
var wrongBinDirs = []struct {
	name   string
	mutate func(t *testing.T, bin string)
	want   []string
}{
	{"the launcher's layout: frp under its version's directory", func(t *testing.T, bin string) {
		if err := os.MkdirAll(filepath.Join(bin, "frp-0.71.1"), 0o755); err != nil {
			t.Fatal(err)
		}
		for _, f := range []string{"frps", "frpc"} {
			if err := os.Rename(filepath.Join(bin, f), filepath.Join(bin, "frp-0.71.1", f)); err != nil {
				t.Fatal(err)
			}
		}
	}, []string{"LIVE_BIN_DIR has no executable frps at its top level", "LIVE_BIN_DIR has no executable frpc at its top level"}},
	{"no frps at the top", func(t *testing.T, bin string) { remove(t, bin, "frps") },
		[]string{"LIVE_BIN_DIR has no executable frps at its top level"}},
	{"no frpc at the top", func(t *testing.T, bin string) { remove(t, bin, "frpc") },
		[]string{"LIVE_BIN_DIR has no executable frpc at its top level"}},
	{"an frps that is not executable", func(t *testing.T, bin string) { chmod(t, bin, "frps", 0o644) },
		[]string{"LIVE_BIN_DIR has no executable frps at its top level"}},
	{"no k3s", func(t *testing.T, bin string) { remove(t, bin, "k3s-1.36.5/k3s") },
		[]string{"LIVE_BIN_DIR has no executable k3s-1.36.5/k3s"}},
	{"a k3s that is not executable", func(t *testing.T, bin string) { chmod(t, bin, "k3s-1.36.5/k3s", 0o644) },
		[]string{"LIVE_BIN_DIR has no executable k3s-1.36.5/k3s"}},
	{"nothing in it, every problem at once", func(t *testing.T, bin string) {
		for _, f := range []string{"frps", "frpc", "k3s-1.36.5/k3s"} {
			remove(t, bin, f)
		}
	}, []string{"LIVE_BIN_DIR has no executable k3s-1.36.5/k3s", "LIVE_BIN_DIR has no executable frps at its top level", "LIVE_BIN_DIR has no executable frpc at its top level"}},
}

func remove(t *testing.T, bin, f string) {
	t.Helper()
	if err := os.Remove(filepath.Join(bin, f)); err != nil {
		t.Fatal(err)
	}
}

func chmod(t *testing.T, bin, f string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(filepath.Join(bin, f), mode); err != nil {
		t.Fatal(err)
	}
}

// Each stack row, on each wrong layout: exit 3, the setting named and its value not, and nothing asked of the cluster (the fake squeue
// is never called, so the slot was not read), nothing built, no job submitted. The control, the same row on a right layout, does read
// the queue: so the absence of a squeue call is the check's doing, not a row that never reads one.
func TestEveryStackRowRefusesAWrongBinDirBeforeTheSlotIsRead(t *testing.T) {
	for _, s := range stackRows(t) {
		for _, c := range wrongBinDirs {
			t.Run(s.row+"/"+c.name, func(t *testing.T) {
				t.Parallel()
				w := newAlphaWorld(t)
				bin := filepath.Join(w.dir, "bindir")
				c.mutate(t, bin)
				res := w.run(nil, "--run", s.row, "--yes")
				if res.code != 3 {
					t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
				}
				w.contains(res, c.want...)
				if strings.Contains(res.out, bin) {
					t.Errorf("the refusal names the setting's value:\n%s", res.out)
				}
				if ev := w.events(); index(ev, "squeue", 1) >= 0 {
					t.Errorf("a row with a wrong LIVE_BIN_DIR read the queue (the slot):\n%s", strings.Join(ev, "\n"))
				}
				if g := w.stateFile("go-argv"); g != "" || w.stateFile("sbatch-argv") != "" || index(w.events(), "testbin", 1) >= 0 {
					t.Errorf("a refused run built, submitted or ran the test: %s %s", g, w.stateFile("sbatch-argv"))
				}
			})
		}
	}
}

// A right layout passes the check: the row goes on, reads the queue and runs, on every stack row.
func TestEveryStackRowWithARightBinDirPassesTheCheckAndReadsTheQueue(t *testing.T) {
	for _, s := range stackRows(t) {
		t.Run(s.row, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			res := w.run(nil, "--run", s.row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d, want 0:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, "LIVE_BIN_DIR") {
				t.Errorf("a right layout was refused:\n%s", res.out)
			}
			if index(w.events(), "squeue", 1) < 0 {
				t.Errorf("the row never read the queue, so the refusals above prove nothing about the order:\n%s", strings.Join(w.events(), "\n"))
			}
		})
	}
}

// The rows that start no stack are not asked for the layout: the accounting rows, the conformance rows, the pre-staging row and the reach
// rows (the host-dialed ones too) need no frp and no k3s from LIVE_BIN_DIR, which here names a directory that does not exist. Each
// is run without --yes, which every cluster row answers with exit 4 once its settings pass, so a refusal for any other reason (exit 3, or
// the layout's words) fails the case; and the row set is the script's own table, read, so that a row added later is in it or named here.
func TestTheRowsThatStartNoStackAreNotAskedForTheBinDir(t *testing.T) {
	w := newWorld(t)
	w.reachSettings(func(m map[string]string) { m["LIVE_BIN_DIR"] = "/no/such/dir" })
	w.useAlphaStubs()
	// The cluster table's rows that the stack rows are not: written out, and checked against the table below.
	noStack := []string{
		"gpu-accounting-helios", "gpu-accounting-athena", "gpu-accounting-athena-now",
		"conformance-helios", "conformance-helios-gh200", "conformance-athena",
		"alpha-athena-now-stage",
		"reach-helios", "reach-athena", "reach-host-dialed-helios", "reach-host-dialed-athena",
	}
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	inTable := map[string]bool{}
	for _, l := range block(t, string(b), "CLUSTER SHAPES") {
		inTable[strings.Split(l, "|")[0]] = true
	}
	stack := map[string]bool{}
	for _, s := range stackRows(t) {
		stack[s.row] = true
	}
	for _, row := range noStack {
		if !inTable[row] {
			t.Errorf("%s is not a row of the script's cluster table", row)
		}
		if stack[row] {
			t.Errorf("%s starts the stack, so it is not a row to leave out of the layout check", row)
		}
	}
	// Every other cluster row is a stack row or is named above; real-engine runs a plan of its own and takes no bin dir either.
	for row := range inTable {
		if !stack[row] && !slices.Contains(noStack, row) && row != "real-engine" {
			t.Errorf("%s is a cluster row that is neither a stack row nor named as one that starts no stack: add it to the right list", row)
		}
	}
	for _, row := range noStack {
		t.Run(row, func(t *testing.T) {
			res := w.run(nil, "--run", row)
			if res.code != 4 || strings.Contains(res.out, "LIVE_BIN_DIR") {
				t.Errorf("exit %d, want 4 (not run, no --yes) and no word of LIVE_BIN_DIR:\n%s", res.code, res.out)
			}
		})
	}
}
