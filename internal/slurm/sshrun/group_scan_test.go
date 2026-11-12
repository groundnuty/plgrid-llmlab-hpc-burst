package sshrun

// The scan of the package's non-test sources (issue 363): the only signal is
// killGroup's, behind the guards, and the only reaps are the barrier's, the
// failed pin's bounded cleanup and the probe's. It matches what is called, not
// what a receiver is named, so an alias of the command does not slip by.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// sourceSite is one place in a function where a signal or a reap is made.
type sourceSite struct{ fn, what string }

// signalAndReapSites lists, in one parsed file, every call that signals a
// process or reaps one, by what is called and not by what its receiver is
// named: a call of a method named Wait with no argument (an exec.Cmd, an
// os.Process or an alias of either), of Kill or Signal on anything, of the
// Wait and kill families of the syscall and unix packages, of os's lookup of
// a process by PID, and the pidfd signal. It sees calls inside functions only.
func signalAndReapSites(f *ast.File) []sourceSite {
	var out []sourceSite
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv, member := types.ExprString(sel.X), sel.Sel.Name
			pkgCall := recv == "syscall" || recv == "unix"
			switch {
			case member == "Wait" && len(call.Args) == 0,
				pkgCall && (member == "Wait4" || member == "Wait3" || member == "Waitid"),
				member == "Kill", member == "Signal",
				pkgCall && (member == "Tgkill" || member == "Tkill" || member == "Killpg" || member == "PidfdSendSignal"),
				recv == "os" && member == "FindProcess":
				out = append(out, sourceSite{fn.Name.Name, recv + "." + member})
			}
			return true
		})
	}
	return out
}

func parseSource(t *testing.T, name, src string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// The scan finds a signal or a reap whatever its receiver is called: an alias
// of the command, a Wait on the process, and the syscall forms. The fixture
// shows each is caught, and that a clean function is not.
func TestTheSignalScanCatchesAliasesAndOtherForms(t *testing.T) {
	src := `package x
func alias(cmd *exec.Cmd) { p := cmd; p.Wait() }
func viaProcess(cmd *exec.Cmd) { cmd.Process.Wait() }
func family() { unix.Wait4(1, nil, 0, nil); syscall.Wait4(1, nil, 0, nil); unix.Waitid(1, 1, nil, 0, nil) }
func killAlias(cmd *exec.Cmd) { p := cmd.Process; p.Kill(); p.Signal(nil) }
func direct() { unix.@K(-5, 9); syscall.@K(-5, 9); os.@F(5) }
func clean() { h(); g.signal(1); leader.reap(cmd) }
`
	src = strings.NewReplacer("@K", "Kill", "@F", "FindProcess").Replace(src)
	got := map[string]int{}
	for _, site := range signalAndReapSites(parseSource(t, "fixture.go", src)) {
		got[site.fn]++
	}
	want := map[string]int{"alias": 1, "viaProcess": 1, "family": 3, "killAlias": 2, "direct": 3}
	for fn, n := range want {
		if got[fn] != n {
			t.Errorf("%s: the scan found %d sites, want %d", fn, got[fn], n)
		}
	}
	if got["clean"] != 0 {
		t.Errorf("the scan flagged a clean function: %d sites", got["clean"])
	}
}

// The package's only numeric signal is killGroup, reached only through the
// sink New sets (the transport's) and newRelay sets (the relays', for ssh -G).
// The pin failure's kill goes through the leader's own pidfd, and only the barrier (groupLeader.reap), the failed pin's bounded cleanup
// and the probe reap a child. A new kill(2), a signal through an os.Process
// (under any alias), a process lookup by PID, a pidfd signal or a reap
// anywhere else fails this scan. It reads the package's non-test sources, so
// it needs no signal to run.
func TestNoUnguardedSignalsInThePackage(t *testing.T) {
	// The calls each function may make, and how often.
	allowed := map[string]map[string]int{
		"killGroup":   {unixKill: 1},
		"reap":        {"cmd.Wait": 1},
		"reapBounded": {"cmd.Wait": 1},
		"start":       {"cmd.Wait": 1},
		"probePidfd":  {"cmd.Wait": 1},
	}
	// Where each name may appear: enclosing function -> its count.
	identUses := map[string]map[string]int{
		"killGroup":       {"New": 1, "newRelay": 1},
		"pidfdSendSignal": {"start": 1, "probePidfd": 2, "probeChild": 1},
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]map[string]int{}
	uses := map[string]map[string]int{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f := parseSource(t, name, mustRead(t, name))
		for _, site := range signalAndReapSites(f) {
			if found[site.fn] == nil {
				found[site.fn] = map[string]int{}
			}
			found[site.fn][site.what]++
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if _, watched := identUses[id.Name]; watched && fn.Name.Name != id.Name {
						if uses[id.Name] == nil {
							uses[id.Name] = map[string]int{}
						}
						uses[id.Name][fn.Name.Name]++
					}
				}
				return true
			})
		}
	}
	for fn, calls := range found {
		for what, n := range calls {
			if allowed[fn][what] != n {
				t.Errorf("%s makes %d calls of %s, and may make %d: signals and reaps belong in the guarded functions", fn, n, what, allowed[fn][what])
			}
		}
	}
	for fn, calls := range allowed {
		for what, n := range calls {
			if found[fn][what] != n {
				t.Errorf("%s makes %d calls of %s, want %d", fn, found[fn][what], what, n)
			}
		}
	}
	for ident, want := range identUses {
		for fn, n := range uses[ident] {
			if want[fn] != n {
				t.Errorf("%s names %s %d times, want %d: it may be named only in %v", fn, ident, n, want[fn], want)
			}
		}
		for fn, n := range want {
			if uses[ident][fn] != n {
				t.Errorf("%s names %s %d times, want %d", fn, ident, uses[ident][fn], n)
			}
		}
	}
}

// unixKill is how the scan spells the one numeric signal's call.
const unixKill = "unix" + "." + "Kill"

func mustRead(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
