package hostdial

// The scan of the package's non-test sources (issue #370, from PR 365's): the
// only group signal is killGroup's, behind the guards, and the only reaps of a
// pinned relay are the barrier's. It matches what is called, not what a
// receiver is named, so an alias of the command does not slip by, and it reads
// package-level declarations and method values as well as calls inside
// functions (the review of PR 378).

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// sourceSite is one place in a function where a signal or a reap is made.
type sourceSite struct{ fn, what string }

// packageLevel is the name the scan gives the package-level declarations (var and const) of a file.
const packageLevel = "<package>"

// classify says whether a selector is a signal or a reap, and what to call it. nargs is the number of arguments
// of the call the selector is the function of, or -1 when it is not called (a method value, or a package-level
// value such as "var k = unix.Kill"): a method named Wait, Run, Output or CombinedOutput, with no argument or
// not called (an exec.Cmd, an os.Process or an alias of either; a sync.WaitGroup's Wait is listed in the scan's
// table), Kill or Signal on anything, the Wait and kill families of the syscall and unix packages, os's lookup
// of a process by PID, and the pidfd signal.
func classify(recv, member string, nargs int) (string, bool) {
	pkgCall := recv == "syscall" || recv == "unix"
	if member == "Signal" && nargs < 0 && (pkgCall || recv == "os") {
		return "", false // the type syscall.Signal, unix.Signal or os.Signal, not a signal sent
	}
	switch {
	case (member == "Wait" || member == "Run" || member == "Output" || member == "CombinedOutput") && nargs <= 0,
		pkgCall && (member == "Wait4" || member == "Wait3" || member == "Waitid"),
		member == "Kill", member == "Signal",
		pkgCall && (member == "Tgkill" || member == "Tkill" || member == "Killpg" || member == "PidfdSendSignal"),
		recv == "os" && member == "FindProcess":
		return recv + "." + member, true
	}
	return "", false
}

// signalAndReapSites lists, in one parsed file, every selector that signals a process or reaps one, by what is
// called and not by what its receiver is named (see classify), in the bodies of functions, in package-level
// declarations (named packageLevel), whether called or only referred to as a value.
func signalAndReapSites(f *ast.File) []sourceSite {
	var out []sourceSite
	for _, d := range f.Decls {
		name := packageLevel
		switch x := d.(type) {
		case *ast.FuncDecl:
			name = x.Name.Name
		case *ast.GenDecl:
			if x.Tok == token.IMPORT {
				continue
			}
		}
		// The selectors that are the function of a call, with the call's argument count.
		called := map[*ast.SelectorExpr]int{}
		ast.Inspect(d, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					called[sel] = len(call.Args)
				}
			}
			return true
		})
		ast.Inspect(d, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			nargs, isCall := called[sel]
			if !isCall {
				nargs = -1
			}
			if what, ok := classify(types.ExprString(sel.X), sel.Sel.Name, nargs); ok {
				out = append(out, sourceSite{name, what})
			}
			return true
		})
	}
	return out
}

// watchedIdentSites lists, in one parsed file, every use of a watched identifier: in functions and in
// package-level declarations (named packageLevel), not the name a declaration defines.
func watchedIdentSites(f *ast.File, watched map[string]bool) []sourceSite {
	var out []sourceSite
	for _, d := range f.Decls {
		name := packageLevel
		defining := map[*ast.Ident]bool{}
		switch x := d.(type) {
		case *ast.FuncDecl:
			name = x.Name.Name
			defining[x.Name] = true
		case *ast.GenDecl:
			if x.Tok == token.IMPORT {
				continue
			}
			for _, sp := range x.Specs {
				switch v := sp.(type) {
				case *ast.ValueSpec:
					for _, id := range v.Names {
						defining[id] = true
					}
				case *ast.TypeSpec:
					defining[v.Name] = true
				}
			}
		}
		ast.Inspect(d, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && watched[id.Name] && !defining[id] {
				out = append(out, sourceSite{name, id.Name})
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
func clean() { h(); g.signal(1); leader.reap(cmd); srv.Run(ctx); q.Wait(ctx); r.Output2() }
var pkgAlias = unix.@K
var pkgAlso, other = syscall.@K, 1
func methodValue(cmd *exec.Cmd) { f := cmd.Process.@K; f() }
func reaps(cmd *exec.Cmd) { cmd.Run(); cmd.Output(); cmd.CombinedOutput() }
func reapValue(cmd *exec.Cmd) { g := cmd.Run; g() }
`
	src = strings.NewReplacer("@K", "Kill", "@F", "FindProcess").Replace(src)
	got := map[string]int{}
	for _, site := range signalAndReapSites(parseSource(t, "fixture.go", src)) {
		got[site.fn]++
	}
	want := map[string]int{"alias": 1, "viaProcess": 1, "family": 3, "killAlias": 2, "direct": 3,
		packageLevel: 2, "methodValue": 1, "reaps": 3, "reapValue": 1}
	for fn, n := range want {
		if got[fn] != n {
			t.Errorf("%s: the scan found %d sites, want %d", fn, got[fn], n)
		}
	}
	if got["clean"] != 0 {
		t.Errorf("the scan flagged a clean function: %d sites", got["clean"])
	}
}

// scanProblems scans the non-test sources of dir and returns what breaks the rule: a signal or a reap outside
// the functions that may make them, and a use of killGroup, groupSink or groupPidfdSend outside the places that
// may name them.
func scanProblems(t *testing.T, dir string) []string {
	t.Helper()
	// file:function -> the calls it may make, and how often.
	allowed := map[string]map[string]int{
		"groupsig.go:killGroup":         {unixKill: 1},
		"groupsig.go:reap":              {"cmd.Wait": 1},
		"owner.go:startGuardedProcess":  {"cmd.Wait": 1},
		"groupprobe.go:probeGroupCalls": {"cmd.Wait": 1},
		"pidfd.go:startPidfdProcess":    {"cmd.Wait": 3, pidfdSignal: 1}, // the keeper's, untouched
		"pidfd.go:signal":               {pidfdSignal: 1},                // the keeper's, untouched
		"job.go:Close":                  {"j.wg.Wait": 1},
		"owner.go:Start":                {"wg.Wait": 1, "o.wg.Wait": 1},
		"owner.go:Stop":                 {"wg.Wait": 1, "o.wg.Wait": 1},
		"owner.go:startLocked":          {"wg.Wait": 1, "a.strays.Wait": 1}, // WaitGroups: the loops, then the strays (relay.holdStray)
		"serve.go:Serve":                {"wg.Wait": 1},
		// JobOptions.Wait, a duration the scan cannot tell from a method value: a count that changes shows here.
		"job.go:StartJob": {"o.Wait": 2},
		"job.go:open":     {"c.j.o.Wait": 2},
		// The seams' initialisers (var groupPidfdSend = unix.PidfdSendSignal, groupWaitid = func ... unix.Waitid):
		// the scan's explicit exceptions; where the seams are used is held by identUses below.
		"groupprobe.go:" + packageLevel: {pidfdSignal: 1, "unix.Waitid": 1},
	}
	// Where each name may appear: file:function -> its count. The one package-level use is the initialiser of
	// groupSink (var groupSink = killGroup), the scan's explicit exception.
	identUses := map[string]map[string]int{
		"groupSink":      {"owner.go:startProcess": 1},
		"killGroup":      {"owner.go:" + packageLevel: 1},
		"groupPidfdSend": {"groupprobe.go:probeGroupCalls": 2},
		// The waitid seam: the pin and the guard's look (WNOWAIT, WNOHANG), the probe's, and the barrier's blocking wait.
		"groupWaitid": {"groupsig.go:pin": 1, "groupsig.go:checked": 1, "groupprobe.go:probeChild": 1, "groupprobe.go:awaitExit": 1},
	}
	watched := map[string]bool{}
	for name := range identUses {
		watched[name] = true
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]map[string]int{}
	uses := map[string]map[string]int{}
	for _, path := range files {
		name := filepath.Base(path)
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f := parseSource(t, name, mustRead(t, path))
		for _, site := range signalAndReapSites(f) {
			key := name + ":" + site.fn
			if found[key] == nil {
				found[key] = map[string]int{}
			}
			found[key][site.what]++
		}
		for _, site := range watchedIdentSites(f, watched) {
			if uses[site.what] == nil {
				uses[site.what] = map[string]int{}
			}
			uses[site.what][name+":"+site.fn]++
		}
	}
	var problems []string
	for fn, calls := range found {
		for what, n := range calls {
			if allowed[fn][what] != n {
				problems = append(problems, fmt.Sprintf("%s makes %d calls of %s, and may make %d: signals and reaps belong in the guarded functions", fn, n, what, allowed[fn][what]))
			}
		}
	}
	for fn, calls := range allowed {
		for what, n := range calls {
			if found[fn][what] != n {
				problems = append(problems, fmt.Sprintf("%s makes %d calls of %s, want %d", fn, found[fn][what], what, n))
			}
		}
	}
	for ident, want := range identUses {
		for fn, n := range uses[ident] {
			if want[fn] != n {
				problems = append(problems, fmt.Sprintf("%s names %s %d times, want %d: it may be named only in %v", fn, ident, n, want[fn], want))
			}
		}
		for fn, n := range want {
			if uses[ident][fn] != n {
				problems = append(problems, fmt.Sprintf("%s names %s %d times, want %d", fn, ident, uses[ident][fn], n))
			}
		}
	}
	sort.Strings(problems)
	return problems
}

// The only group signal on alpha's default path is killGroup's, reached only
// from groupLeader.signal, behind the guards; the only reap of a pinned relay
// is the barrier's (groupLeader.reap), and the unpinned start's background
// wait. A new kill, a signal through an os.Process (under any alias, as a call
// or as a method value), a process lookup by PID, a pidfd signal, or a reap
// (Wait, Run, Output, CombinedOutput) anywhere else in the package's non-test
// sources, in a function or in a package-level declaration, fails this scan.
// It matches by name, not by the receiver's spelling, so the sync.WaitGroup
// waits are listed too. The keeper's sites (pidfd.go, untouched by issue #370)
// are pinned by it as well: a change to what the keeper's path signals or
// reaps shows here.
func TestNoUnguardedGroupSignalsInTheDefaultPath(t *testing.T) {
	for _, p := range scanProblems(t, ".") {
		t.Error(p)
	}
}

// The scan fails on the forms the review of PR 378 named, in a copy of the real sources: a package-level
// alias of a kill, a package-level alias of killGroup beside groupSink's initialiser, a method value of
// Process.Kill, and a reap through cmd.Run, cmd.Output or cmd.CombinedOutput. The control, the copy alone, is
// clean, so each failure comes from the planted file.
func TestTheScanFailsOnAPlantedSignalOrReapInACopyOfTheSources(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	copyTo := func(dir string) {
		for _, name := range files {
			if !strings.HasSuffix(name, "_test.go") {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(mustRead(t, name)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	control := t.TempDir()
	copyTo(control)
	if p := scanProblems(t, control); len(p) != 0 {
		t.Fatalf("the copy of the sources is not clean, so the cases below prove nothing: %v", p)
	}
	for _, c := range []struct{ name, src, want string }{
		{"a package-level alias of a kill", "var k = unix.@K\nfunc f() { k(-5, 9) }", "zz_planted.go:" + packageLevel + " makes 1 calls of unix.Kill"},
		{"a package-level alias of killGroup", "var k = killGroup", "zz_planted.go:" + packageLevel + " names killGroup 1 times"},
		{"a method value of Process.Kill", "func f(cmd *exec.Cmd) { g := cmd.Process.Kill; g() }", "zz_planted.go:f makes 1 calls of cmd.Process.Kill"},
		{"a reap through cmd.Run", "func f(cmd *exec.Cmd) { _ = cmd.Run() }", "zz_planted.go:f makes 1 calls of cmd.Run"},
		{"a reap through cmd.Output", "func f(cmd *exec.Cmd) { _, _ = cmd.Output() }", "zz_planted.go:f makes 1 calls of cmd.Output"},
		{"a reap through cmd.CombinedOutput", "func f(cmd *exec.Cmd) { _, _ = cmd.CombinedOutput() }", "zz_planted.go:f makes 1 calls of cmd.CombinedOutput"},
		{"a method value of cmd.Wait", "func f(cmd *exec.Cmd) { w := cmd.Wait; _ = w() }", "zz_planted.go:f makes 1 calls of cmd.Wait"},
		{"a direct kill in a function", "func f() { unix.@K(-5, 9) }", "zz_planted.go:f makes 1 calls of unix.Kill"},
		{"a new use of the waitid seam", "func f() { _ = groupWaitid(1, 0) }", "zz_planted.go:f names groupWaitid 1 times"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			copyTo(dir)
			src := "package hostdial\n\nimport (\n\t\"os/exec\"\n\t\"golang.org/x/sys/unix\"\n)\n\nvar _ = exec.Command\nvar _ = unix.Getpid\n\n" + strings.ReplaceAll(c.src, "@K", "Kill")
			if err := os.WriteFile(filepath.Join(dir, "zz_planted.go"), []byte(src), 0o600); err != nil {
				t.Fatal(err)
			}
			got := strings.Join(scanProblems(t, dir), "\n")
			if !strings.Contains(got, c.want) {
				t.Errorf("the scan did not report %q:\n%s", c.want, got)
			}
		})
	}
}

// pidfdSignal is how the scan spells the keeper's pidfd signal's call.
const pidfdSignal = "unix" + "." + "PidfdSendSignal"

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
