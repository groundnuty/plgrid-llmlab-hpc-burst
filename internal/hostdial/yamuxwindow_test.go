package hostdial

// The serving relay's yamux window (HBEP-6, The bulk path: "In host-dialed mode
// the separate frpc shares the serving relay. Its connections are streams of
// the relay's yamux session, whose 256 KiB per-stream window bounds the stall to
// a few ms. That window must stay at yamux's default."). The bulk path's whole
// layout rests on it, and nothing guarded it (issue #448, part 4). Two checks:
// the session configuration both ends build has yamux's default window, and,
// since that configuration guards nothing if a session is made without it, every
// yamux session in this package's code is made from it, and no code of the
// package sets the window.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/fatedier/yamux"
)

// hbep6WindowBytes is the window HBEP-6 names: "yamux's 256 KiB per-stream
// window". It is typed here from the HBEP, apart from yamux's own default, so a
// library upgrade that moved the default would fail this test and say so, since
// the HBEP's stall figure rests on 256 KiB.
const hbep6WindowBytes = 256 * 1024

// Both ends of a relay session are built from sessionConfig: the job's end
// (yamux.Client, job.go) and the control-plane end (yamux.Server, serve.go).
// Each has yamux's default per-stream window, which is HBEP-6's 256 KiB.
func TestServingRelayKeepsYamuxDefaultWindow(t *testing.T) {
	def := yamux.DefaultConfig().MaxStreamWindowSize
	if def != hbep6WindowBytes {
		t.Fatalf("yamux's default window is %d bytes, not the 256 KiB (%d) that HBEP-6's stall bound rests on: HBEP-6 and the bulk path's measurements need a second look", def, hbep6WindowBytes)
	}
	for _, side := range []string{"job", "host"} {
		c := sessionConfig(nil, side)
		if c.MaxStreamWindowSize != def {
			t.Errorf("the %s end's session has a per-stream window of %d bytes, yamux's default is %d (HBEP-6: that window must stay at yamux's default)", side, c.MaxStreamWindowSize, def)
		}
		if c.MaxStreamWindowSize != hbep6WindowBytes {
			t.Errorf("the %s end's session has a per-stream window of %d bytes, HBEP-6 names 256 KiB (%d)", side, c.MaxStreamWindowSize, hbep6WindowBytes)
		}
	}
}

// yamuxSiteProblems returns what is wrong with one source file's use of yamux
// sessions: a yamux.Client or yamux.Server call whose configuration is not
// sessionConfig's, a yamux.Config literal, and any mention of the window's field.
func yamuxSiteProblems(fset *token.FileSet, f *ast.File) []string {
	var out []string
	add := func(n ast.Node, format string) {
		out = append(out, fset.Position(n.Pos()).String()+": "+format)
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "yamux" || (sel.Sel.Name != "Client" && sel.Sel.Name != "Server") {
				return true
			}
			if len(x.Args) != 2 {
				add(x, "yamux."+sel.Sel.Name+" without two arguments")
				return true
			}
			cfg, ok := x.Args[1].(*ast.CallExpr)
			if !ok {
				add(x, "yamux."+sel.Sel.Name+" with a configuration that is not a call of sessionConfig")
				return true
			}
			if id, ok := cfg.Fun.(*ast.Ident); !ok || id.Name != "sessionConfig" {
				add(x, "yamux."+sel.Sel.Name+" with a configuration that is not a call of sessionConfig")
			}
		case *ast.CompositeLit:
			if sel, ok := x.Type.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "yamux" && sel.Sel.Name == "Config" {
					add(x, "a yamux.Config literal: configurations come from sessionConfig")
				}
			}
		case *ast.SelectorExpr:
			if x.Sel.Name == "MaxStreamWindowSize" {
				add(x, "MaxStreamWindowSize is read or set: the window stays at yamux's default")
			}
		}
		return true
	})
	return out
}

// The call-site guard: no non-test file of this package makes a yamux session
// from anything but sessionConfig, builds a yamux.Config of its own, or touches
// the window. (internal/hostdial holds the only yamux sessions in the framework's
// code: the relay keeper and the owner use hostdial's Serve, and the agent its
// Job.)
func TestEveryRelaySessionIsMadeFromSessionConfig(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var problems []string
	sites := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		problems = append(problems, yamuxSiteProblems(fset, f)...)
		ast.Inspect(f, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "yamux" && (sel.Sel.Name == "Client" || sel.Sel.Name == "Server") {
						sites++
					}
				}
			}
			return true
		})
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatalf("a relay session is not made from sessionConfig, or the window is touched:\n  %s", strings.Join(problems, "\n  "))
	}
	// The scan must have seen the two sessions (job.go's client and serve.go's
	// server), or it would pass over a package it does not read.
	if sites != 2 {
		t.Fatalf("the scan found %d yamux.Client/yamux.Server calls in the package's code, want the two (job.go and serve.go)", sites)
	}
}

// The scan's positive control: each way to make a session of another
// configuration is reported, and the code as it is passes.
func TestTheSessionConfigScanCatchesEachWayAround(t *testing.T) {
	check := func(src string) []string {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", src, 0)
		if err != nil {
			t.Fatal(err)
		}
		return yamuxSiteProblems(fset, f)
	}
	const head = "package hostdial\nimport \"github.com/fatedier/yamux\"\nfunc f(conn any) {\n"
	good := head + "\t_, _ = yamux.Client(conn, sessionConfig(nil, \"job\"))\n}\n"
	if got := check(good); len(got) != 0 {
		t.Fatalf("the scan reports the code as it is: %v", got)
	}
	for name, src := range map[string]string{
		"the default configuration":   head + "\t_, _ = yamux.Server(conn, yamux.DefaultConfig())\n}\n",
		"a variable":                  head + "\tc := sessionConfig(nil, \"x\")\n\t_, _ = yamux.Client(conn, c)\n}\n",
		"a literal":                   head + "\t_ = &yamux.Config{}\n}\n",
		"the window set":              head + "\tc := sessionConfig(nil, \"x\")\n\tc.MaxStreamWindowSize = 1 << 20\n}\n",
		"the window read":             head + "\tc := sessionConfig(nil, \"x\")\n\t_ = c.MaxStreamWindowSize\n}\n",
		"a call with one argument":    head + "\t_, _ = yamux.Client(conn)\n}\n",
		"another function's config":   head + "\t_, _ = yamux.Server(conn, otherConfig(nil))\n}\n",
		"sessionConfig behind a call": head + "\t_, _ = yamux.Client(conn, wrap(sessionConfig(nil, \"x\")))\n}\n",
	} {
		if got := check(src); len(got) == 0 {
			t.Errorf("the scan passes %s", name)
		}
	}
}
