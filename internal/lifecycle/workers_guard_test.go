package lifecycle

// Every place in the lifecycle controller that names worker-0 outright, read from the package's own
// source. With several workers each such place is either on one worker's path, behind the branch on
// launch.workers (several), or a ready-phase site that #439's part 3b turns per worker, or names worker-0
// by design. A new one fails this test until it is reviewed and listed with its reason, so no site can
// skip the branch unseen; a listed one that is gone fails it too, so the list stays true.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

var workerZeroSites = map[string]string{
	"desired.go:desiredInstance":         "one worker's desired worker; several workers' are desiredWorkers, behind several",
	"verify.go:workerRunning":            "one worker's start (starting, behind several) and its ready-phase healthSignal",
	"verify.go:workerExited":             "as workerRunning",
	"verify.go:worker0":                  "the report's worker-0, for workerRunning, workerExited and healthSignal",
	"writes.go:verificationGeneration":   "one worker's verification generation; several workers' is workerVerificationGeneration",
	"routerhealth.go:verifiedGeneration": "one worker's verifiedGeneration; several workers' is in desiredWorkers",
	"routerhealth.go:routerCheckSignal":  "one worker's router check; several workers' is workerRouterCheck, from readyWorkers",
	"health.go:healthSignal":             "one worker's ready-phase signals; several workers' are workerSignal, behind several in ready",
	"health.go:restart":                  "the ladder from worker-0's rung; several workers name theirs through restartWorker",
	"spec.go:renderLaunch":               "by design: the launch names worker-0's socket, which the agent replaces for each worker",
}

func TestEveryWorkerZeroSiteIsReviewed(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, f := range files {
		name := f.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.SelectorExpr:
					if lit, ok := firstArg(call).(*ast.BasicLit); ok && f.Sel.Name == "WorkerName" && lit.Value == "0" {
						found[name+":"+fn.Name.Name] = true
					}
				case *ast.Ident:
					if f.Name == "worker0" {
						found[name+":"+fn.Name.Name] = true
					}
				}
				return true
			})
		}
	}
	var unlisted, gone []string
	for site := range found {
		if _, ok := workerZeroSites[site]; !ok {
			unlisted = append(unlisted, site)
		}
	}
	for site := range workerZeroSites {
		if !found[site] {
			gone = append(gone, site)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(gone)
	if len(unlisted) > 0 {
		t.Errorf("worker-0 is named outright in %v: review each against several workers and list it with its reason", unlisted)
	}
	if len(gone) > 0 {
		t.Errorf("listed worker-0 sites no longer name it: %v; drop them from the list", gone)
	}
}

func firstArg(call *ast.CallExpr) ast.Expr {
	if len(call.Args) == 0 {
		return nil
	}
	return call.Args[0]
}
