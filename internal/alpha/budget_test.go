package alpha

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Each stack row's worst case, added up by hand from the review of PR 284 (B5), not by the code it checks: a no-overlap
// row at a wait of 2 h is 120 for the predecessor to start, 106 for its lease, 120 for the successor to start (346, the
// figure in the review), 60 and 5 for it to become ready, 1 for the guard's last looks and 5 to close, and before all of it
// 11 for the stack to start: 5 for hpcburst up, then 3 each for the controller and the Provider to be ready, and a minute for each of the
// watch's catch-ups on its path: 2 on the handover and on no-overlap, 1 on a tunnel row (stack.CatchUpWorstCase). The tunnel rows'
// first wait stops a minute before the first end, the lease less the 15-minute drain window.
func TestEachStackRowsWorstCaseIsItsWaitsAddedUp(t *testing.T) {
	long, err := WithTunnelLease(rowOf("tunnel-hour-athena"), 240*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		row  Row
		// pending and want are in minutes, and wantRest the waits past its bounds, which count 1.25 times (MinTimeout).
		pending, want, wantDeadline, wantRest int
	}{
		{"handover, 30", rowOf("alpha-athena-handover"), 30, 11 + 30 + 91 + 5 + 15 + 5 + 1 + 2 + 5, 2*30 + 150, 11 + 5 + 5 + 1 + 2 + 5},
		{"handover, 2 h", rowOf("alpha-athena-handover"), 120, 11 + 120 + 91 + 5 + 15 + 5 + 1 + 2 + 5, 2*120 + 150, 11 + 5 + 5 + 1 + 2 + 5},
		{"no-overlap, 30", rowOf("alpha-helios-nooverlap"), 30, 11 + 30 + 106 + 30 + 60 + 5 + 1 + 2 + 5, 2*30 + 213, 11 + 60 + 5 + 1 + 2 + 5},
		{"no-overlap, 2 h", rowOf("alpha-helios-nooverlap"), 120, 11 + 120 + 106 + 120 + 60 + 5 + 1 + 2 + 5, 2*120 + 213, 11 + 60 + 5 + 1 + 2 + 5},
		{"the hour on Helios, 30", rowOf("tunnel-hour-helios"), 30, 11 + 30 + 122 + 15 + 5 + 1 + 5, 30 + 166, 11 + 5 + 1 + 5},
		{"the hour on Athena, 2 h", rowOf("tunnel-hour-athena"), 120, 11 + 120 + 122 + 15 + 5 + 1 + 5, 120 + 166, 11 + 5 + 1 + 5},
		{"the hour on Athena, its wait capped a minute before the first end", rowOf("tunnel-hour-athena"), 180, 11 + 121 + 122 + 15 + 5 + 1 + 5, 180 + 166, 11 + 5 + 1 + 5},
		{"the hour at the longest lease", long, 120, 11 + 120 + 225 + 15 + 5 + 1 + 5, 120 + 269, 11 + 5 + 1 + 5},
		// Row A on plgrid-now (issue #376): its start wait of 10 is its first clock whatever wait is asked, then its 48-minute
		// lease less the 7-minute drain window to the requeue, the requeue's window (3 for the drain seen, 1 for scontrol, 5
		// for the result, 5 and 2 for the held stream's deadline and the client's margin past it, 3 for the events), the
		// drain window and 5 for the job to end once the deployment goes, three catch-ups and 5 to close. Its run's deadline
		// is its budget, 115, which holds the start wait.
		{"row A on plgrid-now, 30", rowOf("alpha-athena-now-serve"), 30, 11 + 10 + 41 + (3 + 1 + 5 + 5 + 2 + 3) + 7 + 5 + 3 + 5, 115, 11 + 19 + 5 + 3 + 5},
		{"row A on plgrid-now, 2 h asked", rowOf("alpha-athena-now-serve"), 120, 11 + 10 + 41 + (3 + 1 + 5 + 5 + 2 + 3) + 7 + 5 + 3 + 5, 115, 11 + 19 + 5 + 3 + 5},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := time.Duration(c.pending) * time.Minute
			if got, want := c.row.WorstCase(p), time.Duration(c.want)*time.Minute; got != want {
				t.Errorf("WorstCase = %s, want %s", got, want)
			}
			if got, want := c.row.RunDeadline(p), time.Duration(c.wantDeadline)*time.Minute; got != want {
				t.Errorf("RunDeadline = %s, want %s", got, want)
			}
			// The bounds count once, and the rest a quarter more.
			if got, want := c.row.MinTimeout(p), time.Duration(c.want)*time.Minute+time.Duration(c.wantRest)*15*time.Second; got != want {
				t.Errorf("MinTimeout = %s, want %s", got, want)
			}
			// The run's context ends the ledger's read after the scenario, so it must outlast every wait but Close's, which
			// takes a time of its own: otherwise a run in time fails on its last read.
			if got, min := c.row.RunDeadline(p), c.row.WorstCase(p)-CloseTimeout; got < min {
				t.Errorf("the run's deadline %s is below its waits, %s", got, min)
			}
		})
	}
}

// The stack's start is named from the stack (budget.go), and this says the figures above still hold when a change there flows through.
func TestTheStartupInTheWorstCaseIsTheStacksOwn(t *testing.T) {
	if StartupWait != 11*time.Minute {
		t.Errorf("StartupWait = %s; the figures in this file were worked out with 5 for hpcburst up and 3 each for the two waits, 11 minutes", StartupWait)
	}
}

// liveWaits are every bounded wait in the live steps, which no dry run reaches, in full, each with what covers it in Row.WorstCase: the
// delta review of PR 284, T1. A wait added, or one whose length changes, fails here even with all the others in place, so that it
// is added to the sum or said to be covered. Covered by, in turn: "startup"; a "term" of the sum by name; "the lease" (a step before the
// drain start, which ends the run in failure when it has passed, or ends with the job's hard end); "pacing" (a loop's own
// sleep, under a deadline listed here); "a call" (one read's own bound, inside the wait that makes it); "a goroutine" (the guard's, the
// sampler's and the prober's own, which no step waits on past what is listed).
var liveWaits = map[string]string{
	"Close: context.WithTimeout(CloseTimeout)":                                                    "term: CloseTimeout",
	"start: r.waitFor(stack.WaitDeadline)":                                                        "startup",
	"endAt: context.WithTimeout(stack.APICallTimeout)":                                            "a call",
	"firstJobStarted: time.Sleep(time.Second)":                                                    "pacing",
	"firstReady: r.waitFor(stack.WaitDeadline)":                                                   "the lease",
	"firstReady: r.waitFor(r.readyWait())":                                                        "the lease",
	"successorOf: time.Sleep(time.Second)":                                                        "pacing",
	"replacementLate: context.WithTimeout(stack.APICallTimeout)":                                  "a call",
	"guardResult: context.WithTimeout(stack.APICallTimeout)":                                      "a call",
	"guardResult: time.Sleep(time.Second)":                                                        "pacing",
	"lookNow: context.WithTimeout(time.Minute)":                                                   "a call",
	"startGuard: time.After(r.every())":                                                           "a goroutine",
	"startGuard: context.WithTimeout(time.Minute)":                                                "a goroutine",
	"waitFor: stack.WaitFor(d)":                                                                   "the waits that call it, listed here",
	"workerRestart: r.waitFor(r.tm.StartTimeout + 5*time.Minute)":                                 "the lease",
	"untilBefore: r.waitFor(time.Until(at) + time.Minute)":                                        "the lease",
	"Handover: r.successorOf(time.Until(ds) - lead - lead/2)":                                     "the lease",
	"Handover: r.untilBefore(lead)":                                                               "the lease",
	"Handover: r.successorOf(time.Until(ds))":                                                     "the lease",
	"Handover: r.waitFor(r.dw + EndMargin)":                                                       "term: the drain window and EndMargin",
	"NoOverlap: r.untilBefore(lead)":                                                              "the lease",
	"NoOverlap: r.successorOf(time.Until(leaseEnd) + SuccessorWait(r.tm, p.PendingMax))":          "term: SuccessorWait",
	"whyGone: r.syncWatch(stack.KindInstances | stack.KindDeployments)":                           "a path that fails the run: nothing follows it",
	"workerRestart: r.syncWatch(stack.KindInstances)":                                             "term: stack.CatchUpWorstCase",
	"Handover: r.syncWatch(stack.KindInstances | stack.KindAllocations)":                          "term: stack.CatchUpWorstCase",
	"NoOverlap: r.syncWatch(stack.KindInstances | stack.KindAllocations | stack.KindDeployments)": "term: stack.CatchUpWorstCase",
	"endDeployment: r.syncWatch(stack.KindAllocations)":                                           "term: stack.CatchUpWorstCase",
	"endDeployment: r.waitFor(r.dw + EndMargin)":                                                  "term: the drain window and EndMargin",
	"wait: time.After(r.fc.RequestTimeout() + time.Minute)":                                       "term: the streams' MaxRequestDuration; a stream past it fails the run",
	"startSampler: time.After(every)":                                                             "a goroutine",
	"startSampler: context.WithTimeout(10 * time.Second)":                                         "a call",
	"startProber: time.After(time.Until(from))":                                                   "a goroutine",
	"startProber: time.After(time.Second)":                                                        "a goroutine",
	"largeRequests: time.Sleep(tm.UploadPause)":                                                   "pacing",
	"TunnelHour: time.After(rateAsked + r.fc.RequestTimeout() + 2*time.Minute)":                   "the lease",
	"TunnelHour: r.waitFor(tm.MinIdleGap + time.Minute)":                                          "the lease",
	"successorOf: stack.BoundedDeadline(d)":                                                       "the waits that call it, listed here",
	"nowGauges: r.waitFor(stack.WaitDeadline)":                                                    "the lease",
	"requeueSnapshot: r.syncWatch(stack.KindAllocations | stack.KindInstances)":                   "the lease (the time-left check before it leaves it a minute)",
	"requeueOwnJob: r.waitFor(stack.WaitDeadline)":                                                "term: NowRequeueTail",
	"requeueOwnJob: r.syncWatch(stack.KindInstances | stack.KindAllocations)":                     "term: stack.CatchUpWorstCase",
	"requeueResult: stack.BoundedDeadline(RequeueResultWait)":                                     "term: NowRequeueTail",
	"requeueResult: time.Sleep(time.Second)":                                                      "pacing",
	"nowEvents: stack.BoundedDeadline(stack.WaitDeadline)":                                        "term: NowRequeueTail",
	"nowEvents: time.Sleep(time.Second)":                                                          "pacing",
}

// waitOf is how a call reads in liveWaits, or "" when it is not a wait.
func waitOf(fset *token.FileSet, fn string, call *ast.CallExpr) string {
	text := func(e ast.Expr) string {
		var b strings.Builder
		if err := printer.Fprint(&b, fset, e); err != nil {
			return "?"
		}
		return b.String()
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	recv := text(sel.X)
	arg := -1
	switch recv + "." + sel.Sel.Name {
	case "r.waitFor", "time.After", "time.Sleep":
		arg = 0
	case "r.successorOf", "r.untilBefore", "context.WithTimeout", "stack.WaitFor", "r.syncWatch", "stack.BoundedDeadline":
		arg = 1
	}
	if arg < 0 || arg >= len(call.Args) {
		return ""
	}
	return fmt.Sprintf("%s: %s.%s(%s)", fn, recv, sel.Sel.Name, text(call.Args[arg]))
}

// The live steps' whole list of waits against liveWaits, and the few calls that make up a term of the sum. It reads every function of
// the four files that hold them. What it cannot see: a wait inside a function of another package, past the ones the stack's start
// allows, and a wait written some other way than the calls waitOf knows.
func TestTheLiveStepsWaitOnWhatTheSumsCount(t *testing.T) {
	fset := token.NewFileSet()
	found := map[string]int{}
	for _, file := range []string{"steps_live_test.go", "live_flows_test.go", "guard_live_test.go", "now_flows_test.go"} {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if w := waitOf(fset, fn.Name.Name, call); w != "" {
						found[w]++
					}
				}
				return true
			})
		}
	}
	for w, n := range found {
		if _, ok := liveWaits[w]; !ok {
			t.Errorf("a wait in the live steps that the worst case does not cover (%d times): %q. Add it to Row.WorstCase (budget.go) and to liveWaits, or say in liveWaits what covers it", n, w)
		} else if n != 1 {
			t.Errorf("%q is made %d times, and the worst case counts it once", w, n)
		}
	}
	for w := range liveWaits {
		if found[w] == 0 {
			t.Errorf("liveWaits lists %q, which the live steps no longer make", w)
		}
	}
	for file, calls := range map[string][]string{
		"alpha_live_test.go": {"context.WithTimeout(context.Background(), cfg.Row.RunDeadline(cfg.PendingMax))"},
		"steps_live_test.go": {"return r.tm.StartTimeout + ReadyMargin", "r.watch.ResetsDecideNothing()"},
		"guard_live_test.go": {"wait = GuardResultLooks * r.every()", "return GuardEvery"},
		// A catch-up that cannot tell ends a run that judges what it read, whichever job started.
		"live_flows_test.go": {
			"r.syncWatch(ctx, stack.KindInstances|stack.KindAllocations, \"the handover's result\"); err != nil {\n\t\treturn HandoverObs{}, notKnownBeside(err, second)",
			"r.syncWatch(ctx, stack.KindInstances|stack.KindAllocations|stack.KindDeployments, \"the no-overlap row's result\"); err != nil {\n\t\treturn NoOverlapObs{}, notKnownBeside(err, second)",
		},
	} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, call := range calls {
			if strings.Count(string(src), call) != 1 {
				t.Errorf("%s no longer has, once, %q: the worst case (budget.go) adds up what it waits for", file, call)
			}
		}
	}
}

// stack.Recorder.ResetsDecideNothing changes #292's rule for one claim: the coarse rule stands, except where a run judged nothing from the watch
// (the delta review of PR 284). So it has one caller, firstJobStarted. A second, on a run that judges what it read, would turn a reset into a
// NOT RUN or a pass that hides what may be a failure. This reads every Go file of the repository but the forks and the stack's own tests.
func TestOnlyAFirstJobThatNeverStartedSaysResetsDecideNothing(t *testing.T) {
	root := filepath.Join("..", "..")
	var calls []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == ".git" || n == "third_party" || n == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if !strings.HasSuffix(rel, ".go") || (strings.HasPrefix(rel, filepath.Join("internal", "rehearsal", "stack")) && strings.HasSuffix(rel, "_test.go")) {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			where := "a declaration outside a function"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				where = fn.Name.Name
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "ResetsDecideNothing" {
					calls = append(calls, filepath.ToSlash(rel)+": "+where)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"internal/alpha/steps_live_test.go: firstJobStarted"}; !slices.Equal(calls, want) {
		t.Errorf("ResetsDecideNothing is said at %q, want only %q: a run that judges anything it read from the watch must not say it", calls, want)
	}
}
