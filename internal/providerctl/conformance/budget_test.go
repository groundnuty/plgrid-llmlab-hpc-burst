package conformance

// Each row's timeout against its worst case, computed from the code (review of PR 284, B6). The figures used to be typed
// by hand beside the rows, and they drifted: the job name's round trip has two loops of 36 polls 5 seconds apart, 6
// minutes, where the sum said 3, so that the rows' timeouts of 85 and 45 minutes were below the 87 and 47 their tests
// can take. So the test reads the source of each row's test and adds up the bounded waits it holds. A new wait of a
// known kind changes the sum by itself; a length it cannot read, a literal or a constant it was not told of, fails it.
// What it cannot see is said at waitReader.of.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// budgetMargin is what a row's timeout keeps beyond its waits: the calls each poll makes over the transport and the run's own
// cleanup (the cancel of what is left, the staging directory), which no sum holds. Separately, what the code adds past the
// row's own bounds counts alpha.BudgetFactorPercent of itself, for what the scan cannot see.
const budgetMargin = 3 * time.Minute

// waitSum is what a test body can wait for: its start waits, whose length is the row's, and the rest.
type waitSum struct {
	startWaits int
	rest       time.Duration
}

// waitReader adds up the bounded waits of the package's functions, from their source.
type waitReader struct {
	t       *testing.T
	funcs   map[string]*ast.FuncDecl   // the package's functions
	methods map[string][]*ast.FuncDecl // the package's methods, by name
}

func newWaitReader(t *testing.T) *waitReader {
	t.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("the package's source: %v, %d files", err, len(files))
	}
	r := &waitReader{t: t, funcs: map[string]*ast.FuncDecl{}, methods: map[string][]*ast.FuncDecl{}}
	fset := token.NewFileSet()
	for _, name := range files {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			switch {
			case !ok:
			case fn.Recv == nil:
				r.funcs[fn.Name.Name] = fn
			default:
				r.methods[fn.Name.Name] = append(r.methods[fn.Name.Name], fn)
			}
		}
	}
	return r
}

// length reads a wait's length: n * time.Minute or time.Second, or a constant named here. Anything else is a wait the
// sum cannot see, which fails the test, so that it is taught the new form rather than leaving the wait out.
func (r *waitReader) length(e ast.Expr) time.Duration {
	r.t.Helper()
	known := map[string]time.Duration{"duplicateWait": duplicateWait, "accountingEndWait": accountingEndWait, "accountingDeleteWait": accountingDeleteWait, "reachEndWait": reachEndWait}
	switch x := e.(type) {
	case *ast.Ident:
		if d, ok := known[x.Name]; ok {
			return d
		}
	case *ast.BinaryExpr:
		lit, isLit := x.X.(*ast.BasicLit)
		unit, isSel := x.Y.(*ast.SelectorExpr)
		if isLit && isSel && x.Op == token.MUL {
			n, err := strconv.Atoi(lit.Value)
			if err == nil && unit.Sel.Name == "Minute" {
				return time.Duration(n) * time.Minute
			}
			if err == nil && unit.Sel.Name == "Second" {
				return time.Duration(n) * time.Second
			}
		}
	}
	r.t.Errorf("a wait's length this test cannot read (%T): teach it this form, so that the row's worst case holds it", e)
	return 0
}

// sleepIn is the length of the time.Sleep in a pause function.
func (r *waitReader) sleepIn(e ast.Expr) time.Duration {
	r.t.Helper()
	var d time.Duration
	ast.Inspect(e, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Sleep" {
				d = r.length(c.Args[0])
			}
		}
		return true
	})
	if d == 0 {
		r.t.Error("the round trip's pause holds no time.Sleep this test can read")
	}
	return d
}

// pollLoops counts the loops of the round trip, each of which polls up to polls times.
func (r *waitReader) pollLoops() int {
	r.t.Helper()
	fn := r.funcs["roundTripJobName"]
	if fn == nil {
		r.t.Fatal("no roundTripJobName in the package")
	}
	loops := 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if b, ok := n.(*ast.BinaryExpr); ok && b.Op == token.GEQ {
			if y, ok := b.Y.(*ast.Ident); ok && y.Name == "polls" {
				loops++
			}
		}
		return true
	})
	return loops
}

// of adds up fn's waits, and those of what it calls: a function of the package, or a method whose name one type alone has. A wait is a
// start wait (untilRuns), a call with a length (until, endedInAccounting, settleDuplicates, time.Sleep, time.After, time.NewTimer,
// context.WithTimeout), a deadline it makes itself (time.Now().Add), or the job name's round trip. A call made twice counts twice, and a
// time.Sleep inside a loop is that loop's pacing, under a deadline listed here.
//
// What it cannot see, and does not claim to (the delta review of PR 284, T2; this is no proof): a wait inside another package, a method
// whose name several types have, a function called through a variable, and a loop that sleeps a fixed number of times.
func (r *waitReader) of(fn *ast.FuncDecl, onPath map[*ast.FuncDecl]bool) waitSum {
	r.t.Helper()
	var sum waitSum
	if onPath[fn] {
		return sum
	}
	onPath[fn] = true
	defer delete(onPath, fn)
	add := func(s waitSum) { sum.startWaits, sum.rest = sum.startWaits+s.startWaits, sum.rest+s.rest }
	var walk func(n ast.Node, inLoop bool)
	walk = func(n ast.Node, inLoop bool) {
		ast.Inspect(n, func(x ast.Node) bool {
			switch v := x.(type) {
			case *ast.ForStmt:
				if v.Init != nil {
					walk(v.Init, inLoop)
				}
				walk(v.Body, true)
				return false
			case *ast.RangeStmt:
				walk(v.Body, true)
				return false
			case *ast.AssignStmt:
				if len(v.Rhs) == 1 && isNowAdd(v.Rhs[0]) {
					sum.rest += r.length(v.Rhs[0].(*ast.CallExpr).Args[0])
				}
			case *ast.CallExpr:
				add(r.ofCall(v, inLoop, onPath))
				// The round trip's pause is counted with its polls, not again on its own.
				if id, ok := v.Fun.(*ast.Ident); ok && id.Name == "roundTripJobName" {
					return false
				}
			}
			return true
		})
	}
	walk(fn.Body, false)
	return sum
}

// isNowAdd is time.Now().Add(...).
func isNowAdd(e ast.Expr) bool {
	c, ok := e.(*ast.CallExpr)
	if !ok || len(c.Args) != 1 {
		return false
	}
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Add" {
		return false
	}
	now, ok := sel.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	nowSel, ok := now.Fun.(*ast.SelectorExpr)
	if !ok || nowSel.Sel.Name != "Now" {
		return false
	}
	pkg, ok := nowSel.X.(*ast.Ident)
	return ok && pkg.Name == "time"
}

// ofCall is what one call waits for.
func (r *waitReader) ofCall(c *ast.CallExpr, inLoop bool, onPath map[*ast.FuncDecl]bool) waitSum {
	r.t.Helper()
	switch f := c.Fun.(type) {
	case *ast.Ident:
		if f.Name == "roundTripJobName" {
			polls, err := strconv.Atoi(c.Args[3].(*ast.BasicLit).Value)
			if err != nil {
				r.t.Fatal(err)
			}
			return waitSum{rest: time.Duration(r.pollLoops()*polls) * r.sleepIn(c.Args[4])}
		}
		if fn := r.funcs[f.Name]; fn != nil {
			return r.of(fn, onPath)
		}
	case *ast.SelectorExpr:
		id, plain := f.X.(*ast.Ident)
		switch {
		case plain && id.Name == "time" && f.Sel.Name == "Sleep":
			if inLoop {
				return waitSum{}
			}
			return waitSum{rest: r.length(c.Args[0])}
		case plain && id.Name == "time" && (f.Sel.Name == "After" || f.Sel.Name == "NewTimer"):
			return waitSum{rest: r.length(c.Args[0])}
		case plain && id.Name == "context" && f.Sel.Name == "WithTimeout":
			return waitSum{rest: r.length(c.Args[1])}
		case f.Sel.Name == "untilRuns":
			return waitSum{startWaits: 1}
		case f.Sel.Name == "until":
			return waitSum{rest: r.length(c.Args[2])}
		case f.Sel.Name == "endedInAccounting", f.Sel.Name == "settleDuplicates":
			return waitSum{rest: r.length(c.Args[1])}
		}
		if ms := r.methods[f.Sel.Name]; len(ms) == 1 {
			return r.of(ms[0], onPath)
		}
	}
	return waitSum{}
}

// Every conformance, accounting and reach row: its timeout is above the worst case derived from the row's own bounds in
// the table (its start wait) and the waits its test's source holds, with the margin. A row with no start wait has the
// wait in the queue on top of its timeout column (execute_cluster), so the column need not hold it. The stack rows and
// real-engine are hack/livechecks's (TestEveryRowsTimeoutIsDerivedFromItsBounds), which also fails a row of a kind no
// test derives.
func TestEveryRowsTimeoutIsAboveTheWorstCaseItsSourceAddsUpTo(t *testing.T) {
	reader := newWaitReader(t)
	seen := 0
	for _, row := range readShapeRows(t) {
		var body string
		var wantStartWaits int
		switch {
		case notAConformanceRun(row.name), row.name == "real-engine":
			continue
		case strings.HasPrefix(row.name, "conformance-"):
			body, wantStartWaits = "TestConformance", 2
		case strings.HasPrefix(row.name, "gpu-accounting-"):
			body, wantStartWaits = "gpuAccountingBody", 1
		case strings.HasPrefix(row.name, "reach-"):
			body, wantStartWaits = "reachBody", 1
		default:
			t.Errorf("%s has no case here: say which test's source its worst case is added up from", row.name)
			continue
		}
		seen++
		t.Run(row.name, func(t *testing.T) {
			reader.t = t
			fn := reader.funcs[body]
			if fn == nil {
				t.Fatalf("no %s in the package", body)
			}
			waits := reader.of(fn, map[*ast.FuncDecl]bool{})
			if waits.startWaits != wantStartWaits {
				t.Errorf("%s has %d waits for a job to start, want %d", body, waits.startWaits, wantStartWaits)
			}
			// The row's own start waits are exact, and count once; what the code adds counts with the margin.
			worst := alpha.Scaled(waits.rest)
			if row.startWait != "-" {
				n, err := strconv.Atoi(row.startWait)
				if err != nil {
					t.Fatal(err)
				}
				worst += time.Duration(waits.startWaits*n) * time.Minute
			}
			timeout, err := strconv.Atoi(row.timeout)
			if err != nil {
				t.Fatal(err)
			}
			if got := time.Duration(timeout) * time.Minute; got < worst+budgetMargin {
				t.Errorf("timeout %s is below the worst case %s from the source of %s, with its factor of %d%%, and the margin of %s", got, worst, body, alpha.BudgetFactorPercent, budgetMargin)
			}
		})
	}
	if seen == 0 {
		t.Error("no row was read: the check looked at nothing")
	}
}
