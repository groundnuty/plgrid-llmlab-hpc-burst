package conformance

// Each row's timeout against its worst case, computed from the code (review of PR 284, B6). The figures used to be typed
// by hand beside the rows, and they drifted: the job name's round trip has two loops of 36 polls 5 seconds apart, 6
// minutes, where the sum said 3, so that the rows' timeouts of 85 and 45 minutes were below the 87 and 47 their tests
// can take. So the test reads the source of each row's test and adds up every bounded wait it holds. A new wait of a
// known kind changes the sum by itself; a length it cannot read, a literal or a constant it was not told of, fails it.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// budgetMargin is what a row's timeout keeps beyond the sum of its waits: the calls each poll makes over the transport
// and the run's own cleanup (the cancel of what is left, the staging directory), which no sum holds.
const budgetMargin = 3 * time.Minute

// waitSum is what a test body can wait for: its start waits, whose length is the row's, and the rest.
type waitSum struct {
	startWaits int
	rest       time.Duration
}

// waitReader adds up the bounded waits of the package's functions, from their source.
type waitReader struct {
	t       *testing.T
	funcs   map[string]*ast.FuncDecl // the package's functions
	methods map[string]*ast.FuncDecl // the world's methods
}

func newWaitReader(t *testing.T) *waitReader {
	t.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("the package's source: %v, %d files", err, len(files))
	}
	r := &waitReader{t: t, funcs: map[string]*ast.FuncDecl{}, methods: map[string]*ast.FuncDecl{}}
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
				if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
					if id, ok := star.X.(*ast.Ident); ok && id.Name == "world" {
						r.methods[fn.Name.Name] = fn
					}
				}
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

// of adds up fn's waits, and those of the world's methods it calls. A wait is a start wait (untilRuns), a call with a length
// (until, endedInAccounting, settleDuplicates), a deadline it makes itself, or the job name's round trip.
func (r *waitReader) of(fn *ast.FuncDecl, seen map[string]bool) waitSum {
	r.t.Helper()
	var sum waitSum
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if id, ok := x.Lhs[0].(*ast.Ident); ok && id.Name == "deadline" {
				if c, ok := x.Rhs[0].(*ast.CallExpr); ok && len(c.Args) == 1 {
					sum.rest += r.length(c.Args[0])
				}
			}
		case *ast.CallExpr:
			switch f := x.Fun.(type) {
			case *ast.Ident:
				if f.Name == "roundTripJobName" {
					polls, err := strconv.Atoi(x.Args[3].(*ast.BasicLit).Value)
					if err != nil {
						r.t.Fatal(err)
					}
					sum.rest += time.Duration(r.pollLoops()*polls) * r.sleepIn(x.Args[4])
				}
			case *ast.SelectorExpr:
				if id, ok := f.X.(*ast.Ident); !ok || id.Name != "w" {
					return true
				}
				switch f.Sel.Name {
				case "untilRuns":
					sum.startWaits++
				case "until":
					sum.rest += r.length(x.Args[2])
				case "endedInAccounting", "settleDuplicates":
					sum.rest += r.length(x.Args[1])
				default:
					if m := r.methods[f.Sel.Name]; m != nil && !seen[f.Sel.Name] {
						seen[f.Sel.Name] = true
						s := r.of(m, seen)
						sum.startWaits, sum.rest = sum.startWaits+s.startWaits, sum.rest+s.rest
					}
				}
			}
		}
		return true
	})
	return sum
}

// statedWorstCase is the figure the script's comment states for a row, in minutes: "worst case of <row>: <n> min".
var statedWorstCase = regexp.MustCompile(`(?m)^# worst case of ([a-z0-9-]+): (?:the wait \+ )?(\d+) min`)

// Every conformance, accounting and reach row: the timeout is above the worst case its test's source adds up to, with the
// margin, and the worst case the script's comment states is that sum, so a figure in a comment cannot drift either. A
// row with no start wait has the wait in the queue on top of its timeout column (execute_cluster), so the column need
// not hold it. The stack rows and real-engine are hack/livechecks's (TestEveryStackRowsTimeoutIsAboveItsWorstCase and
// TestEveryRowsTimeoutIsAboveItsStatedWorstCase), which also fails a row with no stated figure.
func TestEveryRowsTimeoutIsAboveTheWorstCaseItsSourceAddsUpTo(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "hack", "live-checks.sh"))
	if err != nil {
		t.Fatal(err)
	}
	stated := map[string]int{}
	for _, m := range statedWorstCase.FindAllStringSubmatch(string(raw), -1) {
		stated[m[1]], _ = strconv.Atoi(m[2])
	}
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
			waits := reader.of(fn, map[string]bool{})
			if waits.startWaits != wantStartWaits {
				t.Errorf("%s has %d waits for a job to start, want %d", body, waits.startWaits, wantStartWaits)
			}
			worst := waits.rest
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
				t.Errorf("timeout %s is below the worst case %s from the source of %s and the margin of %s", got, worst, body, budgetMargin)
			}
			if got, ok := stated[row.name]; !ok || time.Duration(got)*time.Minute != worst {
				t.Errorf("the script's comment states a worst case of %d min (stated: %t), and the source of %s adds up to %s", got, ok, body, worst)
			}
		})
	}
	if seen == 0 {
		t.Error("no row was read: the check looked at nothing")
	}
}
