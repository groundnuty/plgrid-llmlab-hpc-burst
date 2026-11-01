package livechecks

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The registry of live tests. A live test is registered by a directive in the
// doc comment of its Test function:
//
//	//livecheck:conformance-helios,conformance-athena   the rows of hack/live-checks.sh that run it
//	//livecheck:none <reason>                           a test of a live file that is not a re-run target, and why
//
// The scanner finds the live files without reading the table: a file is live
// when it holds a Skip that says it needs a real or live system next to an
// environment read, when it names a gate variable of the table, when its name
// has a "live" word, or when it calls a gate helper, a function (declared in
// a test or a non-test file) whose own body skips that way or names a gate.
// Every Test function of a live file must carry a directive, so a new live
// test cannot be added without a decision. Each row's -run pattern must match
// exactly the functions that name the row, so a test in a listed package that
// the pattern leaves out fails the check, and so does a pattern that also
// runs a function that does not name its row.

var (
	liveWord  = regexp.MustCompile(`(?i)\b(real|live)\b`)
	liveFile  = regexp.MustCompile(`(^|_)live(_|\.)`)
	skipDirs  = map[string]bool{".git": true, "spikes": true, "third_party": true, "vendor": true, "testdata": true}
	rowName   = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	directive = "//livecheck:"
)

// testFn is a Test function of a _test.go file with what its directive says.
type testFn struct {
	name, file string
	rows       []string
	none       bool
	problem    string // a malformed directive
	directed   bool
}

// parsedFile is a Go file with the facts the scan needs.
type parsedFile struct {
	path, dir string
	test      bool
	ast       *ast.File
}

type scanned struct {
	files []parsedFile
	live  map[string][]string // test file -> why it is live
	tests map[string][]testFn // directory -> its Test functions, from every _test.go file
}

func parseTree(fsys fs.FS) ([]parsedFile, error) {
	var out []parsedFile
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] || p == "hack/livechecks" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		src, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, src, parser.ParseComments)
		if err != nil {
			return err
		}
		out = append(out, parsedFile{path: p, dir: path.Dir(p), test: strings.HasSuffix(p, "_test.go"), ast: f})
		return nil
	})
	return out, err
}

func strLits(n ast.Node, f func(string)) {
	ast.Inspect(n, func(m ast.Node) bool {
		if lit, ok := m.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if s, err := strconv.Unquote(lit.Value); err == nil {
				f(s)
			}
		}
		return true
	})
}

// skipsLive reports whether n holds a Skip or Skipf call whose message says a real or live system.
func skipsLive(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		call, ok := m.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Skip" || sel.Sel.Name == "Skipf") {
			strLits(call, func(s string) { found = found || liveWord.MatchString(s) })
		}
		return true
	})
	return found
}

func readsEnv(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if call, ok := m.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "os" && (sel.Sel.Name == "Getenv" || sel.Sel.Name == "LookupEnv") {
					found = true
				}
			}
		}
		return true
	})
	return found
}

func namesGate(n ast.Node, gates map[string]bool) bool {
	found := false
	strLits(n, func(s string) { found = found || gates[s] })
	return found
}

func parseDirective(fn *ast.FuncDecl) (rows []string, none bool, problem string, directed bool) {
	if fn.Doc == nil {
		return
	}
	for _, c := range fn.Doc.List {
		if !strings.HasPrefix(c.Text, directive) {
			continue
		}
		if directed {
			return nil, false, "two //livecheck: lines", true
		}
		directed = true
		body := strings.TrimSpace(strings.TrimPrefix(c.Text, directive))
		if body == "none" || strings.HasPrefix(body, "none ") {
			if strings.TrimSpace(strings.TrimPrefix(body, "none")) == "" {
				return nil, false, "none without a reason", true
			}
			none = true
			continue
		}
		for _, r := range strings.Split(body, ",") {
			if !rowName.MatchString(r) {
				return nil, false, "the directive is neither none <reason> nor a comma-separated list of row names", true
			}
			rows = append(rows, r)
		}
	}
	return
}

func isTestFunc(fn *ast.FuncDecl) bool {
	return fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Test") && len(fn.Name.Name) > 4 && fn.Name.Name[4] >= 'A' && fn.Name.Name[4] <= 'Z'
}

func scan(fsys fs.FS, gates map[string]bool) (*scanned, error) {
	files, err := parseTree(fsys)
	if err != nil {
		return nil, err
	}
	res := &scanned{files: files, live: map[string][]string{}, tests: map[string][]testFn{}}

	// Gate helpers: functions that are not tests and whose own body skips for a live system or names a gate.
	sameDir := map[string]map[string]bool{}
	anyDir := map[string]bool{}
	for _, pf := range files {
		for _, decl := range pf.ast.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || (pf.test && isTestFunc(fn)) {
				continue
			}
			if skipsLive(fn.Body) || namesGate(fn.Body, gates) {
				if sameDir[pf.dir] == nil {
					sameDir[pf.dir] = map[string]bool{}
				}
				sameDir[pf.dir][fn.Name.Name] = true
				if fn.Recv == nil {
					anyDir[fn.Name.Name] = true
				}
			}
		}
	}

	for _, pf := range files {
		if !pf.test {
			continue
		}
		var why []string
		switch {
		case skipsLive(pf.ast) && readsEnv(pf.ast):
			why = append(why, "a Skip that says it needs a real system, and an environment read")
		case namesGate(pf.ast, gates):
			why = append(why, "names a gate variable of the table")
		case liveFile.MatchString(path.Base(pf.path)):
			why = append(why, "a live file name")
		default:
			ast.Inspect(pf.ast, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(why) > 0 {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if sameDir[pf.dir][fun.Name] {
						why = append(why, "calls the gate helper "+fun.Name)
					}
				case *ast.SelectorExpr:
					if anyDir[fun.Sel.Name] {
						why = append(why, "calls the gate helper "+fun.Sel.Name)
					}
				}
				return true
			})
		}
		if len(why) > 0 {
			res.live[pf.path] = why
		}
		for _, decl := range pf.ast.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !isTestFunc(fn) {
				continue
			}
			rows, none, problem, directed := parseDirective(fn)
			res.tests[pf.dir] = append(res.tests[pf.dir], testFn{name: fn.Name.Name, file: pf.path, rows: rows, none: none, problem: problem, directed: directed})
		}
	}
	return res, nil
}

// liveDirs returns the directories that hold a live file.
func (s *scanned) liveDirs() []string {
	set := map[string]bool{}
	for f := range s.live {
		set[path.Dir(f)] = true
	}
	var out []string
	for d := range set {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// checkRegistry returns what is wrong with the registration of the live tests of fsys against the table.
func checkRegistry(fsys fs.FS, rows []row) ([]string, *scanned, error) {
	s, err := scan(fsys, gateSet(rows))
	if err != nil {
		return nil, nil, err
	}
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	byName := map[string]row{}
	listed := map[string]bool{}
	for _, r := range rows {
		byName[r.name] = r
		if r.status == "ready" {
			listed[r.pkg] = true
		}
	}

	for _, dir := range s.liveDirs() {
		if !listed[dir] {
			var why []string
			for f, w := range s.live {
				if path.Dir(f) == dir {
					why = append(why, f+": "+strings.Join(w, ", "))
				}
			}
			sort.Strings(why)
			add("package %s holds a live test (%s) that hack/live-checks.sh's table does not name; add a row", dir, strings.Join(why, "; "))
		}
	}

	named := map[string]map[string]bool{} // row -> functions that name it, as dir/name
	for dir, fns := range s.tests {
		for _, fn := range fns {
			_, isLive := s.live[fn.file]
			switch {
			case fn.problem != "":
				add("%s: %s: %s", fn.file, fn.name, fn.problem)
			case !fn.directed && isLive:
				add("%s: %s is in a live file and has no //livecheck: directive; name its rows, or say none and why", fn.file, fn.name)
			}
			for _, rn := range fn.rows {
				r, ok := byName[rn]
				switch {
				case !ok || r.status != "ready":
					add("%s: %s names row %s, which is not a ready row of the table", fn.file, fn.name, rn)
				case r.pkg != dir:
					add("%s: %s names row %s, whose package is %s", fn.file, fn.name, rn, r.pkg)
				case !regexp.MustCompile(r.test).MatchString(fn.name):
					add("%s: %s names row %s, but the row's -run pattern %s does not match it, so the script would never run it", fn.file, fn.name, rn, r.test)
				default:
					if named[rn] == nil {
						named[rn] = map[string]bool{}
					}
					named[rn][dir+"/"+fn.name] = true
				}
			}
		}
	}

	for _, r := range rows {
		if r.status != "ready" {
			continue
		}
		re := regexp.MustCompile(r.test)
		matched := 0
		for _, fn := range s.tests[r.pkg] {
			if !re.MatchString(fn.name) {
				continue
			}
			matched++
			if !named[r.name][r.pkg+"/"+fn.name] {
				add("row %s: its -run pattern %s also runs %s (%s), which does not name the row", r.name, r.test, fn.name, fn.file)
			}
		}
		if matched == 0 {
			add("row %s: no test function of %s matches its -run pattern %s", r.name, r.pkg, r.test)
		}
	}
	sort.Strings(problems)
	return problems, s, nil
}
