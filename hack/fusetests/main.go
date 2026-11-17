// Command fusetests lists the tests that mount a capfs FUSE volume, read from the test sources (#657). launcher-k3s runs the launcher's and
// the command's tests, and on a runner without FUSE (its probe says so) it leaves exactly these out with go test -skip, and says so in its
// summary and an annotation. The list used to be kept by hand, and went stale twice: the volume's tests that came later (the frozen
// volume's, the hint's, the sweep's and the mount's) were not on it, and would have FAILED on such a runner instead of being left out.
// Derived from the sources, it cannot lack a test.
//
//	go run ./hack/fusetests DIR...
//
// Each DIR is searched, recursively, for _test.go files (testdata and hidden directories are skipped). A test needs FUSE when it reaches a
// mount: by naming newCapVolume, mountCapVolume, launchCapServer or fusermountCapfs, or a function that does, through any chain of
// functions and methods of its directory's test files. Names are matched, not types, so a method named like a function that reaches a
// mount counts too: the list may hold a test it need not, and then that test is left out on a runner without FUSE, which costs nothing.
// What the matching cannot see is a function reached only through a variable of function type, or through a string (the victim test
// that testenv.StartChild runs by name is its own test, which reaches the mount itself).
//
// It prints one test name per line, sorted, and exits 0; it exits 1 when it cannot be trusted (a file that does not parse, a mount function
// that no scanned directory defines, which a rename would cause and which would make it find nothing, or no test at all that reaches a
// mount), and 2 for a usage error.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

// mountSinks are the functions through which a test mounts a volume (cmd/hpcburst's capfs files): newCapVolume is what the store-full tests
// and the capfs tests call, launchCapServer starts the child process that serves a volume, mountCapVolume mounts in the calling process (the
// child), and fusermountCapfs is the mount's lowest step, which one test uses itself.
var mountSinks = []string{"newCapVolume", "mountCapVolume", "launchCapServer", "fusermountCapfs"}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./hack/fusetests DIR...")
		os.Exit(2)
	}
	names, err := fuseTests(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "fusetests:", err)
		os.Exit(1)
	}
	for _, n := range names {
		fmt.Println(n)
	}
}

// fuseTests is the sorted names of the tests, in the test files under the roots, that reach a mount.
func fuseTests(roots []string) ([]string, error) {
	dirs, err := testDirs(roots)
	if err != nil {
		return nil, err
	}
	var names []string
	defined := map[string]bool{}
	for _, dir := range dirs {
		tests, defs, err := analyseDir(dir)
		if err != nil {
			return nil, err
		}
		names = append(names, tests...)
		for d := range defs {
			defined[d] = true
		}
	}
	for _, s := range mountSinks {
		if !defined[s] {
			return nil, fmt.Errorf("%s is not defined in any test file under %s: it was renamed or moved, and with it no test would be found (update mountSinks)", s, strings.Join(roots, ", "))
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no test under %s reaches a mount (%s), which cannot be right", strings.Join(roots, ", "), strings.Join(mountSinks, ", "))
	}
	sort.Strings(names)
	return names, nil
}

// testDirs are the directories under the roots that hold a _test.go file, sorted.
func testDirs(roots []string) ([]string, error) {
	seen := map[string]bool{}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if base := d.Name(); path != root && (base == "testdata" || strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_")) {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, "_test.go") {
				seen[filepath.Dir(path)] = true
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("searching %s: %w", root, err)
		}
	}
	var dirs []string
	for d := range seen {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	return dirs, nil
}

// analyseDir reads the test files of one directory. It returns the names of its tests that reach a mount, and the names of its functions
// and methods (so that the caller can check that each mount function exists). A test reaches a mount when its body names a mount function, or
// a function or method of the directory that does, which is a fixed point over the names each function's body mentions.
func analyseDir(dir string) (tests []string, defs map[string]bool, err error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	fset := token.NewFileSet()
	mentions := map[string]map[string]bool{} // function or method name -> every identifier its bodies mention
	defs = map[string]bool{}
	var testNames []string
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			name := fd.Name.Name
			defs[name] = true
			if isTest(fd) {
				testNames = append(testNames, name)
			}
			if fd.Body == nil {
				continue
			}
			if mentions[name] == nil {
				mentions[name] = map[string]bool{}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					mentions[name][id.Name] = true
				}
				return true
			})
		}
	}
	reaches := map[string]bool{}
	for _, s := range mountSinks {
		if defs[s] {
			reaches[s] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for fn, ids := range mentions {
			if reaches[fn] {
				continue
			}
			for id := range ids {
				if reaches[id] {
					reaches[fn] = true
					changed = true
					break
				}
			}
		}
	}
	for _, t := range testNames {
		if reaches[t] {
			tests = append(tests, t)
		}
	}
	return tests, defs, nil
}

// isTest reports whether fd is a test as go test finds it: a function named Test and then anything but a lower-case letter, with the one
// parameter *testing.T. (TestMain takes *testing.M, and is not one.)
func isTest(fd *ast.FuncDecl) bool {
	name := fd.Name.Name
	if fd.Recv != nil || !strings.HasPrefix(name, "Test") {
		return false
	}
	if rest := strings.TrimPrefix(name, "Test"); rest != "" && unicode.IsLower([]rune(rest)[0]) {
		return false
	}
	params := fd.Type.Params.List
	if len(params) != 1 || len(params[0].Names) > 1 {
		return false
	}
	star, ok := params[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "testing" && sel.Sel.Name == "T"
}
