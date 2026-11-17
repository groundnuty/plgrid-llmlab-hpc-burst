package agentcli

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// The credential is loadCaller's, from --kubeconfig or the variable Env carries, and nothing in the package
// may reach the process's environment, the home directory, the launcher's state directory or client-go's
// default loading rules by another way. This test reads the package's SOURCE (like a guard test, it
// reads the files on disk when it runs): a runtime test cannot see a path that was never exercised.

var forbiddenCalls = map[string][]string{
	"os": {"Getenv", "LookupEnv", "Environ", "UserHomeDir", "UserConfigDir", "UserCacheDir", "Stat", "Lstat", "Open", "OpenFile",
		"ReadFile", "ReadDir", "Getwd", "Expand", "ExpandEnv"},
	"clientcmd": {"RecommendedHomeFile", "RecommendedConfigPathEnvVar", "NewDefaultClientConfigLoadingRules", "NewNonInteractiveDeferredLoadingClientConfig",
		"NewInteractiveDeferredLoadingClientConfig", "BuildConfigFromFlags", "BuildConfigFromKubeconfigGetter", "NewClientConfigFromBytes"},
	"rest":    {"InClusterConfig"},
	"homedir": {"HomeDir"},
}

var forbiddenImports = []string{
	"sigs.k8s.io/controller-runtime/pkg/client/config", // registers a global --kubeconfig flag and loads by default rules
	"k8s.io/client-go/util/homedir",
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle", // lifecycle's test imports this package
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher",
}

// forbiddenUses lists what the source reaches that the rule above forbids, by file and line.
func forbiddenUses(filename string, src any) ([]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		for _, bad := range forbiddenImports {
			if path == bad {
				out = append(out, fset.Position(imp.Pos()).String()+": imports "+path)
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		x, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		for _, name := range forbiddenCalls[x.Name] {
			if sel.Sel.Name == name {
				out = append(out, fset.Position(sel.Pos()).String()+": "+x.Name+"."+name)
			}
		}
		return true
	})
	return out, nil
}

func TestNothingReadsTheEnvironmentOrADefaultKubeconfig(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		checked++
		uses, err := forbiddenUses(name, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range uses {
			t.Errorf("%s", u)
		}
	}
	if checked < 10 {
		t.Fatalf("checked %d source files: the glob does not see the package", checked)
	}
}

// A real occurrence of each kind still fails, so the test above can fail.
func TestTheSourceGuardSeesWhatItForbids(t *testing.T) {
	src := `package x

import (
	"os"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func f() {
	_ = os.Getenv("HOME")
	_, _ = os.UserHomeDir()
	_ = clientcmd.NewDefaultClientConfigLoadingRules()
	_, _ = rest.InClusterConfig()
	_ = clientcmd.RecommendedHomeFile
}
`
	got, err := forbiddenUses("x.go", src)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"imports sigs.k8s.io/controller-runtime/pkg/client/config", "os.Getenv", "os.UserHomeDir",
		"clientcmd.NewDefaultClientConfigLoadingRules", "rest.InClusterConfig", "clientcmd.RecommendedHomeFile"} {
		found := false
		for _, g := range got {
			found = found || strings.Contains(g, want)
		}
		if !found {
			t.Errorf("the guard did not see %q in %v", want, got)
		}
	}
	// A constant of os that reads nothing is allowed.
	if ok, _ := forbiddenUses("y.go", "package y\nimport \"os\"\nvar _ = os.PathListSeparator\n"); len(ok) != 0 {
		t.Errorf("the guard refused os.PathListSeparator: %v", ok)
	}
}
