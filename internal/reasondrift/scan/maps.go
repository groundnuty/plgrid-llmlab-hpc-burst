package scan

import (
	"errors"
	"fmt"
	"go/ast"

	"golang.org/x/tools/go/packages"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// A package-level map from a reason to a type is read from the source, as the
// managed controller's badAction is: the literal's keys and values are the
// type-checker's constant values, so a named constant is as good as a string.
// A sink that takes its type from such a map (sink.actionMap) is read through
// it, and a wrapper may pass on a value of it (reasondrift.Forward.Map).

// stringMap reads the composite literal that the package-level map name of the
// package in directory dir (p, as loaded, nil when it was not) is initialized
// with, as a map from string constant to string constant. It fails, naming the
// package, the variable and the position, on an entry that is not a pair of
// string constants (a call, a variable, a literal map element with no key), on
// a variable that is no composite literal, and on an empty map.
func stringMap(p *packages.Package, dir, name string) (map[string]string, error) {
	lit, err := packageVar(p, dir, name)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, el := range lit.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			return nil, fmt.Errorf("an entry of %s's %s is not a key and a value: %s", dir, name, p.Fset.Position(el.Pos()))
		}
		k, kok := constString(p, kv.Key)
		v, vok := constString(p, kv.Value)
		if !kok || !vok {
			return nil, fmt.Errorf("an entry of %s's %s is not a pair of string constants: %s", dir, name, p.Fset.Position(kv.Pos()))
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s's %s is empty", dir, name)
	}
	return out, nil
}

// actionMap returns the map named full (the package directory, a dot, the
// variable) that a sink takes its types from, and whether it could be read. It
// is read on the first call of a sink that names it, once: a map nothing calls
// through is not read, so a tree that has no such call yet (the commit before the
// helper) scans, while one whose call has a map the scan cannot read, or no such
// package, or no such variable, fails: Scan returns the error (mapErr), so a call
// is never read against a map the scan did not see. The guard requires the
// package to be loaded (its requiredPackages), so a lost package fails there.
func (s *scanner) actionMap(full string) (map[string]string, bool) {
	if m, done := s.actionMaps[full]; done {
		return m, m != nil
	}
	var (
		m   map[string]string
		err error
	)
	if dir, name, ok := reasondrift.SplitVar(full); ok {
		m, err = stringMap(s.byDir[dir], dir, name)
	} else {
		err = fmt.Errorf("%q is not a package directory, a dot and a variable", full)
	}
	if err != nil {
		s.mapErr = errors.Join(s.mapErr, fmt.Errorf("the type map of a sink: %w", err))
		m = nil
	}
	s.actionMaps[full] = m
	return m, m != nil
}
