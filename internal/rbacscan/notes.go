package rbacscan

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// notes: "//rbac:resources group/resource ..." comments, by file and line.
func (s *scanner) readNotes() {
	s.notes = map[string]map[int][]string{}
	for _, p := range s.l.pkgs {
		if !inModule(p.PkgPath, s.l.mod) {
			continue
		}
		for _, f := range p.Syntax {
			name := s.l.prog.Fset.Position(f.Pos()).Filename
			for _, cg := range f.Comments {
				for _, c := range cg.List {
					rest, ok := strings.CutPrefix(c.Text, "//rbac:resources")
					if !ok {
						continue
					}
					if s.notes[name] == nil {
						s.notes[name] = map[int][]string{}
					}
					line := s.l.prog.Fset.Position(c.Pos()).Line
					s.notes[name][line] = strings.Fields(rest)
				}
			}
		}
	}
}

// noteFor returns the resources named by an annotation on the call's line or the lines above it back to the start of the
// statement it is in.
func (s *scanner) noteFor(call token.Pos) ([]string, bool) {
	pp := s.l.prog.Fset.Position(call)
	byLine := s.notes[pp.Filename]
	if byLine == nil {
		return nil, false
	}
	start := s.stmtStart(call)
	if start == 0 || start > pp.Line {
		start = pp.Line
	}
	for line := pp.Line; line >= start-1; line-- {
		if r, ok := byLine[line]; ok {
			if s.usedNotes[pp.Filename] == nil {
				s.usedNotes[pp.Filename] = map[int]bool{}
			}
			s.usedNotes[pp.Filename][line] = true
			return r, true
		}
	}
	return nil, false
}

// staleNotes reports each annotation that no call used: the call it was written for moved or went, or its type became
// static, and an annotation left behind would go on naming resources nothing needs.
func (s *scanner) staleNotes() {
	for file, lines := range s.notes {
		for line := range lines {
			if s.usedNotes[file][line] {
				continue
			}
			rel, err := filepath.Rel(s.l.root, file)
			if err != nil {
				rel = file
			}
			s.problem(fmt.Sprintf("%s:%d", filepath.ToSlash(rel), line), "an //rbac:resources annotation that no call needs: delete it, or move it to the call it is for")
		}
	}
}

// stmtStart is the first line of the innermost statement that holds pos.
func (s *scanner) stmtStart(pos token.Pos) int {
	pp := s.l.prog.Fset.Position(pos)
	for _, p := range s.l.pkgs {
		if !inModule(p.PkgPath, s.l.mod) {
			continue
		}
		for _, f := range p.Syntax {
			if s.l.prog.Fset.Position(f.Pos()).Filename != pp.Filename {
				continue
			}
			start := 0
			ast.Inspect(f, func(n ast.Node) bool {
				if n == nil || pos < n.Pos() || pos >= n.End() {
					return false
				}
				if _, ok := n.(ast.Stmt); ok {
					if _, isBlock := n.(*ast.BlockStmt); !isBlock {
						start = s.l.prog.Fset.Position(n.Pos()).Line
					}
				}
				return true
			})
			return start
		}
	}
	return 0
}

// objectResources names the resources of the object a call passes: its static type, the types converted into it where it
// is followed to (a watch list's kinds), or an annotation.
func (s *scanner) objectResources(obj ssa.Value, call token.Pos) ([]Triple, string) {
	if t := concreteType(obj); t != nil {
		if k, ok := s.kindOf(t); ok {
			return []Triple{{Group: k.group, Resource: k.resource}}, ""
		}
		return nil, fmt.Sprintf("the type %s has no resource in the scan's table of kinds", t)
	}
	if notes, ok := s.noteFor(call); ok {
		var out []Triple
		for _, n := range notes {
			t, err := parseResource(n)
			if err != nil {
				return nil, err.Error()
			}
			out = append(out, t)
		}
		return out, ""
	}
	if ts, ok := s.x.objectTypes(obj); ok {
		var out []Triple
		for _, t := range ts {
			k, ok := s.kindOf(t)
			if !ok {
				return nil, fmt.Sprintf("the type %s has no resource in the scan's table of kinds", t)
			}
			out = append(out, Triple{Group: k.group, Resource: k.resource})
		}
		return out, ""
	}
	return nil, fmt.Sprintf("the object is a %s, whose resource is not static: annotate the call with //rbac:resources group/resource", obj.Type())
}

// parseResource reads "group/resource" or "core/resource" or "group/resource/sub" from an annotation.
func parseResource(n string) (Triple, error) {
	parts := strings.SplitN(n, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Triple{}, fmt.Errorf("the annotation %q is not group/resource (core/resource for the core group)", n)
	}
	g := parts[0]
	if g == "core" {
		g = ""
	}
	return Triple{Group: g, Resource: parts[1]}, nil
}

// concreteType returns the named struct type of the object a call passes, or nil when it is not static: an interface that
// is not a conversion of a concrete value.
func concreteType(v ssa.Value) types.Type {
	for {
		t := v.Type()
		if !types.IsInterface(t) {
			if p, ok := t.Underlying().(*types.Pointer); ok {
				t = p.Elem()
			}
			if _, ok := t.(*types.Named); ok {
				return t
			}
			return nil
		}
		switch w := v.(type) {
		case *ssa.MakeInterface:
			v = w.X
		case *ssa.ChangeInterface:
			v = w.X
		default:
			return nil
		}
	}
}
