package rbacscan

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// LibraryEntry stands for the calls a library makes on the module's behalf, which the walk cannot see. It names the call
// that triggers it, so that the rule it supports goes when the call does.
type LibraryEntry struct {
	ID string
	// Trigger is the full name of the function or method whose call, or reference as a value, brings the library's calls:
	// "{mod}/internal/leaderlock.New", "(sigs.k8s.io/controller-runtime/pkg/cluster.Cluster).GetEventRecorder".
	Trigger string
	// For says which identity a trigger is for, by where it is: the package or function that holds it.
	For []Source
	// FromReceiver takes the identity from the manager the triggering method is called on, in place of For.
	FromReceiver bool
	// Rights are what the library's calls need.
	Rights []Triple
	// Cites are the library sources the rights were read from: the sink or the lock that makes the request, and the call
	// that starts it. There must be at least one.
	Cites []Cite
}

// Cite names the pinned library source an entry stands for: the module and version (which must be go.mod's), the file in
// it, and text that must be in the file, so that a citation that no longer says what it was cited for is found.
type Cite struct {
	Module, Version, File, Contains string
}

// collectLibrary finds the triggers of the library entries and checks their citations.
func (s *scanner) collectLibrary() {
	for _, e := range s.o.Library {
		trigger := strings.ReplaceAll(e.Trigger, "{mod}", s.l.mod)
		var found []string
		for _, fn := range s.l.fns {
			if fn.Synthetic != "" && fn.Synthetic != "package initializer" {
				continue // a wrapper the compiler made for a promoted method: the call it forwards is a site of its own
			}
			for _, b := range fn.Blocks {
				for _, in := range b.Instrs {
					for _, hit := range s.triggers(fn, in, trigger) {
						found = append(found, s.pos(in.Pos()))
						s.libraryHit(e, fn, in, hit)
					}
				}
			}
		}
		sort.Strings(found)
		s.res.Entries[e.ID] = found
		if len(found) == 0 {
			s.problem("", fmt.Sprintf("the library entry %q stands for the calls %s makes, but nothing in the module calls or refers to it: delete the entry, with its rights", e.ID, trigger))
		}
		if len(e.Cites) == 0 {
			s.problem("", fmt.Sprintf("library entry %q cites no source: it needs the module, the version, the file and the text in it that it stands for", e.ID))
		}
		for _, c := range e.Cites {
			s.checkCite(e, c)
		}
	}
}

// triggers returns what in an instruction triggers the entry: a call through an interface of the method, or a function
// referred to, as a call's callee or as a value. The result is the receiver of an interface call, or nil.
type triggerHit struct{ recv ssa.Value }

func (s *scanner) triggers(fn *ssa.Function, in ssa.Instruction, trigger string) []triggerHit {
	var out []triggerHit
	if ci, ok := in.(ssa.CallInstruction); ok {
		cc := ci.Common()
		if cc.IsInvoke() && cc.Method.FullName() == trigger {
			out = append(out, triggerHit{recv: cc.Value})
		}
	}
	for _, op := range in.Operands(nil) {
		if f, ok := (*op).(*ssa.Function); ok && funcID(f) == trigger {
			out = append(out, triggerHit{})
		}
	}
	return out
}

func (s *scanner) libraryHit(e LibraryEntry, fn *ssa.Function, in ssa.Instruction, hit triggerHit) {
	pos := s.pos(in.Pos())
	if pos == "(no position)" {
		pos = s.pos(fn.Pos())
	}
	var idents []string
	if e.FromReceiver {
		res := s.x.resolve(hit.recv)
		for _, w := range res.whys {
			s.problem(pos, fmt.Sprintf("library entry %q: cannot follow the receiver: %s", e.ID, w))
		}
		for _, o := range res.sorted() {
			if o.Kind == oManager {
				idents = append(idents, o.Ident)
			}
		}
		if len(idents) == 0 && len(res.whys) == 0 {
			if len(res.unreached) > 0 {
				return // a trigger in code nothing calls
			}
			s.problem(pos, fmt.Sprintf("library entry %q: no manager behind the receiver", e.ID))
			return
		}
	} else {
		name, dir := s.funcName(fn), s.pkgDir(fn)
		var hitSrc *Source
		for i := range e.For {
			if e.For[i].Where == name {
				hitSrc = &e.For[i]
				break
			}
			if e.For[i].Where == dir {
				hitSrc = &e.For[i]
			}
		}
		if hitSrc == nil {
			s.problem(pos, fmt.Sprintf("library entry %q is triggered in %s, which the entry does not say an identity for", e.ID, name))
			return
		}
		idents = []string{hitSrc.Ident}
	}
	for _, id := range idents {
		if id == Exempt {
			continue
		}
		s.res.Sites = append(s.res.Sites, Site{Pos: pos, Func: s.funcName(fn), What: "library: " + e.ID, Ident: id, Need: dedupe(e.Rights), Via: e.ID})
	}
}

// checkCite checks that an entry's citation is real: the module is the one go.mod pins, at the version cited, the file is in
// it, and the file holds the text the entry says it stands for.
func (s *scanner) checkCite(e LibraryEntry, c Cite) {
	s.res.Cites = append(s.res.Cites, c)
	if c.Module == "" || c.Version == "" || c.File == "" || c.Contains == "" {
		s.problem("", fmt.Sprintf("library entry %q cites no source: it needs the module, the version, the file and the text in it that it stands for", e.ID))
		return
	}
	for _, p := range s.l.pkgs {
		if p.Module == nil || p.Module.Path != c.Module {
			continue
		}
		if p.Module.Version != c.Version {
			s.problem("", fmt.Sprintf("library entry %q cites %s %s, but go.mod pins %s: read the new source and update the entry", e.ID, c.Module, c.Version, p.Module.Version))
			return
		}
		b, err := os.ReadFile(filepath.Join(p.Module.Dir, c.File))
		if err != nil {
			s.problem("", fmt.Sprintf("library entry %q cites %s %s: %v", e.ID, c.Module, c.File, err))
			return
		}
		if !strings.Contains(string(b), c.Contains) {
			s.problem("", fmt.Sprintf("library entry %q cites %s %s for %q, which the file does not contain", e.ID, c.Module, c.File, c.Contains))
		}
		return
	}
	s.problem("", fmt.Sprintf("library entry %q cites the module %s, which the scanned packages do not import", e.ID, c.Module))
}
