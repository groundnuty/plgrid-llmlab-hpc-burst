package reasondrift

import (
	"fmt"
	"strings"
)

// Entry allows a mismatch. A field that is empty matches any value, so an
// Entry names the least that identifies what it allows. Why says why the
// mismatch is allowed: an exception states a fact of the design and is by reason.
type Entry struct {
	Kind      string
	Action    string
	Reason    string
	Regarding string
	Package   string
	Func      string
	Why       string
}

// Matches reports whether the entry allows the mismatch.
func (e Entry) Matches(m Mismatch) bool {
	return (e.Kind == "" || e.Kind == m.Kind) &&
		(e.Action == "" || e.Action == m.Action) &&
		(e.Reason == "" || e.Reason == m.Reason) &&
		(e.Regarding == "" || e.Regarding == m.Regarding) &&
		(e.Package == "" || e.Package == m.Package) &&
		(e.Func == "" || e.Func == m.Func)
}

func (e Entry) String() string {
	return Mismatch{Kind: e.Kind, Action: e.Action, Reason: e.Reason, Regarding: e.Regarding, Package: e.Package, Func: e.Func}.String()
}

// Apply takes the allowances off the mismatches, list by list, in order. It
// returns the mismatches no entry allows and the stale entries: those that
// matched nothing that reached them. A stale entry means the allowance is no
// longer needed, so it must go: that is how the erratum shows what it fixes.
func Apply(ms []Mismatch, lists ...[]Entry) (left []Mismatch, stale []Entry) {
	left = ms
	for _, list := range lists {
		used := make([]bool, len(left))
		for _, e := range list {
			hit := false
			for i, m := range left {
				if e.Matches(m) {
					used[i], hit = true, true
				}
			}
			if !hit {
				stale = append(stale, e)
			}
		}
		var rest []Mismatch
		for i, m := range left {
			if !used[i] {
				rest = append(rest, m)
			}
		}
		left = rest
	}
	return left, stale
}

// Wrapper names a function whose Event, Eventf or EventfID call forwards the
// arguments it was called with, so the call is no emit site of its own: the
// sites that call the wrapper are the emit sites. The scan accepts a wrapper
// only if every recorder call in it passes on what Forwards says, so a wrapper
// that changes a reason or a type is found.
type Wrapper struct {
	// Func is the package directory, a dot, and the function, as Emit names
	// them: internal/lifecycle.KubeRecorder.Event.
	Func string
	Why  string
	// Forwards lists what each recorder call in the wrapper must pass on.
	Forwards []Forward
}

// Forward says that argument Arg of a recorder call in a wrapper is the
// wrapper's parameter Param (or its field Field), or, when Of is set, the
// field Field of a value of the struct type named Of, or, when Map is set, the
// value a package-level map holds under the key that parameter Param is.
type Forward struct {
	// Arg counts the arguments of the recorder call, a method's receiver left out.
	Arg int
	// Param counts the wrapper's parameters, its receiver left out.
	Param int
	// Of is the name of a struct type, for an argument that is its field Field
	// of a value that is no parameter (a loop variable, say).
	Of    string
	Field string
	// Map is the full name of a package-level map (the package directory, a dot,
	// the variable: internal/providerctl.allocationEventType), for an argument
	// that is the lookup of the wrapper's parameter Param in that map, with or
	// without the comma-ok form. Param is then the key's parameter and the
	// argument is not Param itself, which is how a type the wrapper takes from
	// the map (as the Provider controller's typed helper does) is told from a
	// type it takes as given. Map excludes Of, Field and Event.
	Map string
	// Event marks the wrapper's parameter Param as one that carries a built
	// event, which every call of the wrapper must pass the value of a known
	// constructor for (event.Normal, provider.NewManagedEvent), so that its
	// reason is read where it is built.
	Event bool
}

// SplitVar splits the full name of a package-level variable, the package
// directory, a dot and the variable (internal/providerctl.allocationEventType),
// at its last dot. ok is false for a name that is not of that form.
func SplitVar(full string) (dir, name string, ok bool) {
	i := strings.LastIndex(full, ".")
	if i <= 0 || i == len(full)-1 {
		return "", "", false
	}
	return full[:i], full[i+1:], true
}

// Validate refuses a wrapper whose list is not well formed, so that a mistake
// in it fails the scan rather than checking less than the list says: a Forward
// with a negative index, or one that names a Map together with Of, Field or
// Event (a lookup is none of those), or whose Map is not a package directory, a
// dot and a variable.
func (w Wrapper) Validate() error {
	for i, f := range w.Forwards {
		switch {
		case f.Arg < 0 || f.Param < 0:
			return fmt.Errorf("wrapper %s: forward %d has a negative argument or parameter", w.Func, i)
		case f.Map == "":
		case f.Of != "" || f.Field != "" || f.Event:
			return fmt.Errorf("wrapper %s: forward %d names a Map, which takes no Of, Field or Event", w.Func, i)
		default:
			if _, _, ok := SplitVar(f.Map); !ok {
				return fmt.Errorf("wrapper %s: forward %d: the Map %q is not a package directory, a dot and a variable", w.Func, i, f.Map)
			}
		}
	}
	return nil
}
