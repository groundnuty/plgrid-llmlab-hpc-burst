package reasondrift

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

// Wrapper names a function whose Event or Eventf call forwards the arguments
// it was called with, so the call is no emit site of its own: the sites that
// call the wrapper are the emit sites. The scan accepts a wrapper only if every
// recorder call in it passes on what Forwards says, so a wrapper that changes a
// reason or a type is found.
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
// field Field of a value of the struct type named Of.
type Forward struct {
	// Arg counts the arguments of the recorder call, a method's receiver left out.
	Arg int
	// Param counts the wrapper's parameters, its receiver left out.
	Param int
	// Of is the name of a struct type, for an argument that is its field Field
	// of a value that is no parameter (a loop variable, say).
	Of    string
	Field string
	// Event marks the wrapper's parameter Param as one that carries a built
	// event, which every call of the wrapper must pass the value of a known
	// constructor for (event.Normal, provider.NewManagedEvent), so that its
	// reason is read where it is built.
	Event bool
}
