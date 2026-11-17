package reasondrift

import (
	"strings"
	"testing"
)

// TestSplitVarSplitsAtTheLastDot: the package directory, a dot, the variable.
func TestSplitVarSplitsAtTheLastDot(t *testing.T) {
	for _, c := range []struct {
		full, dir, name string
		ok              bool
	}{
		{"internal/providerctl.allocationEventType", "internal/providerctl", "allocationEventType", true},
		{"pkg/some.dir/sub.table", "pkg/some.dir/sub", "table", true},
		{"nodot", "", "", false},
		{".name", "", "", false},
		{"internal/x.", "", "", false},
		{"", "", "", false},
	} {
		dir, name, ok := SplitVar(c.full)
		if dir != c.dir || name != c.name || ok != c.ok {
			t.Errorf("SplitVar(%q) = %q, %q, %v, want %q, %q, %v", c.full, dir, name, ok, c.dir, c.name, c.ok)
		}
	}
}

// TestAWrapperListIsValidated: a Forward that names a Map takes no Of, Field or
// Event, and a Map is a package directory, a dot and a variable; an index is
// never negative. The lists of Wrappers, and the shape the Provider controller's
// typed helper takes, are valid.
func TestAWrapperListIsValidated(t *testing.T) {
	const typedMap = "internal/providerctl.allocationEventType"
	for _, w := range Wrappers {
		if err := w.Validate(); err != nil {
			t.Errorf("a listed wrapper is not valid: %v", err)
		}
	}
	typed := Wrapper{Func: "internal/providerctl.external.typed", Forwards: []Forward{
		{Arg: 1, Param: 0}, {Arg: 4, Param: 1}, {Arg: 5, Map: typedMap, Param: 1},
	}}
	if err := typed.Validate(); err != nil {
		t.Errorf("the typed helper's wrapper is not valid: %v", err)
	}
	for _, c := range []struct {
		name string
		f    Forward
		want string
	}{
		{"a Map with a Field", Forward{Arg: 5, Param: 1, Map: typedMap, Field: "Action"}, "takes no Of, Field or Event"},
		{"a Map with an Of", Forward{Arg: 5, Param: 1, Map: typedMap, Of: "ManagedEvent"}, "takes no Of, Field or Event"},
		{"a Map with an Event", Forward{Arg: 5, Param: 1, Map: typedMap, Event: true}, "takes no Of, Field or Event"},
		{"a Map with no dot", Forward{Arg: 5, Param: 1, Map: "allocationEventType"}, "not a package directory, a dot and a variable"},
		{"a Map that ends in a dot", Forward{Arg: 5, Param: 1, Map: "internal/providerctl."}, "not a package directory, a dot and a variable"},
		{"a negative argument", Forward{Arg: -1, Param: 0}, "negative"},
		{"a negative parameter", Forward{Arg: 0, Param: -1}, "negative"},
	} {
		w := Wrapper{Func: "internal/x.f", Forwards: []Forward{{Arg: 0, Param: 0}, c.f}}
		err := w.Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %v, want one that says %q", c.name, err, c.want)
		}
	}
}
