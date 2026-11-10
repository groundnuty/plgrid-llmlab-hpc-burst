package rawjson

import (
	"encoding/json"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// Verdict is what happened to one value between the body sent and the body received.
type Verdict string

const (
	// Kept: the same bytes.
	Kept Verdict = "kept"
	// Reencoded: different bytes for the same value of the same JSON type: a string with other
	// escapes (an escaped e-acute and the raw character), a number with another spelling of the same form (1e2 and 100).
	// Only object member order and whitespace are ignored before this verdict is reached.
	Reencoded Verdict = "kept, re-encoded"
	// Retyped: another JSON type (a string where an array was), or a number whose form moved
	// between integer and fraction or exponent (1 and 1.0), which a Python reader sees as int and
	// float.
	Retyped Verdict = "retyped"
	// Changed: the same JSON type, another value.
	Changed Verdict = "changed"
	// Dropped: in the sent body, absent from the received one.
	Dropped Verdict = "dropped"
	// Injected: absent from the sent body, present in the received one.
	Injected Verdict = "injected"
	// Renamed: dropped under one key and injected with an equal value under a sibling key.
	Renamed Verdict = "renamed"
)

// rank orders verdicts from worst to best, for the verdict of a field that has several diffs.
var rank = map[Verdict]int{Dropped: 0, Retyped: 1, Changed: 2, Renamed: 3, Injected: 4, Reencoded: 5, Kept: 6}

// Worse reports whether a is a worse outcome than b.
func Worse(a, b Verdict) bool { return rank[a] < rank[b] }

// Diff is one value that did not arrive as its exact bytes. Sent and Received are raw JSON
// (empty when the side lacks the value); To is the new path of a Renamed value.
type Diff struct {
	Path     string  `json:"path"`
	Verdict  Verdict `json:"verdict"`
	Sent     string  `json:"sent,omitempty"`
	Received string  `json:"received,omitempty"`
	To       string  `json:"to,omitempty"`
}

// Compare walks the sent tree against the received one and returns every value that is not
// Kept, in the sent body's order, with injected values after the members they follow.
func Compare(sent, recv *Node) []Diff {
	var out []Diff
	walk("", sent, recv, &out)
	return out
}

func walk(path string, s, r *Node, out *[]Diff) {
	if string(s.Raw) == string(r.Raw) {
		return
	}
	if s.Kind != r.Kind {
		*out = append(*out, Diff{Path: path, Verdict: Retyped, Sent: string(s.Raw), Received: string(r.Raw)})
		return
	}
	switch s.Kind {
	case Object:
		walkObject(path, s, r, out)
	case Array:
		n := min(len(s.Elems), len(r.Elems))
		for i := 0; i < n; i++ {
			walk(path+"["+strconv.Itoa(i)+"]", s.Elems[i], r.Elems[i], out)
		}
		for i := n; i < len(s.Elems); i++ {
			*out = append(*out, Diff{Path: path + "[" + strconv.Itoa(i) + "]", Verdict: Dropped, Sent: string(s.Elems[i].Raw)})
		}
		for i := n; i < len(r.Elems); i++ {
			*out = append(*out, Diff{Path: path + "[" + strconv.Itoa(i) + "]", Verdict: Injected, Received: string(r.Elems[i].Raw)})
		}
	default:
		*out = append(*out, Diff{Path: path, Verdict: scalarVerdict(s, r), Sent: string(s.Raw), Received: string(r.Raw)})
	}
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func walkObject(path string, s, r *Node, out *[]Diff) {
	var dropped, injected []Member
	for _, m := range lastByKey(s) {
		if rv := r.Get(m.Key); rv != nil {
			walk(join(path, m.Key), m.Value, rv, out)
		} else {
			dropped = append(dropped, m)
		}
	}
	for _, m := range lastByKey(r) {
		if s.Get(m.Key) == nil {
			injected = append(injected, m)
		}
	}
	used := map[int]bool{}
	for _, d := range dropped {
		to := -1
		for j, in := range injected {
			if !used[j] && Equal(d.Value, in.Value) {
				to = j
				break
			}
		}
		if to >= 0 {
			used[to] = true
			*out = append(*out, Diff{Path: join(path, d.Key), Verdict: Renamed, Sent: string(d.Value.Raw), Received: string(injected[to].Value.Raw), To: join(path, injected[to].Key)})
			continue
		}
		*out = append(*out, Diff{Path: join(path, d.Key), Verdict: Dropped, Sent: string(d.Value.Raw)})
	}
	for j, in := range injected {
		if !used[j] {
			*out = append(*out, Diff{Path: join(path, in.Key), Verdict: Injected, Received: string(in.Value.Raw)})
		}
	}
}

// lastByKey returns an object's members with each key once, its last occurrence, in order.
func lastByKey(n *Node) []Member {
	last := map[string]int{}
	for i, m := range n.Members {
		last[m.Key] = i
	}
	var ms []Member
	for i, m := range n.Members {
		if last[m.Key] == i {
			ms = append(ms, m)
		}
	}
	return ms
}

func scalarVerdict(s, r *Node) Verdict {
	switch s.Kind {
	case String:
		var a, b string
		if json.Unmarshal(s.Raw, &a) == nil && json.Unmarshal(r.Raw, &b) == nil && a == b {
			return Reencoded
		}
		return Changed
	case Number:
		a, aok := new(big.Rat).SetString(string(s.Raw))
		b, bok := new(big.Rat).SetString(string(r.Raw))
		if !aok || !bok || a.Cmp(b) != 0 {
			return Changed
		}
		if isIntLiteral(s.Raw) != isIntLiteral(r.Raw) {
			return Retyped
		}
		return Reencoded
	}
	return Changed // bool and null have one spelling each; differing bytes are another value
}

func isIntLiteral(b []byte) bool { return !strings.ContainsAny(string(b), ".eE") }

// Equal reports whether two values are the same JSON value: object member order, whitespace,
// string escapes and the spelling of a number do not count.
func Equal(a, b *Node) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case Object:
		am, bm := lastByKey(a), lastByKey(b)
		if len(am) != len(bm) {
			return false
		}
		for _, m := range am {
			v := b.Get(m.Key)
			if v == nil || !Equal(m.Value, v) {
				return false
			}
		}
		return true
	case Array:
		if len(a.Elems) != len(b.Elems) {
			return false
		}
		for i := range a.Elems {
			if !Equal(a.Elems[i], b.Elems[i]) {
				return false
			}
		}
		return true
	case Null, Bool:
		return string(a.Raw) == string(b.Raw)
	default:
		v := scalarVerdict(a, b)
		return string(a.Raw) == string(b.Raw) || v == Reencoded || v == Retyped
	}
}

// Lookup returns the value at path ("a.b[0].c"), or nil.
func Lookup(n *Node, path string) *Node {
	for _, step := range splitPath(path) {
		if n == nil {
			return nil
		}
		if step.index >= 0 {
			if n.Kind != Array || step.index >= len(n.Elems) {
				return nil
			}
			n = n.Elems[step.index]
			continue
		}
		if n.Kind != Object {
			return nil
		}
		n = n.Get(step.key)
	}
	return n
}

type step struct {
	key   string
	index int
}

func splitPath(path string) []step {
	var steps []step
	for _, part := range strings.Split(path, ".") {
		if part == "" {
			continue
		}
		key, rest, _ := strings.Cut(part, "[")
		if key != "" {
			steps = append(steps, step{key: key, index: -1})
		}
		for rest != "" {
			num, after, _ := strings.Cut(rest, "]")
			i, err := strconv.Atoi(num)
			if err != nil {
				return append(steps, step{key: "\x00bad", index: -1})
			}
			steps = append(steps, step{index: i})
			rest = strings.TrimPrefix(after, "[")
		}
	}
	return steps
}

// related reports whether one path is the other or contains it.
func related(a, b string) bool {
	return a == b || within(a, b) || within(b, a)
}

// within reports whether path a lies inside path b.
func within(a, b string) bool {
	return b == "" || (len(a) > len(b) && strings.HasPrefix(a, b) && (a[len(b)] == '.' || a[len(b)] == '['))
}

// At returns the verdict of the value at path: the worst verdict among the diffs at the path, inside
// it, or on a value that contains it (a dropped parent drops its children). Kept if there is none.
// A renamed value counts at its old path.
func At(diffs []Diff, path string) (Verdict, []Diff) {
	v := Kept
	var hit []Diff
	for _, d := range diffs {
		if related(d.Path, path) {
			hit = append(hit, d)
			if Worse(d.Verdict, v) {
				v = d.Verdict
			}
		}
	}
	sort.SliceStable(hit, func(i, j int) bool { return Worse(hit[i].Verdict, hit[j].Verdict) })
	return v, hit
}
