package agentapi

import (
	"strings"
	"testing"
)

// seq returns n tokens, from..from+n-1.
func seq(from int32, n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = from + int32(i)
	}
	return out
}

// withAt returns a copy of tokens with the token at index i set to v.
func withAt(tokens []int32, i int, v int32) []int32 {
	out := append([]int32(nil), tokens...)
	out[i] = v
	return out
}

func rep(token int32, n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = token
	}
	return out
}

// The vectors are written by hand from HBEP-7's rule: three outputs that share
// their first 32 tokens (every token, if an output is shorter), none empty or a
// single repeated token.
func TestCorruptionVerdict(t *testing.T) {
	base := seq(1, 40) // 1..40
	cases := []struct {
		name string
		outs [][]int32
		pass bool
		want string // a part of the message of a failure
	}{
		{"three identical outputs", [][]int32{base, base, base}, true, ""},
		{"the 32nd token differs: within the first 32", [][]int32{base, withAt(base, 31, 99), base}, false, "token 32"},
		{"the 33rd token differs: after the first 32 (positive control)", [][]int32{base, withAt(base, 32, 99), base}, true, ""},
		{"the 1st token differs", [][]int32{withAt(base, 0, 99), base, base}, false, "token 1"},
		{"all three differ after the 32nd token", [][]int32{withAt(base, 35, 91), withAt(base, 35, 92), withAt(base, 35, 93)}, true, ""},
		{"a shorter output that is a prefix of the others", [][]int32{base[:10], base, base}, true, ""},
		{"a shorter output that differs within its length", [][]int32{withAt(base[:10], 9, 99), base, base}, false, "token 10"},
		{"an output of one token that matches the others' first", [][]int32{base[:1], base, base}, true, ""},
		{"a prefix agreement that is not transitive: 1 matches 2 and 3, 2 and 3 differ at token 21", [][]int32{base[:5], base, withAt(base, 20, 99)}, false, "outputs 2 and 3"},
		{"an empty output", [][]int32{base, {}, base}, false, "empty"},
		{"a nil output", [][]int32{base, base, nil}, false, "empty"},
		{"three empty outputs", [][]int32{{}, {}, {}}, false, "empty"},
		{"one token repeated, in all three", [][]int32{rep(7, 40), rep(7, 40), rep(7, 40)}, false, "single repeated token"},
		{"one token repeated, in two", [][]int32{rep(7, 40), rep(7, 40), base}, false, "single repeated token"},
		{"two tokens repeated alternately is not a single token", [][]int32{alternate(40), alternate(40), alternate(40)}, true, ""},
		{"a single token output of the one repeated: not repeated", [][]int32{{7}, {7}, {7}}, true, ""},
		{"two outputs", [][]int32{base, base}, false, "got 2 outputs"},
		{"four outputs", [][]int32{base, base, base, base}, false, "got 4 outputs"},
		{"no outputs", nil, false, "got 0 outputs"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pass, msg := CorruptionVerdict(c.outs)
			if pass != c.pass {
				t.Fatalf("pass = %v (%q), want %v", pass, msg, c.pass)
			}
			if c.pass && msg != "" {
				t.Errorf("a pass carries the message %q", msg)
			}
			if !c.pass && !strings.Contains(msg, c.want) {
				t.Errorf("message %q does not mention %q", msg, c.want)
			}
		})
	}
}

func alternate(n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = int32(1 + i%2)
	}
	return out
}

func TestCorruptionVerdictDoesNotChangeItsInput(t *testing.T) {
	outs := [][]int32{seq(1, 40), seq(1, 40), seq(1, 40)}
	CorruptionVerdict(outs)
	for n, o := range outs {
		if len(o) != 40 || o[0] != 1 || o[39] != 40 {
			t.Errorf("output %d changed: %v", n, o)
		}
	}
}

// The numbers are HBEP-7's: three greedy outputs, the first 32 tokens shared.
func TestCorruptionConstants(t *testing.T) {
	if CorruptionOutputs != 3 || CorruptionPrefix != 32 {
		t.Errorf("CorruptionOutputs = %d, CorruptionPrefix = %d; want 3 and 32", CorruptionOutputs, CorruptionPrefix)
	}
}
