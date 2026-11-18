package e2e

import "testing"

// The matcher that (e) uses finds a value standing alone, however it is quoted, and not a short string inside a
// longer random one; and a real occurrence among embedded ones is still found. No cluster here: this is the one
// test of the package that starts none.
func TestTheTokenMatcherFindsAWholeTokenOnly(t *testing.T) {
	const m = "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name, text string
		want       bool
	}{
		{"alone", m, true},
		{"in JSON quotes", `{"password":"` + m + `"}`, true},
		{"after a space, before a newline", "x " + m + "\n", true},
		{"after an equals sign and before a comma", "k=" + m + ",", true},
		{"at the start of a line", m + " tail", true},
		{"inside a longer hex string", "ff" + m + "ff", false},
		{"preceded by a letter", "a" + m, false},
		{"followed by a digit", m + "9", false},
		{"followed by a padding sign: an unpadded form inside a padded one is still a leak", m + "=", true},
		{"preceded by a dash, as in a name", "name-" + m, false},
		{"embedded first, alone second", "z" + m + " and " + m, true},
		{"two embedded", "a" + m + "b " + "c" + m + "d", false},
		{"absent", "nothing here", false},
		{"empty text", "", false},
	} {
		if got := containsToken([]byte(tc.text), m); got != tc.want {
			t.Errorf("%s: containsToken(%q) = %v, want %v", tc.name, tc.text, got, tc.want)
		}
	}
	if containsToken([]byte("anything"), "") {
		t.Errorf("an empty token is found")
	}
}
