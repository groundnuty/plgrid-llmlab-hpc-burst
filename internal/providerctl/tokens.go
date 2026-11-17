package providerctl

// Whole-token matching of a job ID in text. A job ID is a run of digits, so a
// substring match would also find it inside any longer number (a time stamp, a
// count, a path): the tests of IMPL-COMMON's "never contains X" rule match a
// token, and so does the code that keeps the ID out of a note (HBEP-8,
// Allocation events, No job ID in a note; R2 of #625).

import "strings"

// isTokenChar reports whether c can be part of a token: a letter, a digit or an
// underscore. A job ID is a run of digits, so a token match finds it as a
// whole number and not inside a longer one (a time stamp, a count).
func isTokenChar(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// containsToken reports whether s holds token as a whole token: the byte
// before and after it, where there is one, is not a token character. An empty
// token is in nothing.
func containsToken(s, token string) bool {
	return indexToken(s, token, 0) >= 0
}

// indexToken is the index of the first whole-token occurrence of token in s at
// or after from, or -1.
func indexToken(s, token string, from int) int {
	if token == "" {
		return -1
	}
	for from <= len(s)-len(token) {
		i := strings.Index(s[from:], token)
		if i < 0 {
			return -1
		}
		i += from
		end := i + len(token)
		if (i == 0 || !isTokenChar(s[i-1])) && (end == len(s) || !isTokenChar(s[end])) {
			return i
		}
		from = i + 1
	}
	return -1
}

// redactToken returns s with each whole-token occurrence of token replaced by
// repl. It is how text that comes from outside the controller (the scheduler's
// state reason, a relay's cause, a release cause a person wrote) is kept free
// of the job ID before it is copied into a note.
func redactToken(s, token, repl string) string {
	if token == "" {
		return s
	}
	var b strings.Builder
	at := 0
	for {
		i := indexToken(s, token, at)
		if i < 0 {
			b.WriteString(s[at:])
			return b.String()
		}
		b.WriteString(s[at:i])
		b.WriteString(repl)
		at = i + len(token)
	}
}
