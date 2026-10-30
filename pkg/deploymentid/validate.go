package deploymentid

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Errors returned by Compute and ComputeManaged. A caller tests for them with
// errors.Is. An input the library cannot hash always yields an error; none is
// replaced by a default.
var (
	// ErrInvalid: an input is empty, malformed or ambiguous.
	ErrInvalid = errors.New("invalid deployment ID input")
	// ErrShortAlias: a flag name starts with a single dash, such as -tp, or is
	// one of vLLM's short options written without its dash, such as tp. The
	// spec lists long names without leading dashes; no command line is parsed.
	ErrShortAlias = errors.New("short flag alias")
	// ErrSecret: a key (api-key, hf-token, HF_TOKEN). A key is referenced, never
	// written in the spec, so that rotating it never changes an ID.
	ErrSecret = errors.New("secret flag or variable")
	// ErrFrameworkPassed: the spec lists model, revision or chat-template, which
	// the framework passes itself from checkpoint and the template.
	ErrFrameworkPassed = errors.New("setting passed by the framework")
	// ErrUnsupportedEngine: scheme v1 has no classification table for the engine.
	ErrUnsupportedEngine = errors.New("engine has no scheme v1 table")
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// requireText checks that a field is a non-empty, valid UTF-8 string.
func requireText(field, s string) error {
	if s == "" {
		return invalidf("%s is empty", field)
	}
	if !utf8.ValidString(s) {
		return invalidf("%s is not valid UTF-8", field)
	}
	return nil
}

// requireSHA256 checks for 64 lowercase hex characters.
func requireSHA256(field, s string) error {
	if !isLowerHex(s, 64) {
		return invalidf("%s must be 64 lowercase hex characters", field)
	}
	return nil
}

// requireCommit checks for a full git commit id: 40 lowercase hex characters
// (SHA-1 object format) or 64 (SHA-256 object format).
func requireCommit(field, s string) error {
	if !isLowerHex(s, 40) && !isLowerHex(s, 64) {
		return invalidf("%s must be a full commit id, 40 or 64 lowercase hex characters", field)
	}
	return nil
}

// checkName validates the name of a flag or, with env true, of an environment
// variable, before any spelling is resolved. A name is non-empty UTF-8. A flag
// name has no leading dash: "-tp" is ErrShortAlias and "--dtype" ErrInvalid,
// since the spec lists long names without dashes. An environment variable name
// matches [A-Z_][A-Z0-9_]* and is never rewritten. A flag name's character set
// is checked by resolveFlag, after underscores are read as dashes. Both sets
// are ASCII, so every key of the canonical object is ASCII and JCS's UTF-16
// key order is the bytewise order (HBEP-3).
func checkName(name string, env bool) error {
	kind := "flag name"
	if env {
		kind = "environment variable name"
	}
	if err := requireText(kind, name); err != nil {
		return err
	}
	if env {
		if !isEnvName(name) {
			return invalidf("%s %q must match [A-Z_][A-Z0-9_]*", kind, name)
		}
		return nil
	}
	if name[0] == '-' {
		if len(name) > 1 && name[1] != '-' {
			return fmt.Errorf("%w: %q starts with a single dash; list the long name without leading dashes",
				ErrShortAlias, name)
		}
		return invalidf("%s %q starts with dashes; list it without leading dashes", kind, name)
	}
	return nil
}

// checkPath validates a file path relative to the checkpoint root: not empty,
// not absolute, with '/' separators and no empty, "." or ".." segment.
func checkPath(p string) error {
	if err := requireText("file path", p); err != nil {
		return err
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return invalidf("file path %q is not a clean relative path", p)
		}
	}
	return nil
}
