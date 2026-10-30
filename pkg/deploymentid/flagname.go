package deploymentid

import (
	"fmt"
	"strings"
)

// resolveFlag turns the name a serving spec gives a flag into the one name the
// ID uses for it, or refuses the spelling (HBEP-3, "How the serving spec gives
// flags"). Classification stays an exact lookup (table.classify); this step
// makes sure every spelling vLLM would resolve to a flag reaches that lookup
// under the flag's own name, so that two spellings of one setting never give
// two IDs and a refused flag cannot pass under another spelling.
//
// vLLM v0.32.0 resolves a command-line spelling to an option in three ways
// besides the exact long name:
//
//   - Underscores. FlexibleArgumentParser.parse_args replaces "_" with "-" in
//     everything between the leading "--" and the first "." (vllm/utils/
//     argparse_utils.py@v0.32.0, L337-357). resolveFlag does the same, before
//     the character set is checked, so max_num_seqs is max-num-seqs and the ID
//     key is the dashed name.
//   - Dotted sub-keys. "--X.k V" is folded into "--X '{"k": V}'" (same file,
//     L425-462), which sets --X. A name with a "." is refused for every flag:
//     the JSON value belongs in the value, under the long name.
//   - Prefix abbreviation. The parser leaves argparse's allow_abbrev at its
//     default, so a unique prefix of an option resolves to it, and api-k
//     reaches --api-key. Every proper prefix of a name in the table is refused;
//     the table is the library's list of known long names. A prefix of a
//     refused flag gets that flag's error, so api-k is ErrSecret. Abbreviations
//     of the many vLLM flags the table does not list are not detected: they are
//     unlisted names, hashed as inputs, and can only split an ID.
//
// The checks run in this order, so a spelling of a refused flag gets the
// refused flag's error whichever trick it uses:
//
//  1. the name is non-empty UTF-8 and has no leading dash (checkName);
//  2. the base is the name with "_" as "-", up to the first ".";
//  3. a base that is a credential, a framework-passed flag or a refused
//     flag: ErrSecret, ErrFrameworkPassed or ErrRefused;
//  4. a base that is a short option (tp, pp, dp, ...): ErrShortAlias;
//  5. a base that is not listed and is a proper prefix of a listed name:
//     that flag's error if it is refused (the first of credential,
//     framework-passed, refused), otherwise ErrInvalid;
//  6. "no-" and a listed name: ErrInvalid, booleans use the positive name;
//  7. a "." in the name: ErrInvalid;
//  8. a base outside [a-z0-9][a-z0-9-]*: ErrInvalid.
//
// Step 8 comes last only so that steps 3 to 7 give the informative error for
// a name such as api-key.x; every name those steps accept matches the set
// already. The error never contains a flag's value.
func (t *table) resolveFlag(name string) (string, error) {
	if err := checkName(name, false); err != nil {
		return "", err
	}
	norm := strings.ReplaceAll(name, "_", "-")
	base, _, dotted := strings.Cut(norm, ".")
	if base == "" {
		return "", invalidf("flag name %q has nothing before its '.'", name)
	}
	if r, ok := t.flags[base]; ok && refused(r.class) {
		return "", spelledAs(refuse(r.class, "flag", r.name, r.reason), name, r.name)
	}
	if long, ok := t.aliases[base]; ok {
		return "", fmt.Errorf("%w: %q is vLLM's short option for --%s; list the long name", ErrShortAlias, name, long)
	}
	if _, listed := t.flags[base]; !listed {
		if r, ok := t.abbreviationOf(base); ok {
			if refused(r.class) {
				return "", spelledAs(refuse(r.class, "flag", r.name, r.reason), name, r.name)
			}
			return "", invalidf("flag %q abbreviates --%s, which vLLM's argument parser accepts; write the full long name", name, r.name)
		}
	}
	if positive, ok := strings.CutPrefix(base, "no-"); ok {
		if _, listed := t.flags[positive]; listed {
			return "", invalidf("flag %q is a negated boolean; use the positive name %q with the value \"false\"", name, positive)
		}
	}
	if dotted {
		return "", invalidf("flag name %q has a '.'; vLLM folds --name.key into a JSON value, so pass the JSON value under the long name", name)
	}
	if !isFlagName(base) {
		return "", invalidf("flag name %q must match [a-z0-9][a-z0-9-]* (underscores are read as dashes)", name)
	}
	return base, nil
}

// refused reports whether a class is one the library will not hash.
func refused(c class) bool {
	return c == classNever || c == classFrameworkPassed || c == classRefused
}

// spelledAs says that name is a spelling of the refused flag, keeping the
// refusal's error class.
func spelledAs(err error, name, flag string) error {
	if name == flag {
		return err
	}
	return fmt.Errorf("%w (spelled %q: vLLM's argument parser resolves it to --%s)", err, name, flag)
}

// abbreviationOf returns the listed flag of which base is a proper prefix,
// preferring a refused one: a credential, then a framework-passed flag, then a
// refused one, then any other, and among equals the name that sorts first, so
// the result does not depend on map order.
func (t *table) abbreviationOf(base string) (rule, bool) {
	rank := func(c class) int {
		switch c {
		case classNever:
			return 3
		case classFrameworkPassed:
			return 2
		case classRefused:
			return 1
		}
		return 0
	}
	var best rule
	found := false
	for _, r := range t.flags {
		if len(base) >= len(r.name) || !strings.HasPrefix(r.name, base) {
			continue
		}
		if !found || rank(r.class) > rank(best.class) || (rank(r.class) == rank(best.class) && r.name < best.name) {
			best, found = r, true
		}
	}
	return best, found
}

// isFlagName reports whether s matches [a-z0-9][a-z0-9-]*, which every vLLM
// long option does. HBEP-3 needs ASCII names so that JCS key order and
// bytewise order agree.
func isFlagName(s string) bool {
	if s == "" || s[0] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

// isEnvName reports whether s matches [A-Z_][A-Z0-9_]*, a POSIX variable name
// in the upper case that vLLM's variables use.
func isEnvName(s string) bool {
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}
