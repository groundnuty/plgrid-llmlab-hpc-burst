package deploymentid

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
)

var vllm = tables[engineVLLM]

// className and the cases below take their expectations from vLLM's parser
// (vllm/utils/argparse_utils.py@v0.32.0) and from the owner's decisions on
// PR #46, not from resolveFlag.

// Two spellings of one setting are one setting: an underscore is a dash.
func TestResolveFlagReadsUnderscoresAsDashes(t *testing.T) {
	for in, want := range map[string]string{
		"max_num_seqs":           "max-num-seqs",
		"max-num-seqs":           "max-num-seqs",
		"max_model_len":          "max-model-len",
		"some_new_flag":          "some-new-flag",
		"a_b_c":                  "a-b-c",
		"gpu_memory_utilization": "gpu-memory-utilization",
		"dtype":                  "dtype",
		"x1":                     "x1",
	} {
		got, err := vllm.resolveFlag(in)
		if err != nil || got != want {
			t.Errorf("resolveFlag(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

// max_num_seqs is classified exactly as max-num-seqs is: not an input.
func TestUnderscoreSpellingIsClassifiedLikeTheDashedOne(t *testing.T) {
	for _, r := range vllmV0320 {
		if r.env || !strings.Contains(r.name, "-") {
			continue
		}
		spelled := strings.ReplaceAll(r.name, "-", "_")
		got, err := vllm.resolveFlag(spelled)
		switch r.class {
		case classNever:
			if !errors.Is(err, ErrSecret) {
				t.Errorf("%s: got %q, %v; want ErrSecret", spelled, got, err)
			}
		case classFrameworkPassed:
			if !errors.Is(err, ErrFrameworkPassed) {
				t.Errorf("%s: got %q, %v; want ErrFrameworkPassed", spelled, got, err)
			}
		case classRefused:
			if !errors.Is(err, ErrRefused) {
				t.Errorf("%s: got %q, %v; want ErrRefused", spelled, got, err)
			}
		default:
			if err != nil || got != r.name {
				t.Errorf("%s: got %q, %v; want %q", spelled, got, err, r.name)
				continue
			}
			if c, _ := vllm.classify(got, false); c != r.class {
				t.Errorf("%s is classified %v, %s is %v", spelled, c, r.name, r.class)
			}
		}
	}
}

func TestUnderscoreSpellingGivesTheSameID(t *testing.T) {
	// A not-input flag drops out under either spelling.
	spec, accel, files := baseSpec()
	want := mustCompute(t, spec, accel, files)
	spec.Flags = append(spec.Flags, Flag{"max_num_seqs", "8"}, Flag{"gpu_memory_utilization", "0.5"})
	if got := mustCompute(t, spec, accel, files); got.SHA256 != want.SHA256 {
		t.Errorf("a not-input flag in underscore spelling changed the ID\n got %s\nwant %s", got.Canonical, want.Canonical)
	}

	// An input flag is written under its dashed name, so both spellings give the
	// same bytes, and the underscore name never reaches the object.
	dashed, accel, files := baseSpec()
	dashed.Flags = append(dashed.Flags, Flag{"max-model-len", "4096"}, Flag{"some-future-flag", "x"})
	under, _, _ := baseSpec()
	under.Flags = append(under.Flags, Flag{"max_model_len", "4096"}, Flag{"some_future_flag", "x"})
	a, b := mustCompute(t, dashed, accel, files), mustCompute(t, under, accel, files)
	if !bytes.Equal(a.Canonical, b.Canonical) {
		t.Errorf("two spellings gave two IDs\n dashed %s\n under  %s", a.Canonical, b.Canonical)
	}
	if !bytes.Contains(b.Canonical, []byte(`"max-model-len":"4096"`)) || bytes.Contains(b.Canonical, []byte("_len")) {
		t.Errorf("the underscore spelling reached the object: %s", b.Canonical)
	}

	// Both spellings of one repeated flag are one array, in the order given.
	mixed, _, _ := baseSpec()
	mixed.Flags = []Flag{{"allowed_origins", "a"}, {"allowed-origins", "b"}, {"allowed_origins", "c"}}
	plain, _, _ := baseSpec()
	plain.Flags = []Flag{{"allowed-origins", "a"}, {"allowed-origins", "b"}, {"allowed-origins", "c"}}
	if m, p := mustCompute(t, mixed, accel, files), mustCompute(t, plain, accel, files); !bytes.Equal(m.Canonical, p.Canonical) ||
		!bytes.Contains(m.Canonical, []byte(`"allowed-origins":["a","b","c"]`)) {
		t.Errorf("mixed spellings of a repeated flag\n got  %s\n want %s", m.Canonical, p.Canonical)
	}
}

// Environment names are exact: HF_TOKEN is a key, and nothing rewrites it into
// a name that the table does not list.
func TestEnvNamesAreNotRewritten(t *testing.T) {
	spec, accel, files := baseSpec()
	spec.Env = []EnvVar{{"HF_TOKEN", "SECRET-VALUE"}}
	if _, err := Compute(spec, accel, files); !errors.Is(err, ErrSecret) {
		t.Errorf("HF_TOKEN: got %v, want ErrSecret", err)
	}
	spec.Env = []EnvVar{{"MY_VAR_2", "x"}, {"_PRIVATE", "y"}}
	id := mustCompute(t, spec, accel, files)
	if !bytes.Contains(id.Canonical, []byte(`"env":{"MY_VAR_2":"x","_PRIVATE":"y"}`)) {
		t.Errorf("env names did not survive as given: %s", id.Canonical)
	}
}

// wantErr is the error a refused class gives, or nil.
func wantErr(c class) error {
	return map[class]error{classNever: ErrSecret, classFrameworkPassed: ErrFrameworkPassed, classRefused: ErrRefused}[c]
}

// For each credential, framework-passed or refused flag of the table, every
// spelling vLLM's parser resolves to it is refused with the flag's own error:
// an underscore spelling, a dotted sub-key, and each proper prefix that is not
// itself a listed name and not a prefix of another refused flag.
func TestEverySpellingOfARefusedFlagIsRefused(t *testing.T) {
	const secret = "SECRET-VALUE"
	checked := 0
	for _, r := range vllmV0320 {
		want := wantErr(r.class)
		if r.env || want == nil {
			continue
		}
		spellings := []string{r.name, strings.ReplaceAll(r.name, "-", "_"), r.name + ".x", r.name + ".x.y",
			strings.ReplaceAll(r.name, "-", "_") + ".x"}
		for n := 1; n < len(r.name); n++ {
			prefix := r.name[:n]
			if _, alias := vllmShortAliases[prefix]; alias {
				continue // "r" is both a prefix of revision and vLLM's -r
			}
			if vllmListed(prefix) != nil || sharedWithAnotherRefused(prefix, r.name) {
				continue
			}
			spellings = append(spellings, prefix, strings.ReplaceAll(prefix, "-", "_"), prefix+".x")
		}
		for _, sp := range spellings {
			spec, accel, files := baseSpec()
			spec.Flags = append(spec.Flags, Flag{sp, secret})
			_, err := Compute(spec, accel, files)
			if !errors.Is(err, want) {
				t.Errorf("%q (of %s): got %v, want %v", sp, r.name, err, want)
			} else if strings.Contains(err.Error(), secret) {
				t.Errorf("%q: the error repeats the value: %v", sp, err)
			}
			checked++
		}
	}
	if checked < 100 {
		t.Errorf("only %d spellings were tried", checked)
	}
}

// sharedWithAnotherRefused reports whether prefix is a proper prefix of a
// refused flag of the table other than name.
func sharedWithAnotherRefused(prefix, name string) bool {
	for _, o := range vllmV0320 {
		if !o.env && o.name != name && wantErr(o.class) != nil && len(prefix) < len(o.name) && strings.HasPrefix(o.name, prefix) {
			return true
		}
	}
	return false
}

// Every proper prefix of every flag the table lists is refused (HBEP-3: vLLM's
// parser resolves abbreviations), unless it is itself a listed name or one of
// vLLM's short options. A prefix of only flags that are inputs or not inputs is
// ErrInvalid, so "max-num-s" does not reach --max-num-seqs under a name the
// table does not know. A prefix of a refused flag gets that flag's error.
func TestEveryProperPrefixOfAKnownFlagIsRefused(t *testing.T) {
	checked, plain := 0, 0
	for _, r := range vllmV0320 {
		if r.env {
			continue
		}
		for n := 1; n < len(r.name); n++ {
			prefix := r.name[:n]
			if vllmListed(prefix) != nil {
				continue
			}
			if _, alias := vllmShortAliases[prefix]; alias {
				continue
			}
			_, err := vllm.resolveFlag(prefix)
			if err == nil {
				t.Errorf("%q, a prefix of %s, was accepted", prefix, r.name)
				continue
			}
			checked++
			if sharedWithAnotherRefused(prefix, "") || wantErr(r.class) != nil {
				continue // which refused flag it names is TestEverySpellingOfARefusedFlagIsRefused's concern
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("%q, a prefix of %s: got %v, want ErrInvalid", prefix, r.name, err)
			}
			plain++
		}
	}
	if checked < 300 || plain < 100 {
		t.Errorf("only %d prefixes (%d of non-refused flags) were tried", checked, plain)
	}
	// Written out: abbreviations of flags that are not refused.
	for _, name := range []string{"max-num-s", "max-num", "max-model-le", "enable-prefix", "gpu-memory", "tensor-parallel-siz", "d", "dty", "tool-call-pars"} {
		if _, err := vllm.resolveFlag(name); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: got %v, want ErrInvalid", name, err)
		}
	}
	// A name that extends a listed name is not a prefix of one.
	for _, name := range []string{"dtype-x", "max-model-length", "port-2", "seedling"} {
		if got, err := vllm.resolveFlag(name); err != nil || got != name {
			t.Errorf("%q: got %q, %v; want it accepted as an unlisted input", name, got, err)
		}
	}
}

// vllmListed returns the rule of a listed flag name, or nil.
func vllmListed(name string) *rule {
	if r, ok := vllm.flags[name]; ok {
		return &r
	}
	return nil
}

// Every flag of the table has a negated spelling that is refused, and
// every short option of vLLM that the table knows is refused without its dash.
func TestNegatedBooleansAndShortOptionsAreRefused(t *testing.T) {
	for _, r := range vllmV0320 {
		if r.env {
			continue
		}
		spec, accel, files := baseSpec()
		spec.Flags = append(spec.Flags, Flag{"no-" + r.name, ""})
		if _, err := Compute(spec, accel, files); !errors.Is(err, ErrInvalid) {
			t.Errorf("no-%s: got %v, want ErrInvalid", r.name, err)
		}
	}
	for alias := range vllmShortAliases {
		for _, spelled := range []string{alias, "-" + alias} {
			spec, accel, files := baseSpec()
			spec.Flags = append(spec.Flags, Flag{spelled, "2"})
			if _, err := Compute(spec, accel, files); !errors.Is(err, ErrShortAlias) {
				t.Errorf("%q: got %v, want ErrShortAlias", spelled, err)
			}
		}
	}
	for _, must := range []string{"tp", "pp", "dp"} { // owner decision 4, written out
		if _, ok := vllmShortAliases[must]; !ok {
			t.Errorf("%s is missing from the short options", must)
		}
	}
}

// A flag the table does not list, and no rule touches, is an input under its
// own name; an abbreviation of a flag that is not refused is such a flag too.
func TestUnlistedNamesStayInputs(t *testing.T) {
	for _, name := range []string{"some-future-flag", "middleware", "x", "0", "9lives", "a-"} {
		got, err := vllm.resolveFlag(name)
		if err != nil || got != name {
			t.Errorf("resolveFlag(%q) = %q, %v; want the name itself", name, got, err)
		}
		if _, listed := vllm.flags[name]; !listed {
			if c, _ := vllm.classify(got, false); c != classInput {
				t.Errorf("%q is classified %v, want input", name, c)
			}
		}
	}
	// "r" abbreviates revision and is vLLM's -r (--node-rank); either way it is refused.
	if _, err := vllm.resolveFlag("r"); !errors.Is(err, ErrShortAlias) {
		t.Errorf("r: got %v, want ErrShortAlias", err)
	}
}

// The prefix rule applies to a name that is not listed. A listed name that
// happens to be a prefix of another is accepted as what it is. When several
// flags share a prefix the answer is fixed: a credential, then a
// framework-passed flag, then a refused one, then any other; among equals the
// name that sorts first.
func TestPrefixRuleOnASyntheticTable(t *testing.T) {
	tab, err := newTable([]rule{
		{"ab", classNotInput, false, "capacity"},
		{"abc", classNever, false, "a key"},
		{"xa", classFrameworkPassed, false, "passed"},
		{"xb", classNever, false, "a key"},
		{"ya", classFrameworkPassed, false, "passed"},
		{"yb", classFrameworkPassed, false, "passed"},
		{"wa", classRefused, false, "refused"},
		{"wb", classInput, false, "input"},
		{"vq", classNotInput, false, "capacity"},
		{"vr", classInput, false, "input"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := tab.resolveFlag("ab"); err != nil || got != "ab" {
		t.Errorf("a listed prefix: got %q, %v; want it accepted", got, err)
	}
	for name, want := range map[string]error{
		"a": ErrSecret,          // abbreviates abc (a key) and ab
		"x": ErrSecret,          // a key before a framework-passed flag
		"y": ErrFrameworkPassed, // two framework-passed flags
		"w": ErrRefused,         // a refused flag before an input
		"v": ErrInvalid,         // only flags that are not refused
	} {
		if _, err := tab.resolveFlag(name); !errors.Is(err, want) {
			t.Errorf("%s: got %v, want %v", name, err, want)
		}
	}
	for i := 0; i < 50; i++ { // map order must not matter
		if r, ok := tab.abbreviationOf("y"); !ok || r.name != "ya" {
			t.Fatalf("y: got %+v, %v; want ya, the first by name", r, ok)
		}
		if r, ok := tab.abbreviationOf("v"); !ok || r.name != "vq" {
			t.Fatalf("v: got %+v, %v; want vq, the first by name", r, ok)
		}
	}
	if _, err := tab.resolveFlag("abcd"); err != nil {
		t.Errorf("abcd extends a refused name and is not an abbreviation of it: %v", err)
	}
}

func TestIsFlagNameAndIsEnvName(t *testing.T) {
	for s, want := range map[string]bool{
		"dtype": true, "max-num-seqs": true, "0": true, "9x": true, "a-": true, "a--b": true,
		"": false, "-a": false, "A": false, "a_b": false, "a.b": false, "a b": false, "é": false, "aé": false, "a\n": false,
	} {
		if got := isFlagName(s); got != want {
			t.Errorf("isFlagName(%q) = %v, want %v", s, got, want)
		}
	}
	for s, want := range map[string]bool{
		"A": true, "_": true, "_A": true, "A1_B2": true, "VLLM_USE_DEEP_GEMM": true,
		"": false, "1A": false, "a": false, "A-B": false, "A.B": false, "A B": false, "É": false, "A\n": false,
	} {
		if got := isEnvName(s); got != want {
			t.Errorf("isEnvName(%q) = %v, want %v", s, got, want)
		}
	}
}

// Flags are kept in the order given among the values of one name, also across
// spellings, whatever else is in the list.
func TestSpellingsKeepTheOrderOfValues(t *testing.T) {
	spec, accel, files := baseSpec()
	spec.Flags = []Flag{{"seed_x", "2"}, {"port", "1"}, {"seed-x", "1"}}
	id := mustCompute(t, spec, accel, files)
	if !bytes.Contains(id.Canonical, []byte(`"seed-x":["2","1"]`)) {
		t.Errorf("got %s", id.Canonical)
	}
	rev := slices.Clone(spec.Flags)
	slices.Reverse(rev)
	spec.Flags = rev
	if other := mustCompute(t, spec, accel, files); other.SHA256 == id.SHA256 {
		t.Error("reversing the values of one flag, spelled two ways, kept the ID")
	}
}

// HBEP-3 checks a name in this order: underscores to dashes, then the
// character set, then the known-name rules. The character set is checked on
// the normalised name, so an underscore never fails it by itself, and
// whatever the normalisation leaves outside it still does.
func TestUnderscoresAreNormalisedBeforeTheCharacterSet(t *testing.T) {
	for name, wantOK := range map[string]bool{
		"some_new_flag": true, "a_b": true, "_x": false, "x_": true, "__": false, "a__b": true,
		"A_b": false, "a_B": false, "a_\u00e9": false, "a_b.c": false, "a_ b": false,
	} {
		got, err := vllm.resolveFlag(name)
		if wantOK != (err == nil) {
			t.Errorf("resolveFlag(%q) = %q, %v; want ok=%v", name, got, err, wantOK)
		}
		if err == nil && strings.Contains(got, "_") {
			t.Errorf("resolveFlag(%q) = %q still has an underscore", name, got)
		}
	}
}
