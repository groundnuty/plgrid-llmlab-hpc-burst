package deploymentid

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

type tableKey struct {
	name string
	env  bool
}

// hbepTable parses the rows of HBEP-3's "v1 table for vLLM". The `v1` column
// is "input", "not an input", "not in `flags`", "never", "refused" or "refused
// in alpha"; a row names several
// flags or variables in backticks. A name with an upper-case letter is an
// environment variable, since every vLLM flag is lower-case.
func hbepTable(t *testing.T) map[tableKey]class {
	t.Helper()
	lines := strings.Split(readHBEP3(t), "\n")
	start := -1
	for i, l := range lines {
		if strings.HasPrefix(l, "  | Flag or variable | `v1` | Reason |") {
			start = i + 2
			break
		}
	}
	if start < 0 {
		t.Fatal("HBEP-3's classification table not found")
	}
	byLabel := map[string]class{
		"input": classInput, "not an input": classNotInput,
		"not in `flags`": classFrameworkPassed, "never": classNever,
		"refused": classRefused, "refused in alpha": classRefused,
	}
	name := regexp.MustCompile("`([^`]+)`")
	got := map[tableKey]class{}
	for _, l := range lines[start:] {
		if !strings.HasPrefix(strings.TrimSpace(l), "|") {
			break
		}
		cells := strings.Split(strings.TrimSpace(l), "|")
		c, ok := byLabel[strings.TrimSpace(cells[2])]
		if !ok {
			t.Fatalf("unknown class %q in row %q", cells[2], l)
		}
		for _, m := range name.FindAllStringSubmatch(cells[1], -1) {
			k := tableKey{name: m[1], env: m[1] != strings.ToLower(m[1])}
			if _, dup := got[k]; dup {
				t.Fatalf("HBEP-3 lists %q twice", m[1])
			}
			got[k] = c
		}
	}
	return got
}

// The data table must be HBEP-3's table, row for row: no row missing, none
// added, none classified differently.
func TestTableMatchesHBEP3(t *testing.T) {
	want := hbepTable(t)
	if len(want) < 50 {
		t.Fatalf("parsed only %d names from HBEP-3's table; the parser has drifted from the text", len(want))
	}
	got := map[tableKey]class{}
	for _, r := range vllmV0320 {
		got[tableKey{r.name, r.env}] = r.class
	}
	if len(got) != len(vllmV0320) {
		t.Errorf("the data table lists a name twice")
	}
	for k, c := range want {
		if g, ok := got[k]; !ok {
			t.Errorf("%+v is in HBEP-3's table but not in the data", k)
		} else if g != c {
			t.Errorf("%+v: data says %v, HBEP-3 says %v", k, g, c)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("%+v is in the data but not in HBEP-3's table", k)
		}
	}
}

func TestClassify(t *testing.T) {
	tab := tables[engineVLLM]
	cases := []struct {
		name string
		env  bool
		want class
	}{
		{"dtype", false, classInput},
		{"max-model-len", false, classInput},
		{"VLLM_USE_DEEP_GEMM", true, classInput},
		{"port", false, classNotInput},
		{"max-num-seqs", false, classNotInput},
		{"gpu-memory-utilization", false, classNotInput},
		{"host", false, classNotInput},
		{"HF_HOME", true, classNotInput},
		{"api-key", false, classNever},
		{"hf-token", false, classNever},
		{"HF_TOKEN", true, classNever},
		{"model", false, classFrameworkPassed},
		{"revision", false, classFrameworkPassed},
		{"chat-template", false, classFrameworkPassed},
		// An unlisted name is an input.
		{"some-future-flag", false, classInput},
		{"SOME_FUTURE_VAR", true, classInput},
		// Flags and variables are separate namespaces.
		{"HF_HOME", false, classInput},
		{"port", true, classInput},
		{"HF_TOKEN", false, classInput},
		{"api-key", true, classInput},
		// Names are matched exactly.
		{"Port", false, classInput},
		{"max_num_seqs", false, classInput},
	}
	for _, c := range cases {
		if got, _ := tab.classify(c.name, c.env); got != c.want {
			t.Errorf("classify(%q, env=%v) = %v, want %v", c.name, c.env, got, c.want)
		}
	}
}

func TestNewTableRejectsDuplicates(t *testing.T) {
	if _, err := newTable([]rule{{"a", classInput, false, ""}, {"a", classNotInput, false, ""}}); err == nil {
		t.Error("a name listed twice in one namespace was accepted")
	}
	if _, err := newTable([]rule{{"a", classInput, false, ""}, {"a", classNotInput, true, ""}}); err != nil {
		t.Errorf("the same name in the flag and variable namespaces was refused: %v", err)
	}
}

func TestUnsupportedEngineIsRefused(t *testing.T) {
	spec, accel, files := baseSpec()
	spec.Engine.Name = "other-engine"
	_, err := Compute(spec, accel, files)
	if !errors.Is(err, ErrUnsupportedEngine) {
		t.Errorf("got %v, want ErrUnsupportedEngine", err)
	}
}
