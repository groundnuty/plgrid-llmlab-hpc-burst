package naming

import (
	"strings"
	"testing"
)

// The accepted names are HBEP-3's Test plan list, with the parts its Grammar
// gives; the refused ones are the error cases its Grammar step 5 names.
func TestParseAcceptsHBEP3Examples(t *testing.T) {
	cases := []struct {
		in   string
		want Name
	}{
		{"example-27b", Name{Model: "example-27b", Level: LevelModel}},
		{"example-27b@helios-a", Name{Model: "example-27b", Provider: "helios-a", Level: LevelProvider}},
		{"example-27b@helios-a/0123456789ab", Name{Model: "example-27b", Provider: "helios-a", DeploymentID: "0123456789ab", Level: LevelPinned}},
		{"org/m@helios-a/0123456789ab", Name{Model: "org/m", Provider: "helios-a", DeploymentID: "0123456789ab", Level: LevelPinned}},
		{"a@b@helios-a", Name{Model: "a@b", Provider: "helios-a", Level: LevelProvider}},
		{"org/m", Name{Model: "org/m", Level: LevelModel}},
	}
	for _, c := range cases {
		got, err := Parse(c.in)
		if err != nil {
			t.Errorf("Parse(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("Parse(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

func TestParseRefusesGrammarErrors(t *testing.T) {
	for _, in := range []string{
		"",                          // empty name
		"m@",                        // empty provider part
		"@helios-a",                 // empty model part
		"m@helios-a/",               // trailing slash
		"m@/0123456789ab",           // empty provider before the ID
		"m@helios-a/0123",           // short ID
		"m@helios-a/0123456789abcd", // long ID
		"m@helios-a/0123456789AB",   // upper case
		"m@helios-a/0123456789ag",   // not hex
		"m@helios-a/0123456789ab/x", // trailing segment
	} {
		if got, err := Parse(in); err != ErrGrammar {
			t.Errorf("Parse(%q) = %+v, %v; want ErrGrammar", in, got, err)
		}
	}
}

// HBEP-3, Grammar step 0 (beta, proposed): a "#<workload>" suffix is split off
// at the name's only "#", before the last "@", on every naming level. The
// expected parts are written out from that text.
func TestParseSplitsTheWorkloadSuffixAtTheOnlyHash(t *testing.T) {
	label63 := strings.Repeat("a", 63)
	cases := []struct {
		in   string
		want Name
	}{
		{"org/m#wl", Name{Model: "org/m", Level: LevelModel, Workload: "wl"}},
		{"example-27b#study-1", Name{Model: "example-27b", Level: LevelModel, Workload: "study-1"}},
		{"org/m@p#wl", Name{Model: "org/m", Provider: "p", Level: LevelProvider, Workload: "wl"}},
		{"org/m@p/0123456789ab#wl", Name{Model: "org/m", Provider: "p", DeploymentID: "0123456789ab", Level: LevelPinned, Workload: "wl"}},
		{"a@b@p#wl", Name{Model: "a@b", Provider: "p", Level: LevelProvider, Workload: "wl"}},
		{"org/m#" + label63, Name{Model: "org/m", Level: LevelModel, Workload: label63}},
		{"org/m#a", Name{Model: "org/m", Level: LevelModel, Workload: "a"}},
		{"org/m#9-9", Name{Model: "org/m", Level: LevelModel, Workload: "9-9"}},
		{"org/m", Name{Model: "org/m", Level: LevelModel}}, // no suffix, no Workload
	}
	for _, c := range cases {
		got, err := Parse(c.in)
		if err != nil {
			t.Errorf("Parse(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("Parse(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

func TestParseRefusesWorkloadSuffixGrammarErrors(t *testing.T) {
	for _, in := range []string{
		"org/m##wl",                        // two "#"
		"org/m#a#b",                        // two "#", each part a label
		"org/m#",                           // empty Workload
		"#wl",                              // empty model
		"#",                                // both empty
		"org/m#Bad_Name",                   // not a DNS-1123 label
		"org/m#Upper",                      // upper case
		"org/m#-a",                         // starts with "-"
		"org/m#a-",                         // ends with "-"
		"org/m#a.b",                        // a dot
		"org/m#" + strings.Repeat("a", 64), // 64 characters
		"org/m#a@p",                        // "@" is not a label character: the suffix comes first
		"org/m@p#",                         // empty Workload after a provider
		"@p#wl",                            // empty model before the provider
		"org/m@#wl",                        // empty provider
		"org/m@p/0123#wl",                  // a short deployment ID under a suffix
	} {
		if got, err := Parse(in); err != ErrGrammar {
			t.Errorf("Parse(%q) = %+v, %v; want ErrGrammar", in, got, err)
		}
	}
}

func TestParseNeverSetsTheHeaderWorkload(t *testing.T) {
	n, err := Parse("org/m#wl")
	if err != nil || n.HeaderWorkload != "" {
		t.Errorf("Parse(org/m#wl) = %+v, %v: the header's Workload is the front's to set", n, err)
	}
}
