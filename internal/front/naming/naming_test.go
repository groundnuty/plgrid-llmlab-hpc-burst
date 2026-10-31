package naming

import "testing"

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
		"",                     // empty name
		"m@",                   // empty provider part
		"@helios-a",            // empty model part
		"m@helios-a/",          // trailing slash
		"m@/0123456789ab",      // empty provider before the ID
		"m@helios-a/0123",      // short ID
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
