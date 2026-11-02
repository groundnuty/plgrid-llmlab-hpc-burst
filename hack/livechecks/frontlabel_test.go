package livechecks

// Where the front ran (issue #224). Every row that writes a text to post names the host role that ran the front and
// its network position, from the setting LIVE_FRONT_HOST_LABEL, and refuses to run without a valid one. The label is
// written here, in the test, and looked for in the text the row's own test wrote: it is not a constant the code
// writes by itself, so finding it proves the setting went through.

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// exampleFrontLabel is the label of the worlds' settings: a role and a position, with an apostrophe, as a real one has.
const exampleFrontLabel = "an example development host (outside Cyfronet's network)"

// labelledRows are the rows whose text to post names where the front ran: the rows that write a posts.md.
var labelledRows = []string{"alpha-athena-handover", "alpha-helios-nooverlap", "tunnel-hour-helios", "tunnel-hour-athena", "gpu-accounting-helios", "gpu-accounting-athena"}

// The six rows run their own compiled test in dry mode, through the script, and the text they wrote names the label
// the settings gave: the second line, right after the verdict. With the label unset a row refuses, exit 3, naming the
// setting, before anything is built, estimated, submitted or cancelled.
func TestEveryRowThatPostsNamesWhereTheFrontRanAndRefusesWithoutIt(t *testing.T) {
	for _, row := range labelledRows {
		var d dryRow
		for _, r := range dryRows {
			if r.row == row {
				d = r
			}
		}
		if d.row == "" {
			t.Fatalf("%s is not a dry row", row)
		}
		t.Run(row+"/named", func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			w.installDry(dryWrapper, realTestBinary(t, d.env, d.pkg), filepath.Join(repoRoot(t), d.pkg))
			res := w.run([]string{"STUB_DRY_FAULT="}, "--run", row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			ls := strings.Split(w.postsOf(row), "\n")
			if len(ls) < 3 || ls[1] != "- front ran on: "+exampleFrontLabel {
				t.Errorf("the second line of the text to post is not the front line:\n%s", strings.Join(ls, "\n"))
			}
			if strings.Contains(res.out, exampleFrontLabel) {
				t.Errorf("the label reached the screen:\n%s", res.out)
			}
		})
		t.Run(row+"/unset", func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			w.alphaSettings(func(m map[string]string) { delete(m, "LIVE_FRONT_HOST_LABEL") })
			res := w.run(nil, "--run", row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, "setting LIVE_FRONT_HOST_LABEL is missing")
			for name, got := range map[string]string{"go": w.stateFile("go-argv"), "sbatch": w.stateFile("sbatch-argv"), "scancel": w.stateFile("cancelled"), "events": w.stateFile("events")} {
				if got != "" {
					t.Errorf("a refused run left a trace in %s: %s", name, got)
				}
			}
		})
	}
}

// The script and the package hold the label to one form. Each label is written out here with the verdict of both: the
// script's (a row run without --yes reaches the estimate, exit 4, or is refused, exit 3, naming the setting) and the
// package's validator.
func TestTheFrontLabelIsHeldToOneFormByTheScriptAndThePackage(t *testing.T) {
	long := strings.Repeat("a", 112) + " outside" // 120 bytes
	for _, c := range []struct {
		name, label string
		ok          bool
	}{
		{"a role outside", "the development host, outside Cyfronet's network", true},
		{"a role inside", "a Cyfronet-network host (inside the network)", true},
		{"the position in capitals", "Development host - OUTSIDE the network", true},
		{"a semicolon and a hyphen", "dev host; inside - a Cyfronet-network host", true},
		{"the longest", long, true},
		{"one byte over", long + "x", false},
		{"no position", "the development host", false},
		{"a position inside a word", "the outsider host", false},
		{"a position followed by a digit", "host inside2", true},
		{"a host name", "dev1.example.org, outside the network", false},
		{"an IPv4 address", "192.0.2.1 outside", false},
		{"an IPv6 address", "2001:db8::1 outside", false},
		{"a URL", "https://front.example outside", false},
		{"a user and host", "me@front outside", false},
		{"a path", "/srv/front outside", false},
		{"a backslash", `the host\ outside`, false},
		{"a leading space", " outside the network", false},
		{"a dollar sign", "the host $HOME outside", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := alpha.ValidFrontLabel(c.label) == nil; got != c.ok {
				t.Errorf("the package's verdict on %q is %t, want %t", c.label, got, c.ok)
			}
			w := newAlphaWorld(t)
			w.alphaSettings(func(m map[string]string) { m["LIVE_FRONT_HOST_LABEL"] = c.label })
			res := w.run(nil, "--run", "tunnel-hour-athena")
			want := 4
			if !c.ok {
				want = 3
			}
			if res.code != want {
				t.Fatalf("the script's exit on %q is %d, want %d:\n%s", c.label, res.code, want, res.out)
			}
			if !c.ok {
				w.contains(res, "LIVE_FRONT_HOST_LABEL")
				if strings.Contains(res.out, c.label) {
					t.Errorf("the refusal repeats the label:\n%s", res.out)
				}
			}
		})
	}
}

// The script's table of the rows that need the label is the rows that write a text to post, by the package's own rows
// and the hand-written list above.
func TestTheFrontLabelTableIsTheRowsThatWriteAText(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	got := block(t, string(b), "FRONT LABEL")
	var want []string
	for _, r := range append(alpha.Rows(), alpha.AccountingRows()...) {
		want = append(want, r.Name)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("the script's table is %v, the package's rows %v", got, want)
	}
	hand := append([]string(nil), labelledRows...)
	sort.Strings(hand)
	if strings.Join(got, ",") != strings.Join(hand, ",") {
		t.Errorf("the script's table is %v, the hand-written list %v", got, hand)
	}
}

// The label reaches the test of a row that posts it, exactly as set; a value the caller's shell holds never does, and a
// row that writes no text to post gets none.
func TestTheLabelReachesOnlyTheRowsThatPostIt(t *testing.T) {
	const inherited = "inherited from the caller, outside"
	for _, s := range alphaShapes {
		t.Run(s.row, func(t *testing.T) {
			w := newAlphaWorld(t)
			if res := w.run([]string{"HPCBURST_FRONT_HOST_LABEL=" + inherited}, "--run", s.row, "--yes"); res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			got := lines(w.stateFile("test-env-" + s.test))
			if !containsString(got, "HPCBURST_FRONT_HOST_LABEL="+exampleFrontLabel) {
				t.Errorf("the test did not get the label from the settings:\n%s", strings.Join(got, "\n"))
			}
			if containsString(got, "HPCBURST_FRONT_HOST_LABEL="+inherited) {
				t.Errorf("the caller's value reached the test")
			}
		})
	}
	t.Run("conformance-athena", func(t *testing.T) {
		w := newAlphaWorld(t)
		if res := w.run([]string{"HPCBURST_FRONT_HOST_LABEL=" + inherited}, "--run", "conformance-athena", "--yes"); res.code != 0 {
			t.Fatalf("exit %d:\n%s", res.code, res.out)
		}
		if env := w.stateFile("test-env-TestConformance"); strings.Contains(env, "HPCBURST_FRONT_HOST_LABEL") {
			t.Errorf("a label reached a row that writes no text to post:\n%s", env)
		}
	})
}
