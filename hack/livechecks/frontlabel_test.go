package livechecks

// Where the front ran (issue #224). Every row that writes a text to post names the host role that ran the front and
// its network position, from the setting LIVE_FRONT_HOST_LABEL, and refuses to run without a valid one. The label is
// written here, in the test, and looked for in the text the row's own test wrote: it is not a constant the code
// writes by itself, so finding it proves the setting went through.

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// exampleFrontLabel is the label of the worlds' settings: a role, a position and a note, with an apostrophe, as a real one has.
const exampleFrontLabel = "the development host, outside Cyfronet's network (3.4 ms to an example login)"

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

// The script and the package hold the label to one form (PR 244, B1: a role from the fixed list, a position exactly inside
// or outside Cyfronet, an optional note of words and numbers with a unit). Each label is written out here with the verdict
// of both: the script's (a row run without --yes reaches the estimate, exit 4, or is refused, exit 3, naming the setting
// and the part that is wrong) and the package's validator. The refusal of each part has its own cases, behind a valid role
// and position where the part is the note, so that a check that stops working is caught by its own case. The key phrases
// are the same in both: begin with a role, inside or outside Cyfronet, may hold only, longer than 120 bytes.
func TestTheFrontLabelIsHeldToOneFormByTheScriptAndThePackage(t *testing.T) {
	const (
		role, pos       = "the development host", "outside Cyfronet"
		noRole, noPos   = "begin with a role", "inside or outside Cyfronet"
		noNote, tooLong = "may hold only", "longer than 120 bytes"
		stem            = "the development host, outside Cyfronet"
	)
	// 120 and 121 bytes, each valid in every other respect: a role, a position and a note of letters only.
	long := stem + " (" + strings.Repeat("a", 120-len(stem)-3) + ")"
	over := stem + " (" + strings.Repeat("a", 120-len(stem)-2) + ")"
	for _, c := range []struct {
		name, label, want string // want is a key phrase of the refusal, "" for a label both accept
	}{
		{"a role outside", "the development host, outside Cyfronet's network", ""},
		{"a role inside", "a Cyfronet-network host, inside Cyfronet's network", ""},
		{"the shorter position", "a cloud host, inside Cyfronet", ""},
		{"the science agent's example", "the development host, outside Cyfronet (3.4 ms to the Helios login, behind a gateway)", ""},
		{"a rate in the note", "the development host, outside Cyfronet's network (40 MB/s, 5% loss)", ""},
		{"each unit of a number", "the development host, outside Cyfronet (3.4 ms; 2 s; 40 Mb/s; 7%; 7 %)", ""},
		{"an apostrophe and a hyphen in the note", "a cloud host, outside Cyfronet (behind the owner's gateway; a two-hop path)", ""},
		{"the longest", long, ""},
		{"one byte over", over, tooLong},
		{"inside alone", "inside", noRole},
		{"a short host name and a position", "gw-01, outside", noRole},
		{"a short host name joined to the position", "gw-01-outside", noRole},
		{"a host name in the role's place", "dev1.example.org, outside Cyfronet", noRole},
		{"a position and a note, no role", "outside Cyfronet (gw.example)", noRole},
		{"an address in a note, no role", "outside (10.0.0.1)", noRole},
		{"a word with a digit in a note, no role", "outside (node7)", noRole},
		{"a role not on the list", "the other host, outside Cyfronet", noRole},
		{"the role without its comma", "the development host outside Cyfronet", noRole},
		{"a role alone", "the development host", noRole},
		{"a leading space", " the development host, outside Cyfronet", noRole},
		{"a position inside a word", "the development host, the outsider host", noPos},
		{"a position that is the end of a word", "the development host, xoutside Cyfronet", noPos},
		{"a note that is not closed", "the development host, outside Cyfronet (a gateway", noPos},
		{"text after the note", "the development host, outside Cyfronet (a gateway) more", noPos},
		{"a role and a comma", "the development host, ", noPos},
		{"no position", "the development host, a few hops away", noPos},
		{"a capital position", "the development host, OUTSIDE Cyfronet", noPos},
		{"a position joined to a digit", "the development host, inside2 Cyfronet", noPos},
		{"the position without Cyfronet", "the development host, outside", noPos},
		{"another network", "the development host, outside Example's network", noPos},
		{"a note with no position", "the development host, (outside Cyfronet)", noPos},
		{"a second parenthesis", role + ", " + pos + " (a) (b)", noNote},
		{"a dotted name", role + ", " + pos + " (gw.example)", noNote},
		{"an address", role + ", " + pos + " (10.0.0.1)", noNote},
		{"a name with a digit", role + ", " + pos + " (node7)", noNote},
		{"a hyphenated name with digits", role + ", " + pos + " (gw-01)", noNote},
		{"a digit after a word", role + ", " + pos + " (behind gw1)", noNote},
		{"a colon", role + ", " + pos + " (host: front)", noNote},
		{"an IPv6 address", role + ", " + pos + " (2001:db8::1)", noNote},
		{"a bare number", role + ", " + pos + " (7)", noNote},
		{"a number with a unit it is not given", role + ", " + pos + " (3.4 parsecs)", noNote},
		{"a dotted number with no unit", role + ", " + pos + " (3.4)", noNote},
		{"an empty note", role + ", " + pos + " ()", noNote},
		{"a slash", role + ", " + pos + " (a/b)", noNote},
		{"an at sign", role + ", " + pos + " (me@front)", noNote},
		{"a URL", role + ", " + pos + " (https://front.example)", noNote},
		{"a backslash", role + ", " + pos + ` (a\b)`, noNote},
		{"a dollar sign", role + ", " + pos + " (a $HOME b)", noNote},
		// A letter the bash range [A-Za-z] could take in another locale: both must refuse it.
		{"a non-ASCII letter", role + ", " + pos + " (caf\u00e9)", noNote},
		{"a trailing comma in the note", role + ", " + pos + " (a gateway,)", noNote},
		{"a word with a digit and a unit run together", role + ", " + pos + " (gw1ms)", noNote},
	} {
		t.Run(c.name, func(t *testing.T) {
			pkgErr := alpha.ValidFrontLabel(c.label)
			if (pkgErr == nil) != (c.want == "") || (pkgErr != nil && !strings.Contains(pkgErr.Error(), c.want)) {
				t.Errorf("the package's verdict on %q is %v, want a refusal that says %q (empty: none)", c.label, pkgErr, c.want)
			}
			w := newAlphaWorld(t)
			w.alphaSettings(func(m map[string]string) { m["LIVE_FRONT_HOST_LABEL"] = c.label })
			res := w.run(nil, "--run", "tunnel-hour-athena")
			want := 4
			if c.want != "" {
				want = 3
			}
			if res.code != want {
				t.Fatalf("the script's exit on %q is %d, want %d:\n%s", c.label, res.code, want, res.out)
			}
			if c.want != "" {
				w.contains(res, "LIVE_FRONT_HOST_LABEL", c.want)
				// The refusal names the roles that are allowed, so a label that is one of them is in it; nothing else may be.
				if !slices.Contains(alpha.FrontRoles, c.label) && strings.Contains(res.out, c.label) {
					t.Errorf("the refusal repeats the label:\n%s", res.out)
				}
			}
		})
	}
}

// The roles the script allows are the package's: a role added in one place and not the other would be a label one of them
// refuses. Both lists are read here, the script's from its table.
func TestTheFrontRolesTableIsThePackagesList(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	got := block(t, string(b), "FRONT ROLES")
	if strings.Join(got, "|") != strings.Join(alpha.FrontRoles, "|") {
		t.Errorf("the script's roles are %v, the package's %v", got, alpha.FrontRoles)
	}
	// A role is a thing a note's words cannot be mistaken for, and it ends no label by itself.
	for _, r := range got {
		if strings.ContainsAny(r, "(),;:@/\\.0123456789") {
			t.Errorf("the role %q holds a character a host name or an address needs", r)
		}
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
	// The science agent's ruling 1 (2 Nov 2026): no label on the reach, conformance or real-engine rows. They do not
	// measure through the front (the reach rows have LIVE_PROBE_LABEL). The script gives them none, whatever the caller's
	// shell holds, and it does not refuse them for want of one.
	for _, row := range []string{"conformance-athena", "conformance-helios-gh200", "reach-helios", "reach-athena", "real-engine"} {
		t.Run(row, func(t *testing.T) {
			var w *world
			if row == "real-engine" || strings.HasPrefix(row, "conformance-") {
				w = newWorld(t)
				w.useStubGo()
			} else {
				w = newReachWorld(t)
				w.reachSettings(func(m map[string]string) { delete(m, "LIVE_FRONT_HOST_LABEL") })
			}
			res := w.run([]string{"HPCBURST_FRONT_HOST_LABEL=" + inherited}, "--run", row, "--yes")
			if res.code != 0 {
				t.Fatalf("%s with no label set: exit %d, want 0:\n%s", row, res.code, res.out)
			}
			files, _ := filepath.Glob(filepath.Join(w.state, "test-env-*"))
			for _, f := range files {
				if b, _ := os.ReadFile(f); strings.Contains(string(b), "HPCBURST_FRONT_HOST_LABEL") {
					t.Errorf("a label reached %s, a row that writes no text through the front:\n%s", filepath.Base(f), b)
				}
			}
		})
	}
}
