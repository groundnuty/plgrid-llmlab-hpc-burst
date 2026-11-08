package alpha

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// testFrontLabel is a label of the shape a run states. It is written here, not in the package's code, so a test that
// finds it in a text to post has found what was passed in, not a constant the code writes by itself.
const testFrontLabel = "the development host, outside Cyfronet's network"

// The label is a role from the fixed list, a position that is exactly inside or outside Cyfronet, and an optional note
// (PR 244, B1). Each case names the part it is about; a refusal says which part is wrong, in the words in want, and never
// repeats the label.
func TestAFrontLabelStatesARoleAndAPositionAndNothingThatNamesAHost(t *testing.T) {
	const (
		role, pos       = "the development host", "outside Cyfronet"
		noRole, noPos   = "begin with a role", "inside or outside Cyfronet"
		noNote, tooLong = "may hold only", "longer than 120 bytes"
		unset, okLabel  = "is not set", ""
		stem            = "the development host, outside Cyfronet"
	)
	// 120 and 121 bytes, each valid in every other respect: a role, a position and a note of letters only.
	atLimit := stem + " (" + strings.Repeat("a", MaxFrontLabel-len(stem)-3) + ")"
	for _, c := range []struct {
		name, label, want string // want is a phrase of the refusal, "" for a label that is accepted
	}{
		// What is accepted.
		{"a role outside", "the development host, outside Cyfronet's network", okLabel},
		{"a role inside", "a Cyfronet-network host, inside Cyfronet's network", okLabel},
		{"the shorter position", "a cloud host, inside Cyfronet", okLabel},
		{"the science agent's example", "the development host, outside Cyfronet (3.4 ms to the Helios login, behind a gateway)", okLabel},
		{"a rate in the note", "the development host, outside Cyfronet's network (40 MB/s, 5% loss)", okLabel},
		{"each unit of a number", "the development host, outside Cyfronet (3.4 ms; 2 s; 40 Mb/s; 7%; 7 %)", okLabel},
		{"an apostrophe and a hyphen in the note", "a cloud host, outside Cyfronet (behind the owner's gateway; a two-hop path)", okLabel},
		{"the longest", atLimit, okLabel},
		// No role from the list.
		{"inside alone", "inside", noRole},
		{"a short host name and a position", "gw-01, outside", noRole},
		{"a short host name joined to the position", "gw-01-outside", noRole},
		{"a host name in the role's place", "dev1.example.org, outside Cyfronet", noRole},
		{"a position and a note, no role", "outside Cyfronet (gw.example)", noRole},
		{"an address in a note, no role", "outside (10.0.0.1)", noRole},
		{"a word with a digit in a note, no role", "outside (node7)", noRole},
		{"a role not on the list", "the other host, outside Cyfronet", noRole},
		{"the role without its comma", "the development host outside Cyfronet", noRole},
		{"a leading space", " the development host, outside Cyfronet", noRole},
		{"a role alone", "the development host", noRole},
		// No position, or not exactly inside or outside Cyfronet.
		{"a newline", "the development host, outside Cyfronet\nand more", noPos},
		{"a role and a comma", "the development host, ", noPos},
		{"no position", "the development host, a few hops away", noPos},
		{"a capital position", "the development host, OUTSIDE Cyfronet", noPos},
		{"a position that is the end of a word", "the development host, xoutside Cyfronet", noPos},
		{"a note that is not closed", "the development host, outside Cyfronet (a gateway", noPos},
		{"text after the note", "the development host, outside Cyfronet (a gateway) more", noPos},
		{"a position joined to a digit", "the development host, inside2 Cyfronet", noPos},
		{"the position without Cyfronet", "the development host, outside", noPos},
		{"another network", "the development host, outside Example's network", noPos},
		{"a note with no position", "the development host, (outside Cyfronet)", noPos},
		// The note: each refusal behind a valid role and position, so that only the note can be at fault.
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
		{"a non-ASCII letter", role + ", " + pos + " (caf\u00e9)", noNote},
		{"a trailing comma in the note", role + ", " + pos + " (a gateway,)", noNote},
		{"a word with a digit and a unit run together", role + ", " + pos + " (gw1ms)", noNote},
		// The whole.
		{"empty", "", unset},
		{"one byte over", stem + " (" + strings.Repeat("a", MaxFrontLabel-len(stem)-2) + ")", tooLong},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := ValidFrontLabel(c.label)
			if c.want == "" {
				if err != nil {
					t.Fatalf("ValidFrontLabel(%q) = %v, want it accepted", c.label, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("ValidFrontLabel(%q) = %v, want a refusal that says %q", c.label, err, c.want)
			}
			// The refusal names the roles that are allowed, so a label that is one of them is in it; nothing else may be.
			if c.label != "" && !slices.Contains(FrontRoles, c.label) && strings.Contains(err.Error(), c.label) {
				t.Errorf("the refusal repeats the label: %v", err)
			}
		})
	}
	if len(atLimit) != MaxFrontLabel {
		t.Errorf("the longest label is %d bytes, want %d", len(atLimit), MaxFrontLabel)
	}
}

// The line goes right after the verdict line, whatever the row's text is, and the text keeps every other line.
func TestTheFrontLineSitsAfterTheVerdictLine(t *testing.T) {
	got := InsertFrontLine("row: PASS (posts to #145)\n- a\n- b\n", testFrontLabel)
	want := "row: PASS (posts to #145)\n- front ran on: " + testFrontLabel + "\n- a\n- b\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := InsertFrontLine("row: PASS", testFrontLabel); got != "row: PASS\n- front ran on: "+testFrontLabel+"\n" {
		t.Errorf("a one-line text: %q", got)
	}
}

// A run with no usable label is refused before the scenario starts, and the settings are refused too: dry or live.
func TestARunWithNoFrontLabelIsRefused(t *testing.T) {
	row := rowOf("tunnel-hour-athena")
	for _, label := range []string{"", "the development host", "dev1.example.org outside"} {
		cfg := Config{Row: row, Dry: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: label}
		rec, err := NewRecorder(cfg.Out)
		if err != nil {
			t.Fatal(err)
		}
		st := &closing{notWired: notWired{}}
		if _, err := Run(context.Background(), cfg, st, rec, t.Logf); err == nil || !strings.Contains(err.Error(), "front host label") {
			t.Errorf("label %q: err = %v, want a refusal that names the front host label", label, err)
		}
		if !st.closed {
			t.Errorf("label %q: the steps were not closed", label)
		}
		if _, err := os.Stat(filepath.Join(cfg.Out, PostsFile)); err == nil {
			t.Errorf("label %q: a text to post was written", label)
		}
	}
	// The settings: a dry run needs the label as a live run does.
	for _, dry := range []string{"1", ""} {
		m := liveSettings("tunnel-hour-athena")
		delete(m, EnvFrontLabel)
		if dry == "1" {
			m[envDry] = "1"
			for _, k := range liveEnv {
				delete(m, k)
			}
		}
		if _, _, err := ConfigFromEnv(env(m), "tunnel-hour-athena"); err == nil || !strings.Contains(err.Error(), EnvFrontLabel) {
			t.Errorf("dry %q: err = %v, want a refusal that names %s", dry, err, EnvFrontLabel)
		}
	}
	cfg, _, err := ConfigFromEnv(env(liveSettings("tunnel-hour-athena")), "tunnel-hour-athena")
	if err != nil || cfg.FrontLabel != testFrontLabel {
		t.Errorf("with the label set: err %v, label %q", err, cfg.FrontLabel)
	}
}

// Every row's text to post names where the front ran, on its second line, with the label the run was given.
func TestEveryRowsTextNamesWhereTheFrontRan(t *testing.T) {
	for _, row := range Rows() {
		t.Run(row.Name, func(t *testing.T) {
			cfg := Config{Row: row, Dry: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			if f, err := Run(context.Background(), cfg, newDrySteps(t, cfg, familyOf(row.Name)), rec, t.Logf); err != nil || len(f) != 0 {
				t.Fatalf("failures %v, err %v", f, err)
			}
			posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			lines := strings.Split(string(posts), "\n")
			if len(lines) < 3 || lines[1] != "- front ran on: "+testFrontLabel {
				t.Errorf("the second line of the text to post is not the front line:\n%s", posts)
			}
			rec2, _ := os.ReadFile(filepath.Join(cfg.Out, RecordFile))
			if !strings.Contains(string(rec2), `"frontLabel": "`+testFrontLabel+`"`) {
				t.Errorf("the record lacks the label:\n%s", rec2)
			}
		})
	}
}
