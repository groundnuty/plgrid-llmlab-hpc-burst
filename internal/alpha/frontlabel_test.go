package alpha

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testFrontLabel is a label of the shape a run states. It is written here, not in the package's code, so a test that
// finds it in a text to post has found what was passed in, not a constant the code writes by itself.
const testFrontLabel = "the development host, outside Cyfronet's network"

func TestAFrontLabelStatesARoleAndAPositionAndNothingThatNamesAHost(t *testing.T) {
	for _, c := range []struct {
		name, label string
		ok          bool
	}{
		{"a role outside", "the development host, outside Cyfronet's network", true},
		{"a role inside", "a Cyfronet-network host (inside the network)", true},
		{"the position in capitals", "Development host - OUTSIDE the network", true},
		{"empty", "", false},
		{"no position", "the development host", false},
		{"a host name", "dev1.example.org, outside the network", false},
		{"an IPv4 address", "192.0.2.1 outside", false},
		{"an IPv6 address", "2001:db8::1 outside", false},
		{"a URL", "https://front.example outside", false},
		{"a user and host", "me@front outside", false},
		{"a path", "/srv/front outside", false},
		{"a newline", "outside the network\nand more", false},
		{"a leading space", " outside the network", false},
		{"the longest", strings.Repeat("a", MaxFrontLabel-8) + " outside", true},
		{"one byte over", strings.Repeat("a", MaxFrontLabel-7) + " outside", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := ValidFrontLabel(c.label)
			if (err == nil) != c.ok {
				t.Fatalf("ValidFrontLabel(%q) = %v, want ok %t", c.label, err, c.ok)
			}
			if err != nil && c.label != "" && strings.Contains(err.Error(), c.label) {
				t.Errorf("the refusal repeats the label: %v", err)
			}
		})
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
