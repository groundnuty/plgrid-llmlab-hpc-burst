package alpha

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// A failed live run keeps the stack's own logs (issue #503). The state directory below has the names the launcher writes; what is kept is
// typed out: the controller's, the front's, the supervisor's and the frps logs, not k3s's or the audit log, and nothing that is not a
// log. Each file is mode 0600 in a directory of mode 0700, with the bytes it had, and a file that was there before, wider, is replaced.
func TestKeepStackLogsKeepsTheStacksOwnLogsPrivately(t *testing.T) {
	state, out := t.TempDir(), filepath.Join(t.TempDir(), "record")
	logs := filepath.Join(state, "logs")
	if err := os.MkdirAll(filepath.Join(logs, "sub.log"), 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{"controller.log": "controller line\n", "front.log": "front line\n", "supervisor.log": "supervisor line\n",
		"frps.log": "frps line\n", "frps-bulk.log": "bulk line\n", "k3s.log": "k3s line\n", "audit.log": "audit line\n", "notes.txt": "not a log\n"}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(logs, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A kept file from an earlier run, wider than it should be.
	if err := os.MkdirAll(filepath.Join(out, EvidenceDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, EvidenceDir, "front.log"), []byte("an older front line, longer than the new one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	kept, err := KeepStackLogs(state, out)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"controller.log", "front.log", "frps-bulk.log", "frps.log", "supervisor.log"}; !slices.Equal(kept, want) {
		t.Fatalf("kept %q, want %q", kept, want)
	}
	for _, name := range kept {
		path := filepath.Join(out, EvidenceDir, name)
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o600 {
			t.Errorf("%s has mode %o, want 600", name, fi.Mode().Perm())
		}
		if b, _ := os.ReadFile(path); string(b) != files[name] {
			t.Errorf("%s holds %q, want %q", name, b, files[name])
		}
	}
	for _, name := range []string{"k3s.log", "audit.log", "notes.txt", "sub.log"} {
		if _, err := os.Stat(filepath.Join(out, EvidenceDir, name)); err == nil {
			t.Errorf("%s was kept", name)
		}
	}
	// The directory this made is private. (The one that was there before keeps its own mode: it is the record directory's business.)
	made := filepath.Join(t.TempDir(), "fresh")
	if _, err := KeepStackLogs(state, made); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(made, EvidenceDir)); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("the evidence directory is %v (%v), want mode 700", fi, err)
	}
}

// A stack that never started has no logs directory: nothing is kept and nothing is wrong.
func TestKeepStackLogsOfAStackThatNeverStartedKeepsNothing(t *testing.T) {
	out := t.TempDir()
	kept, err := KeepStackLogs(filepath.Join(t.TempDir(), "never"), out)
	if err != nil || len(kept) != 0 {
		t.Errorf("kept %q, error %v; want nothing and no error", kept, err)
	}
	if _, err := os.Stat(filepath.Join(out, EvidenceDir)); err == nil {
		t.Error("an evidence directory was made for nothing")
	}
}

// keeperSteps count the times Run tells them to keep the evidence of a failed run.
type keeperSteps struct {
	*drySteps
	kept int
}

func (k *keeperSteps) KeepEvidence() { k.kept++ }

// brokenScenario is row A's steps whose scenario breaks: the run itself fails, with no verdict.
type brokenScenario struct{ *keeperSteps }

func (brokenScenario) NowServe(context.Context, Plan) (NowServeObs, error) {
	return NowServeObs{}, errors.New("the scenario broke")
}

// Run tells the steps to keep their evidence when the run failed, by a failed check or a broken scenario, and not for a pass or for a NOT
// RUN: a refused requeue is the case here, which is not a failure.
func TestRunTellsTheStepsToKeepTheEvidenceOfAFailedRunOnly(t *testing.T) {
	row := rowOf("alpha-athena-now-serve")
	for _, c := range []struct {
		name    string
		fault   string
		broken  bool
		want    int
		wantErr bool
		failed  bool
	}{
		{"a pass", "", false, 0, false, false},
		{"a failed check, the long stream cut", "cut", false, 1, false, true},
		{"a NOT RUN, the requeue refused", "requeue-refused", false, 0, true, false},
		{"a broken scenario", "", true, 1, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := Config{Row: row, Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			k := &keeperSteps{drySteps: newDrySteps(t, cfg, "nowserve")}
			var st Steps = k
			if c.broken {
				st = brokenScenario{k}
			}
			failures, err := Run(context.Background(), cfg, st, rec, t.Logf)
			if (err != nil) != c.wantErr || (len(failures) > 0) != c.failed {
				t.Fatalf("failures %v, err %v: not the run this case is for", failures, err)
			}
			if k.kept != c.want {
				t.Errorf("the steps were told to keep their evidence %d times, want %d", k.kept, c.want)
			}
		})
	}
}
