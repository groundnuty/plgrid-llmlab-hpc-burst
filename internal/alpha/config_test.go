package alpha

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func liveSettings(row string) map[string]string {
	r, _ := RowByName(row)
	m := map[string]string{
		envRow: row, envOut: "/private/out", envAccount: "SECRETACCT", envCreds: "/private/SECRETCREDS",
		envHostLock: "/locks/host.lock", envControlDir: "/ctl", envBinDir: "/bin-dir", envBuildDir: "/build-dir",
		envMode: "direct", envAddr: "gateway.example:29227", envPartition: r.Partition, envQOS: r.QOS,
		envGPUs: "1", envPending: "7200",
	}
	return m
}

func TestTheGateIsTheRowsName(t *testing.T) {
	_, skip, err := ConfigFromEnv(env(nil), "tunnel-hour-athena")
	if skip == "" || err != nil {
		t.Errorf("with no row named: skip %q, err %v, want a skip", skip, err)
	}
	_, skip, err = ConfigFromEnv(env(map[string]string{envRow: "alpha-athena-handover"}), "tunnel-hour-athena")
	if skip == "" || err != nil {
		t.Errorf("with another row named: skip %q, err %v, want a skip", skip, err)
	}
	_, skip, err = ConfigFromEnv(env(map[string]string{envRow: "no-such-row", envOut: "/o"}), "no-such-row")
	if skip != "" || err == nil {
		t.Errorf("with an unknown row served: skip %q, err %v, want an error", skip, err)
	}
}

func TestALiveRunWithItsSettingsHasAConfig(t *testing.T) {
	m := liveSettings("alpha-helios-nooverlap")
	delete(m, envQOS)
	if cfg, _, err := ConfigFromEnv(env(m), "alpha-helios-nooverlap"); err == nil {
		t.Fatalf("a QOS of the row's that the settings omit must be refused, got %+v", cfg)
	}
	m[envQOS] = "now"
	cfg, skip, err := ConfigFromEnv(env(m), "alpha-helios-nooverlap")
	if skip != "" || err != nil {
		t.Fatalf("skip %q, err %v", skip, err)
	}
	if cfg.Dry || cfg.PendingMax != 2*time.Hour || cfg.Row.Name != "alpha-helios-nooverlap" || cfg.Account != "SECRETACCT" {
		t.Errorf("config %+v", cfg)
	}
	if !strings.HasPrefix(cfg.Mode(), "LIVE RUN") {
		t.Errorf("mode %q", cfg.Mode())
	}
}

func TestEveryMissingSettingIsReportedAtOnce(t *testing.T) {
	_, _, err := ConfigFromEnv(env(map[string]string{envRow: "tunnel-hour-athena", envOut: "/o"}), "tunnel-hour-athena")
	if err == nil {
		t.Fatal("no error")
	}
	for _, k := range []string{envAccount, envCreds, envHostLock, envControlDir, envBinDir, envBuildDir, envMode, envAddr, envPartition, envGPUs, envPending} {
		if !strings.Contains(err.Error(), k+" is not set") {
			t.Errorf("the error does not name %s: %v", k, err)
		}
	}
}

func TestSettingsThatContradictTheRowAreAnError(t *testing.T) {
	for name, edit := range map[string]func(map[string]string){
		"partition":                 func(m map[string]string) { m[envPartition] = "plgrid" },
		"qos":                       func(m map[string]string) { m[envQOS] = "now" },
		"gpus":                      func(m map[string]string) { m[envGPUs] = "2" },
		"mode":                      func(m map[string]string) { m[envMode] = "tcp" },
		"ssh no key":                func(m map[string]string) { m[envMode] = "ssh" },
		"pending":                   func(m map[string]string) { m[envPending] = "0" },
		"relative out":              func(m map[string]string) { m[envOut] = "out" },
		"a dry fault in a live run": func(m map[string]string) { m[envDryFault] = "cut" },
	} {
		t.Run(name, func(t *testing.T) {
			m := liveSettings("tunnel-hour-athena")
			edit(m)
			_, _, err := ConfigFromEnv(env(m), "tunnel-hour-athena")
			if err == nil {
				t.Fatal("no error")
			}
			// An error names settings, never their values.
			for _, secret := range []string{"SECRETACCT", "SECRETCREDS", "gateway.example"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("the error holds a setting's value %q: %v", secret, err)
				}
			}
		})
	}
}

func TestADryRunNeedsNoLiveSettingAndRefusesAnyOfThem(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	cfg, skip, err := ConfigFromEnv(env(map[string]string{envRow: "tunnel-hour-athena", envOut: dir, envDry: "1", envDryFault: "short"}), "tunnel-hour-athena")
	if skip != "" || err != nil || !cfg.Dry || cfg.DryFault != "short" || !strings.HasPrefix(cfg.Mode(), "DRY RUN") {
		t.Fatalf("cfg %+v skip %q err %v", cfg, skip, err)
	}
	for _, k := range liveEnv {
		_, _, err := ConfigFromEnv(env(map[string]string{envRow: "tunnel-hour-athena", envOut: dir, envDry: "1", k: "/a/live/value"}), "tunnel-hour-athena")
		if err == nil || !strings.Contains(err.Error(), k) {
			t.Errorf("a dry run with %s set: err %v, want a refusal naming it", k, err)
		}
	}
}

func TestTagsAreWrittenDownBeforeTheyReturn(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	r, err := NewRecorder(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "pre-aaaa", "hb-", "hb-a b", "hb-a\nhb-b", "hb-" + strings.Repeat("a", 61)} {
		if err := r.Tag(bad); err == nil {
			t.Errorf("the tag %q was accepted", bad)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(dir, TagsFile)); len(b) != 0 {
		t.Errorf("a refused tag left %q in the file", b)
	}
	for _, good := range []string{"hb-pre-aaaaaaaaaaaa", "hb-suc-bbbbbbbbbbbb", "hb-" + strings.Repeat("a", 60)} {
		if err := r.Tag(good); err != nil {
			t.Fatal(err)
		}
		// Read back at once: the tag is on disk when Tag returns.
		b, _ := os.ReadFile(filepath.Join(dir, TagsFile))
		if !strings.HasSuffix(string(b), good+"\n") {
			t.Errorf("after Tag(%q) the file ends %q", good, b)
		}
		if !r.Tagged(good) {
			t.Errorf("%q is not Tagged", good)
		}
	}
	if fi, _ := os.Stat(filepath.Join(dir, TagsFile)); fi.Mode().Perm() != 0o600 {
		t.Errorf("tags file mode %v", fi.Mode().Perm())
	}
	if r.Tagged("hb-other-cccccccccccc") {
		t.Error("an unwritten tag is Tagged")
	}
}

func TestTheRecordDirectoryMustBePrivate(t *testing.T) {
	if _, err := NewRecorder("relative/dir"); err == nil {
		t.Error("a relative directory was accepted")
	}
	open := filepath.Join(t.TempDir(), "open")
	if err := os.Mkdir(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRecorder(open); err == nil {
		t.Error("a directory of mode 755 was accepted")
	}
	if fi, _ := os.Stat(open); fi.Mode().Perm() != 0o755 {
		t.Errorf("the run loosened or tightened a mode it did not set: %v", fi.Mode().Perm())
	}
	fresh := filepath.Join(t.TempDir(), "a", "b")
	if _, err := NewRecorder(fresh); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(fresh); fi.Mode().Perm() != 0o700 {
		t.Errorf("a new directory has mode %v", fi.Mode().Perm())
	}
}
