package front

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigDefaultsAreHBEP5s(t *testing.T) {
	path := filepath.Join(t.TempDir(), "front.json")
	writeMode600(t, path, `{"dialHomeListen":"127.0.0.1:9000","clientKeysFile":"/k","usageDir":"/u"}`)
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	// HBEP-5, Parameters.
	want := map[string]time.Duration{
		"maxRequestDuration": 2 * time.Hour, "idleTimeout": 30 * time.Minute, "connectTimeout": 10 * time.Second,
		"upstreamIdleTimeout": 4 * time.Second, "clientKeepAlive": 120 * time.Second, "queueTimeout": 60 * time.Second,
		"pauseDuration": 30 * time.Second,
	}
	got := map[string]time.Duration{
		"maxRequestDuration": c.MaxRequestDuration.D(), "idleTimeout": c.IdleTimeout.D(), "connectTimeout": c.ConnectTimeout.D(),
		"upstreamIdleTimeout": c.UpstreamIdleTimeout.D(), "clientKeepAlive": c.ClientKeepAlive.D(), "queueTimeout": c.QueueTimeout.D(),
		"pauseDuration": c.PauseDuration.D(),
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %s, want %s", k, got[k], w)
		}
	}
	if c.ClientListen != "127.0.0.1:8000" || c.PauseAfterFailures != 3 || len(c.SessionHeaders) != 1 || c.SessionHeaders[0] != "x-session-id" {
		t.Errorf("clientListen %q, pauseAfterFailures %d, sessionHeaders %v", c.ClientListen, c.PauseAfterFailures, c.SessionHeaders)
	}
}

func TestConfigRefusesUnknownAndMissing(t *testing.T) {
	dir := t.TempDir()
	unknown := filepath.Join(dir, "u.json")
	writeMode600(t, unknown, `{"dialHomeListen":"127.0.0.1:9000","clientKeysFile":"/k","usageDir":"/u","idleTimout":"1m"}`)
	if _, err := LoadConfig(unknown); err == nil || !strings.Contains(err.Error(), "idleTimout") {
		t.Errorf("a misspelt setting must be refused, got %v", err)
	}
	missing := filepath.Join(dir, "m.json")
	writeMode600(t, missing, `{}`)
	_, err := LoadConfig(missing)
	// dialHomeListen is not among them: its default depends on the mode
	// (DialHomeAddr, tested in dialhome_test.go).
	for _, want := range []string{"clientKeysFile", "usageDir"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s not named in %v", want, err)
		}
	}
}

func TestClientKeysFile(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "keys.json")
	writeMode600(t, good, `{"keys":[{"id":"alice","key":"k-alice"},{"id":"bob","key":"k-bob"}]}`)
	ks, err := ReadClientKeys(good)
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := ks.Match("k-bob"); !ok || id != "bob" {
		t.Errorf("Match(k-bob) = %q, %v", id, ok)
	}
	if _, ok := ks.Match("k-bo"); ok {
		t.Error("a prefix of a key matched")
	}
	if _, ok := ks.Match(""); ok {
		t.Error("an empty key matched")
	}
	open := filepath.Join(dir, "open.json")
	writeMode600(t, open, `{"keys":[{"id":"a","key":"k"}]}`)
	if err := os.Chmod(open, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadClientKeys(open); err == nil || !strings.Contains(err.Error(), "0600") {
		t.Errorf("a group-readable keys file must be refused, got %v", err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(good, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadClientKeys(link); err == nil {
		t.Error("a symlinked keys file must be refused")
	}
	for name, body := range map[string]string{
		"dup id":  `{"keys":[{"id":"a","key":"k1"},{"id":"a","key":"k2"}]}`,
		"dup key": `{"keys":[{"id":"a","key":"k1"},{"id":"b","key":"k1"}]}`,
		"empty":   `{"keys":[]}`,
		"no key":  `{"keys":[{"id":"a","key":""}]}`,
	} {
		p := filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".json")
		writeMode600(t, p, body)
		if _, err := ReadClientKeys(p); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), "k1") {
			t.Errorf("%s: the error shows a key: %v", name, err)
		}
	}
}

// TestClientServerHasNoDeadlineBelowTheFronts: the client listener's server
// sets no read, write or total deadline, so only maxRequestDuration and
// idleTimeout bound a request; idle client connections are kept
// clientKeepAlive (HBEP-5, Time-outs and keep-alive).
func TestClientServerHasNoDeadlineBelowTheFronts(t *testing.T) {
	cfg := Config{}.WithDefaults()
	s := clientServer(cfg, nil)
	if s.ReadTimeout != 0 || s.WriteTimeout != 0 {
		t.Errorf("ReadTimeout %s, WriteTimeout %s: want none", s.ReadTimeout, s.WriteTimeout)
	}
	if s.IdleTimeout != 120*time.Second {
		t.Errorf("IdleTimeout %s, want clientKeepAlive 120s", s.IdleTimeout)
	}
	tr := upstreamTransport(nil, cfg, false)
	if tr.ResponseHeaderTimeout != 0 || tr.IdleConnTimeout != 4*time.Second || !tr.DisableCompression {
		t.Errorf("upstream transport: ResponseHeaderTimeout %s, IdleConnTimeout %s, DisableCompression %v",
			tr.ResponseHeaderTimeout, tr.IdleConnTimeout, tr.DisableCompression)
	}
}
