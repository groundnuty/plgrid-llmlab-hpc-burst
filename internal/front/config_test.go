package front

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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
	_, err := LoadConfig(unknown)
	if err == nil || !names(err, unknown, "idleTimout") {
		t.Errorf("a misspelt setting must be refused, got %v", err)
	}
	missing := filepath.Join(dir, "m.json")
	writeMode600(t, missing, `{}`)
	_, err = LoadConfig(missing)
	// dialHomeListen is not among them: its default depends on the mode
	// (DialHomeAddr, tested in dialhome_test.go).
	for _, want := range []string{"clientKeysFile", "usageDir"} {
		if err == nil || !names(err, missing, want) {
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
	if _, err := ReadClientKeys(open); err == nil || !names(err, open, "0600") {
		t.Errorf("a group-readable keys file must be refused, got %v", err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(good, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadClientKeys(link); err == nil {
		t.Error("a symlinked keys file must be refused")
	}
	// The invalid files are in a directory called k1, the first key below, so the path of every
	// file holds a key's text whatever TMPDIR is: an error names its file, and that must not look
	// like a key shown (issue #209). The check takes the path out of the error first.
	secretDir := filepath.Join(dir, "k1")
	if err := os.Mkdir(secretDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"dup id":  `{"keys":[{"id":"a","key":"k1"},{"id":"a","key":"k2"}]}`,
		"dup key": `{"keys":[{"id":"a","key":"k1"},{"id":"b","key":"k1"}]}`,
		"empty":   `{"keys":[]}`,
		"no key":  `{"keys":[{"id":"a","key":""}]}`,
	} {
		p := filepath.Join(secretDir, strings.ReplaceAll(name, " ", "-")+".json")
		writeMode600(t, p, body)
		_, err := ReadClientKeys(p)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if shown := secretsShownBy(err, p, "k1", "k2"); len(shown) > 0 {
			t.Errorf("%s: the error shows the key %q: %v", name, shown, err)
		}
	}
}

// withoutPath is err's text with the path of the file it names taken out, as the path is written
// and as Go quotes it (%q escapes a quote, a backslash and a newline). The path comes from TMPDIR
// and the test's directory names, which can hold any string: a key's text, a field's name, a mode.
// It is not evidence about what the error says (issue #209).
func withoutPath(err error, path string) string {
	text := strings.ReplaceAll(err.Error(), strconv.Quote(path), "")
	return strings.ReplaceAll(text, path, "")
}

// names says whether err says want (a field, a setting, a mode) apart from the file's path.
func names(err error, path, want string) bool {
	return err != nil && strings.Contains(withoutPath(err, path), want)
}

// secretsShownBy returns the secrets that err's text shows apart from the file's path.
func secretsShownBy(err error, path string, secrets ...string) []string {
	text := withoutPath(err, path)
	var shown []string
	for _, s := range secrets {
		if strings.Contains(text, s) {
			shown = append(shown, s)
		}
	}
	return shown
}

// The control for the check above: a path with a secret's text in it is not a leak, and a key in
// the message is, in either form the path can take. The escaped path has a quote, a backslash and
// a newline, which %q writes differently from the path itself, so only removing the quoted form
// takes it out.
func TestSecretsShownByIgnoresThePathAndSeesAKey(t *testing.T) {
	plain := "/tmp/k195/k1/dup-key.json"
	escaped := "/tmp/k195/k1/a\"b\\c\nd.json"
	for _, tc := range []struct {
		name string
		path string
		err  string
		want []string
	}{
		{"the path alone", plain, "client keys file " + plain + " holds no key", nil},
		{"the path quoted", plain, "open " + strconv.Quote(plain) + ": no such file", nil},
		{"a key beside the path", plain, "client keys file " + plain + `: entry "a" repeats the key k1`, []string{"k1"}},
		{"another key beside the path", plain, "client keys file " + plain + `: the key is k2`, []string{"k2"}},
		{"a key and no path", plain, `entry "a" repeats the key k1`, []string{"k1"}},
		{"an escaped path quoted", escaped, "open " + strconv.Quote(escaped) + ": no such file", nil},
		{"an escaped path quoted and another key", escaped, "open " + strconv.Quote(escaped) + ": entry repeats the key k2", []string{"k2"}},
		{"an escaped path quoted and the key its path holds", escaped, "open " + strconv.Quote(escaped) + ": entry repeats the key k1", []string{"k1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := secretsShownBy(errors.New(tc.err), tc.path, "k1", "k2")
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("secretsShownBy(%q) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// The same for the diagnostics the other tests look for (issue #209, review of PR 218): a field's
// name, a setting or a mode in the path is not the error naming it.
func TestNamesIgnoresThePath(t *testing.T) {
	path := "/tmp/idleTimout-0600/clientKeysFile/usageDir/x.json"
	silent := errors.New("front configuration " + path + ": unreadable")
	said := errors.New("front configuration " + path + `: json: unknown field "idleTimout"`)
	for _, w := range []string{"idleTimout", "0600", "clientKeysFile", "usageDir"} {
		if names(silent, path, w) {
			t.Errorf("an error that says nothing but the path is taken to name %q", w)
		}
	}
	if !names(said, path, "idleTimout") {
		t.Error("an error that names the unknown field is not taken to name it")
	}
	if names(said, path, "0600") || names(nil, path, "idleTimout") {
		t.Error("names accepts a diagnostic the error lacks, or a nil error")
	}
}

// TestClientServerHasNoDeadlineBelowTheFronts: the client listener's server
// sets no read, write or total deadline, so only maxRequestDuration and
// idleTimeout bound a request; idle client connections are kept
// clientKeepAlive (HBEP-5, Time-outs and keep-alive).
func TestClientServerHasNoDeadlineBelowTheFronts(t *testing.T) {
	cfg := Config{}.WithDefaults()
	s := clientServer(cfg, nil, context.Background())
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

// TestConfigTunnelSettings: HBEP-5's defaults for the tunnel's set-up and
// probe (Parameters), frps's addresses from the launcher's port base
// (base + 9 and base + 10) unless set, and the refusals.
func TestConfigTunnelSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "front.json")
	writeMode600(t, path, `{"clientKeysFile":"/k","usageDir":"/u"}`)
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.TunnelSetupTimeout.D() != 5*time.Second || c.ProbeInterval.D() != 10*time.Second || c.ProbeTimeout.D() != 10*time.Second || c.ProbeFailures != 2 {
		t.Errorf("tunnelSetupTimeout %s, probeInterval %s, probeTimeout %s, probeFailures %d; want 5 s, 10 s, 10 s, 2",
			c.TunnelSetupTimeout.D(), c.ProbeInterval.D(), c.ProbeTimeout.D(), c.ProbeFailures)
	}
	if c.ConnectTimeout.D() != 10*time.Second {
		t.Errorf("connectTimeout %s, want 10 s for LLMLab", c.ConnectTimeout.D())
	}
	host := agentapi.Host{PortBase: 29220}
	if bind, mux, err := c.FrpsAddrs(host); err != nil || bind != "127.0.0.1:29229" || mux != "127.0.0.1:29230" {
		t.Errorf("FrpsAddrs = %q, %q, %v; want 127.0.0.1:29229 and 127.0.0.1:29230", bind, mux, err)
	}
	if _, _, err := c.FrpsAddrs(agentapi.Host{}); err == nil {
		t.Error("frps's addresses with no port base and none set: accepted")
	}
	c.FrpsBindAddr, c.FrpsMuxAddr = "127.0.0.1:7001", "127.0.0.1:7002"
	if bind, mux, err := c.FrpsAddrs(agentapi.Host{}); err != nil || bind != c.FrpsBindAddr || mux != c.FrpsMuxAddr {
		t.Errorf("set addresses: %q, %q, %v; want them as given", bind, mux, err)
	}
	bad := filepath.Join(t.TempDir(), "bad.json")
	writeMode600(t, bad, `{"clientKeysFile":"/k","usageDir":"/u","tunnelSetupTimeout":"-1s","probeFailures":-1,"frpsMuxAddr":"nowhere"}`)
	_, err = LoadConfig(bad)
	for _, want := range []string{"tunnelSetupTimeout", "probeFailures", "frpsMuxAddr"} {
		if err == nil || !names(err, bad, want) {
			t.Errorf("a bad %s not named in %v", want, err)
		}
	}
}
