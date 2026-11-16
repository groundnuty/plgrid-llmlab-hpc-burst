package node

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// parseConfErr parses a configuration text the way the process does, with nats-server's own parser.
func parseConfErr(t *testing.T, text string) (*server.Options, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return server.ProcessConfigFile(path)
}

func parseConf(t *testing.T, text string) *server.Options {
	t.Helper()
	opts, err := parseConfErr(t, text)
	if err != nil {
		t.Fatalf("nats-server does not parse %q: %v", text, err)
	}
	return opts
}

// goodConf is written out by hand, not rendered by the launcher's ConfText: the expectation must not
// come from the code that writes the file.
const goodConf = `listen: 127.0.0.1:28500
http: 127.0.0.1:28501
authorization { token: "aa" }
jetstream {
  store_dir: "/run/st/eventlog"
  sync_interval: always
}
`

func TestCheckParsedAcceptsWhatTheLauncherWrites(t *testing.T) {
	if diffs := CheckParsed(parseConf(t, goodConf), "/run/st/eventlog"); len(diffs) != 0 {
		t.Fatalf("a good configuration was refused: %v", diffs)
	}
}

// Each row changes one thing in the good text, and what nats-server parsed then differs: the check
// reads those options, so a setting nats-server did not take is found (S8, Finding 8). A misspelt
// key is another case: nats-server v2.15.2's parser refuses it by name, at the top level and inside
// the jetstream block (the process reports that as a refusal too, which the process tests show), so
// the read-back is for what parses but is not what HBEP-8 says: a value, a place, a missing block.
func TestCheckParsedNamesEachSettingThatIsNotAsParsed(t *testing.T) {
	for name, c := range map[string]struct {
		from, to string
		store    string
		want     string
		parseErr string // the row is one nats-server's parser refuses, by name
	}{
		"sync_interval with a time":    {from: "sync_interval: always", to: "sync_interval: 30s", want: "sync_interval"},
		"a store elsewhere":            {from: `store_dir: "/run/st/eventlog"`, to: `store_dir: "/tmp/elsewhere"`, want: "store_dir"},
		"a store the launcher moved":   {store: "/run/other/eventlog", want: "store_dir"},
		"no token":                     {from: `authorization { token: "aa" }`, to: "", want: "token"},
		"listening on every interface": {from: "listen: 127.0.0.1:28500", to: "listen: 0.0.0.0:28500", want: "client listener"},
		"no listen at all":             {from: "listen: 127.0.0.1:28500", to: "", want: "client listener"},
		"monitoring on every address":  {from: "http: 127.0.0.1:28501", to: "http: 0.0.0.0:28501", want: "monitoring listener"},
		"no monitoring":                {from: "http: 127.0.0.1:28501", to: "", want: "monitoring listener"},
		"jetstream misspelt":           {from: "jetstream {", to: "jetstreem {", parseErr: "jetstreem"},
		"a misspelt sync_interval":     {from: "sync_interval: always", to: "sync_intervall: always", parseErr: "sync_intervall"},
		"a misspelt store_dir":         {from: "store_dir:", to: "store_dirr:", parseErr: "store_dirr"},
		"a misspelt authorization":     {from: "authorization {", to: "authorisation {", parseErr: "authorisation"},
		"a misspelt http":              {from: "http: 127.0.0.1:28501", to: "htttp: 127.0.0.1:28501", parseErr: "htttp"},
	} {
		text := goodConf
		if c.from != "" {
			if !strings.Contains(text, c.from) {
				t.Fatalf("%s: the good text has no %q", name, c.from)
			}
			text = strings.Replace(text, c.from, c.to, 1)
		}
		opts, err := parseConfErr(t, text)
		if c.parseErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.parseErr) {
				t.Errorf("%s: the parser said %v, want an error naming %q", name, err, c.parseErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: nats-server does not parse the text: %v", name, err)
			continue
		}
		diffs := CheckParsed(opts, cmpOr(c.store, "/run/st/eventlog"))
		if !slices.ContainsFunc(diffs, func(d string) bool { return strings.Contains(d, c.want) }) {
			t.Errorf("%s: %v, want a difference naming %q", name, diffs, c.want)
		}
	}
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func TestCheckParsedListsEveryDifferenceAtOnce(t *testing.T) {
	diffs := CheckParsed(parseConf(t, "port: 4222\n"), "/run/st/eventlog")
	for _, want := range []string{"sync_interval", "jetstream", "store_dir", "client listener", "monitoring listener", "token"} {
		if !slices.ContainsFunc(diffs, func(d string) bool { return strings.Contains(d, want) }) {
			t.Errorf("a bare configuration: %v, want a difference naming %q", diffs, want)
		}
	}
}

func goodEvents() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: "HB_EVENTS", Subjects: []string{"hb.ev.>"}, Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
		Replicas: 1, Discard: jetstream.DiscardNew, Duplicates: 2 * time.Minute, AllowDirect: true,
		Compression: jetstream.S2Compression, MaxBytes: 2147483648, MaxMsgs: -1, MaxMsgsPerSubject: -1,
	}
}

// The wanted configuration is spelt out here from HBEP-8 (Settings, Streams, Retention), not taken
// from wantConfig: a stream built from the same function would agree with it whatever it said.
func TestWantConfigIsHBEP8sFortheTwoStreams(t *testing.T) {
	caps := eventlog.Caps{Events: 1000, Usage: 2000}
	ev := wantConfig("HB_EVENTS", caps)
	if diffs := DiffStream(goodEvents(), ev); len(diffs) == 0 {
		t.Fatal("a stream with a 2 GiB cap passed for a 1000-byte cap")
	}
	want := goodEvents()
	want.MaxBytes = 1000
	if diffs := DiffStream(want, ev); len(diffs) != 0 {
		t.Fatalf("HB_EVENTS: %v", diffs)
	}
	us := goodEvents()
	us.Name, us.Subjects, us.Discard, us.MaxBytes = "HB_USAGE", []string{"hb.usage"}, jetstream.DiscardOld, 2000
	if diffs := DiffStream(us, wantConfig("HB_USAGE", caps)); len(diffs) != 0 {
		t.Fatalf("HB_USAGE: %v", diffs)
	}
}

func TestDiffStreamNamesEachSetting(t *testing.T) {
	for name, c := range map[string]struct {
		change func(*jetstream.StreamConfig)
		want   string
	}{
		"subjects":          {func(c *jetstream.StreamConfig) { c.Subjects = []string{"hb.>"} }, "subjects"},
		"memory storage":    {func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage }, "storage"},
		"three replicas":    {func(c *jetstream.StreamConfig) { c.Replicas = 3 }, "replicas"},
		"interest":          {func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy }, "retention"},
		"discard old":       {func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld }, "discard"},
		"duplicate window":  {func(c *jetstream.StreamConfig) { c.Duplicates = time.Hour }, "duplicate window"},
		"no direct get":     {func(c *jetstream.StreamConfig) { c.AllowDirect = false }, "allow_direct"},
		"no compression":    {func(c *jetstream.StreamConfig) { c.Compression = jetstream.NoCompression }, "compression"},
		"another cap":       {func(c *jetstream.StreamConfig) { c.MaxBytes = 5 }, "max_bytes"},
		"a max age":         {func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour }, "max_age"},
		"a message limit":   {func(c *jetstream.StreamConfig) { c.MaxMsgs = 10 }, "max_msgs"},
		"a per-subject cap": {func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 }, "max_msgs_per_subject"},
		"sealed":            {func(c *jetstream.StreamConfig) { c.Sealed = true }, "sealed"},
	} {
		got := goodEvents()
		c.change(&got)
		diffs := DiffStream(got, goodEvents())
		if len(diffs) != 1 || !strings.Contains(diffs[0], c.want) || !strings.Contains(diffs[0], "HB_EVENTS") {
			t.Errorf("%s: %v, want exactly one difference naming %q and the stream", name, diffs, c.want)
		}
	}
	// Unlimited is spelt -1 by nats-server and 0 by an unset field: both are no limit.
	got := goodEvents()
	got.MaxMsgs, got.MaxMsgsPerSubject = 0, 0
	if diffs := DiffStream(got, goodEvents()); len(diffs) != 0 {
		t.Errorf("an unset message limit is a difference: %v", diffs)
	}
}

func TestDiffStreamListsEveryDifferenceAtOnce(t *testing.T) {
	got := goodEvents()
	got.Discard, got.Compression, got.MaxAge = jetstream.DiscardOld, jetstream.NoCompression, time.Hour
	if diffs := DiffStream(got, goodEvents()); len(diffs) != 3 {
		t.Fatalf("three settings differ, %d differences: %v", len(diffs), diffs)
	}
}
