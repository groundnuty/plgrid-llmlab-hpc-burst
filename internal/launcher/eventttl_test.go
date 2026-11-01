package launcher

// Tests of the event TTL (HBEP-8, Retention; issue #109). The expected values are literals, never
// the constants the code uses. The tests that show a real API server honouring the argument, and an
// upgrade bringing it to a host that lacks it, are in eventttl_k3s_test.go.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// ttlArgs returns the values of the event-ttl arguments in a k3s argument list.
func ttlArgs(args []string) []string {
	var out []string
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, "--kube-apiserver-arg=event-ttl="); ok {
			out = append(out, v)
		}
	}
	return out
}

// TestK3sArgsEventTTL: the argument is passed once, as 168h by default, as the test sets it, and not
// at all for a host started the way the releases before #109 did. This looks at the arguments only;
// it is no proof that the API server honours them (eventttl_k3s_test.go is).
func TestK3sArgsEventTTL(t *testing.T) {
	s := testSpec(t, 0)
	if got := ttlArgs(s.k3sArgs()); len(got) != 1 || got[0] != "168h" {
		t.Errorf("default: event-ttl arguments %q, want exactly 168h", got)
	}
	s.eventTTL = "2m"
	if got := ttlArgs(s.k3sArgs()); len(got) != 1 || got[0] != "2m" {
		t.Errorf("eventTTL 2m: event-ttl arguments %q", got)
	}
	s.omitEventTTL = true
	if got := ttlArgs(s.k3sArgs()); len(got) != 0 {
		t.Errorf("omitEventTTL: event-ttl arguments %q", got)
	}
}

// TestNewRecordKeepsTheEventTTL: the first record of a start names the event-ttl passed to k3s, in
// state.json under event_ttl, and leaves the key out when no argument was passed. The file is read
// with a map of its own, not with State.
func TestNewRecordKeepsTheEventTTL(t *testing.T) {
	for _, c := range []struct {
		name    string
		mod     func(*launchSpec)
		want    string
		hasKey  bool
		wantArg string
	}{
		{"default", func(*launchSpec) {}, "168h", true, "168h"},
		{"a test's TTL", func(s *launchSpec) { s.eventTTL = "2m" }, "2m", true, "2m"},
		{"no argument", func(s *launchSpec) { s.omitEventTTL = true }, "", false, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			spec := testSpec(t, 0)
			c.mod(&spec)
			rec, err := newRec("k3s", os.Getpid())
			if err != nil {
				t.Fatal(err)
			}
			st := newRecord(spec, rec, Front{}, time.Now())
			if st.EventTTL != c.want {
				t.Errorf("record EventTTL %q, want %q", st.EventTTL, c.want)
			}
			if got := ttlArgs(spec.k3sArgs()); (c.wantArg == "") != (len(got) == 0) || (len(got) == 1 && got[0] != c.wantArg) {
				t.Errorf("the arguments carry %q, want %q", got, c.wantArg)
			}
			if err := os.MkdirAll(spec.l.dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := spec.l.saveState(st); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(spec.l.stateFile())
			if err != nil {
				t.Fatal(err)
			}
			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			v, has := m["event_ttl"]
			if has != c.hasKey || (has && v != c.want) {
				t.Errorf("state.json: event_ttl %v (present %v), want %q (present %v)", v, has, c.want, c.hasKey)
			}
		})
	}
}

// TestStatusReportsTheRecordedEventTTL: status reports what the record says, which a constant
// cannot: 36h for a record that names it, 168h for one that names that, and for a record without it
// (a host that an earlier release started) the API server's default of 1h, flagged as the default,
// with the host still healthy: an upgrade refuses one that is not. The API is not asked, so --no-api
// changes nothing.
func TestStatusReportsTheRecordedEventTTL(t *testing.T) {
	for _, c := range []struct {
		name, recorded, ttl string
		isDefault           bool
	}{
		{"168h", "168h", "168h", false},
		{"another value", "36h", "36h", false},
		{"a record from before the change", "", "1h", true},
	} {
		for _, noAPI := range []bool{true, false} {
			t.Run(c.name+map[bool]string{true: ", no API", false: ""}[noAPI], func(t *testing.T) {
				h := newFakeHost(t)
				h.st.EventTTL = c.recorded
				if err := h.l.saveState(h.st); err != nil {
					t.Fatal(err)
				}
				rep, err := Status(context.Background(), StatusOptions{StateDir: h.l.dir, NoAPI: noAPI, NoGCProbe: true})
				if err != nil {
					t.Fatal(err)
				}
				if rep.Health != Healthy {
					t.Errorf("%s %v: the event TTL must not make the host unhealthy", rep.Health, rep.Reasons)
				}
				if rep.EventTTL != c.ttl || rep.EventTTLDefault != c.isDefault {
					t.Errorf("event TTL %q (default %v), want %q (default %v)", rep.EventTTL, rep.EventTTLDefault, c.ttl, c.isDefault)
				}
				b, err := json.Marshal(rep)
				if err != nil {
					t.Fatal(err)
				}
				var m map[string]any
				if err := json.Unmarshal(b, &m); err != nil {
					t.Fatal(err)
				}
				if def, has := m["event_ttl_default"]; m["event_ttl"] != c.ttl || has != c.isDefault || (has && def != true) {
					t.Errorf("JSON: event_ttl %v, event_ttl_default %v (present %v)", m["event_ttl"], def, has)
				}
			})
		}
	}
}

// TestStatusOfAStoppedHostHasNoEventTTL: without a record there is nothing to report.
func TestStatusOfAStoppedHostHasNoEventTTL(t *testing.T) {
	rep, err := Status(context.Background(), StatusOptions{StateDir: shortDir(t)})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Health != Stopped || rep.EventTTL != "" || rep.EventTTLDefault {
		t.Errorf("%s: event TTL %q (default %v)", rep.Health, rep.EventTTL, rep.EventTTLDefault)
	}
}
