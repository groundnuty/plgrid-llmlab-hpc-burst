package launcher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFrontNormalize(t *testing.T) {
	key := filepath.Join(t.TempDir(), "restricted_key")
	if err := os.WriteFile(key, []byte("not a real key"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok := []struct {
		name string
		in   Front
		want Front
	}{
		{"nothing given", Front{}, Front{}},
		{"direct", Front{Mode: "direct", Addr: "front.example.test:8443"}, Front{Mode: "direct", Addr: "front.example.test:8443"}},
		{"ssh without a port means 22", Front{Mode: "ssh", Addr: "front.example.test", KeyPath: key}, Front{Mode: "ssh", Addr: "front.example.test:22", KeyPath: key}},
		{"ssh with port 22", Front{Mode: "ssh", Addr: "127.0.0.1:22", KeyPath: key}, Front{Mode: "ssh", Addr: "127.0.0.1:22", KeyPath: key}},
		// The owner routes a cluster through a port forward on the gateway at its own port.
		{"ssh on another port", Front{Mode: "ssh", Addr: "gateway.example.test:2222", KeyPath: key}, Front{Mode: "ssh", Addr: "gateway.example.test:2222", KeyPath: key}},
		{"ssh on the highest port", Front{Mode: "ssh", Addr: "gateway.example.test:65535", KeyPath: key}, Front{Mode: "ssh", Addr: "gateway.example.test:65535", KeyPath: key}},
		// Host-dialed has no address and no key: the host dials the jobs (HBEP-6, Dial-home modes).
		{"host-dialed with nothing else", Front{Mode: "host-dialed"}, Front{Mode: "host-dialed"}},
	}
	for _, c := range ok {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.in.Normalize()
			if err != nil || got != c.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, c.want)
			}
		})
	}
	bad := []struct {
		name    string
		in      Front
		message string
	}{
		{"address without a mode", Front{Addr: "x:1"}, "mode"},
		{"unknown mode", Front{Mode: "tunnel", Addr: "x:1"}, "direct, ssh or host-dialed"},
		{"mode without an address", Front{Mode: "direct"}, "--front-addr"},
		{"ssh without an address", Front{Mode: "ssh", KeyPath: key}, "--front-addr"},
		{"host-dialed with an address", Front{Mode: "host-dialed", Addr: "front.example.test:22"}, "takes no --front-addr"},
		{"host-dialed with a port-less address", Front{Mode: "host-dialed", Addr: "front.example.test"}, "takes no --front-addr"},
		{"host-dialed with a key", Front{Mode: "host-dialed", KeyPath: key}, "takes no key"},
		{"host-dialed with an address and a key", Front{Mode: "host-dialed", Addr: "x:22", KeyPath: key}, "takes no --front-addr"},
		{"direct without a port", Front{Mode: "direct", Addr: "front.example.test"}, "host:port"},
		{"port out of range", Front{Mode: "direct", Addr: "x:99999"}, "not a port"},
		{"ssh with port 0", Front{Mode: "ssh", Addr: "x:0", KeyPath: key}, "not a port"},
		{"ssh with port out of range", Front{Mode: "ssh", Addr: "x:99999", KeyPath: key}, "not a port"},
		{"ssh with a port that is not a number", Front{Mode: "ssh", Addr: "x:ssh", KeyPath: key}, "not a port"},
		{"ssh without a key", Front{Mode: "ssh", Addr: "x"}, "--front-key"},
		{"ssh with a missing key", Front{Mode: "ssh", Addr: "x", KeyPath: filepath.Join(t.TempDir(), "nope")}, "front key"},
		{"ssh with a directory as key", Front{Mode: "ssh", Addr: "x", KeyPath: t.TempDir()}, "regular file"},
		{"direct with a key", Front{Mode: "direct", Addr: "x:1", KeyPath: key}, "takes no key"},
		{"key alone", Front{KeyPath: key}, "mode"},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.in.Normalize()
			if err == nil || !strings.Contains(err.Error(), c.message) {
				t.Fatalf("want an error containing %q, got %v", c.message, err)
			}
		})
	}
	if (Front{}).Set() || !(Front{Mode: "direct"}).Set() || !(Front{Mode: "host-dialed"}).Set() {
		t.Error("Set() is wrong")
	}
}

// TestFrontModeErrorsNameAllThreeModes: an operator who mistypes the mode, or who names none, is
// told every mode there is (HBEP-6, Dial-home modes), and the no-front warning does too.
func TestFrontModeErrorsNameAllThreeModes(t *testing.T) {
	for _, f := range []Front{{Mode: "tunnel", Addr: "x:1"}, {Addr: "x:1"}} {
		_, err := f.Normalize()
		if err == nil {
			t.Fatalf("%+v: no error", f)
		}
		for _, m := range []string{"direct", "ssh", "host-dialed"} {
			if !strings.Contains(err.Error(), m) {
				t.Errorf("%+v: the error %q does not name %s", f, err, m)
			}
		}
	}
	for _, m := range []string{"direct", "ssh", "host-dialed"} {
		if !strings.Contains(frontWarning, m) {
			t.Errorf("the no-front warning does not name %s", m)
		}
	}
}
