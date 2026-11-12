package launcher

// The controller's command line and the bulk path (beta; HBEP-6, The bulk path; #448 part 3): the
// launcher appends --bulk-path to hpcburst-controller's arguments only when its state records the bulk
// path, as it appends --relay-keeper-socket only with the keeper (supervise.go). Without it the command
// line is byte for byte what it was. Every vector is typed from the controller's contract
// (cmd/hpcburst-controller), not computed from the code under test.

import (
	"path/filepath"
	"slices"
	"testing"
)

// TestControllerArgsWithoutBulkAreUnchanged: the controller's argv for each front, typed out, with the bulk
// path off (the pin), and again with it on (the control): the same line and --bulk-path after it, nothing
// else changed. A recorded bulk path with no frps adds nothing, since no second frps then runs.
func TestControllerArgsWithoutBulkAreUnchanged(t *testing.T) {
	frps := &FrpsRunConfig{Binary: "/opt/frp/frps", Version: "0.71.1"}
	for _, c := range []struct {
		name  string
		front Front
		want  []string
	}{
		{"no front", Front{}, []string{"--state-dir", "/run/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226"}},
		{"direct front", Front{Mode: "direct", Addr: "front.example.test:8443"},
			[]string{"--state-dir", "/run/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226", "--front-mode", "direct", "--front-addr", "front.example.test:8443"}},
		{"host-dialed front", Front{Mode: "host-dialed"},
			[]string{"--state-dir", "/run/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226", "--front-mode", "host-dialed"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := &State{
				Ports:       Ports{API: 29220, Controller: 29226},
				Front:       c.front,
				Controllers: &ControllerConfig{Binary: "/opt/hpcburst-controller"},
				Frps:        frps,
			}
			argv := func() []string {
				t.Helper()
				set, err := st.supervisedSet(layout{"/run/st"})
				if err != nil {
					t.Fatal(err)
				}
				if set[0].Name != "controller" {
					t.Fatalf("the first entry is %q, want the controller", set[0].Name)
				}
				return set[0].Args
			}
			if got := argv(); !slices.Equal(got, c.want) {
				t.Errorf("without the bulk path\n got %q\nwant %q", got, c.want)
			}
			st.BulkPath = true
			if got, want := argv(), append(slices.Clone(c.want), "--bulk-path"); !slices.Equal(got, want) {
				t.Errorf("with the bulk path\n got %q\nwant %q", got, want)
			}
			// No frps: the record is incoherent, and nothing of the bulk path runs, controller included.
			st.Frps = nil
			if got := argv(); !slices.Equal(got, c.want) {
				t.Errorf("a recorded bulk path with no frps\n got %q\nwant the line without it %q", got, c.want)
			}
		})
	}
}

// TestControllerArgsWithKeeperAndBulk: both flags, each only with its option. The relay keeper's socket
// is appended after the bulk flag, as the keeper's entry is added after frps-bulk's.
func TestControllerArgsWithKeeperAndBulk(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	ports, err := PortsFromBase(30020)
	if err != nil {
		t.Fatal(err)
	}
	st := &State{Ports: ports, Front: Front{Mode: "host-dialed"}, RelayKeeper: true,
		Controllers: &ControllerConfig{Binary: "/opt/hpcburst-controller"}, Frps: &FrpsRunConfig{Binary: "/opt/frp/frps", Version: "0.71.1"}}
	sock := l.host(ports).RelayKeeperSocket()
	line := []string{"--state-dir", l.dir, "--port-base", "30020", "--health-addr", ports.ControllerAddr(), "--front-mode", "host-dialed"}

	set, err := st.supervisedSet(l)
	if err != nil {
		t.Fatal(err)
	}
	if want := append(slices.Clone(line), "--relay-keeper-socket", sock); !slices.Equal(set[0].Args, want) {
		t.Errorf("the keeper without the bulk path\n got %q\nwant %q", set[0].Args, want)
	}
	st.BulkPath = true
	if set, err = st.supervisedSet(l); err != nil {
		t.Fatal(err)
	}
	if want := append(slices.Clone(line), "--bulk-path", "--relay-keeper-socket", sock); !slices.Equal(set[0].Args, want) {
		t.Errorf("the keeper with the bulk path\n got %q\nwant %q", set[0].Args, want)
	}
}
