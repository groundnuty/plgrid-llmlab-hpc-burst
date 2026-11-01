package launcher

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestSupervisedSetRunsFrps: with frps recorded, the set ends with frps's entry, whose command line
// and environment are written here from HBEP-6 and frps_proc.go's contract: the verified
// descriptor /proc/self/fd/3, the configuration file under <state>/frps, strict parsing, and an
// environment of PATH alone. Its health is base+11 and its ports exactly base+9 to base+11.
func TestSupervisedSetRunsFrps(t *testing.T) {
	st := &State{
		Ports:       Ports{API: 29220, Controller: 29226},
		Controllers: &ControllerConfig{Binary: "/opt/hpcburst-controller"},
		Frps:        &FrpsRunConfig{Binary: "/opt/frp/frps", Version: "0.71.1"},
	}
	set, err := st.supervisedSet(layout{"/run/st"})
	if err != nil {
		t.Fatal(err)
	}
	if len(set) != 2 || set[1].Name != "frps" {
		t.Fatalf("supervised set %+v", set)
	}
	f := set[1]
	if f.Path != "/proc/self/fd/3" || f.Health != "127.0.0.1:29231" || !f.WaitReady || f.Open == nil {
		t.Errorf("frps entry %+v", f)
	}
	if want := []string{"-c", "/run/st/frps/frps.toml", "--strict-config=true"}; !slices.Equal(f.Args, want) {
		t.Errorf("frps argv\n got %q\nwant %q", f.Args, want)
	}
	if want := []string{"PATH=/usr/bin:/bin"}; !slices.Equal(f.Env, want) {
		t.Errorf("frps environment %q, want %q", f.Env, want)
	}
	if want := []int{29229, 29230, 29231}; !slices.Equal(f.Ports, want) {
		t.Errorf("frps ports %v, want %v", f.Ports, want)
	}
	st.Frps = nil
	if set, err := st.supervisedSet(layout{"/run/st"}); err != nil || len(set) != 1 {
		t.Errorf("without frps recorded the set is the controller alone: %+v, %v", set, err)
	}
}

// TestFrpsEntryChecksThePinAtEveryStart: the entry opens the binary and checks it against the pin
// before each start, so a binary replaced after up fails its start instead of running.
func TestFrpsEntryChecksThePinAtEveryStart(t *testing.T) {
	fake := filepath.Join(t.TempDir(), "frps")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	st := &State{Ports: Ports{API: 29220}, Frps: &FrpsRunConfig{Binary: fake, Version: DefaultFrpVersion}}
	files, err := st.frpsEntry(layout{"/run/st"}).Open()
	if err == nil {
		for _, f := range files {
			f.Close()
		}
		t.Fatal("a binary that does not match the pin was opened for a start")
	}
}

// TestResolveFrpsRun: the default frps (frp-<version>/frps under the bin dir) that is absent means
// no frps, without an error; a given path that is missing, or a default that does not match the
// pin, is an error.
func TestResolveFrpsRun(t *testing.T) {
	bin := t.TempDir()
	if run, err := resolveFrpsRun(bin, "", DefaultFrpVersion); run != nil || err != nil {
		t.Errorf("no default frps: %+v, %v; want nil, nil", run, err)
	}
	if _, err := resolveFrpsRun(bin, filepath.Join(bin, "nope"), DefaultFrpVersion); err == nil {
		t.Error("a given frps that is missing was accepted")
	}
	dir := filepath.Join(bin, FrpDirName(DefaultFrpVersion))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "frps"), []byte("not frps"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveFrpsRun(bin, "", DefaultFrpVersion); err == nil || !strings.Contains(err.Error(), "frps") {
		t.Errorf("a default frps that does not match the pin: %v; want an error", err)
	}
}

// TestCheckEntryExactPorts: an entry with a fixed set of ports (frps) must hold exactly those, on
// loopback. The fake controller, a real child of this process acting as the supervisor, holds only
// its health port: naming that port alone passes (the control), naming one more is reported.
func TestCheckEntryExactPorts(t *testing.T) {
	f := newCtlFixture(t, Front{})
	f.supervise(SuperviseOptions{})
	e := f.awaitRunning()
	awaitHealthy(t, f.ports.ControllerAddr())
	sup, v, d := openRec(f.st.Procs[0]) // this process
	if v != Ours {
		t.Fatalf("this process is %v: %s", v, d)
	}
	t.Cleanup(sup.Close)
	e.Ports = []int{f.ports.Controller}
	if _, problems := checkEntry(sup, os.Getpid(), e); len(problems) != 0 {
		t.Fatalf("holding exactly its port: %v", problems)
	}
	other := freePort(t)
	e.Ports = []int{f.ports.Controller, other}
	_, problems := checkEntry(sup, os.Getpid(), e)
	if !strings.Contains(strings.Join(problems, "; "), "expected listener on port "+strconv.Itoa(other)+" is missing") {
		t.Fatalf("a missing port is not reported: %v", problems)
	}
}
