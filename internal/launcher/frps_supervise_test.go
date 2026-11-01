package launcher

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
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
	if !strings.Contains(strings.Join(problems, "; "), "expected listener tcp 127.0.0.1:"+strconv.Itoa(other)+" is missing") {
		t.Fatalf("a missing port is not reported: %v", problems)
	}
}

// awaitEntry polls supervisor.json until cond holds for the named entry.
func (f *ctlFixture) awaitEntry(name, what string, cond func(e EntryRecord) bool) EntryRecord {
	f.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if sf, err := readSupervisorFile(f.l); err == nil {
			for _, e := range sf.Entries {
				if e.Name == name && cond(e) {
					return e
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("timed out waiting for %s; supervisor.json: %s", what, fileOf(f.l.supervisorFile()))
	return EntryRecord{}
}

// extraBound waits for the fake's extra-bound-<pid> file (listen-tcp, listen-udp) and returns the
// sockets it names, as "tcp 127.0.0.2:1234" or "udp 127.0.0.1:5678".
func (f *ctlFixture) extraBound(pid int) []string {
	f.t.Helper()
	var v []string
	f.readJSON(fmt.Sprintf("extra-bound-%d", pid), &v)
	return v
}

// supervisorHandle is a verified handle on this process, which plays the supervisor.
func (f *ctlFixture) supervisorHandle() *procHandle {
	f.t.Helper()
	sup, v, d := openRec(f.st.Procs[0])
	if v != Ours {
		f.t.Fatalf("this process is %v: %s", v, d)
	}
	f.t.Cleanup(sup.Close)
	return sup
}

// TestCheckEntryExactSockets: a process with a fixed set of ports (frps) must hold exactly those, as
// TCP on 127.0.0.1, and no other reachable socket, matched by socket inode. The fake controller, a
// real child of this process, binds what the test asks; its health port alone passes in
// TestCheckEntryExactPorts, the control.
func TestCheckEntryExactSockets(t *testing.T) {
	t.Run("an extra UDP socket", func(t *testing.T) {
		// KCP and QUIC listen on UDP: a TCP-only view would see exactly the expected ports.
		f := newCtlFixture(t, Front{})
		f.steer("listen-udp", "127.0.0.1:0")
		f.supervise(SuperviseOptions{})
		e := f.awaitRunning()
		bound := f.extraBound(e.Proc.PID)
		awaitHealthy(t, f.ports.ControllerAddr())
		e.Ports = []int{f.ports.Controller}
		eh, problems := checkEntry(f.supervisorHandle(), os.Getpid(), e)
		if len(bound) != 1 || !strings.Contains(strings.Join(problems, "; "), "it also listens on ["+bound[0]+"]") || eh.HealthzOK != nil {
			t.Fatalf("an extra UDP socket %v: %+v, problems %v", bound, eh, problems)
		}
	})
	t.Run("the right port at another address", func(t *testing.T) {
		// The child listens on 127.0.0.2:P, and this test process (a foreign listener) on
		// 127.0.0.1:P, answering /healthz with 200. Matching port numbers alone would call the
		// child the holder of 127.0.0.1:P, and count the foreign answer.
		f := newCtlFixture(t, Front{})
		port := freePort(t)
		foreign, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}
		go srv.Serve(foreign)
		t.Cleanup(func() { srv.Close() })
		f.steer("listen-tcp", "127.0.0.2:"+strconv.Itoa(port))
		f.supervise(SuperviseOptions{})
		e := f.awaitRunning()
		if bound := f.extraBound(e.Proc.PID); len(bound) != 1 || bound[0] != "tcp 127.0.0.2:"+strconv.Itoa(port) {
			t.Fatalf("the child bound %v", bound)
		}
		awaitHealthy(t, f.ports.ControllerAddr())
		addr := "127.0.0.1:" + strconv.Itoa(port)
		if code, err := healthzCode(addr); err != nil || code != 200 {
			t.Fatalf("the foreign listener does not answer: %d, %v", code, err)
		}
		sup := f.supervisorHandle()

		// As the health address: the foreign answer must not be asked for.
		e.Ports = []int{f.ports.Controller, port}
		asHealth := e
		asHealth.Health = addr
		eh, problems := checkEntry(sup, os.Getpid(), asHealth)
		if !strings.Contains(strings.Join(problems, "; "), "does not hold its health address "+addr) || eh.HealthzOK != nil {
			t.Errorf("health on the foreign address: %+v, problems %v", eh, problems)
		}
		// As one of the fixed ports (frps's bind or tcpmux port).
		_, problems = checkEntry(sup, os.Getpid(), e)
		got := strings.Join(problems, "; ")
		for _, want := range []string{"expected listener tcp " + addr + " is missing", "it also listens on [tcp 127.0.0.2:" + strconv.Itoa(port) + "]", "other processes listen on its ports: [tcp " + addr + "]"} {
			if !strings.Contains(got, want) {
				t.Errorf("a fixed port at another address: problems %v, want %q", problems, want)
			}
		}
	})
}
