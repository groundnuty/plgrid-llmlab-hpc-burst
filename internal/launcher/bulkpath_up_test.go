package launcher

// Tests of up --bulk-path's wiring (HBEP-6, The bulk path; the science agent's rulings S1 and S2 on
// #448): the port block grows only with the flag, up refuses the flag without a front, in ssh mode,
// without frps, and at a base whose block does not fit, and the record and the supervised set carry
// the second frps only with it.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestPortsWithoutBulkPathAreTheThirteen: without the bulk path the block is base..base+12 and
// its JSON has no bulk key, exactly as before (S1). The control: WithBulkPath grows the same block
// to base..base+15 and sets the key.
func TestPortsWithoutBulkPathAreTheThirteen(t *testing.T) {
	p, err := PortsFromBase(29220)
	if err != nil {
		t.Fatal(err)
	}
	var want []int
	for port := 29220; port <= 29232; port++ {
		want = append(want, port)
	}
	if got := p.All(); !slices.Equal(got, want) {
		t.Errorf("without the bulk path: %v, want %v", got, want)
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "bulk") {
		t.Errorf("the recorded ports name the bulk path without it: %s", b)
	}
	bp, err := p.WithBulkPath()
	if err != nil {
		t.Fatal(err)
	}
	if got := bp.All(); !slices.Equal(got, append(want, 29233, 29234, 29235)) {
		t.Errorf("with the bulk path: %v, want base..base+15", got)
	}
	if b, _ := json.Marshal(bp); !strings.Contains(string(b), `"bulk_path":true`) {
		t.Errorf("the recorded ports with the bulk path: %s", b)
	}
	if !slices.Equal(p.All(), want) {
		t.Error("WithBulkPath changed the Ports it was called on")
	}
}

// TestBulkPathRefusesABaseWhoseBlockDoesNotFit: base 65520 is the highest whose block of 16 ends at
// 65535; 65521, and 65523 (the highest base without the bulk path), are refused, naming the top
// port (S1).
func TestBulkPathRefusesABaseWhoseBlockDoesNotFit(t *testing.T) {
	p, err := PortsFromBase(65520)
	if err != nil {
		t.Fatal(err)
	}
	if bp, err := p.WithBulkPath(); err != nil || bp.All()[15] != 65535 {
		t.Errorf("base 65520 with the bulk path: %v, %v; want the block to end at 65535", bp.All(), err)
	}
	for base, top := range map[int]string{65521: "65536", 65523: "65538"} {
		p, err := PortsFromBase(base)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.WithBulkPath(); err == nil || !strings.Contains(err.Error(), "end at port "+top) || !strings.Contains(err.Error(), "65520") {
			t.Errorf("base %d with the bulk path: %v, want a refusal naming port %s and the highest base 65520", base, err, top)
		}
	}
}

// fakeFrontRun writes a front configuration and an executable stand-in for the front, so up resolves
// a front to run (resolveFrontRun). Nothing runs it: the tests that use it are refused first.
func fakeFrontRun(t *testing.T) (config, bin string) {
	t.Helper()
	dir := t.TempDir()
	config, bin = filepath.Join(dir, "front.json"), filepath.Join(dir, "hpcburst-front")
	if err := os.WriteFile(config, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return config, bin
}

// fakeFrps is an executable stand-in at a path for --frps-bin. up checks it against the pin and
// refuses it: a test that is refused before that point never gets there.
func fakeFrps(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "frps")
	if err := os.WriteFile(p, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestUpRefusesTheBulkPathWithoutAFront: up --bulk-path is refused, with its reason, before
// anything starts, with no front mode, in ssh mode, with no front to run, and with no frps (S2), and
// at a base whose block does not fit (S1). Nothing is recorded and no certificate is written. The
// control: host-dialed and direct mode with a front pass these checks and fail only later, on the
// stand-in frps's pin, so the refusals above are the bulk path's own.
func TestUpRefusesTheBulkPathWithoutAFront(t *testing.T) {
	cfg, frontBin := fakeFrontRun(t)
	ctl := fakeControllerBin(t)
	hostDialed := Front{Mode: string(agentapi.DialHomeHostDialed)}
	direct := Front{Mode: "direct", Addr: "front.example.test:8443"}
	key := filepath.Join(t.TempDir(), "id_example")
	if err := os.WriteFile(key, []byte("not read: up records the path only\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ssh := Front{Mode: "ssh", Addr: "gateway.example.test:22", KeyPath: key}
	for _, c := range []struct {
		name string
		o    UpOptions
		want string
	}{
		{"no front mode", UpOptions{Front: Front{}, FrontConfig: cfg, FrontBin: frontBin}, "--bulk-path needs a front (--front-mode"},
		{"ssh mode", UpOptions{Front: ssh, FrontConfig: cfg, FrontBin: frontBin}, "in ssh mode the agent starts no bulk frpc"},
		{"no front to run", UpOptions{Front: hostDialed}, "--bulk-path needs the front to run (--front-config)"},
		{"a base whose block does not fit", UpOptions{Front: hostDialed, FrontConfig: cfg, FrontBin: frontBin, PortBase: 65521}, "would end at port 65536"},
		{"control: host-dialed passes them", UpOptions{Front: hostDialed, FrontConfig: cfg, FrontBin: frontBin, FrpsBin: fakeFrps(t)}, "frps"},
		{"control: direct passes them", UpOptions{Front: direct, FrontConfig: cfg, FrontBin: frontBin, FrpsBin: fakeFrps(t)}, "frps"},
	} {
		t.Run(c.name, func(t *testing.T) {
			l, err := newLayout(filepath.Join(shortDir(t), "st"))
			if err != nil {
				t.Fatal(err)
			}
			o := c.o
			o.BulkPath, o.StateDir, o.ControllerBin = true, l.dir, ctl
			if o.PortBase == 0 {
				o.PortBase = 30320 // refused before any port is checked or bound
			}
			_, err = Up(context.Background(), o)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("up --bulk-path: %v; want refused, naming %q", err, c.want)
			}
			if strings.HasPrefix(c.name, "control") && strings.Contains(err.Error(), "--bulk-path") {
				t.Fatalf("the control was refused by the bulk path's checks: %v", err)
			}
			if _, err := os.Stat(l.stateFile()); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("a record after the refusal: %v", err)
			}
			h := agentapi.Host{StateDir: l.dir, PortBase: o.PortBase}
			if _, err := os.Stat(h.BulkCertFile()); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("a second certificate after the refusal: %v", err)
			}
		})
	}
}

// TestUpRefusesTheBulkPathWithoutFrps: with a front but no frps, the bulk path's second frps has no
// binary to run, so up refuses it, naming frps.
func TestUpRefusesTheBulkPathWithoutFrps(t *testing.T) {
	cfg, frontBin := fakeFrontRun(t)
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	o := UpOptions{BulkPath: true, StateDir: l.dir, PortBase: 30320, ControllerBin: fakeControllerBin(t),
		Front: Front{Mode: string(agentapi.DialHomeHostDialed)}, FrontConfig: cfg, FrontBin: frontBin,
		BinDir: t.TempDir()} // no frp-<version>/frps there
	_, err = Up(context.Background(), o)
	if !errors.Is(err, errBulkPathNoFrps) {
		t.Fatalf("up --bulk-path with no frps: %v, want errBulkPathNoFrps", err)
	}
	if _, err := os.Stat(l.stateFile()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a record after the refusal: %v", err)
	}
}

// TestSupervisedSetWithoutTheBulkPathIsUnchanged: without the bulk path the set is the controller
// and frps, with the controller's command line as before (S5: the first frps's entry is pinned by
// TestSupervisedSetRunsFrps). The control: with it, exactly one entry more, frps-bulk, running the
// same verified binary with its own configuration, health and three ports, base+13 to base+15, and
// the controller's command line gains --bulk-path (part 3).
func TestSupervisedSetWithoutTheBulkPathIsUnchanged(t *testing.T) {
	st := &State{
		Ports:       Ports{API: 29220, Controller: 29226},
		Controllers: &ControllerConfig{Binary: "/opt/hpcburst-controller"},
		Frps:        &FrpsRunConfig{Binary: "/opt/frp/frps", Version: "0.71.1"},
	}
	plain, err := st.supervisedSet(layout{"/run/st"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) != 2 || plain[0].Name != "controller" || plain[1].Name != "frps" {
		t.Fatalf("supervised set without the bulk path: %+v", plain)
	}
	wantCtl := []string{"--state-dir", "/run/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226"}
	if !slices.Equal(plain[0].Args, wantCtl) {
		t.Errorf("the controller's command line without the bulk path\n got %q\nwant %q", plain[0].Args, wantCtl)
	}
	st.BulkPath = true
	with, err := st.supervisedSet(layout{"/run/st"})
	if err != nil {
		t.Fatal(err)
	}
	if len(with) != 3 || with[2].Name != "frps-bulk" {
		t.Fatalf("supervised set with the bulk path: %+v", with)
	}
	// Part 3 (#448): the controller reads frps-bulk's list and serves its plugin path, so with the bulk
	// path recorded its command line gains --bulk-path at its end, and nothing else.
	if want := append(slices.Clone(wantCtl), "--bulk-path"); !slices.Equal(with[0].Args, want) {
		t.Errorf("the controller's command line with the bulk path\n got %q\nwant %q", with[0].Args, want)
	}
	b := with[2]
	if b.Path != "/proc/self/fd/3" || b.Argv0 != "/opt/frp/frps" || b.Health != "127.0.0.1:29235" || !b.WaitReady || b.Open == nil {
		t.Errorf("frps-bulk entry %+v", b)
	}
	if want := []string{"-c", "/run/st/frps-bulk/frps.toml", "--strict-config=true"}; !slices.Equal(b.Args, want) {
		t.Errorf("frps-bulk argv\n got %q\nwant %q", b.Args, want)
	}
	if want := []int{29233, 29234, 29235}; !slices.Equal(b.Ports, want) {
		t.Errorf("frps-bulk ports %v, want %v", b.Ports, want)
	}
	if want := []string{"PATH=/usr/bin:/bin"}; !slices.Equal(b.Env, want) {
		t.Errorf("frps-bulk environment %q, want %q", b.Env, want)
	}
	st.Frps = nil
	if set, err := st.supervisedSet(layout{"/run/st"}); err != nil || len(set) != 1 {
		t.Errorf("the bulk path without frps runs neither frps: %+v, %v", set, err)
	}
}

// TestStateWithoutTheBulkPathHasNoBulkKey: a record without the bulk path has no bulk key at all,
// so a state file is byte for byte what it was; with it, the key is there and survives a reload.
func TestStateWithoutTheBulkPathHasNoBulkKey(t *testing.T) {
	st := State{Version: "v1.36.5+k3s1", Ports: Ports{API: 29220}}
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "bulk") {
		t.Errorf("a record without the bulk path names it: %s", b)
	}
	st.BulkPath = true
	b, err = json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var back State
	if err := json.Unmarshal(b, &back); err != nil || !back.BulkPath || !strings.Contains(string(b), `"bulk_path":true`) {
		t.Errorf("the record with the bulk path: %s -> %+v, %v", b, back.BulkPath, err)
	}
}

// TestOpenFilesCoversFrpsBulk: the second frps is a tunnel process, whose open files status reports
// and up warns about, as the first's (HBEP-6, The bulk path; #431).
func TestOpenFilesCoversFrpsBulk(t *testing.T) {
	for name, want := range map[string]bool{"frps": true, "frps-bulk": true, "front": true, "controller": false, "relaykeeper": false} {
		if got := tunnelEntry(name); got != want {
			t.Errorf("tunnelEntry(%q) = %v, want %v", name, got, want)
		}
	}
}

// TestStatusFrpsBulkMustPostToTheFrontsBulkPlugin: with the bulk path and a front, status finds
// the second frps's file posting to the front's bulk path, and says so when it does not; without
// the bulk path it reads no such file.
func TestStatusFrpsBulkMustPostToTheFrontsBulkPlugin(t *testing.T) {
	l := layout{dir: shortDir(t)}
	ports := Ports{API: 29220, FrontHealth: 29224}
	st := &State{Ports: ports, Frps: &FrpsRunConfig{Binary: "/opt/frp/frps"}, FrontRun: &FrontRunConfig{Binary: "/opt/front", Config: "/etc/front.json"}}
	var probs []string
	bad := func(f string, a ...any) { probs = append(probs, f) }
	checkFrpsBulkNamesTheFront(l, st, bad)
	if len(probs) != 0 {
		t.Fatalf("without the bulk path status read the second frps's file: %v", probs)
	}
	st.BulkPath = true
	checkFrpsBulkNamesTheFront(l, st, bad)
	if len(probs) != 1 || !strings.Contains(probs[0], "the bulk path's frps configuration") {
		t.Fatalf("with no file: %v", probs)
	}
	h := l.host(ports)
	probs = nil
	if _, err := writeFrpsBulkFiles(h, ""); err != nil {
		t.Fatal(err)
	}
	checkFrpsBulkNamesTheFront(l, st, bad)
	if len(probs) != 1 || !strings.Contains(probs[0], "does not post to the front's plugin") {
		t.Fatalf("a file without the front's plugin: %v", probs)
	}
	probs = nil
	if _, err := writeFrpsBulkFiles(h, ports.FrontHealthAddr()); err != nil {
		t.Fatal(err)
	}
	checkFrpsBulkNamesTheFront(l, st, bad)
	if len(probs) != 0 {
		t.Errorf("the file up writes was found wanting: %v", probs)
	}
	// The first frps's file posting to the bulk path, or the reverse, is not the same block.
	if strings.Contains(frontPluginBlock(ports.FrontHealthAddr()), "/bulk") {
		t.Error("the first frps's front block names the bulk path")
	}
}

// TestAnUpgradeKeepsTheBulkPath: upgrade starts the new release with the record's bulk path, ports
// included, as it keeps the relay keeper (TestAnUpgradeKeepsTheRelayKeeper); a host without the bulk
// path stays without it.
func TestAnUpgradeKeepsTheBulkPath(t *testing.T) {
	for _, bulk := range []bool{true, false} {
		t.Run(map[bool]string{true: "a bulk-path host", false: "an alpha host"}[bulk], func(t *testing.T) {
			l := storeLayout(t)
			putStore(t, l, "v1", "tok", "")
			if err := l.saveStoreVersion("v1.35.9+k3s1", "up"); err != nil {
				t.Fatal(err)
			}
			ports, err := PortsFromBase(30020)
			if err != nil {
				t.Fatal(err)
			}
			if bulk {
				if ports, err = ports.WithBulkPath(); err != nil {
					t.Fatal(err)
				}
			}
			st := &State{Version: "v1.35.9+k3s1", Ports: ports, Front: Front{Mode: string(agentapi.DialHomeHostDialed)},
				Controllers: &ControllerConfig{Binary: "/opt/example/hpcburst-controller"}, BulkPath: bulk}
			ops := &fakeHostOps{t: t, l: l}
			if _, _, _, _, err := replaceHost(context.Background(), l, st, Binary{Version: mustVersion(t, "v1.36.5+k3s1")}, ops, time.Second, time.Second, nil); err != nil {
				t.Fatal(err)
			}
			if ops.spec.bulkPath != bulk || ops.spec.ports.BulkPath != bulk {
				t.Fatalf("the new host's launch spec carries bulkPath %v and a block with the bulk path %v, want %v", ops.spec.bulkPath, ops.spec.ports.BulkPath, bulk)
			}
		})
	}
}

// TestEachCertificateNamesExactlyItsOwnName (the review of #456 and #458): one certificate carrying
// both names would pass both checks, so the launcher's issuance keeps them apart. The second
// certificate names the bulk server alone; the first names localhost, the token's server and
// 127.0.0.1, as it did before the bulk path, and never the bulk server.
func TestEachCertificateNamesExactlyItsOwnName(t *testing.T) {
	h := testHost(t)
	if err := ensureAgentEndpointPair(h, nil); err != nil {
		t.Fatal(err)
	}
	if err := ensureBulkCert(h, nil); err != nil {
		t.Fatal(err)
	}
	first, bulk := certFile(t, h.AgentCertFile()), certFile(t, h.BulkCertFile())
	if !slices.Equal(first.DNSNames, []string{"localhost", "dial-home.hpcburst.invalid"}) || len(first.IPAddresses) != 1 || !first.IPAddresses[0].Equal(net.IPv4(127, 0, 0, 1)) ||
		len(first.URIs) != 0 || len(first.EmailAddresses) != 0 {
		t.Errorf("the first certificate names %v %v %v %v; want localhost and dial-home.hpcburst.invalid, and 127.0.0.1", first.DNSNames, first.IPAddresses, first.URIs, first.EmailAddresses)
	}
	if !slices.Equal(bulk.DNSNames, []string{bulkName}) || len(bulk.IPAddresses) != 0 || len(bulk.URIs) != 0 || len(bulk.EmailAddresses) != 0 {
		t.Errorf("the second certificate names %v %v %v %v; want %s alone", bulk.DNSNames, bulk.IPAddresses, bulk.URIs, bulk.EmailAddresses, bulkName)
	}
	if first.VerifyHostname(bulkName) == nil || bulk.VerifyHostname("dial-home.hpcburst.invalid") == nil {
		t.Error("a certificate passes the other's name")
	}
}
