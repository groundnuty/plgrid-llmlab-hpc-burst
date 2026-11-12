package main

// up --bulk-path on a real host (HBEP-6, The bulk path; #448, part 2): the second frps runs under the
// supervisor on exactly base+13 to base+15, beside the first, the second certificate is issued for
// the agent endpoint's key, and the first certificate, which an Allocation staged before the switch
// verifies its token tunnel with, keeps its bytes, inode and modification time. down leaves nothing on
// the block. The base, 30320, is in #448's port block (30280-30359, IMPL-COMMON): this test binds
// 30320-30335 and the front's client listener at 30336.

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const (
	bulkUpBase       = 30320
	bulkUpClientPort = 30336
)

// certMark is a file's bytes' hash, inode and modification time: what changes when it is written.
type certMark struct {
	sum   [32]byte
	ino   uint64
	mtime time.Time
}

func certMarkOf(t *testing.T, path string) certMark {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return certMark{sum: sha256.Sum256(b), ino: fi.Sys().(*syscall.Stat_t).Ino, mtime: fi.ModTime()}
}

// spkiOf is the SHA-256 of a PEM certificate file's SubjectPublicKeyInfo, parsed here.
func spkiOf(t *testing.T, path string) ([32]byte, *x509.Certificate) {
	t.Helper()
	blk, _ := pem.Decode([]byte(fileOf(path)))
	if blk == nil {
		t.Fatalf("%s holds no PEM block", path)
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(c.RawSubjectPublicKeyInfo), c
}

// entryPID returns the PID of the named supervised entry once it runs.
func entryPID(t *testing.T, dir, name string) int {
	t.Helper()
	var pid int
	waitFor(t, 60*time.Second, name+" to run", func() bool {
		var r struct {
			Entries []frontEntryRecord `json:"entries"`
		}
		if err := json.Unmarshal([]byte(fileOf(filepath.Join(dir, "controller", "supervisor.json"))), &r); err != nil {
			return false
		}
		for _, e := range r.Entries {
			if e.Name == name && e.Proc != nil && e.State == "running" {
				pid = e.Proc.PID
				return true
			}
		}
		return false
	})
	return pid
}

func TestUpBulkPath(t *testing.T) {
	h := newHost(t)
	ctl, _ := fakeController(t)
	fb := frontBinary(t)
	work := shortTmp(t, false)
	keys := filepath.Join(work, "client-keys.json")
	if err := os.WriteFile(keys, []byte(`{"keys":[{"id":"e2e","key":"bulk-e2e-client-key-`+strings.Repeat("8", 20)+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(work, "front.json")
	body := fmt.Sprintf(`{"clientListen":"127.0.0.1:%d","clientKeysFile":%q,"usageDir":%q}`, bulkUpClientPort, keys, filepath.Join(work, "usage"))
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	up := func(extra ...string) cliResult {
		return cli(t, h.upArgs(bulkUpBase, append(frpsArgs(t), append([]string{"--json", "--controller-bin", ctl, "--front-config", cfg, "--front-bin", fb,
			"--front-mode", "host-dialed"}, extra...)...)...)...)
	}
	block := func() []string { return listeningInRange(t, bulkUpBase, bulkUpBase+15) }

	// ---- before the bulk path: the first certificate, as an Allocation staged now would get it ----
	if r := up(); r.code != exitOK {
		t.Fatalf("up without --bulk-path exited %d: %s", r.code, r.err)
	}
	hostAPI := agentapi.Host{StateDir: h.dir, PortBase: bulkUpBase}
	first := certMarkOf(t, hostAPI.AgentCertFile())
	key := certMarkOf(t, hostAPI.AgentKeyFile())
	if _, err := os.Stat(hostAPI.BulkCertFile()); !os.IsNotExist(err) {
		t.Errorf("up without --bulk-path wrote the second certificate: %v", err)
	}
	if got := listeningInRange(t, bulkUpBase+13, bulkUpBase+15); len(got) != 0 {
		t.Errorf("up without --bulk-path: listeners on base+13..15: %v", got)
	}
	if r := cli(t, h.args("down")...); r.code != exitOK {
		t.Fatalf("down exited %d: %s", r.code, r.err)
	}

	// ---- with the bulk path ----
	time.Sleep(20 * time.Millisecond) // a write in the next step would show in the modification time
	r := up("--bulk-path")
	if r.code != exitOK {
		t.Fatalf("up --bulk-path exited %d: %s", r.code, r.err)
	}
	var rep struct {
		FrpsBulkPorts []int `json:"frps_bulk_ports"`
		Ports         struct {
			BulkPath bool `json:"bulk_path"`
		} `json:"ports"`
	}
	parseJSON(t, r.out, &rep)
	if want := []int{bulkUpBase + 13, bulkUpBase + 14, bulkUpBase + 15}; !slices.Equal(rep.FrpsBulkPorts, want) || !rep.Ports.BulkPath {
		t.Errorf("up --bulk-path reports frps-bulk on %v and bulk_path %v; want %v and true", rep.FrpsBulkPorts, rep.Ports.BulkPath, want)
	}
	if certMarkOf(t, hostAPI.AgentCertFile()) != first || certMarkOf(t, hostAPI.AgentKeyFile()) != key {
		t.Error("up --bulk-path wrote the first certificate or the key: an Allocation staged before would lose its token tunnel")
	}
	firstSPKI, _ := spkiOf(t, hostAPI.AgentCertFile())
	bulkSPKI, bulkCert := spkiOf(t, hostAPI.BulkCertFile())
	if bulkSPKI != firstSPKI || !slices.Equal(bulkCert.DNSNames, []string{"bulk.dial-home.hpcburst.invalid"}) {
		t.Errorf("the second certificate: same key %v, names %v; want the first's key and the bulk name alone", bulkSPKI == firstSPKI, bulkCert.DNSNames)
	}
	if !strings.Contains(fileOf(filepath.Join(h.dir, "frps-bulk", "frps.toml")), "transport.tcpMux = false\n") {
		t.Error("frps-bulk's configuration does not turn tcpMux off")
	}

	// frps-bulk is a child of the supervisor, and holds exactly its three ports, on loopback.
	pid := entryPID(t, h.dir, "frps-bulk")
	if sup := recordedProcs(t, h.dir)[1]; ppidOfPID(t, pid) != sup.PID {
		t.Fatalf("frps-bulk (pid %d) is not a child of the supervisor (pid %d)", pid, sup.PID)
	}
	want := []string{"127.0.0.1:" + itoa(bulkUpBase+13), "127.0.0.1:" + itoa(bulkUpBase+14), "127.0.0.1:" + itoa(bulkUpBase+15)}
	waitFor(t, 30*time.Second, "frps-bulk's three listeners", func() bool {
		return slices.Equal(sortedStrings(listenersOf(t, pid)), sortedStrings(want))
	})
	s := cli(t, h.args("status", "--json", "--no-gc-probe")...)
	var st struct {
		Reasons     []string `json:"reasons"`
		Controllers []struct {
			Name      string `json:"name"`
			Running   bool   `json:"running"`
			HealthzOK *bool  `json:"healthz_ok"`
		} `json:"controllers"`
	}
	parseJSON(t, s.out, &st)
	found := false
	for _, e := range st.Controllers {
		if e.Name == "frps-bulk" {
			found = e.Running && e.HealthzOK != nil && *e.HealthzOK
		}
	}
	if !found {
		t.Errorf("status does not report frps-bulk running and healthy: %d\n%s\n%s", s.code, s.out, s.err)
	}
	for _, reason := range st.Reasons {
		if strings.Contains(reason, "frps-bulk") || strings.Contains(reason, "the bulk path's frps") {
			t.Errorf("status finds frps-bulk wanting: %s", reason)
		}
	}

	// ---- down leaves nothing on the block, the bulk ports included ----
	if r := cli(t, h.args("down")...); r.code != exitOK {
		t.Fatalf("down exited %d: %s", r.code, r.err)
	}
	waitFor(t, 30*time.Second, "the block to be free", func() bool { return len(block()) == 0 })
	if running(pid) {
		t.Errorf("frps-bulk (pid %d) still runs after down", pid)
	}
	if certMarkOf(t, hostAPI.AgentCertFile()) != first {
		t.Error("down wrote the first certificate")
	}
}
