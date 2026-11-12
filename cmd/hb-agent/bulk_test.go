package main

// The agent's bulk frpc (beta; HBEP-6, The bulk path; issue #448, part 4):
// with agent.json's bulk block, and not in ssh mode, the agent runs a second
// frpc beside the token frpc, to the same target from the same serving socket,
// with its own files and its own restarts, and stops both when the Instance
// ends. Without the block it runs exactly one frpc, as before. The stand-in
// frpc is the shell script stageStubFrpc stages; the real frpc is in
// internal/agent's bulk tests. Every process the tests signal is signalled
// through a pidfd, never by PID or process group.

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The two server names, typed from HBEP-6.
const (
	testTokenName = "dial-home.hpcburst.invalid"
	testBulkName  = "bulk.dial-home.hpcburst.invalid"
)

// pidfdSignal sends sig to the process the ID names, through a pidfd opened for
// it, after checking that PID still has the recorded start time (a reused PID
// has another).
func pidfdSignal(id agent.ProcessID, sig syscall.Signal) error {
	fd, err := unix.PidfdOpen(id.PID, 0)
	if err != nil {
		return fmt.Errorf("pidfd_open %d: %w", id.PID, err)
	}
	defer unix.Close(fd)
	st, err := procStart(id.PID)
	if err != nil {
		return fmt.Errorf("process %d is gone: %w", id.PID, err)
	}
	if st != id.StartTime {
		return fmt.Errorf("PID %d has start time %d, not the recorded %d: it was reused", id.PID, st, id.StartTime)
	}
	return unix.PidfdSendSignal(fd, sig, nil, 0)
}

// leftUnder finds the live processes, other than this one, whose command line
// names root, kills them through a pidfd, and returns their command lines.
func leftUnder(root string) []string {
	var left []string
	entries, _ := os.ReadDir("/proc")
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil || !strings.Contains(string(b), root) {
			continue
		}
		st, err := procStart(pid)
		if err != nil {
			continue
		}
		id := agent.ProcessID{PID: pid, StartTime: st}
		if !id.Alive() {
			continue
		}
		left = append(left, strings.ReplaceAll(string(b), "\x00", " "))
		_ = pidfdSignal(id, syscall.SIGKILL)
	}
	return left
}

// waitUntil polls cond until it holds or d passes.
func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// writeBulkCert issues the bulk path's second certificate for the key
// writeCert made, as the launcher does (part 2): a self-signed leaf for the
// bulk name only, not a CA, with the same key and so the same pin. It writes it
// to path.
func writeBulkCert(t *testing.T, host agentapi.Host, path string) {
	t.Helper()
	kb, err := os.ReadFile(host.AgentKeyFile())
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(kb)
	key, err := x509.ParseECPrivateKey(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "agent-endpoint"},
		DNSNames:              []string{testBulkName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
}

// pinOfFile is the SHA-256 of the DER SubjectPublicKeyInfo of the certificate
// in a PEM file (HBEP-7), computed here independently of agentapi.PinOf.
func pinOfFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(b)
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

// startLog records the processes an Frpc started.
type startLog struct {
	mu  sync.Mutex
	ids []agent.ProcessID
}

func (s *startLog) add(id agent.ProcessID) {
	s.mu.Lock()
	s.ids = append(s.ids, id)
	s.mu.Unlock()
}

func (s *startLog) all() []agent.ProcessID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]agent.ProcessID(nil), s.ids...)
}

func (s *startLog) count() int { return len(s.all()) }

func (s *startLog) last() agent.ProcessID {
	a := s.all()
	if len(a) == 0 {
		return agent.ProcessID{}
	}
	return a[len(a)-1]
}

// tRig is a started transport and what the test needs to see of it.
type tRig struct {
	t        *testing.T
	j        job
	root     string
	runDir   string
	tr       *transport
	out      *bytes.Buffer
	tokenSt  *startLog
	bulkSt   *startLog
	stopOnce sync.Once
}

type rigOptions struct {
	mode agentapi.DialHomeMode
	bulk bool // agent.json carries the bulk block
	// bulkCert is what is staged as dial-home-bulk.crt: "" the second certificate,
	// "first" the first certificate, "none" nothing.
	bulkCert string
	// backoff is both frpc's restart backoff (the test sets it on the transport's
	// Frpc after it is built).
	backoff time.Duration
}

// newTRig builds the job and starts the transport, as run does, with the stand-in
// frpc. The transport is stopped at the test's end (and by stop, earlier).
func newTRig(t *testing.T, o rigOptions) *tRig {
	t.Helper()
	root := shortRoot(t)
	host := agentapi.Host{StateDir: filepath.Join(root, "state"), PortBase: 29220}
	pin := writeCert(t, host)
	cfg := agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: o.mode,
		Endpoint: "127.0.0.1:9", CertSPKISHA256: pin, FrpcDigest: strings.Repeat("d", 64),
	}
	switch o.mode {
	case agentapi.DialHomeHostDialed:
		cfg.Endpoint = ""
		cfg.Allocation = eUIDHostDialed
	case agentapi.DialHomeSSH:
		cfg.Endpoint = "127.0.0.1:22"
		cfg.SSH = &agentapi.SSHForward{User: "hb", AgentPort: 29301, DialHomePort: 29302}
	}
	block := &agentapi.BulkPath{ServerName: testBulkName}
	if o.bulk && o.mode != agentapi.DialHomeSSH {
		cfg.Bulk = block
	}
	j := newJob(t, root, cfg)
	if o.mode == agentapi.DialHomeSSH {
		dh := filepath.Join(root, "dial-home")
		if err := os.MkdirAll(dh, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, f := range []string{agentapi.DialHomeKeyFile, agentapi.DialHomeKnownHostsFile} {
			if err := os.WriteFile(filepath.Join(dh, f), []byte("test\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	bulkFile := filepath.Join(j.allocDir, agentapi.StagedBulkDialHomeCertName)
	switch o.bulkCert {
	case "":
		writeBulkCert(t, host, bulkFile)
	case "first":
		b, err := os.ReadFile(filepath.Join(j.allocDir, agentapi.StagedDialHomeCertName))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(bulkFile, b, 0o644); err != nil {
			t.Fatal(err)
		}
	case "none":
	default:
		t.Fatalf("bulkCert %q", o.bulkCert)
	}
	aj := agent.Job{AllocDir: j.allocDir, Config: j.cfg, Credential: testCredential, JobID: eJobID, TmpDir: filepath.Join(root, "tmp")}
	if o.bulk {
		// In ssh mode the block cannot be encoded (AgentConfig.Validate refuses
		// it), so it is set on the job by hand, which only a test can do.
		aj.Config.Bulk = block
	}
	runDir, err := agent.MakeRunDir(aj.TmpDir)
	if err != nil {
		t.Fatal(err)
	}
	r := &tRig{t: t, j: j, root: root, runDir: runDir, out: &bytes.Buffer{}, tokenSt: &startLog{}, bulkSt: &startLog{}}
	logOut := &syncWriter{w: r.out}
	log := slog.New(slog.NewTextHandler(logOut, nil))
	getenv := func(k string) string {
		switch k {
		case "TMPDIR":
			return aj.TmpDir
		case "HPCBURST_SSH":
			return filepath.Join(j.bin, "ssh")
		}
		return ""
	}
	t.Setenv("HB_FAKE_SSH_LOG", j.sshLog)
	tr, err := startTransport(aj, runDir, "aid", getenv, io.Discard, log)
	if err != nil {
		t.Fatalf("startTransport: %v", err)
	}
	r.tr = tr
	// The recorders go in before the first Serve, which is what starts anything.
	// They run after any hook the transport set, never in its place.
	chain := func(prev func(agent.ProcessID), rec func(agent.ProcessID)) func(agent.ProcessID) {
		return func(id agent.ProcessID) {
			if prev != nil {
				prev(id)
			}
			rec(id)
		}
	}
	tr.frpc.OnStart = chain(tr.frpc.OnStart, r.tokenSt.add)
	if tr.bulk != nil {
		tr.bulk.OnStart = chain(tr.bulk.OnStart, r.bulkSt.add)
	}
	if o.backoff > 0 {
		tr.frpc.MinBackoff, tr.frpc.MaxBackoff = o.backoff, 4*o.backoff
		if tr.bulk != nil {
			tr.bulk.MinBackoff, tr.bulk.MaxBackoff = o.backoff, 4*o.backoff
		}
	}
	t.Cleanup(func() {
		r.stop()
		if left := leftUnder(root); len(left) > 0 && !t.Failed() {
			t.Errorf("processes left behind after the test: %q", left)
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", logOut.String())
		}
	})
	return r
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (s *syncWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.String()
}

func (r *tRig) stop() { r.stopOnce.Do(r.tr.stop) }

// stubStarts returns the stand-in frpc's starts as its own log has them: PID and
// the configuration file it was told to read.
func (r *tRig) stubStarts() (pids []int, configPaths []string) {
	for _, line := range strings.Split(readFile(r.j.frpcLog), "\n") {
		f := strings.Fields(line)
		if len(f) < 4 || f[0] != "start" || f[2] != "-c" {
			continue
		}
		pid, _ := strconv.Atoi(f[1])
		pids = append(pids, pid)
		configPaths = append(configPaths, f[3])
	}
	return pids, configPaths
}

// waitStubs waits until n stand-in frpc have written their start lines and their
// copies of the configuration and of the certificate they trust (the stand-in
// writes them as its first actions, after OnStart is told of the process, so a
// test that reads them waits for them: issue #141's lesson).
func (r *tRig) waitStubs(n int) {
	r.t.Helper()
	waitUntil(r.t, 20*time.Second, fmt.Sprintf("%d stand-in frpc to write their start lines and copies", n), func() bool {
		pids, _ := r.stubStarts()
		if len(pids) < n {
			return false
		}
		for _, pid := range pids {
			if r.stubConfigOf(pid) == "" || r.stubCertOf(pid) == "" {
				return false
			}
		}
		return true
	})
}

func (r *tRig) names() []string {
	es, _ := os.ReadDir(r.runDir)
	var out []string
	for _, e := range es {
		out = append(out, e.Name())
	}
	return out
}

// serverOf returns serverAddr:serverPort of a copy of a stand-in's configuration.
func serverOf(t *testing.T, cfg string) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^serverAddr = "(.*)"\nserverPort = ([0-9]+)$`).FindStringSubmatch(cfg)
	if m == nil {
		t.Fatalf("no server in the configuration:\n%s", cfg)
	}
	return m[1] + ":" + m[2]
}

// stubConfigOf returns the copy of the configuration a stand-in with this PID
// was started with.
func (r *tRig) stubConfigOf(pid int) string {
	return readFile(r.j.frpcLog + "." + strconv.Itoa(pid) + ".toml")
}
func (r *tRig) stubCertOf(pid int) string {
	return readFile(r.j.frpcLog + "." + strconv.Itoa(pid) + ".crt")
}

// ---- the criteria ----

// The bulk frpc runs only with the block and outside ssh mode (HBEP-6: "Ssh mode
// gets no bulk frpc"). Validate refuses the block in ssh mode first, so this
// table is the only thing that decides the mode in the transport.
func TestBulkWantedOnlyWithTheBlockAndNotInSSHMode(t *testing.T) {
	block := &agentapi.BulkPath{ServerName: testBulkName}
	for _, tc := range []struct {
		name string
		mode agentapi.DialHomeMode
		bulk *agentapi.BulkPath
		want bool
	}{
		{"direct with the block", agentapi.DialHomeDirect, block, true},
		{"host-dialed with the block", agentapi.DialHomeHostDialed, block, true},
		{"ssh with the block", agentapi.DialHomeSSH, block, false},
		{"direct without", agentapi.DialHomeDirect, nil, false},
		{"host-dialed without", agentapi.DialHomeHostDialed, nil, false},
		{"ssh without", agentapi.DialHomeSSH, nil, false},
	} {
		if got := bulkWanted(agentapi.AgentConfig{Mode: tc.mode, Bulk: tc.bulk}); got != tc.want {
			t.Errorf("%s: bulkWanted = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Alpha pin: without the block the agent starts exactly one frpc, from its own
// files, and no frpc-bulk.toml (HBEP-6: "Without the bulk path, everything works
// as it does today"). There is no second Frpc at all, which is what rules out a
// late second start; the tunnel the agent's loop talks to is the token Frpc
// itself. The control, in the next test, is the same setup with the block: two.
func TestOneFrpcWithoutBulkBlock(t *testing.T) {
	for _, mode := range []agentapi.DialHomeMode{agentapi.DialHomeDirect, agentapi.DialHomeHostDialed} {
		t.Run(string(mode), func(t *testing.T) {
			r := newTRig(t, rigOptions{mode: mode, bulk: false, bulkCert: "none"})
			if r.tr.bulk != nil {
				t.Fatal("the transport built a bulk frpc without the bulk block")
			}
			if got, ok := r.tr.tunnel().(*agent.Frpc); !ok || got != r.tr.frpc {
				t.Fatalf("the agent's tunnel is %T, want the token frpc itself, as before", r.tr.tunnel())
			}
			r.tr.tunnel().Serve(eInst)
			waitUntil(t, 20*time.Second, "the token frpc to start", func() bool { return r.tokenSt.count() >= 1 })
			r.waitStubs(1)
			r.stop()
			pids, paths := r.stubStarts()
			if len(pids) != 1 || r.tokenSt.count() != 1 {
				t.Fatalf("the stand-in frpc started %d times (OnStart %d), want exactly one", len(pids), r.tokenSt.count())
			}
			if filepath.Base(paths[0]) != "frpc.toml" {
				t.Fatalf("the one frpc read %s, want frpc.toml", paths[0])
			}
			for _, n := range r.names() {
				if strings.Contains(n, "bulk") {
					t.Errorf("the run directory holds %s without the bulk block", n)
				}
			}
			for _, p := range paths {
				if strings.Contains(p, "bulk") {
					t.Errorf("an frpc read %s", p)
				}
			}
		})
	}
}

// The control of the alpha pin, and the wiring: with the block the agent starts
// two frpc, each from its own files, to the same target (the front's dial-home
// address in direct mode, the serving listener in host-dialed mode), the bulk one
// with its own certificate copy, tcpMux off and the bulk server name.
func TestTwoFrpcWithTheBulkBlock(t *testing.T) {
	for _, mode := range []agentapi.DialHomeMode{agentapi.DialHomeDirect, agentapi.DialHomeHostDialed} {
		t.Run(string(mode), func(t *testing.T) {
			r := newTRig(t, rigOptions{mode: mode, bulk: true})
			if r.tr.bulk == nil {
				t.Fatal("the transport built no bulk frpc with the bulk block")
			}
			r.tr.tunnel().Serve(eInst)
			waitUntil(t, 20*time.Second, "both frpc to start", func() bool { return r.tokenSt.count() >= 1 && r.bulkSt.count() >= 1 })
			r.waitStubs(2)
			r.stop()
			pids, paths := r.stubStarts()
			if len(pids) != 2 || r.tokenSt.count() != 1 || r.bulkSt.count() != 1 {
				t.Fatalf("the stand-in frpc started %d times (OnStart %d and %d), want one token and one bulk", len(pids), r.tokenSt.count(), r.bulkSt.count())
			}
			byBase := map[string]int{}
			for i, p := range paths {
				byBase[filepath.Base(p)] = pids[i]
				if filepath.Dir(p) != r.runDir {
					t.Errorf("an frpc read %s, outside the run directory %s", p, r.runDir)
				}
			}
			tokPID, bulkPID := byBase["frpc.toml"], byBase["frpc-bulk.toml"]
			if tokPID == 0 || bulkPID == 0 {
				t.Fatalf("the two frpc read %v, want frpc.toml and frpc-bulk.toml", paths)
			}
			tokCfg, bulkCfg := r.stubConfigOf(tokPID), r.stubConfigOf(bulkPID)
			// The same target: the dial-home address in direct mode, the serving
			// listener in host-dialed mode (which the agent logs once it binds it).
			want := "127.0.0.1:9" // agent.json's endpoint
			if mode == agentapi.DialHomeHostDialed {
				m := regexp.MustCompile(`host-dialed: listening for the relays.* serving=([0-9]+)`).FindStringSubmatch(r.out.String())
				if m == nil {
					t.Fatalf("the agent did not log its serving listener:\n%s", r.out.String())
				}
				want = "127.0.0.1:" + m[1]
			}
			if got := serverOf(t, tokCfg); got != want {
				t.Errorf("the token frpc dials %s, want %s", got, want)
			}
			if got := serverOf(t, bulkCfg); got != want {
				t.Errorf("the bulk frpc dials %s, want the token frpc's %s", got, want)
			}
			for _, w := range []string{
				"transport.tcpMux = false\n",
				`transport.tls.serverName = "` + testBulkName + `"` + "\n",
				`transport.tls.trustedCaFile = "` + filepath.Join(r.runDir, "dial-home-bulk.crt") + `"` + "\n",
				`log.to = "` + filepath.Join(r.runDir, "frpc-bulk.log") + `"` + "\n",
				`name = "` + eInst + `"`,
			} {
				if !strings.Contains(bulkCfg, w) {
					t.Errorf("the bulk frpc's configuration lacks %s:\n%s", strings.TrimSpace(w), bulkCfg)
				}
			}
			if strings.Contains(tokCfg, "transport.tcpMux = ") || strings.Contains(tokCfg, testBulkName) {
				t.Errorf("the token frpc's configuration carries the bulk path's settings:\n%s", tokCfg)
			}
			// Each trusts the checked copy of its own staged certificate.
			first, _ := os.ReadFile(filepath.Join(r.j.allocDir, agentapi.StagedDialHomeCertName))
			second, _ := os.ReadFile(filepath.Join(r.j.allocDir, agentapi.StagedBulkDialHomeCertName))
			if got := r.stubCertOf(tokPID); got != string(first) || got == "" {
				t.Errorf("the token frpc trusts a certificate that is not the staged first one")
			}
			if got := r.stubCertOf(bulkPID); got != string(second) || got == "" {
				t.Errorf("the bulk frpc trusts a certificate that is not the staged second one")
			}
			if pinOfFile(t, filepath.Join(r.j.allocDir, agentapi.StagedBulkDialHomeCertName)) != r.j.cfg.CertSPKISHA256 {
				t.Errorf("the staged second certificate has a pin other than agent.json's: the two share one key")
			}
		})
	}
}

// In ssh mode the transport builds no bulk frpc even when agent.json's block is
// set (HBEP-6: "Ssh mode gets no bulk frpc"). AgentConfig.Validate refuses the
// block in ssh mode first, so LoadJob never gives the transport this job; the
// job is built by hand to show the transport's own guard. The fake ssh carries
// connection 2 so the token frpc starts, and exactly one frpc runs.
func TestNoBulkFrpcInSSHMode(t *testing.T) {
	r := newTRig(t, rigOptions{mode: agentapi.DialHomeSSH, bulk: true})
	if r.tr.bulk != nil {
		t.Fatal("the transport built a bulk frpc in ssh mode")
	}
	r.tr.tunnel().Serve(eInst)
	waitUntil(t, 30*time.Second, "the token frpc to start behind connection 2's forward", func() bool { return r.tokenSt.count() >= 1 })
	r.waitStubs(1)
	r.stop()
	pids, paths := r.stubStarts()
	if len(pids) != 1 || filepath.Base(paths[0]) != "frpc.toml" {
		t.Fatalf("frpc started %d times (%v) in ssh mode, want exactly the token frpc", len(pids), paths)
	}
}

// The Instance's end stops both frpc: the agent's loop tells its tunnel to serve
// no Instance, and each frpc's process is gone (stopped by its own supervisor,
// recorded by PID and start time), with no file removed but the configurations.
func TestInstanceEndStopsBothFrpc(t *testing.T) {
	r := newTRig(t, rigOptions{mode: agentapi.DialHomeDirect, bulk: true, backoff: 200 * time.Millisecond})
	r.tr.tunnel().Serve(eInst)
	waitUntil(t, 20*time.Second, "both frpc to start", func() bool { return r.tokenSt.count() >= 1 && r.bulkSt.count() >= 1 })
	tok, bulk := r.tokenSt.last(), r.bulkSt.last()
	if !tok.Alive() || !bulk.Alive() {
		t.Fatalf("an frpc is not running: token %+v bulk %+v", tok, bulk)
	}
	r.tr.tunnel().Serve("")
	waitUntil(t, 20*time.Second, "both frpc to stop", func() bool { return !tok.Alive() && !bulk.Alive() })
	// And no frpc comes back for no Instance: the next Serve starts them again,
	// which shows that the stop was the Instance's end and not the transport's.
	if r.tokenSt.count() != 1 || r.bulkSt.count() != 1 {
		t.Fatalf("an frpc started again after the Instance ended: token %d, bulk %d starts", r.tokenSt.count(), r.bulkSt.count())
	}
	r.tr.tunnel().Serve(eInst)
	waitUntil(t, 20*time.Second, "both frpc to start for the next Instance", func() bool { return r.tokenSt.count() >= 2 && r.bulkSt.count() >= 2 })
}

// A kill of the bulk frpc, through a pidfd, leaves the token frpc's process ID
// and start time as they were, recorded before the kill, and starts the bulk one
// again with backoff on its own (HBEP-6: "the agent restarts it with backoff on
// its own, apart from the token frpc"). The kill is the check's own positive
// control: the bulk frpc does restart, so a restart that disturbed the token frpc
// would show.
func TestBulkFrpcRestartsAlone(t *testing.T) {
	r := newTRig(t, rigOptions{mode: agentapi.DialHomeDirect, bulk: true, backoff: 200 * time.Millisecond})
	r.tr.tunnel().Serve(eInst)
	waitUntil(t, 20*time.Second, "both frpc to start", func() bool { return r.tokenSt.count() >= 1 && r.bulkSt.count() >= 1 })
	tok, bulk := r.tokenSt.last(), r.bulkSt.last()
	if tok.PID == 0 || tok.StartTime == 0 || bulk.PID == 0 || bulk.StartTime == 0 {
		t.Fatalf("recorded IDs %+v and %+v lack a PID or a start time", tok, bulk)
	}
	killedAt := time.Now()
	if err := pidfdSignal(bulk, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 20*time.Second, "the bulk frpc to start again", func() bool { return r.bulkSt.count() >= 2 })
	if gap := time.Since(killedAt); gap < 200*time.Millisecond {
		t.Errorf("the bulk frpc started again %s after its kill, sooner than its 200 ms backoff", gap)
	}
	if again := r.bulkSt.last(); again == bulk {
		t.Fatalf("the bulk frpc's second start is the killed process %+v", bulk)
	}
	if !tok.Alive() {
		t.Fatalf("the token frpc %+v did not survive the bulk frpc's kill", tok)
	}
	if got := r.tokenSt.all(); len(got) != 1 || got[0] != tok {
		t.Fatalf("the token frpc was started again: %+v, was one start %+v", got, tok)
	}
}

// The reverse: a kill of the token frpc leaves the bulk frpc alone.
func TestTokenFrpcRestartLeavesBulk(t *testing.T) {
	r := newTRig(t, rigOptions{mode: agentapi.DialHomeDirect, bulk: true, backoff: 200 * time.Millisecond})
	r.tr.tunnel().Serve(eInst)
	waitUntil(t, 20*time.Second, "both frpc to start", func() bool { return r.tokenSt.count() >= 1 && r.bulkSt.count() >= 1 })
	tok, bulk := r.tokenSt.last(), r.bulkSt.last()
	if tok.PID == 0 || tok.StartTime == 0 || bulk.PID == 0 || bulk.StartTime == 0 {
		t.Fatalf("recorded IDs %+v and %+v lack a PID or a start time", tok, bulk)
	}
	if err := pidfdSignal(tok, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 20*time.Second, "the token frpc to start again", func() bool { return r.tokenSt.count() >= 2 })
	if again := r.tokenSt.last(); again == tok {
		t.Fatalf("the token frpc's second start is the killed process %+v", tok)
	}
	if !bulk.Alive() {
		t.Fatalf("the bulk frpc %+v did not survive the token frpc's kill", bulk)
	}
	if got := r.bulkSt.all(); len(got) != 1 || got[0] != bulk {
		t.Fatalf("the bulk frpc was started again: %+v, was one start %+v", got, bulk)
	}
}

// A bulk frpc that the agent refuses (the first certificate staged as the bulk
// file) ends its own Run with the reason and nothing else: the token frpc starts
// and serves, the transport answers a Serve, and it stops cleanly. The bulk frpc
// never starts. The control is TestTwoFrpcWithTheBulkBlock, the same setup with
// the right file.
func TestARefusedBulkFrpcDoesNotStopTheAgentOrTheTokenFrpc(t *testing.T) {
	r := newTRig(t, rigOptions{mode: agentapi.DialHomeDirect, bulk: true, bulkCert: "first", backoff: 200 * time.Millisecond})
	r.tr.tunnel().Serve(eInst)
	waitUntil(t, 20*time.Second, "the token frpc to start", func() bool { return r.tokenSt.count() >= 1 })
	waitUntil(t, 20*time.Second, "the agent to log the bulk frpc's refusal", func() bool {
		return strings.Contains(r.out.String(), "the bulk frpc") && strings.Contains(r.out.String(), "ended")
	})
	tok := r.tokenSt.last()
	if r.bulkSt.count() != 0 {
		t.Fatalf("the bulk frpc started with the first certificate as its file: %d starts", r.bulkSt.count())
	}
	if !strings.Contains(r.out.String(), testBulkName) {
		t.Errorf("the refusal does not name the server name the certificate lacks (%s):\n%s", testBulkName, r.out.String())
	}
	// The token frpc and the transport go on: a Serve for another Instance
	// restarts the token frpc for it.
	r.tr.tunnel().Serve("bbbbbbbb-1111-2222-3333-444444444444")
	waitUntil(t, 20*time.Second, "the token frpc to follow the next Instance", func() bool { return r.tokenSt.count() >= 2 })
	waitUntil(t, 20*time.Second, "the first token frpc to stop", func() bool { return !tok.Alive() })
	if r.bulkSt.count() != 0 {
		t.Fatalf("the bulk frpc started later: %d starts", r.bulkSt.count())
	}
	_, paths := r.stubStarts()
	for _, p := range paths {
		if strings.Contains(p, "bulk") {
			t.Errorf("an frpc read %s", p)
		}
	}
	r.stop() // the transport stops cleanly with a refused bulk frpc
}

// A missing bulk certificate is not a mismatch: like the token frpc's it is
// retried (a purge of shared storage, which the Provider stages again), the
// token frpc runs, and the bulk one starts once the file appears.
func TestAMissingBulkCertificateIsRetried(t *testing.T) {
	r := newTRig(t, rigOptions{mode: agentapi.DialHomeDirect, bulk: true, bulkCert: "none", backoff: 100 * time.Millisecond})
	r.tr.tunnel().Serve(eInst)
	waitUntil(t, 20*time.Second, "the token frpc to start", func() bool { return r.tokenSt.count() >= 1 })
	waitUntil(t, 20*time.Second, "the agent to log that the bulk certificate cannot be read yet", func() bool {
		return strings.Contains(r.out.String(), "frpc's files cannot be read yet")
	})
	if r.bulkSt.count() != 0 {
		t.Fatal("the bulk frpc started without its certificate")
	}
	host := agentapi.Host{StateDir: filepath.Join(r.root, "state"), PortBase: 29220}
	writeBulkCert(t, host, filepath.Join(r.j.allocDir, agentapi.StagedBulkDialHomeCertName))
	waitUntil(t, 20*time.Second, "the bulk frpc to start once its certificate appears", func() bool { return r.bulkSt.count() >= 1 })
}

// ---- the whole agent, in its own process, through run and main's wiring ----

// startAgent runs the agent in its own process, as the job's bootstrap does (as
// job.start does), but the agent is signalled only through a pidfd of its own
// (clone's PidFD), and the processes it leaves, if it is killed, are found by
// their command lines and killed through pidfds too.
func (j job) startAgent(t *testing.T) (*agentProc, *syncBytes) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self, j.allocDir)
	cmd.Env = append(os.Environ(),
		"HB_TEST_ROLE="+roleAgent,
		"SLURM_JOB_ID="+eJobID,
		"SLURM_RESTART_COUNT=0",
		"TMPDIR="+filepath.Join(j.root, "tmp"),
		"HPCBURST_APPTAINER="+filepath.Join(j.bin, "apptainer"),
		"HPCBURST_SSH="+filepath.Join(j.bin, "ssh"),
		"HPCBURST_NVIDIA_SMI="+filepath.Join(j.bin, "no-nvidia-smi"),
		"HB_FAKE_APPTAINER_LOG="+j.appLog,
		"HB_FAKE_SSH_LOG="+j.sshLog,
		"GORACE=atexit_sleep_ms=0",
	)
	out := &syncBytes{}
	cmd.Stdout, cmd.Stderr = out, out
	fd := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &agentProc{done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0)
			<-p.done
		}
		_ = unix.Close(fd)
		if left := leftUnder(j.root); len(left) > 0 && !t.Failed() {
			t.Errorf("processes left behind after the test: %q", left)
		}
		if t.Failed() {
			t.Logf("agent output:\n%s", out.String())
		}
	})
	return p, out
}

// agentProc is an agent process and its one waiter.
type agentProc struct {
	done chan struct{}
	err  error // cmd.Wait's, valid once done is closed
}

// exit waits for the agent to exit, and returns its Wait error.
func (p *agentProc) exit(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(d):
		t.Fatalf("the agent did not exit within %s", d)
		return nil
	}
}

// syncBytes is a buffer the agent's two streams write and the test reads.
type syncBytes struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBytes) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBytes) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// runAgentE2E runs the real agent (direct mode) against the real agent endpoint
// with the stand-in frpc, tells it to run the Instance, waits for its worker, and
// then, with want frpc started, tells it to stop and returns the job and the
// agent's output. The agent must exit 0.
func runAgentE2E(t *testing.T, bulk bool, bulkCert string, want int) (job, string) {
	t.Helper()
	root := shortRoot(t)
	ep := startEndpoint(t, root)
	cfg := agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeDirect,
		Endpoint: ep.addr, CertSPKISHA256: ep.pin, FrpcDigest: strings.Repeat("d", 64),
	}
	if bulk {
		cfg.Bulk = &agentapi.BulkPath{ServerName: testBulkName}
	}
	j := newJob(t, root, cfg)
	host := agentapi.Host{StateDir: filepath.Join(root, "state"), PortBase: 29220}
	bulkFile := filepath.Join(j.allocDir, agentapi.StagedBulkDialHomeCertName)
	switch bulkCert {
	case "":
		if bulk {
			writeBulkCert(t, host, bulkFile)
		}
	case "first":
		b, err := os.ReadFile(filepath.Join(j.allocDir, agentapi.StagedDialHomeCertName))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(bulkFile, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmd, out := j.startAgent(t)
	ep.backend.SetDesired(eUID, runState(j.engLog, nil))
	waitReport(t, ep.backend, "the worker running", func(r agentapi.Report) bool {
		return r.Instance != nil && len(r.Instance.Workers) == 1 && r.Instance.Workers[0].Phase == agentapi.PartRunning
	})
	// The stand-in writes its start line, its configuration copy and its
	// certificate copy as its first actions.
	waitUntil(t, 20*time.Second, fmt.Sprintf("%d stand-in frpc to write their start lines and copies", want), func() bool {
		pids, configs, _ := frpcStarts(t, j)
		if len(pids) < want {
			return false
		}
		for i, pid := range pids {
			if configs[i] == "" || readFile(fmt.Sprintf("%s.%d.crt", j.frpcLog, pid)) == "" {
				return false
			}
		}
		return true
	})
	ep.backend.SetDesired(eUID, agentapi.DesiredState{Action: agentapi.ActionStop, Settings: agentapi.AgentSettings{StopGrace: agentapi.Duration(2 * time.Second)}})
	if err := cmd.exit(t, 30*time.Second); err != nil {
		t.Fatalf("the agent exited with %v after a stop, want 0", err)
	}
	return j, out.String()
}

func frpcConfigPaths(j job) []string {
	var out []string
	for _, line := range strings.Split(readFile(j.frpcLog), "\n") {
		f := strings.Fields(line)
		if len(f) >= 4 && f[0] == "start" && f[2] == "-c" {
			out = append(out, f[3])
		}
	}
	return out
}

// With the bulk block, through the whole agent: two frpc start, each from its own
// configuration file in the run directory, the Instance's end stops them both
// (neither outlives the agent), and the agent exits 0.
func TestAgentRunsTheBulkFrpcBesideTheTokenFrpc(t *testing.T) {
	j, out := runAgentE2E(t, true, "", 2)
	paths := frpcConfigPaths(j)
	if len(paths) != 2 {
		t.Fatalf("the agent started %d frpc (%v), want the token frpc and the bulk frpc:\n%s", len(paths), paths, out)
	}
	bases := []string{filepath.Base(paths[0]), filepath.Base(paths[1])}
	if !(bases[0] == "frpc.toml" && bases[1] == "frpc-bulk.toml") && !(bases[0] == "frpc-bulk.toml" && bases[1] == "frpc.toml") {
		t.Fatalf("the two frpc read %v, want frpc.toml and frpc-bulk.toml", bases)
	}
	pids, configs, _ := frpcStarts(t, j)
	for i, c := range configs {
		bulk := filepath.Base(paths[i]) == "frpc-bulk.toml"
		if bulk != strings.Contains(c, "transport.tcpMux = false\n") {
			t.Errorf("%s: tcpMux off = %v, want %v:\n%s", paths[i], !bulk, bulk, c)
		}
	}
	for _, pid := range pids {
		if processLives(pid) {
			t.Errorf("frpc %d still runs after the agent exited", pid)
		}
	}
	if !strings.Contains(out, "frpc=bulk") {
		t.Errorf("the agent's log never names the bulk frpc:\n%s", out)
	}
}

// The control of the alpha pin through the whole agent: without the block, one
// frpc, from frpc.toml, and no bulk file anywhere in what the agent did.
func TestAgentRunsOneFrpcWithoutTheBulkBlock(t *testing.T) {
	j, out := runAgentE2E(t, false, "", 1)
	paths := frpcConfigPaths(j)
	if len(paths) != 1 || filepath.Base(paths[0]) != "frpc.toml" {
		t.Fatalf("the agent started frpc from %v, want exactly frpc.toml:\n%s", paths, out)
	}
	if strings.Contains(out, "bulk") {
		t.Errorf("the agent's log names the bulk path without the bulk block:\n%s", out)
	}
	pids, _, _ := frpcStarts(t, j)
	for _, pid := range pids {
		if processLives(pid) {
			t.Errorf("frpc %d still runs after the agent exited", pid)
		}
	}
}

// A refused bulk frpc (the first certificate staged as its file) stops neither
// the agent nor the token frpc: the agent registers, runs its worker, starts the
// token frpc and exits 0 at the stop; the refusal is in its log; no bulk frpc ran.
func TestAgentWithARefusedBulkFrpcRunsOn(t *testing.T) {
	j, out := runAgentE2E(t, true, "first", 1)
	paths := frpcConfigPaths(j)
	if len(paths) != 1 || filepath.Base(paths[0]) != "frpc.toml" {
		t.Fatalf("the agent started frpc from %v, want only frpc.toml:\n%s", paths, out)
	}
	if !strings.Contains(out, "the bulk frpc") || !strings.Contains(out, "refused") {
		t.Errorf("the agent's log does not say that the bulk frpc was refused:\n%s", out)
	}
}
