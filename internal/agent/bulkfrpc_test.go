package agent_test

// The bulk frpc (beta; HBEP-6, The bulk path; issue #448, part 4): its
// configuration, its own files, and the certificate check that runs before it
// starts. Every expected value is typed from HBEP-6 ("frpc in the job" and
// "The bulk path"), never produced by the renderer under test. The tests here
// use a stand-in frpc (a shell script) and no frp binary; the real frpc is in
// bulkfrpc_real_test.go.

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// The two server names, typed from HBEP-6 (the token frpc's, and the bulk
// frpc's, "bulk.dial-home.hpcburst.invalid").
const (
	tokenName = "dial-home.hpcburst.invalid"
	bulkName  = "bulk.dial-home.hpcburst.invalid"
)

// bulkPair makes the two certificates of one launcher key, as the launcher
// issues them: the first names the token server, the second the bulk server
// only. Their pin is the same, since it is the key's.
type bulkPair struct {
	first, second testCert
	pin           string
}

func makeBulkPair(t *testing.T) bulkPair {
	t.Helper()
	first := makeCert(t, tokenName)
	second := makeCertWith(t, certOpts{names: []string{bulkName}, key: first.tls.PrivateKey.(*ecdsa.PrivateKey)})
	if first.pin != second.pin {
		t.Fatalf("the fixture's two certificates have pins %s and %s, want one key's", first.pin, second.pin)
	}
	return bulkPair{first: first, second: second, pin: first.pin}
}

// bulkConfig is a bulk frpc's settings with HBEP-6's placeholders filled.
func bulkConfig() agent.FrpcConfig {
	return agent.FrpcConfig{
		ServerAddr: "127.0.0.1", ServerPort: 41001,
		LogFile: "/tmp/hb-1/frpc-bulk.log", Token: hCredential,
		CAFile: "/tmp/hb-1/dial-home-bulk.crt", ServerName: bulkName,
		UID: hInstUID, Socket: "/tmp/hb-1/serve.sock", TCPMuxOff: true,
	}
}

// The bulk frpc's configuration is the token frpc's block with the bulk
// server name, its own certificate, and transport.tcpMux = false (HBEP-6, The
// bulk path: "the same wss login as the token frpc, with serverName
// bulk.dial-home.hpcburst.invalid, the second certificate as its trustedCaFile,
// and transport.tcpMux = false"; "S1's heartbeat set, explicit, and poolCount
// = 0"; "one tcpmux proxy with the httpconnect multiplexer, named by the
// Instance's UID, from the agent's serving socket").
func TestBulkFrpcConfigRender(t *testing.T) {
	got, err := bulkConfig().Render()
	if err != nil {
		t.Fatal(err)
	}
	want := `serverAddr = "127.0.0.1"
serverPort = 41001
loginFailExit = false
log.to = "/tmp/hb-1/frpc-bulk.log"
metadatas.token = "` + hCredential + `"
transport.protocol = "wss"
transport.tls.trustedCaFile = "/tmp/hb-1/dial-home-bulk.crt"
transport.tls.serverName = "bulk.dial-home.hpcburst.invalid"
transport.poolCount = 0
transport.tcpMux = false
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatInterval = 10
transport.heartbeatTimeout = 25
transport.dialServerKeepalive = 30

[[proxies]]
name = "` + hInstUID + `"
type = "tcpmux"
multiplexer = "httpconnect"
customDomains = ["` + hInstUID + `"]
[proxies.plugin]
type = "unix_domain_socket"
unixPath = "/tmp/hb-1/serve.sock"
`
	if string(got) != want {
		t.Fatalf("the bulk frpc's configuration:\n%s\nwant HBEP-6's:\n%s", got, want)
	}
}

// With TCPMuxOff unset the rendering has no tcpMux line at all, so the token
// frpc's is what it was: the same inputs, rendered with and without the field,
// differ in exactly the lines HBEP-6 gives the bulk frpc (its log, its
// certificate, its server name, and tcpMux). The positive control is that the
// two do differ, so the comparison can fail. (The byte-for-byte golden of the
// token frpc is TestFrpcConfigIsHBEP6sBlock, which this change leaves as it was.)
func TestBulkFrpcConfigDiffersFromTokenOnlyInItsFourLines(t *testing.T) {
	tok := bulkConfig()
	tok.TCPMuxOff = false
	tok.LogFile = "/tmp/hb-1/frpc.log"
	tok.CAFile = "/tmp/hb-1/dial-home.crt"
	tok.ServerName = tokenName
	tb, err := tok.Render()
	if err != nil {
		t.Fatal(err)
	}
	bb, err := bulkConfig().Render()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(tb), "tcpMux =") {
		t.Fatalf("the token frpc's configuration has a tcpMux line:\n%s", tb)
	}
	tl, bl := strings.Split(string(tb), "\n"), strings.Split(string(bb), "\n")
	var onlyToken, onlyBulk []string
	has := func(lines []string, l string) bool {
		for _, x := range lines {
			if x == l {
				return true
			}
		}
		return false
	}
	for _, l := range tl {
		if !has(bl, l) {
			onlyToken = append(onlyToken, l)
		}
	}
	for _, l := range bl {
		if !has(tl, l) {
			onlyBulk = append(onlyBulk, l)
		}
	}
	sort.Strings(onlyToken)
	sort.Strings(onlyBulk)
	wantToken := []string{
		`log.to = "/tmp/hb-1/frpc.log"`,
		`transport.tls.serverName = "dial-home.hpcburst.invalid"`,
		`transport.tls.trustedCaFile = "/tmp/hb-1/dial-home.crt"`,
	}
	wantBulk := []string{
		`log.to = "/tmp/hb-1/frpc-bulk.log"`,
		`transport.tcpMux = false`,
		`transport.tls.serverName = "bulk.dial-home.hpcburst.invalid"`,
		`transport.tls.trustedCaFile = "/tmp/hb-1/dial-home-bulk.crt"`,
	}
	sort.Strings(wantToken)
	sort.Strings(wantBulk)
	if strings.Join(onlyToken, "\n") != strings.Join(wantToken, "\n") || strings.Join(onlyBulk, "\n") != strings.Join(wantBulk, "\n") {
		t.Fatalf("the two configurations differ in\n  token only: %q\n  bulk only: %q\nwant\n  token only: %q\n  bulk only: %q", onlyToken, onlyBulk, wantToken, wantBulk)
	}
}

// fileFacts is what a touch of a file changes: its bytes, its inode (a rename
// over it changes this), its modification time and its mode.
type fileFacts struct {
	sum   string
	ino   uint64
	mtime int64
	mode  os.FileMode
}

func inodeOf(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Ino
	}
	return 0
}

func factsOf(t *testing.T, path string) fileFacts {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return fileFacts{sum: hex.EncodeToString(sum[:]), ino: inodeOf(fi), mtime: fi.ModTime().UnixNano(), mode: fi.Mode().Perm()}
}

// stubRig runs frpc stand-ins in one run directory, as the agent runs its
// token frpc and its bulk frpc, and records each start's process ID.
type stubRig struct {
	t      *testing.T
	dir    string
	runDir string
	bin    string
	digest string
	pair   bulkPair
	token  string // the staged certificates
	bulk   string
}

func newStubRig(t *testing.T) *stubRig {
	t.Helper()
	dir := shortTemp(t)
	bin, digest := stubFrpc(t, dir)
	run := filepath.Join(dir, "run")
	if err := os.Mkdir(run, 0o700); err != nil {
		t.Fatal(err)
	}
	r := &stubRig{t: t, dir: dir, runDir: run, bin: bin, digest: digest, pair: makeBulkPair(t)}
	r.token = writeFile(t, filepath.Join(dir, "staged-first.crt"), r.pair.first.pem)
	r.bulk = writeFile(t, filepath.Join(dir, "staged-second.crt"), r.pair.second.pem)
	return r
}

// frpc is a stand-in frpc settings for the token or the bulk variant, with its
// own staged certificate.
func (r *stubRig) frpc(bulk bool) (*agent.Frpc, <-chan agent.ProcessID) {
	r.t.Helper()
	server, err := agent.DirectServer("dial-home.example:443")
	if err != nil {
		r.t.Fatal(err)
	}
	starts := make(chan agent.ProcessID, 16)
	cert := r.token
	if bulk {
		cert = r.bulk
	}
	return &agent.Frpc{
		Binary: r.bin, FrpcDigest: r.digest, CertFile: cert, CertSPKISHA256: r.pair.pin,
		Credential: hCredential, RunDir: r.runDir, Socket: filepath.Join(r.runDir, "serve.sock"),
		Server: server, StopGrace: 200 * time.Millisecond, Bulk: bulk,
		OnStart: func(id agent.ProcessID) { starts <- id },
	}, starts
}

func waitStart(t *testing.T, starts <-chan agent.ProcessID, what string) agent.ProcessID {
	t.Helper()
	select {
	case id := <-starts:
		return id
	case <-time.After(10 * time.Second):
		t.Fatalf("no start of %s within 10 s", what)
		return agent.ProcessID{}
	}
}

func runNames(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// The bulk frpc's files are its own, in the job's private run directory, mode
// 600, and no file of the token frpc is touched when it starts (HBEP-6, The
// bulk path: "its own configuration and log files in the job's private
// temporary directory (mode 600)"; "the second certificate is staged as its own
// file, which only the bulk frpc trusts"). The bulk frpc's own certificate copy
// is the checked bytes of the second certificate.
func TestBulkFrpcHasItsOwnFiles(t *testing.T) {
	r := newStubRig(t)
	tok, tokStarts := r.frpc(false)
	tok.Serve(hInstUID)
	runBackground(t, "the token frpc", tok.Run)
	waitStart(t, tokStarts, "the token frpc")
	if got, want := runNames(t, r.runDir), []string{"dial-home.crt", "frpc.toml"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("the run directory holds %v with only the token frpc started, want %v", got, want)
	}
	before := map[string]fileFacts{}
	for _, n := range []string{"frpc.toml", "dial-home.crt"} {
		before[n] = factsOf(t, filepath.Join(r.runDir, n))
	}

	bulk, bulkStarts := r.frpc(true)
	bulk.Serve(hInstUID)
	runBackground(t, "the bulk frpc", bulk.Run)
	waitStart(t, bulkStarts, "the bulk frpc")
	got := runNames(t, r.runDir)
	want := []string{"dial-home-bulk.crt", "dial-home.crt", "frpc-bulk.toml", "frpc.toml"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("the run directory holds %v with both started, want %v", got, want)
	}
	for _, n := range []string{"frpc.toml", "dial-home.crt"} {
		if after := factsOf(t, filepath.Join(r.runDir, n)); after != before[n] {
			t.Errorf("starting the bulk frpc touched the token frpc's %s: %+v, was %+v", n, after, before[n])
		}
	}
	for _, n := range want {
		if m := factsOf(t, filepath.Join(r.runDir, n)).mode; m != 0o600 {
			t.Errorf("%s has mode %o, want 600", n, m)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(r.runDir, "dial-home-bulk.crt")); string(b) != string(r.pair.second.pem) {
		t.Errorf("dial-home-bulk.crt is not the checked second certificate")
	}
	if b, _ := os.ReadFile(filepath.Join(r.runDir, "dial-home.crt")); string(b) != string(r.pair.first.pem) {
		t.Errorf("dial-home.crt is not the first certificate")
	}
	bcfg, _ := os.ReadFile(filepath.Join(r.runDir, "frpc-bulk.toml"))
	tcfg, _ := os.ReadFile(filepath.Join(r.runDir, "frpc.toml"))
	for _, w := range []string{
		`transport.tcpMux = false`,
		`transport.tls.serverName = "bulk.dial-home.hpcburst.invalid"`,
		`transport.tls.trustedCaFile = "` + filepath.Join(r.runDir, "dial-home-bulk.crt") + `"`,
		`log.to = "` + filepath.Join(r.runDir, "frpc-bulk.log") + `"`,
	} {
		if !strings.Contains(string(bcfg), w+"\n") {
			t.Errorf("frpc-bulk.toml lacks %s:\n%s", w, bcfg)
		}
	}
	// "tcpMux" alone would match transport.tcpMuxKeepaliveInterval, which the
	// token frpc carries too: match the whole setting.
	for _, w := range []string{"transport.tcpMux = ", "bulk", "frpc-bulk"} {
		if strings.Contains(string(tcfg), w) {
			t.Errorf("frpc.toml names %q:\n%s", w, tcfg)
		}
	}
	if !strings.Contains(string(tcfg), `transport.tls.trustedCaFile = "`+filepath.Join(r.runDir, "dial-home.crt")+`"`) {
		t.Errorf("frpc.toml does not trust dial-home.crt:\n%s", tcfg)
	}
}

// The positive control of "no token file is touched": the comparison above can
// fail, because a second token-variant Frpc in the same directory does rewrite
// frpc.toml (the write is a rename over it, a new inode).
func TestStartingAnotherTokenFrpcDoesTouchTheTokenFiles(t *testing.T) {
	r := newStubRig(t)
	first, firstStarts := r.frpc(false)
	first.Serve(hInstUID)
	runBackground(t, "the first token frpc", first.Run)
	waitStart(t, firstStarts, "the first token frpc")
	before := factsOf(t, filepath.Join(r.runDir, "frpc.toml"))
	second, secondStarts := r.frpc(false)
	second.Serve(hInstUID)
	runBackground(t, "the second token frpc", second.Run)
	waitStart(t, secondStarts, "the second token frpc")
	if after := factsOf(t, filepath.Join(r.runDir, "frpc.toml")); after == before {
		t.Fatal("a second token frpc left frpc.toml untouched: the compare in TestBulkFrpcHasItsOwnFiles cannot fail")
	}
}

// Each frpc starts only after its own certificate passed its own name's check
// (HBEP-6, The bulk path: "VerifyDialHomeCert and the Provider's pre-stage check
// verify each file against its own name"; Test plan, Certificate): the first
// certificate staged as the bulk file is refused, and the second as the token's.
// Run returns the refusal at once and starts no process. The controls are the
// same setups with each file where it belongs, which start.
func TestBulkFrpcRefusesTheFirstCertificate(t *testing.T) {
	r := newStubRig(t)
	other := makeCert(t, bulkName) // another key: the pin is wrong, whatever the name
	otherFile := writeFile(t, filepath.Join(r.dir, "other.crt"), other.pem)
	both := makeCertWith(t, certOpts{names: []string{tokenName, bulkName}, key: r.pair.first.tls.PrivateKey.(*ecdsa.PrivateKey)})
	bothFile := writeFile(t, filepath.Join(r.dir, "both.crt"), both.pem)
	cases := []struct {
		name   string
		bulk   bool
		cert   string
		refuse bool
	}{
		{"the first certificate as the bulk file", true, r.token, true},
		{"the second certificate as the token file", false, r.bulk, true},
		{"a certificate of another key as the bulk file", true, otherFile, true},
		{"the second certificate as the bulk file", true, r.bulk, false},
		{"the first certificate as the token file", false, r.token, false},
		// A certificate that names both is no refusal at the name check: the
		// launcher issues the second with the bulk name only (part 2), and
		// agentapi's name check is about the name frpc checks.
		{"a certificate that names both, as the bulk file", true, bothFile, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, starts := r.frpc(tc.bulk)
			f.CertFile = tc.cert
			f.RunDir = filepath.Join(r.dir, strings.ReplaceAll(tc.name, " ", "-"))
			if err := os.Mkdir(f.RunDir, 0o700); err != nil {
				t.Fatal(err)
			}
			f.Serve(hInstUID)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- f.Run(ctx) }()
			if tc.refuse {
				select {
				case err := <-done:
					cancel()
					if err == nil {
						t.Fatal("Run returned nil: the certificate was accepted")
					}
					if strings.Contains(err.Error(), hCredential) {
						t.Fatalf("the refusal holds the credential: %v", err)
					}
					if !strings.Contains(err.Error(), "dial-home certificate") {
						t.Fatalf("the refusal does not name the certificate: %v", err)
					}
				case <-time.After(5 * time.Second):
					cancel()
					<-done
					t.Fatal("Run did not refuse within 5 s")
				}
				select {
				case id := <-starts:
					t.Fatalf("frpc started (%+v) with a certificate that must be refused", id)
				default:
				}
				return
			}
			id := waitStart(t, starts, "frpc")
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Run: %v", err)
			}
			if id.Alive() {
				t.Fatalf("frpc %+v runs after Run returned", id)
			}
		})
	}
}

// A bulk frpc that the agent refuses is its own Run's end: it returns the
// reason, and a token Frpc beside it, in the same run directory, goes on
// (HBEP-6, The bulk path: the agent runs both on their own; transport.go's
// part of this is TestARefusedBulkFrpcDoesNotStopTheAgentOrTheTokenFrpc in
// cmd/hb-agent).
func TestARefusedBulkFrpcLeavesTheTokenFrpcRunning(t *testing.T) {
	r := newStubRig(t)
	tok, tokStarts := r.frpc(false)
	tok.Serve(hInstUID)
	stop := runBackground(t, "the token frpc", tok.Run)
	tokID := waitStart(t, tokStarts, "the token frpc")
	bulk, bulkStarts := r.frpc(true)
	bulk.CertFile = r.token // the first certificate as the bulk file
	bulk.Serve(hInstUID)
	var mu sync.Mutex
	var bulkErr error
	done := make(chan struct{})
	go func() {
		err := bulk.Run(context.Background())
		mu.Lock()
		bulkErr = err
		mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the bulk frpc was not refused within 5 s")
	}
	mu.Lock()
	err := bulkErr
	mu.Unlock()
	if err == nil {
		t.Fatal("the bulk frpc accepted the first certificate")
	}
	select {
	case id := <-bulkStarts:
		t.Fatalf("the refused bulk frpc started: %+v", id)
	default:
	}
	if !tokID.Alive() {
		t.Fatal("the token frpc stopped when the bulk frpc was refused")
	}
	if _, err := os.Stat(filepath.Join(r.runDir, "dial-home-bulk.crt")); err == nil {
		t.Fatal("a refused certificate was copied into the run directory as the bulk frpc's trusted one")
	}
	stop()
}
