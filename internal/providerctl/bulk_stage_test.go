package providerctl

// What Create stages and writes for the bulk path (beta; HBEP-6, The bulk path: "A second
// certificate"; the science agent's ruling S3 on #448; #448 part 3). With FrontSettings.BulkCertPEM
// set and a mode that isn't ssh, Create stages alloc/<tag>/dial-home-bulk.crt, which only the bulk
// frpc trusts, and writes agent.json's bulk block; with it empty, or in ssh mode, the staged files and
// agent.json's bytes are exactly what they were before.
//
// The certificates are made in the test (ctltest.CertPair: one key, the first naming
// dial-home.hpcburst.invalid, the second bulk.dial-home.hpcburst.invalid only) and judged here with
// crypto/x509, never by a helper that made them: a staged file is checked for the name it carries and
// the key it has, not compared with the bytes the test passed in. agent.json's expected bytes are typed
// out below.

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The names, typed from HBEP-6 and the ruling, not read from agentapi's constants.
const (
	tokenName = "dial-home.hpcburst.invalid"
	bulkName  = "bulk.dial-home.hpcburst.invalid"
)

// bulkFront gives the env's front the two certificates of one key, the first as CertPEM and the pin
// of that key, and the second as BulkCertPEM. It returns both PEMs.
func bulkFront(e *env) (first, bulk []byte) {
	e.t.Helper()
	first, bulk, pin := ctltest.CertPair(e.t)
	e.opts.Front.CertPEM, e.opts.Front.BulkCertPEM, e.opts.Front.CertPin = first, bulk, pin
	return first, bulk
}

// parseCert parses the one certificate in a staged file, with crypto/x509.
func parseCert(t *testing.T, b []byte) *x509.Certificate {
	t.Helper()
	blk, rest := pem.Decode(b)
	if blk == nil || blk.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) != 0 {
		t.Fatalf("the staged file is not exactly one PEM certificate: %q", b)
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// spkiPin is the SHA-256 of a certificate's DER SubjectPublicKeyInfo, computed here.
func spkiPin(c *x509.Certificate) string {
	sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

// names reports the two names a staged file can be checked against: whether crypto/x509 takes it for a
// certificate of that host name.
func names(c *x509.Certificate) (token, bulk bool) {
	return c.VerifyHostname(tokenName) == nil, c.VerifyHostname(bulkName) == nil
}

// TestStagedBulkCertIsTheBulkFile: with the bulk path on, the Allocation's directory holds the first
// certificate, naming dial-home.hpcburst.invalid, and beside it dial-home-bulk.crt, naming
// bulk.dial-home.hpcburst.invalid only, for the first one's key, so the pin in agent.json is both's. Each
// is parsed here: a staging that wrote the first certificate under the bulk file's name would pass a
// comparison with the bytes it was given, and fails this. The order is HBEP-4's with the bulk file
// before agent.json, which stays last.
func TestStagedBulkCertIsTheBulkFile(t *testing.T) {
	var first, bulk []byte
	e := newEnv(t, func(e *env) { first, bulk = bulkFront(e) })
	a := e.newAlloc("a")
	e.run("a", 3)
	if n := e.count("sbatch"); n != 1 {
		t.Fatalf("%d sbatch calls, want 1 (reason %q)", n, e.get("a").Status.AtProvider.Reason)
	}
	tag := tagOf(a)
	files := e.batch.Files()

	tokenFile, ok := files["alloc/"+tag+"/dial-home.crt"]
	if !ok {
		t.Fatalf("no dial-home.crt; staged: %v", stagedNames(files))
	}
	bulkFile, ok := files["alloc/"+tag+"/dial-home-bulk.crt"]
	if !ok {
		t.Fatalf("no dial-home-bulk.crt; staged: %v", stagedNames(files))
	}
	tc, bc := parseCert(t, tokenFile.Content), parseCert(t, bulkFile.Content)
	if tok, blk := names(tc); !tok || blk {
		t.Errorf("dial-home.crt names the token server %t and the bulk server %t, want true and false", tok, blk)
	}
	if tok, blk := names(bc); tok || !blk {
		t.Errorf("dial-home-bulk.crt names the token server %t and the bulk server %t, want false and true (crypto/x509's reading of the staged file)", tok, blk)
	}
	if len(bc.DNSNames) != 1 || bc.DNSNames[0] != bulkName {
		t.Errorf("dial-home-bulk.crt's DNS names are %v, want only %s", bc.DNSNames, bulkName)
	}
	cfg, err := agentapi.DecodeAgentConfig(files["alloc/"+tag+"/agent.json"].Content)
	if err != nil {
		t.Fatal(err)
	}
	if spkiPin(tc) != cfg.CertSPKISHA256 || spkiPin(bc) != cfg.CertSPKISHA256 {
		t.Errorf("pins: dial-home.crt %s, dial-home-bulk.crt %s, agent.json %s: one key, one pin", spkiPin(tc), spkiPin(bc), cfg.CertSPKISHA256)
	}
	if string(tokenFile.Content) != string(first) || string(bulkFile.Content) != string(bulk) {
		t.Error("a staged certificate is not the launcher's, unchanged")
	}
	if tokenFile.Mode.Perm() != 0o644 || bulkFile.Mode.Perm() != 0o644 {
		t.Errorf("modes %o and %o, want 644 for both: public certificates", tokenFile.Mode.Perm(), bulkFile.Mode.Perm())
	}
	if cfg.Bulk == nil || cfg.Bulk.ServerName != bulkName {
		t.Errorf("agent.json's bulk block is %+v, want {serverName: %s}", cfg.Bulk, bulkName)
	}

	// The control: the check above can fail. The first certificate, read as the bulk file, is taken
	// for the token server's and not the bulk server's.
	if tok, blk := names(parseCert(t, first)); !tok || blk {
		t.Error("the check is blind: the first certificate reads as the bulk server's")
	}

	// The order of the staging calls: agent, frpc, credential, certificate, bulk certificate, agent.json.
	var order []string
	for _, c := range e.batch.Calls() {
		if c.Op == "ensure-file" {
			order = append(order, c.Args[0])
		}
	}
	want := []string{
		"agent/" + e.digest + "/hb-agent",
		"frpc/" + e.opts.Front.FrpcDigests[testArch] + "/frpc",
		"alloc/" + tag + "/credential",
		"alloc/" + tag + "/dial-home.crt",
		"alloc/" + tag + "/dial-home-bulk.crt",
		"alloc/" + tag + "/agent.json",
	}
	if strings.Join(order, "\n") != strings.Join(want, "\n") {
		t.Errorf("files staged, in order:\n%s\nwant:\n%s", strings.Join(order, "\n"), strings.Join(want, "\n"))
	}
}

// agentJSON is agent.json as Encode writes it for the direct mode of newEnv's Provider, typed out from
// the alpha format (agentapi.AgentConfig's fields in order, indented by two spaces, one newline at the
// end); bulk is the block's text, spliced in after the last field as the ruling's JSON shows it, or "".
func agentJSON(tag, pin, frpcDigest, bulk string) string {
	s := "{\n" +
		`  "protocolVersion": "v1",` + "\n" +
		`  "tag": "` + tag + `",` + "\n" +
		`  "mode": "direct",` + "\n" +
		`  "endpoint": "front.example.org:29443",` + "\n" +
		`  "certSPKISHA256": "` + pin + `",` + "\n" +
		`  "frpcDigest": "` + frpcDigest + `",` + "\n" +
		`  "stagingDir": "/scratch/example-user/stage",` + "\n" +
		`  "dialHomeDir": "/home/example-user/.hpcburst/dial-home"`
	if bulk != "" {
		s += ",\n" + bulk
	}
	return s + "\n}\n"
}

// TestAgentJSONHasNoBulkBlockWithoutBulkPath: without BulkCertPEM the staged file list and agent.json's
// bytes are exactly as before, the bytes compared with the alpha text typed above, and the files are
// the five of HBEP-4. The control: with the certificate set, the same Allocation's agent.json differs by
// exactly the block "bulk": {"serverName": ...} and the list by exactly dial-home-bulk.crt.
func TestAgentJSONHasNoBulkBlockWithoutBulkPath(t *testing.T) {
	staged := func(t *testing.T, mods ...envOption) (tag string, files map[string][]byte, pin, frpcDigest string) {
		e := newEnv(t, mods...)
		a := e.newAlloc("a")
		e.run("a", 3)
		if n := e.count("sbatch"); n != 1 {
			t.Fatalf("%d sbatch calls, want 1 (reason %q)", n, e.get("a").Status.AtProvider.Reason)
		}
		files = map[string][]byte{}
		for p, f := range e.batch.Files() {
			files[p] = f.Content
		}
		return tagOf(a), files, e.opts.Front.CertPin, e.opts.Front.FrpcDigests[testArch]
	}

	tag, without, pin, digest := staged(t)
	if got, want := string(without["alloc/"+tag+"/agent.json"]), agentJSON(tag, pin, digest, ""); got != want {
		t.Errorf("agent.json without the bulk path\n got %q\nwant %q", got, want)
	}
	var perAlloc []string
	for p := range without {
		if strings.HasPrefix(p, "alloc/") {
			perAlloc = append(perAlloc, p)
		}
	}
	if !equalSets(perAlloc, []string{"alloc/" + tag + "/credential", "alloc/" + tag + "/dial-home.crt", "alloc/" + tag + "/agent.json"}) {
		t.Errorf("the Allocation's files without the bulk path: %v", perAlloc)
	}

	// The control.
	block := "  \"bulk\": {\n    \"serverName\": \"" + bulkName + "\"\n  }"
	tag2, with, pin2, digest2 := staged(t, bulkOnly)
	if got, want := string(with["alloc/"+tag2+"/agent.json"]), agentJSON(tag2, pin2, digest2, block); got != want {
		t.Errorf("agent.json with the bulk path\n got %q\nwant %q", got, want)
	}
	if _, ok := with["alloc/"+tag2+"/dial-home-bulk.crt"]; !ok {
		t.Error("the control stages no dial-home-bulk.crt: the comparison above could not have told the runs apart")
	}
	var js map[string]any
	if err := json.Unmarshal(without["alloc/"+tag+"/agent.json"], &js); err != nil {
		t.Fatal(err)
	}
	if _, ok := js["bulk"]; ok {
		t.Error("agent.json has a bulk key without the bulk path")
	}
}

// TestNoBulkBlockInSSHMode: in ssh mode neither the bulk file nor the block is staged or written, though
// the settings carry a bulk certificate (which the controller's parse refuses to produce): the agent
// starts no bulk frpc there (HBEP-6, The bulk path), and agent.json's Validate refuses the block in ssh
// mode, so a Create that wrote it could not submit. The Allocation submits as before. The control: direct
// mode with the same settings gets both.
func TestNoBulkBlockInSSHMode(t *testing.T) {
	sshFront := func(e *env) {
		f := e.opts.Front
		f.Mode, f.Endpoint, f.SSHUser, f.AgentPort, f.DialHomePort = agentapi.DialHomeSSH, "control.example.org:22", "example-user", 29444, 29448
	}
	for _, c := range []struct {
		name     string
		mods     []envOption
		wantBulk bool
	}{
		{"ssh mode", []envOption{bulkOnly, sshFront}, false},
		{"the control: direct mode", []envOption{bulkOnly}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t, c.mods...)
			a := e.newAlloc("a")
			e.run("a", 3)
			if n := e.count("sbatch"); n != 1 {
				t.Fatalf("%d sbatch calls, want 1 (reason %q)", n, e.get("a").Status.AtProvider.Reason)
			}
			tag := tagOf(a)
			files := e.batch.Files()
			_, hasFile := files["alloc/"+tag+"/dial-home-bulk.crt"]
			var js map[string]any
			if err := json.Unmarshal(files["alloc/"+tag+"/agent.json"].Content, &js); err != nil {
				t.Fatal(err)
			}
			_, hasBlock := js["bulk"]
			if hasFile != c.wantBulk || hasBlock != c.wantBulk {
				t.Errorf("bulk file staged %t, block written %t, want both %t", hasFile, hasBlock, c.wantBulk)
			}
		})
	}
}

// bulkOnly sets the bulk certificate beside the first, like bulkFront (a mod).
func bulkOnly(e *env) { bulkFront(e) }

// TestPreStageCheckVerifiesEachFileAgainstItsOwnName (HBEP-6: "VerifyDialHomeCert and the Provider's
// pre-stage check verify each file against its own name, the second only when the bulk path is on"):
// the first certificate offered as the bulk file is refused, and the bulk one offered as the first, and
// so is a bulk certificate of another key (another pin), whose name is right. Each refusal names the
// check that failed, and a Create with such settings stages nothing and makes no platform call beyond
// the listing. The controls: the right pair passes both checks, and a front with no bulk certificate
// passes the bulk check at once (the bulk path is off).
func TestPreStageCheckVerifiesEachFileAgainstItsOwnName(t *testing.T) {
	first, bulk, pin := ctltest.CertPair(t)
	_, otherBulk, _ := ctltest.CertPair(t) // another key
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	settings := func(firstPEM, bulkPEM []byte) *FrontSettings {
		return &FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "front.example.org:29443", CertPin: pin, CertPEM: firstPEM, BulkCertPEM: bulkPEM}
	}

	good := settings(first, bulk)
	if err := good.CheckCertificate(now); err != nil {
		t.Errorf("the right pair, the first check: %v", err)
	}
	if err := good.CheckBulkCertificate(now); err != nil {
		t.Errorf("the right pair, the bulk check: %v", err)
	}
	if err := settings(first, nil).CheckBulkCertificate(now); err != nil {
		t.Errorf("no bulk certificate (the bulk path off): %v, want none", err)
	}

	for _, c := range []struct {
		name  string
		f     *FrontSettings
		check func(*FrontSettings) error
		want  string
	}{
		{"the first certificate as the bulk file", settings(first, first), func(f *FrontSettings) error { return f.CheckBulkCertificate(now) }, "does not name " + bulkName},
		{"the bulk certificate as the first file", settings(bulk, bulk), func(f *FrontSettings) error { return f.CheckCertificate(now) }, "does not name " + tokenName},
		{"a bulk certificate of another key", settings(first, otherBulk), func(f *FrontSettings) error { return f.CheckBulkCertificate(now) }, "its pin"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := c.check(c.f)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("the check answered %v, want a refusal containing %q", err, c.want)
			}
		})
	}

	// At Create: swapped files are refused in the prerequisites, with no platform call and no file.
	for name, mod := range map[string]func(*env){
		"the first certificate as the bulk file": func(e *env) {
			f, _ := bulkFront(e)
			e.opts.Front.BulkCertPEM = f
		},
		"the bulk certificate as the first file": func(e *env) {
			_, b := bulkFront(e)
			e.opts.Front.CertPEM = b
		},
	} {
		t.Run("Create with "+name, func(t *testing.T) {
			e := newEnv(t, mod)
			e.newAlloc("a")
			e.run("a", 3)
			if n := e.count("sbatch"); n != 0 {
				t.Errorf("%d sbatch calls with a certificate the agent would refuse, want 0", n)
			}
			if n := e.count("ensure-file"); n != 0 {
				t.Errorf("%d files staged with a certificate the agent would refuse, want 0", n)
			}
			if reason := e.get("a").Status.AtProvider.Reason; !strings.Contains(reason, "the dial-home certificate: it does not name") {
				t.Errorf("the Allocation's reason is %q, want the agent's own refusal", reason)
			}
		})
	}
}

// TestHostDialedModeGetsTheBulkPathToo: host-dialed mode is a bulk mode as direct is (HBEP-6: "every
// Instance in direct or host-dialed mode gets a bulk frpc"): the bulk file is staged and the block
// written, beside the Allocation's UID that the agent greets its relays with, and no endpoint.
func TestHostDialedModeGetsTheBulkPathToo(t *testing.T) {
	e := newEnv(t, hostDialed, bulkOnly)
	runRelays(e)
	a := e.newAlloc("a")
	e.run("a", 4)
	if n := e.count("sbatch"); n != 1 {
		t.Fatalf("%d sbatch calls, want 1 (reason %q)", n, e.get("a").Status.AtProvider.Reason)
	}
	tag := tagOf(a)
	files := e.batch.Files()
	bulkFile, ok := files["alloc/"+tag+"/dial-home-bulk.crt"]
	if !ok {
		t.Fatalf("no dial-home-bulk.crt in host-dialed mode; staged: %v", stagedNames(files))
	}
	if tok, blk := names(parseCert(t, bulkFile.Content)); tok || !blk {
		t.Errorf("the staged bulk file names the token server %t and the bulk server %t, want false and true", tok, blk)
	}
	cfg, err := agentapi.DecodeAgentConfig(files["alloc/"+tag+"/agent.json"].Content)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != agentapi.DialHomeHostDialed || cfg.Allocation != string(a.UID) || cfg.Endpoint != "" {
		t.Errorf("agent.json mode %q, allocation %q, endpoint %q; want host-dialed, the Allocation's UID and no endpoint", cfg.Mode, cfg.Allocation, cfg.Endpoint)
	}
	if cfg.Bulk == nil || cfg.Bulk.ServerName != bulkName {
		t.Errorf("agent.json's bulk block is %+v, want {serverName: %s}", cfg.Bulk, bulkName)
	}
}
