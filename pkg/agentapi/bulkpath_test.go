package agentapi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The bulk path's names, written out from HBEP-6 (The bulk path), never read
// from the constants they check: the server name (:488-489), the plugin paths
// (:501), and the second frps's ports at base+13 to base+15 (:485).
func TestBulkPathConstants(t *testing.T) {
	if BulkDialHomeServerName != "bulk.dial-home.hpcburst.invalid" {
		t.Errorf("BulkDialHomeServerName = %q, HBEP-6 names bulk.dial-home.hpcburst.invalid", BulkDialHomeServerName)
	}
	if FrpsBulkPluginPath != "/frp/v1/handler/bulk" || FrontFrpsBulkPluginPath != "/frp/v1/front/bulk" {
		t.Errorf("bulk plugin paths %q and %q; HBEP-6 proposes /frp/v1/handler/bulk and /frp/v1/front/bulk", FrpsBulkPluginPath, FrontFrpsBulkPluginPath)
	}
	// The first frps's paths stay as they are (HBEP-6: "The first frps keeps its paths").
	if FrpsPluginPath != "/frp/v1/handler" || FrontFrpsPluginPath != "/frp/v1/front" {
		t.Errorf("the first frps's plugin paths changed: %q and %q", FrpsPluginPath, FrontFrpsPluginPath)
	}
	for name, c := range map[string]struct{ got, want int }{
		"FrpsBulkBindPortOffset":   {FrpsBulkBindPortOffset, 13},
		"FrpsBulkTCPMuxPortOffset": {FrpsBulkTCPMuxPortOffset, 14},
		"FrpsBulkWebPortOffset":    {FrpsBulkWebPortOffset, 15},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, HBEP-6 puts it at base+%d", name, c.got, c.want)
		}
	}
}

// The second frps's ports and files, at the same example host as TestHostLayout.
// No bulk port meets a port of the 13-port block below it (base to base+12).
func TestBulkHostLayout(t *testing.T) {
	h := Host{StateDir: "/home/example-user/.local/state/hpcburst", PortBase: 29220}
	for got, want := range map[string]string{
		h.FrpsBulkBindAddr():        "127.0.0.1:29233",
		h.FrpsBulkTCPMuxAddr():      "127.0.0.1:29234",
		h.FrpsBulkWebAddr():         "127.0.0.1:29235",
		h.FrpsBulkWebPasswordFile(): "/home/example-user/.local/state/hpcburst/frps-bulk/web-password",
		h.BulkCertFile():            "/home/example-user/.local/state/hpcburst/agent-endpoint/tls-bulk.crt",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	for name, c := range map[string]struct{ got, want int }{
		"FrpsBulkBindPort":   {h.FrpsBulkBindPort(), 29233},
		"FrpsBulkTCPMuxPort": {h.FrpsBulkTCPMuxPort(), 29234},
		"FrpsBulkWebPort":    {h.FrpsBulkWebPort(), 29235},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", name, c.got, c.want)
		}
	}
	if h.FrpsBulkWebPasswordFile() == h.FrpsWebPasswordFile() || h.BulkCertFile() == h.AgentCertFile() {
		t.Error("the second frps's password file or the second certificate shares the first's path")
	}
	block := map[int]string{}
	for off := 0; off <= 12; off++ {
		block[29220+off] = "base+" + strconv.Itoa(off)
	}
	for _, p := range []int{h.FrpsBulkBindPort(), h.FrpsBulkTCPMuxPort(), h.FrpsBulkWebPort()} {
		if at, ok := block[p]; ok {
			t.Errorf("bulk port %d is %s of the 13-port block", p, at)
		}
	}
}

// certForKey makes a self-signed certificate for the given key and names, as
// the launcher's are: a leaf, not a CA. Its pin is computed here from the DER
// SubjectPublicKeyInfo, not by PinOf.
func certForKey(t *testing.T, key *ecdsa.PrivateKey, serial int64, names []string, now time.Time) testCert {
	t.Helper()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "example launcher"},
		DNSNames: names, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return testCert{pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), der: der, pin: hex.EncodeToString(sum[:])}
}

// Each file is verified against its own name (HBEP-6, The bulk path): the
// launcher's two certificates share one key, so one pin passes both, and only
// the name tells them apart. The token check (VerifyDialHomeCert) is the check
// against DialHomeServerName, so a bulk-only certificate never passes it.
func TestVerifyDialHomeCertForEachName(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	first := certForKey(t, key, 1, []string{"localhost", DialHomeServerName}, now)
	bulk := certForKey(t, key, 2, []string{BulkDialHomeServerName}, now)
	if first.pin != bulk.pin {
		t.Fatalf("two certificates for one key have pins %s and %s", first.pin, bulk.pin)
	}
	for _, tc := range []struct {
		name  string
		b     []byte
		check string // the name verified against
		want  string // "" accepts
	}{
		{"the first against its name", first.pem, DialHomeServerName, ""},
		{"the second against the bulk name", bulk.pem, BulkDialHomeServerName, ""},
		{"the second against the token name", bulk.pem, DialHomeServerName, "does not name " + DialHomeServerName},
		{"the first against the bulk name", first.pem, BulkDialHomeServerName, "does not name " + BulkDialHomeServerName},
		{"a name no frpc uses", bulk.pem, "other.hpcburst.invalid", `"other.hpcburst.invalid" is not a dial-home server name`},
		{"no name", first.pem, "", `"" is not a dial-home server name`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyDialHomeCertFor(tc.b, first.pin, tc.check, now)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("refused: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want) || !strings.HasPrefix(err.Error(), "the dial-home certificate: ")):
				t.Errorf("error %v, want one that starts with the dial-home certificate and contains %q", err, tc.want)
			}
		})
	}
	// The token check is the check against DialHomeServerName: the first passes
	// it, and the second, which names only the bulk server, does not.
	if err := VerifyDialHomeCert(first.pem, first.pin, now); err != nil {
		t.Errorf("VerifyDialHomeCert refused the first certificate: %v", err)
	}
	if err := VerifyDialHomeCert(bulk.pem, bulk.pin, now); err == nil || !strings.Contains(err.Error(), "does not name "+DialHomeServerName) {
		t.Errorf("VerifyDialHomeCert on the bulk-only certificate: %v, want a refusal for the token name", err)
	}
}

// directConfig and hostDialedConfig are validConfig in the two modes the bulk path is built for.
func directConfig() AgentConfig {
	c := validConfig()
	c.Mode, c.Endpoint, c.SSH = DialHomeDirect, "front.example:8443", nil
	return c
}

func hostDialedConfig() AgentConfig {
	c := validConfig()
	c.Mode, c.Endpoint, c.SSH, c.Allocation = DialHomeHostDialed, "", nil, "00000000-0000-4000-8000-000000000001"
	return c
}

// TestAgentConfigWithoutBulkEncodesAsBefore: an agent.json without the bulk block is byte for byte
// the file a Provider wrote before the bulk path, typed here as JSON, not produced by the encoder
// under test: no "bulk" key, not even a null one. The control: with the block, the bytes differ by
// exactly the block, written as the science agent ruled it (#448, S3).
func TestAgentConfigWithoutBulkEncodesAsBefore(t *testing.T) {
	const before = `{
  "protocolVersion": "v1",
  "tag": "hb-helios-a-x7k2p-1bda24d00504",
  "mode": "direct",
  "endpoint": "front.example:8443",
  "certSPKISHA256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "frpcDigest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "stagingDir": "/scratch/example-user",
  "dialHomeDir": "/home/example-user/.hpcburst/dial-home"
}
`
	c := directConfig()
	got, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != before {
		t.Errorf("agent.json without the bulk block changed:\n%s", got)
	}
	c.Bulk = &BulkPath{ServerName: "bulk.dial-home.hpcburst.invalid"}
	with, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(before, "\n}\n", ",\n  \"bulk\": {\n    \"serverName\": \"bulk.dial-home.hpcburst.invalid\"\n  }\n}\n", 1)
	if string(with) != want {
		t.Errorf("agent.json with the bulk block:\n%s\nwant:\n%s", with, want)
	}
	back, err := DecodeAgentConfig(with)
	if err != nil || back.Bulk == nil || back.Bulk.ServerName != BulkDialHomeServerName {
		t.Errorf("the bulk block does not round-trip: %+v, %v", back.Bulk, err)
	}
	if back, err := DecodeAgentConfig([]byte(before)); err != nil || back.Bulk != nil {
		t.Errorf("an agent.json from before the bulk path decodes with a bulk block %+v, %v", back.Bulk, err)
	}
}

// TestAgentConfigBulkBlock: the bulk block is taken in direct and host-dialed mode with exactly
// the bulk server's name, and refused in ssh mode and with any other name (#448, S3: Validate
// requires exactly that name and refuses it in ssh mode). Each refusal is also refused by Encode,
// so no Provider writes such a file.
func TestAgentConfigBulkBlock(t *testing.T) {
	for name, c := range map[string]AgentConfig{"direct": directConfig(), "host-dialed": hostDialedConfig()} {
		c.Bulk = &BulkPath{ServerName: "bulk.dial-home.hpcburst.invalid"}
		if err := c.Validate(); err != nil {
			t.Errorf("%s mode with the bulk block was refused: %v", name, err)
		}
	}
	for _, tc := range []struct {
		name       string
		c          AgentConfig
		serverName string
		want       string
	}{
		{"ssh mode", validConfig(), "bulk.dial-home.hpcburst.invalid", "bulk is set in ssh mode"},
		{"the token's name", directConfig(), "dial-home.hpcburst.invalid", `bulk.serverName "dial-home.hpcburst.invalid"`},
		{"no name", directConfig(), "", `bulk.serverName ""`},
		{"another case", directConfig(), "BULK.dial-home.hpcburst.invalid", `bulk.serverName "BULK.dial-home.hpcburst.invalid"`},
		{"a trailing dot", hostDialedConfig(), "bulk.dial-home.hpcburst.invalid.", `bulk.serverName "bulk.dial-home.hpcburst.invalid."`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.c
			c.Bulk = &BulkPath{ServerName: tc.serverName}
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate: %v, want an error containing %q", err, tc.want)
			}
			if _, err := c.Encode(); err == nil {
				t.Error("Encode wrote it")
			}
		})
	}
}

// TestStagedBulkDialHomeCertPath: the staged file's name, typed from the ruling (#448, S3), beside
// the token frpc's certificate in the Allocation's directory and never the same file.
func TestStagedBulkDialHomeCertPath(t *testing.T) {
	const tag = "hb-helios-a-x7k2p-1bda24d00504"
	if StagedBulkDialHomeCertName != "dial-home-bulk.crt" {
		t.Errorf("StagedBulkDialHomeCertName = %q, the ruling names dial-home-bulk.crt", StagedBulkDialHomeCertName)
	}
	if got, want := StagedBulkDialHomeCertPath(tag), "alloc/"+tag+"/dial-home-bulk.crt"; got != want {
		t.Errorf("StagedBulkDialHomeCertPath = %q, want %q", got, want)
	}
	if StagedBulkDialHomeCertPath(tag) == StagedDialHomeCertPath(tag) {
		t.Error("the two staged certificates share a path")
	}
}
