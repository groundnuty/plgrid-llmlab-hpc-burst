package conformance

// The tests of the reach probe's own pieces (issue #225): the script, run for real against sockets it connects to;
// the target's settings; the ClientHello; the text to post. The expected values are written out here, from the
// probe's header (testdata/reach-probe.sh), not computed by the code under test.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
)

// Short limits for the tests of the script: the script takes them as parameters (probeTarget.agent), the live run
// uses reachConnectSeconds and reachReadSeconds. The full-queue stand-in costs the connect limit.
const (
	testConnectSeconds = 2
	testReadSeconds    = 2
)

// runProbe runs the probe script for the target with the given limits, as bash script dir, and returns the line it
// left (empty when it left none) and everything it printed. pathDir, when set, goes first on its PATH.
func runProbe(t *testing.T, target probeTarget, connect, read int, pathDir string) (line, printed string) {
	t.Helper()
	script := filepath.Join(t.TempDir(), "probe.sh")
	if err := os.WriteFile(script, target.agent(connect, read), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cmd := exec.Command("bash", script, dir)
	if pathDir != "" {
		cmd.Env = append(os.Environ(), "PATH="+pathDir)
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	start := time.Now()
	if err := cmd.Run(); err != nil {
		t.Fatalf("the probe exited with an error: %v\n%s", err, out.String())
	}
	if d := time.Since(start); d > time.Duration(connect+2*read+10)*time.Second {
		t.Errorf("the probe took %s, more than its limits allow", d)
	}
	b, err := os.ReadFile(filepath.Join(dir, reachFile))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(b), out.String()
}

func targetOf(s ctltest.ReachStub) probeTarget {
	return probeTarget{host: s.Host, port: s.Port, label: "example-gateway"}
}

// Each outcome and each answer is seen by the probe's own commands against a socket that behaves so, and the
// stand-in is asked: a speaking stand-in must have seen the probe (so an answer is never one from another socket),
// and the others must have seen none. The wants are written out from the probe's header.
func TestTheProbeSeesEachOutcomeOnARealSocket(t *testing.T) {
	for _, tc := range []struct {
		kind ctltest.ReachKind
		want string
		seen bool // the stand-in accepted at least one connection
	}{
		{ctltest.StubSSH, "outcome=reachable answer=ssh\n", true},
		{ctltest.StubTLS, "outcome=reachable answer=tls\n", true},
		{ctltest.StubData, "outcome=reachable answer=data\n", true},
		{ctltest.StubSilent, "outcome=reachable answer=silent\n", true},
		// What a hostile gateway sends never leaves the shell: control and escape bytes are "data", and a banner that
		// starts like SSH's and goes on with escapes, a line break and a megabyte is "ssh", and nothing more.
		{ctltest.StubBinary, "outcome=reachable answer=data\n", true},
		{ctltest.StubHostile, "outcome=reachable answer=ssh\n", true},
		{ctltest.StubRefused, "outcome=refused answer=none\n", false},
		{ctltest.StubTimeout, "outcome=timeout answer=none\n", false},
		{ctltest.StubUnresolved, "outcome=unresolved answer=none\n", false},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			t.Parallel()
			stub := ctltest.StartReachStub(t, tc.kind, "127.0.0.1", 0)
			got, printed := runProbe(t, targetOf(stub), testConnectSeconds, testReadSeconds, "")
			if got != tc.want {
				t.Errorf("the probe left %q, want %q", got, tc.want)
			}
			if printed != "" {
				t.Errorf("the probe printed %q, want nothing: what it prints would name the host", printed)
			}
			if tc.seen != (stub.Accepted() > 0) {
				t.Errorf("the stand-in accepted %d connection(s), seen=%t wanted: the outcome may be another socket's", stub.Accepted(), tc.seen)
			}
			for _, s := range []string{stub.Host, strconv.Itoa(stub.Port)} {
				if strings.Contains(got, s) {
					t.Errorf("the result line holds %q", s)
				}
			}
		})
	}
}

// A server that answers the ClientHello with something that is not TLS, and one that closes at once, are the two
// ways "something answers" and "silent" differ from a TLS endpoint; the stand-ins above speak first or not at all.
func TestTheProbeTellsPlainHTTPAndAClosedConnectionFromTLS(t *testing.T) {
	t.Run("a plain HTTP server answers the ClientHello with an error page", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		t.Cleanup(srv.Close)
		a := srv.Listener.Addr().(*net.TCPAddr)
		got, _ := runProbe(t, probeTarget{host: a.IP.String(), port: a.Port, label: "example-gateway"}, testConnectSeconds, testReadSeconds, "")
		if want := "outcome=reachable answer=data\n"; got != want {
			t.Errorf("the probe left %q, want %q", got, want)
		}
	})
	t.Run("a server that closes the connection at once", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				c.Close()
			}
		}()
		a := l.Addr().(*net.TCPAddr)
		got, _ := runProbe(t, probeTarget{host: a.IP.String(), port: a.Port, label: "example-gateway"}, testConnectSeconds, testReadSeconds, "")
		if want := "outcome=reachable answer=silent\n"; got != want {
			t.Errorf("the probe left %q, want %q", got, want)
		}
	})
}

// The outcomes that the network's own text decides cannot be had from a loopback socket, so the connect is given a
// bash that fails with the text a kernel gives (failingBash: the shell's two lines, the second naming the target).
// The probe's other commands, and its interpreter, are the real ones. Every reason is tried on a plain address and on
// hostile names, each spelling another reason: the outcome is the reason's, never the target's (review of PR 235,
// B4: refused.gateway.example with "No route to host" was read as refused). A getent that finds every name lets the
// hostile names reach the connect.
func TestTheProbeClassifiesTheConnectErrorsFromTheReasonAlone(t *testing.T) {
	fake := t.TempDir()
	for name, body := range map[string]string{"bash": failingBash, "getent": "#!/bin/sh\nexit 0\n"} {
		if err := os.WriteFile(filepath.Join(fake, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	hosts := []string{"127.0.0.1", "refused.gateway.example", "connection-refused.example", "name-or-service-not-known.example",
		"no-route-to-host.example", "network-is-unreachable.example", "unreachable", "temporary-failure-in-name-resolution.example", "permission-denied.example"}
	for _, tc := range []struct{ reason, want string }{
		{"No route to host", "outcome=unreachable answer=none\n"},
		{"Network is unreachable", "outcome=unreachable answer=none\n"},
		{"Connection refused", "outcome=refused answer=none\n"},
		{"Name or service not known", "outcome=unresolved answer=none\n"},
		{"Temporary failure in name resolution", "outcome=unresolved answer=none\n"},
		{"Permission denied", "outcome=error answer=none\n"},
	} {
		for _, host := range hosts {
			t.Run(tc.reason+"/"+host, func(t *testing.T) {
				t.Setenv("FAKE_REASON", tc.reason)
				got, printed := runProbe(t, probeTarget{host: host, port: 9, label: "example-gateway"}, testConnectSeconds, testReadSeconds, fake+":"+os.Getenv("PATH"))
				if got != tc.want {
					t.Errorf("the probe left %q for the reason %q on the host %q, want %q", got, tc.reason, host, tc.want)
				}
				if printed != "" {
					t.Errorf("the probe printed %q, want nothing", printed)
				}
			})
		}
	}
}

// Without timeout(1) the probe cannot bound the connect: it says error and does not connect unbounded.
func TestTheProbeSaysErrorWithoutTimeout(t *testing.T) {
	bin := t.TempDir()
	for _, tool := range []string{"mv"} {
		p, err := exec.LookPath(tool)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(p, filepath.Join(bin, tool)); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := runProbe(t, probeTarget{host: "127.0.0.1", port: 9, label: "example-gateway"}, testConnectSeconds, testReadSeconds, bin)
	if want := "outcome=error answer=none\n"; got != want {
		t.Errorf("the probe left %q, want %q", got, want)
	}
}

// The step before the connect does its own job: a name that the resolver cannot find is unresolved without a
// connect, even where a connect would have worked. The resolver is a getent that finds nothing (exit 2, as the real
// one does for an unknown name) and a listener that is there: a probe that connected would reach it.
func TestTheProbeDoesNotConnectToAnUnresolvedName(t *testing.T) {
	fake := t.TempDir()
	if err := os.WriteFile(filepath.Join(fake, "getent"), []byte("#!/bin/sh\nexit 2\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	stub := ctltest.StartReachStub(t, ctltest.StubSilent, "127.0.0.1", 0)
	got, _ := runProbe(t, targetOf(stub), testConnectSeconds, testReadSeconds, fake+":"+os.Getenv("PATH"))
	if want := "outcome=unresolved answer=none\n"; got != want {
		t.Errorf("the probe left %q, want %q", got, want)
	}
	if stub.Accepted() != 0 {
		t.Errorf("the probe connected %d time(s) although the name did not resolve", stub.Accepted())
	}
}

// The ClientHello is built from fields, so its lengths are checked here against the record layout, written out: a
// record of type 22, version 3.1, a length; a handshake of type 1 with a 3-byte length; a body of version 3.3, 32
// bytes of random, an empty session id, then the lengths of the suites, the compression methods and the extensions.
func TestTheClientHelloIsWellFormed(t *testing.T) {
	h := reachClientHello()
	be := func(b []byte) int {
		n := 0
		for _, c := range b {
			n = n<<8 | int(c)
		}
		return n
	}
	if h[0] != 22 || h[1] != 3 || h[2] != 1 {
		t.Fatalf("record header % x, want 16 03 01", h[:3])
	}
	if got := be(h[3:5]); got != len(h)-5 {
		t.Fatalf("record length %d, the handshake is %d bytes", got, len(h)-5)
	}
	hs := h[5:]
	if hs[0] != 1 || be(hs[1:4]) != len(hs)-4 {
		t.Fatalf("handshake type %d length %d, the body is %d bytes", hs[0], be(hs[1:4]), len(hs)-4)
	}
	b := hs[4:]
	if b[0] != 3 || b[1] != 3 {
		t.Fatalf("client version % x, want 03 03", b[:2])
	}
	off := 2 + 32
	if b[off] != 0 {
		t.Fatalf("session id length %d, want 0", b[off])
	}
	off++
	suites := be(b[off : off+2])
	off += 2 + suites
	comp := int(b[off])
	off += 1 + comp
	exts := be(b[off : off+2])
	off += 2
	if suites == 0 || suites%2 != 0 || comp != 1 || off+exts != len(b) {
		t.Fatalf("suites %d, compression methods %d, extensions %d at %d of %d: the lengths do not add up", suites, comp, exts, off, len(b))
	}
	// Each extension's length fits inside the block.
	for e := b[off:]; len(e) > 0; {
		if len(e) < 4 || 4+be(e[2:4]) > len(e) {
			t.Fatalf("an extension runs past the block: % x", e[:min(len(e), 8)])
		}
		e = e[4+be(e[2:4]):]
	}
}

// A real TLS server answers it with a handshake record, not an alert: it is a hello a server accepts.
func TestAGoTLSServerAnswersTheClientHelloWithAHandshake(t *testing.T) {
	stub := ctltest.StartReachStub(t, ctltest.StubTLS, "127.0.0.1", 0)
	c, err := net.DialTimeout("tcp", stub.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write(reachClientHello()); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := make([]byte, 3)
	if _, err := c.Read(got); err != nil {
		t.Fatal(err)
	}
	if got[0] != 22 || got[1] != 3 {
		t.Errorf("the server's first bytes are % x, want a handshake record (16 03)", got)
	}
}

func TestReadProbeTarget(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	good := map[string]string{"P_HOST": "gw-seed.example", "P_PORT": "40123", "P_LABEL": "example-gateway"}
	without := func(k string) map[string]string {
		m := map[string]string{}
		for a, b := range good {
			if a != k {
				m[a] = b
			}
		}
		return m
	}
	with := func(k, v string) map[string]string {
		m := map[string]string{}
		for a, b := range good {
			m[a] = b
		}
		m[k] = v
		return m
	}
	// The host and the port are in the label of no good set of settings, whatever the spelling.
	p, err := readProbeTarget(get(good), "P_")
	if err != nil || p != (probeTarget{host: "gw-seed.example", port: 40123, label: "example-gateway"}) {
		t.Fatalf("the good settings: %+v, %v", p, err)
	}
	for _, tc := range []struct {
		name string
		m    map[string]string
		want string // a part of the one message; it names the setting
	}{
		{"no host", without("P_HOST"), "P_HOST is missing"},
		{"no port", without("P_PORT"), "P_PORT is missing"},
		{"no label", without("P_LABEL"), "P_LABEL is missing"},
		{"a host with a quote", with("P_HOST", "x';touch${IFS}z;'"), "P_HOST must be"},
		{"a host with a space", with("P_HOST", "gw seed"), "P_HOST must be"},
		{"a host that starts with a dash", with("P_HOST", "-gw"), "P_HOST must be"},
		{"port zero", with("P_PORT", "0"), "P_PORT must be"},
		{"a port over 65535", with("P_PORT", "65536"), "P_PORT must be"},
		{"a port with a sign", with("P_PORT", "+22"), "P_PORT must be"},
		{"a port with a leading zero", with("P_PORT", "022"), "P_PORT must be"},
		{"a label with a space", with("P_LABEL", "my gateway"), "P_LABEL must be"},
		{"a label over 40 characters", with("P_LABEL", strings.Repeat("a", 41)), "P_LABEL must be"},
		{"a label that holds the host", with("P_LABEL", "the-gw-seed.example-one"), "P_LABEL holds the host"},
		{"a label that holds the port", with("P_LABEL", "gateway-40123"), "P_LABEL holds the port"},
		// The same name in other spellings is still the name (review of PR 235, B1).
		{"a label that is the host in capitals", with("P_LABEL", "GW-SEED.EXAMPLE"), "P_LABEL holds the host"},
		{"a label that holds the host in capitals among other characters", with("P_LABEL", "via-Gw-Seed.Example-1"), "P_LABEL holds the host"},
		{"a host with the root dot and a label without it", func() map[string]string {
			m := with("P_HOST", "gw-seed.example.")
			m["P_LABEL"] = "gw-seed.example"
			return m
		}(), "P_LABEL holds the host"},
		{"a label that spells the host with other separators", with("P_LABEL", "gw_seed-example"), "P_LABEL holds the host"},
		{"a label that runs the host together", with("P_LABEL", "gwseedexample"), "P_LABEL holds the host"},
		{"a label that holds the port between separators", with("P_LABEL", "gateway-401-23"), "P_LABEL holds the port"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readProbeTarget(get(tc.m), "P_")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want one containing %q", err, tc.want)
			}
			// The message names the settings, never their values.
			for k, v := range tc.m {
				if k != "P_LABEL" && strings.Contains(err.Error(), v) {
					t.Errorf("the message holds the value of %s: %v", k, err)
				}
			}
		})
	}
	// Every problem is reported together.
	_, err = readProbeTarget(get(map[string]string{}), "P_")
	for _, k := range []string{"P_HOST", "P_PORT", "P_LABEL"} {
		if err == nil || !strings.Contains(err.Error(), k) {
			t.Errorf("with no settings the error is %v, want it to name %s", err, k)
		}
	}
}

// The script renders to text with no placeholder, the host and the port exactly where the script reads them, and
// the hello as printf escapes only.
func TestTheRenderedProbeHasItsTargetAndNoPlaceholder(t *testing.T) {
	s := string(probeTarget{host: "gw-seed.example", port: 40123, label: "example-gateway"}.agent(5, 3))
	for _, want := range []string{"host='gw-seed.example'\n", "port='40123'\n", "connect_s=5\n", "read_s=3\n", `printf "\x16\x03\x01`} {
		if !strings.Contains(s, want) {
			t.Errorf("the script lacks %q", want)
		}
	}
	if m := regexp.MustCompile(`@[A-Z_]+@`).FindString(s); m != "" {
		t.Errorf("the placeholder %s is left in the script", m)
	}
	if strings.Count(s, "gw-seed.example") != 1 || strings.Count(s, "40123") != 1 {
		t.Errorf("the host or the port is in the script more than once")
	}
	if err := exec.Command("bash", "-n", "-c", s).Run(); err != nil {
		t.Errorf("the rendered script does not parse: %v", err)
	}
}

// Every line the probe can write has its words in the text to post, and the text names the label and the row's
// partition and qos, and nothing else of the target.
func TestThePostsCarryTheLabelAndNotTheTarget(t *testing.T) {
	lines := reachLines()
	if len(lines) != 5+len(reachOutcomes) {
		t.Fatalf("%d lines, want %d: the outcomes and answers the probe's header gives", len(lines), 5+len(reachOutcomes))
	}
	row, _ := alpha.RowByName("reach-helios")
	for _, oa := range lines {
		outcome, answer := oa[0], oa[1]
		verdict := "FAIL"
		if outcome == "reachable" {
			verdict = "PASS"
		}
		got := reachPosts(row, "example-gateway", verdict, outcome, answer, qosUsed("now", ""), 1, 1, "1Gi")
		for _, want := range []string{"reach-helios: " + verdict, "as example-gateway: outcome " + outcome, "partition plgrid-gpu-gh200, qos now (requested)", "1 GPU, 1 CPU and 1Gi"} {
			if !strings.Contains(got, want) {
				t.Errorf("%s/%s: the text lacks %q:\n%s", outcome, answer, want, got)
			}
		}
		if outcome == "reachable" && !strings.Contains(got, "something answered") && !strings.Contains(got, "bare TCP connect succeeded") {
			t.Errorf("reachable/%s says nothing of what answered:\n%s", answer, got)
		}
		if strings.Count(got, "\n") != 4 {
			t.Errorf("%s/%s: want the verdict line and three more:\n%s", outcome, answer, got)
		}
	}
	for o := range outcomeText {
		if _, ok := reachLines()[sumOf(reachLine(o, "none"))]; !ok {
			t.Errorf("outcome %s has a text but is not a line the probe writes", o)
		}
	}
}

func TestTheQOSUsedNamesTheRequestedAndThePartitionsOwn(t *testing.T) {
	for _, tc := range []struct{ requested, own, want string }{
		{"now", "plgrid", "now (requested; the partition's own is plgrid)"},
		{"now", "", "now (requested)"},
		{"", "plgrid-now", "plgrid-now (the partition's own; none requested)"},
		{"", "", "none requested"},
	} {
		if got := qosUsed(tc.requested, tc.own); got != tc.want {
			t.Errorf("qosUsed(%q, %q) = %q, want %q", tc.requested, tc.own, got, tc.want)
		}
	}
}

func TestADryRunConnectsOnlyToTheLoopbackOrAnInvalidName(t *testing.T) {
	for host, want := range map[string]bool{"127.0.0.1": true, "127.77.1.2": true, "no-such-gateway.invalid": true, "192.0.2.1": false, "example.org": false, "10.1.2.3": false} {
		if got := loopbackOnly(host); got != want {
			t.Errorf("loopbackOnly(%q) = %t, want %t", host, got, want)
		}
	}
}

func sumOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// labelHolds, on its own: what a public label gives away, in any case and with any separators, and what it does not.
func TestALabelHoldsTheHostAndThePortInAnySpelling(t *testing.T) {
	for _, tc := range []struct {
		label, host string
		port        int
		want        string
	}{
		{"example-gateway", "gw-seed.example", 40123, ""},
		{"gateway.private.example", "gateway.private.example", 40123, "host"},
		{"GATEWAY.PRIVATE.EXAMPLE", "gateway.private.example", 40123, "host"},
		{"gateway.private.example", "GATEWAY.PRIVATE.EXAMPLE.", 40123, "host"},
		{"gateway.private.example.", "gateway.private.example", 40123, "host"},
		{"Gateway-Private-Example", "gateway.private.example", 40123, "host"},
		{"a-gatewayprivateexample-b", "gateway.private.example", 40123, "host"},
		{"10-1-2-3", "10.1.2.3", 40123, "host"},
		{"gateway", "gateway.private.example", 40123, ""}, // a part of the host is not the host
		{"x40123x", "gw.example", 40123, "port"},
		{"40-123", "gw.example", 40123, "port"},
		{"4012", "gw.example", 40123, ""},
		{"example-gateway", "gw.example", 0, ""},
	} {
		if got := labelHolds(tc.label, tc.host, tc.port); got != tc.want {
			t.Errorf("labelHolds(%q, %q, %d) = %q, want %q", tc.label, tc.host, tc.port, got, tc.want)
		}
	}
}
