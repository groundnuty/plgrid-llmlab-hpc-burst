package agentapi

import (
	"strings"
	"testing"
)

var exampleRelay = RelayArgs{Channel: RelayServing, Dir: "/scratch/example/stage/alloc/hb-example-0123456789ab",
	Allocation: "0a1b2c3d-0000-4000-8000-000000000001", Epoch: 3, Seq: 7}

// TestRelayLineIsTheHBEPsLine pins the line to HBEP-6's text (Dial-home modes,
// host-dialed mode, at b12aa9f): "srun -u --overlap --jobid=<job> --ntasks=1
// <the job's agent binary> relay <channel> --dir <Allocation directory>
// --allocation <UID> --epoch <E> --seq <N>". The expected string is written
// out here from the HBEP, not built with RelayLine's own parts.
func TestRelayLineIsTheHBEPsLine(t *testing.T) {
	agent := "/scratch/example/stage/agent/" + strings.Repeat("a", 64) + "/hb-agent"
	want := "srun -u --overlap --jobid=4242 --ntasks=1 " + agent + " relay serving --dir /scratch/example/stage/alloc/hb-example-0123456789ab" +
		" --allocation 0a1b2c3d-0000-4000-8000-000000000001 --epoch 3 --seq 7"
	got, err := RelayLine("4242", agent, exampleRelay)
	if err != nil || got != want {
		t.Errorf("RelayLine = %q, %v\nwant %q", got, err, want)
	}
}

// TestRelayLineWithoutUnbufferedFails is the Test plan's argument-list test:
// an srun command line without -u fails it. It reads the line's words, so a
// line that dropped -u, or moved it after the program where srun would not
// read it, fails here.
func TestRelayLineWithoutUnbufferedFails(t *testing.T) {
	line, err := RelayLine("4242", "/scratch/example/hb-agent", exampleRelay)
	if err != nil {
		t.Fatal(err)
	}
	words := strings.Fields(line)
	if len(words) < 2 || words[0] != "srun" {
		t.Fatalf("the relay line %q does not run srun", line)
	}
	prog := -1
	for i, w := range words[1:] {
		if !strings.HasPrefix(w, "-") {
			prog = i + 1
			break
		}
	}
	if prog < 0 {
		t.Fatalf("the relay line %q names no program", line)
	}
	opts := words[1:prog]
	for _, need := range []string{"-u", "--overlap", "--jobid=4242", "--ntasks=1"} {
		found := false
		for _, o := range opts {
			if o == need {
				found = true
			}
		}
		if !found {
			t.Errorf("the relay line %q lacks srun's %s before the program (HBEP-6: -u is required, or srun holds output that ends without a newline)", line, need)
		}
	}
	if words[prog] != "/scratch/example/hb-agent" || words[prog+1] != "relay" {
		t.Errorf("the relay line's program is %q, want the agent binary and relay", words[prog:])
	}
	back, err := ParseRelayArgs(words[prog+2:])
	if err != nil || back != exampleRelay {
		t.Errorf("the relay's own parse of its arguments = %+v, %v; want %+v", back, err, exampleRelay)
	}
}

func TestRelayLineAndArgsRefuse(t *testing.T) {
	for name, c := range map[string]struct {
		job, agent string
		mod        func(*RelayArgs)
	}{
		"no job":             {"", "/x/hb-agent", nil},
		"a job that is text": {"12a", "/x/hb-agent", nil},
		"a relative agent":   {"12", "x/hb-agent", nil},
		"a shell word":       {"12", "/x/hb-agent;rm", nil},
		"another channel":    {"12", "/x/hb-agent", func(a *RelayArgs) { a.Channel = "plugin" }},
		"a relative dir":     {"12", "/x/hb-agent", func(a *RelayArgs) { a.Dir = "alloc/x" }},
		"a dot-dot dir":      {"12", "/x/hb-agent", func(a *RelayArgs) { a.Dir = "/s/../x" }},
		"a UID with a space": {"12", "/x/hb-agent", func(a *RelayArgs) { a.Allocation = "a b" }},
		"no UID":             {"12", "/x/hb-agent", func(a *RelayArgs) { a.Allocation = "" }},
		"a negative epoch":   {"12", "/x/hb-agent", func(a *RelayArgs) { a.Epoch = -1 }},
		"a zero sequence":    {"12", "/x/hb-agent", func(a *RelayArgs) { a.Seq = 0 }},
	} {
		a := exampleRelay
		if c.mod != nil {
			c.mod(&a)
		}
		if line, err := RelayLine(c.job, c.agent, a); err == nil {
			t.Errorf("%s: RelayLine = %q, want an error", name, line)
		}
	}
	for name, args := range map[string][]string{
		"nothing":         nil,
		"no flags":        {"control"},
		"no seq":          {"control", "--dir", "/d", "--allocation", "u", "--epoch", "1"},
		"an extra word":   {"control", "--dir", "/d", "--allocation", "u", "--epoch", "1", "--seq", "1", "x"},
		"an unknown flag": {"control", "--dir", "/d", "--allocation", "u", "--epoch", "1", "--seq", "1", "--port", "9"},
	} {
		if a, err := ParseRelayArgs(args); err == nil {
			t.Errorf("%s: ParseRelayArgs = %+v, want an error", name, a)
		}
	}
}

func TestRelayExchangeLines(t *testing.T) {
	if got := RelayPreamble("u-1"); got != "hpcburst-relay/1 u-1" {
		t.Errorf("RelayPreamble = %q, want HBEP-6's \"hpcburst-relay/1 <Allocation UID>\"", got)
	}
	if got := RelaySocketFile("/s/alloc/hb-x"); got != "/s/alloc/hb-x/relay-socket" {
		t.Errorf("RelaySocketFile = %q", got)
	}
	ch, e, n, err := ParseRelayHello(RelayHello(exampleRelay))
	if err != nil || ch != RelayServing || e != 3 || n != 7 {
		t.Errorf("the hello's round trip: %s %d %d %v", ch, e, n, err)
	}
	for _, bad := range []string{"", "relay/1 serving 3", "relay/2 serving 3 7", "relay/1 plugin 3 7", "relay/1 serving x 7", "relay/1 serving 3 0"} {
		if _, _, _, err := ParseRelayHello(bad); err == nil {
			t.Errorf("ParseRelayHello(%q) was accepted", bad)
		}
	}
}

func TestRelayChannelPortsAreTheAgentEndpointAndTheDialHomeListener(t *testing.T) {
	h := Host{PortBase: 29780}
	got := h.RelayChannelPorts()
	if got[RelayControl] != 29783 || got[RelayServing] != 29787 || len(got) != 2 {
		t.Errorf("RelayChannelPorts = %v, want control 29783 (base+3) and serving 29787 (base+7)", got)
	}
}

func TestHostDialedConfig(t *testing.T) {
	c := AgentConfig{
		ProtocolVersion: ProtocolVersion, Tag: "hb-example-0123456789ab", Mode: DialHomeHostDialed,
		CertSPKISHA256: strings.Repeat("b", 64), FrpcDigest: strings.Repeat("c", 64),
		StagingDir: "/scratch/example/stage", DialHomeDir: "/home/example/.hpcburst/dial-home",
		Allocation: "0a1b2c3d-0000-4000-8000-000000000001",
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("a host-dialed agent.json: %v", err)
	}
	noUID := c
	noUID.Allocation = ""
	if err := noUID.Validate(); err == nil {
		t.Error("a host-dialed agent.json without the Allocation's UID was accepted: the agent's greeting needs it")
	}
	withEndpoint := c
	withEndpoint.Endpoint = "front.example:8443"
	if err := withEndpoint.Validate(); err == nil || !strings.Contains(err.Error(), "host-dialed") {
		t.Errorf("an endpoint in host-dialed mode: %v, want a refusal that names the mode", err)
	}
	withSSH := c
	withSSH.SSH = &SSHForward{User: "example", AgentPort: 1, DialHomePort: 2}
	if err := withSSH.Validate(); err == nil {
		t.Error("ssh settings in host-dialed mode were accepted")
	}
	noDigest := c
	noDigest.FrpcDigest = ""
	if err := noDigest.Validate(); err == nil {
		t.Error("a host-dialed agent.json without frpc's digest was accepted: frpc runs in every mode")
	}
	direct := c
	direct.Mode = DialHomeDirect
	if err := direct.Validate(); err == nil {
		t.Error("direct mode with no endpoint was accepted")
	}
	direct.Endpoint = "front.example:8443"
	if err := direct.Validate(); err == nil || !strings.Contains(err.Error(), "allocation") {
		t.Errorf("an Allocation UID in direct mode: %v, want a refusal", err)
	}
	if !ValidDialHomeMode(DialHomeHostDialed) || ValidDialHomeMode("host") {
		t.Error("ValidDialHomeMode")
	}
}
