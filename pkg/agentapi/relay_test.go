package agentapi

import (
	"strings"
	"testing"
)

// TestRelayLineIsTheHBEPsLine pins the line to HBEP-6's text (Dial-home modes,
// host-dialed mode): "srun -u --overlap --jobid=<job> --ntasks=1 <the job's
// agent binary> relay <channel>". The expected strings are written out here
// from the HBEP, not built with RelayLine's own parts.
func TestRelayLineIsTheHBEPsLine(t *testing.T) {
	for ch, want := range map[RelayChannel]string{
		RelayControl: "srun -u --overlap --jobid=4242 --ntasks=1 /scratch/example/stage/agent/" + strings.Repeat("a", 64) + "/hb-agent relay control",
		RelayServing: "srun -u --overlap --jobid=4242 --ntasks=1 /scratch/example/stage/agent/" + strings.Repeat("a", 64) + "/hb-agent relay serving",
	} {
		got, err := RelayLine("4242", "/scratch/example/stage/agent/"+strings.Repeat("a", 64)+"/hb-agent", ch)
		if err != nil {
			t.Fatalf("RelayLine(%s): %v", ch, err)
		}
		if got != want {
			t.Errorf("RelayLine(%s) = %q, want %q", ch, got, want)
		}
	}
}

// TestRelayLineWithoutUnbufferedFails is the Test plan's argument-list test:
// an srun command line without -u fails it. It reads the line's words, so a
// line that dropped -u, or moved it after the program where srun would not
// read it, fails here.
func TestRelayLineWithoutUnbufferedFails(t *testing.T) {
	line, err := RelayLine("4242", "/scratch/example/hb-agent", RelayServing)
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
	if got := strings.Join(words[prog:], " "); got != "/scratch/example/hb-agent relay serving" {
		t.Errorf("the relay line's program is %q, want the agent binary, relay and the channel", got)
	}
}

func TestRelayLineRefuses(t *testing.T) {
	for name, c := range map[string]struct {
		job, agent string
		ch         RelayChannel
	}{
		"no job":             {"", "/x/hb-agent", RelayControl},
		"a job that is text": {"12a", "/x/hb-agent", RelayControl},
		"a leading zero":     {"0123", "/x/hb-agent", RelayControl},
		"a relative agent":   {"12", "x/hb-agent", RelayControl},
		"a space":            {"12", "/x y/hb-agent", RelayControl},
		"a dot-dot":          {"12", "/x/../hb-agent", RelayControl},
		"a shell word":       {"12", "/x/hb-agent;rm", RelayControl},
		"another channel":    {"12", "/x/hb-agent", "plugin"},
	} {
		if line, err := RelayLine(c.job, c.agent, c.ch); err == nil {
			t.Errorf("%s: RelayLine = %q, want an error", name, line)
		}
	}
}

func TestRelayDirPrefixAndCheck(t *testing.T) {
	p, err := RelayDirPrefix("4242")
	if err != nil || p != "hb-relay-4242-" {
		t.Fatalf("RelayDirPrefix = %q, %v", p, err)
	}
	if err := CheckRelayDir("/tmp", "4242", "/tmp/hb-relay-4242-918273645"); err != nil {
		t.Errorf("a directory the agent makes: %v", err)
	}
	if s := RelaySocket("/tmp/hb-relay-4242-9", RelayServing); s != "/tmp/hb-relay-4242-9/serving.sock" {
		t.Errorf("RelaySocket = %q", s)
	}
	for name, dir := range map[string]string{
		"the predictable name": "/tmp/hb-relay-4242",
		"another job's":        "/tmp/hb-relay-4243-1",
		"another root":         "/var/tmp/hb-relay-4242-1",
		"a dot-dot":            "/tmp/x/../hb-relay-4242-1",
		"a nested one":         "/tmp/a/hb-relay-4242-1",
		"a slash in the rest":  "/tmp/hb-relay-4242-1/x",
		"too long":             "/tmp/hb-relay-4242-" + strings.Repeat("9", 90),
	} {
		if err := CheckRelayDir("/tmp", "4242", dir); err == nil {
			t.Errorf("%s: CheckRelayDir accepted %q", name, dir)
		}
	}
	if _, err := RelayDirPrefix("12a"); err == nil {
		t.Error("a job ID of text was accepted")
	}
}

func TestRelayPointerPathAndStagingDir(t *testing.T) {
	if p, err := RelayPointerPath("/scratch/example/stage", "4242"); err != nil || p != "/scratch/example/stage/relay/4242" {
		t.Errorf("RelayPointerPath = %q, %v", p, err)
	}
	if _, err := RelayPointerPath("stage", "4242"); err == nil {
		t.Error("a relative staging directory was accepted")
	}
	agent := "/scratch/example/stage/agent/" + strings.Repeat("a", 64) + "/hb-agent"
	if s, err := StagingDirOfAgent(agent); err != nil || s != "/scratch/example/stage" {
		t.Errorf("StagingDirOfAgent = %q, %v", s, err)
	}
	for _, bad := range []string{"/usr/bin/hb-agent", "/s/agent/abc/hb-agent", "/s/agent/" + strings.Repeat("a", 64) + "/other", "hb-agent"} {
		if s, err := StagingDirOfAgent(bad); err == nil {
			t.Errorf("StagingDirOfAgent(%q) = %q, want an error", bad, s)
		}
	}
}

// TestRelayRootIsNotTMPDIR: the root never follows TMPDIR, which a step that
// srun starts from the login node does not share with the batch script.
func TestRelayRootIsNotTMPDIR(t *testing.T) {
	env := map[string]string{"TMPDIR": "/dev/shm/job-1"}
	if r := RelayRoot(func(k string) string { return env[k] }); r != DefaultRelayRoot {
		t.Errorf("RelayRoot = %q with only TMPDIR set, want %q", r, DefaultRelayRoot)
	}
	env[EnvRelayRoot] = "/home/example/t/r"
	if r := RelayRoot(func(k string) string { return env[k] }); r != "/home/example/t/r" {
		t.Errorf("RelayRoot = %q, want the override", r)
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
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("a host-dialed agent.json: %v", err)
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
	if !ValidDialHomeMode(DialHomeHostDialed) || ValidDialHomeMode("host") {
		t.Error("ValidDialHomeMode")
	}
}
