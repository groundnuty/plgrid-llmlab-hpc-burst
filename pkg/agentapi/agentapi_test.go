package agentapi

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func validConfig() AgentConfig {
	return AgentConfig{
		ProtocolVersion: "v1",
		Tag:             "hb-helios-a-x7k2p-1bda24d00504",
		Mode:            DialHomeSSH,
		Endpoint:        "control.example:22",
		SSH:             &SSHForward{User: "example-user", AgentPort: 29225},
		CertSPKISHA256:  strings.Repeat("a", 64),
		StagingDir:      "/scratch/example-user",
		DialHomeDir:     "/home/example-user/.hpcburst/dial-home",
	}
}

func TestAgentConfigEncodeIsStableAndRoundTrips(t *testing.T) {
	c := validConfig()
	a, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Encode()
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("two encodings differ: %v\n%s\n%s", err, a, b)
	}
	// The field names are the wire contract: check them in the bytes, not
	// through the decoder under test.
	for _, key := range []string{`"protocolVersion": "v1"`, `"tag": "hb-helios-a-x7k2p-1bda24d00504"`, `"mode": "ssh"`,
		`"endpoint": "control.example:22"`, `"user": "example-user"`, `"agentPort": 29225`,
		`"certSPKISHA256": "` + strings.Repeat("a", 64) + `"`, `"stagingDir": "/scratch/example-user"`,
		`"dialHomeDir": "/home/example-user/.hpcburst/dial-home"`} {
		if !bytes.Contains(a, []byte(key)) {
			t.Errorf("agent.json lacks %s:\n%s", key, a)
		}
	}
	if a[len(a)-1] != '\n' {
		t.Error("agent.json does not end with a newline")
	}
	got, err := DecodeAgentConfig(a)
	if err != nil || *got.SSH != *c.SSH || got.Tag != c.Tag || got.StagingDir != c.StagingDir {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	// A field a newer provider adds is ignored, never refused (HBEP-7, Compatibility).
	newer := bytes.Replace(a, []byte(`"mode"`), []byte(`"futureField": 1, "mode"`), 1)
	if _, err := DecodeAgentConfig(newer); err != nil {
		t.Errorf("an unknown field was refused: %v", err)
	}
}

func TestAgentConfigValidate(t *testing.T) {
	direct := validConfig()
	direct.Mode, direct.Endpoint, direct.SSH = DialHomeDirect, "front.example:8443", nil
	if err := direct.Validate(); err != nil {
		t.Errorf("a direct-mode config was refused: %v", err)
	}
	for name, edit := range map[string]func(*AgentConfig){
		"another protocol version":        func(c *AgentConfig) { c.ProtocolVersion = "v2" },
		"a tag that is not a job tag":     func(c *AgentConfig) { c.Tag = "my-job" },
		"a 57-character tag":              func(c *AgentConfig) { c.Tag = "hb-" + strings.Repeat("a", 41) + "-1bda24d00504" },
		"an endpoint without a port":      func(c *AgentConfig) { c.Endpoint = "control.example" },
		"ssh mode on another port":        func(c *AgentConfig) { c.Endpoint = "control.example:2222" },
		"ssh mode without ssh settings":   func(c *AgentConfig) { c.SSH = nil },
		"ssh settings in direct mode":     func(c *AgentConfig) { c.Mode, c.Endpoint = DialHomeDirect, "front.example:8443" },
		"an unknown mode":                 func(c *AgentConfig) { c.Mode = "tunnel" },
		"no agent port":                   func(c *AgentConfig) { c.SSH.AgentPort = 0 },
		"a user with a space":             func(c *AgentConfig) { c.SSH.User = "a b" },
		"a short pin":                     func(c *AgentConfig) { c.CertSPKISHA256 = "abc" },
		"a staging dir with a variable":   func(c *AgentConfig) { c.StagingDir = "$SCRATCH/x" },
		"a relative staging dir":          func(c *AgentConfig) { c.StagingDir = "scratch" },
		"a dial-home dir with ~":          func(c *AgentConfig) { c.DialHomeDir = "~/.hpcburst/dial-home" },
		"a staging dir that climbs":       func(c *AgentConfig) { c.StagingDir = "/scratch/../etc" },
		"a staging dir with a semicolon":  func(c *AgentConfig) { c.StagingDir = "/scratch;rm" },
		"a staging dir with a whitespace": func(c *AgentConfig) { c.StagingDir = "/scratch dir" },
	} {
		c := validConfig()
		ssh := *c.SSH
		c.SSH = &ssh
		edit(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s was accepted", name)
		}
		if _, err := c.Encode(); err == nil {
			t.Errorf("%s was encoded", name)
		}
	}
}

// One pin, computed outside PinOf from the certificate's SubjectPublicKeyInfo,
// is the only key the agent trusts.
func TestPinnedTLS(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS13}
	srv.StartTLS()
	defer srv.Close()
	cert := srv.Certificate()
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	want := hex.EncodeToString(sum[:])
	if got := PinOf(cert); got != want {
		t.Fatalf("PinOf = %s, want %s", got, want)
	}

	get := func(pin string) error {
		client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: ClientTLSConfig(pin)}}
		resp, err := client.Get(srv.URL + PathRegister)
		if err == nil {
			resp.Body.Close()
		}
		return err
	}
	if err := get(want); err != nil {
		t.Errorf("the pinned key was refused: %v", err)
	}
	if err := get(strings.Repeat("0", 64)); err == nil {
		t.Error("another pin was accepted")
	}
	if err := VerifyPin(nil, want); err == nil {
		t.Error("no certificate was accepted")
	}
}

func TestErrorStatusesAndBody(t *testing.T) {
	for code, status := range map[Code]int{
		CodeBadRequest: 400, CodeUnauthenticated: 401, CodeJobMismatch: 403,
		CodeNotRegistered: 409, CodeUnavailable: 503, CodeInternal: 500, "SomethingNew": 500,
	} {
		if got := (&Error{Code: code}).HTTPStatus(); got != status {
			t.Errorf("%s: status %d, want %d", code, got, status)
		}
	}
	b, err := json.Marshal(Errorf(CodeJobMismatch, "job %s is not the recorded one", "1001"))
	if err != nil || string(b) != `{"code":"JobMismatch","message":"job 1001 is not the recorded one"}` {
		t.Errorf("error body = %s, %v", b, err)
	}
	var e *Error
	if !errors.As(error(Errorf(CodeNotRegistered, "x")), &e) || e.Code != CodeNotRegistered {
		t.Error("Errorf does not give an *Error")
	}
}

func TestDesiredStateVersion(t *testing.T) {
	base := DesiredState{Action: ActionRun, Instance: &DesiredInstance{
		Name: "inst-a", UID: "u1", Workers: []DesiredWorker{{Name: "worker-0", Generation: 1, EngineSocket: "/hpcburst/run/worker-0.sock"}},
	}}
	v1 := base.WithVersion().Version
	if len(v1) != 16 {
		t.Fatalf("version %q is not 16 hex characters", v1)
	}
	// The same content, built separately and with a stale version, gets the same version.
	again := DesiredState{Version: "stale", Action: ActionRun, Instance: &DesiredInstance{
		Name: "inst-a", UID: "u1", Workers: []DesiredWorker{{Name: "worker-0", Generation: 1, EngineSocket: "/hpcburst/run/worker-0.sock"}},
	}}
	if got := again.WithVersion().Version; got != v1 {
		t.Errorf("equal states got versions %s and %s", v1, got)
	}
	// Raising a restart generation changes it.
	raised := base
	inst := *base.Instance
	inst.Workers = []DesiredWorker{{Name: "worker-0", Generation: 2, EngineSocket: "/hpcburst/run/worker-0.sock"}}
	raised.Instance = &inst
	if got := raised.WithVersion().Version; got == v1 {
		t.Error("a raised worker generation kept the version")
	}
	idle := DesiredState{Action: ActionIdle}
	if idle.WithVersion().Version == v1 {
		t.Error("idle and run share a version")
	}
}

func TestDurationJSON(t *testing.T) {
	b, err := json.Marshal(AgentSettings{ProbeInterval: Duration(10 * time.Second), StopGrace: Duration(1500 * time.Millisecond)})
	if err != nil || string(b) != `{"probeInterval":"10s","stopGrace":"1.5s"}` {
		t.Fatalf("marshal = %s, %v", b, err)
	}
	var s AgentSettings
	if err := json.Unmarshal([]byte(`{"probeInterval":"2m","stopGrace":"250ms"}`), &s); err != nil ||
		time.Duration(s.ProbeInterval) != 2*time.Minute || time.Duration(s.StopGrace) != 250*time.Millisecond {
		t.Fatalf("unmarshal = %+v, %v", s, err)
	}
	for _, bad := range []string{`{"stopGrace":10}`, `{"stopGrace":"soon"}`} {
		if err := json.Unmarshal([]byte(bad), &s); err == nil {
			t.Errorf("%s was accepted", bad)
		}
	}
}

func TestLayout(t *testing.T) {
	digest := strings.Repeat("3", 64)
	for got, want := range map[string]string{
		StagedAgentPath(digest):                      "agent/" + digest + "/hb-agent",
		AllocationDir("hb-a-1bda24d00504"):           "alloc/hb-a-1bda24d00504",
		StagedCredentialPath("hb-a-1bda24d00504"):    "alloc/hb-a-1bda24d00504/credential",
		StagedConfigPath("hb-a-1bda24d00504"):        "alloc/hb-a-1bda24d00504/agent.json",
		StagedCheckpointDir("example-org/m", "abc"):  "checkpoints/example-org/m/abc",
		AllocationCredentialPath("/state/cred", "u"): "/state/cred/allocations/u",
		InstanceKeyPath("/state/cred", "u"):          "/state/cred/instances/u",
		AgentBuildName("aarch64"):                    "hb-agent-aarch64",
		ContainerEngineSocket(WorkerName(0)):         "/hpcburst/run/worker-0.sock",
		ContainerChatTemplate:                        "/hpcburst/run/chat-template.jinja",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	// The worker names here and in the API's restart-generation annotations agree.
	for i := range 3 {
		if WorkerName(i) != v1alpha1.WorkerName(i) {
			t.Errorf("WorkerName(%d): %q here, %q in the API", i, WorkerName(i), v1alpha1.WorkerName(i))
		}
	}
}

func TestCheckCredential(t *testing.T) {
	if err := CheckCredential([]byte(strings.Repeat("0123456789abcdef", 4))); err != nil {
		t.Errorf("a credential was refused: %v", err)
	}
	secret := strings.Repeat("0123456789ABCDEF", 4)
	for _, bad := range []string{"", strings.Repeat("a", 63), strings.Repeat("a", 64) + "\n", secret} {
		err := CheckCredential([]byte(bad))
		if err == nil {
			t.Errorf("%q was accepted", bad)
			continue
		}
		if bad != "" && strings.Contains(err.Error(), bad) {
			t.Errorf("the error repeats the credential: %v", err)
		}
	}
}

// The image format the agent is sent, its staging path, and the identity on
// the wire: a random agent instance ID in a header, and the scheduler's restart
// count in the registration (owner ruling: never the kernel's boot ID).
func TestImageFormatAndIdentityOnTheWire(t *testing.T) {
	digest := "sha256:" + strings.Repeat("ab", 32)
	hex := strings.Repeat("ab", 32)
	for got, want := range map[string]string{
		StagedImagePath(digest, ImageFormatSIF):     "images/" + hex + ".sif",
		StagedImagePath(digest, ImageFormatSandbox): "images/" + hex + ".sandbox",
		StagedImagePath(digest, ""):                 "images/" + hex + ".sif",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if string(ImageFormatSIF) != string(v1alpha1.ImageFormatSIF) || string(ImageFormatSandbox) != string(v1alpha1.ImageFormatSandbox) {
		t.Error("the wire's image formats differ from the API's")
	}

	var s AgentSettings
	if err := json.Unmarshal([]byte(`{"probeInterval":"10s","stopGrace":"10s","imageFormat":"sandbox"}`), &s); err != nil {
		t.Fatal(err)
	}
	if s.ImageFormat != ImageFormatSandbox || time.Duration(s.StopGrace) != 10*time.Second {
		t.Errorf("settings = %+v", s)
	}
	// An older controller sends no format; the zero value means sif.
	var older AgentSettings
	if err := json.Unmarshal([]byte(`{"probeInterval":"10s","stopGrace":"10s"}`), &older); err != nil || older.ImageFormat != "" {
		t.Errorf("settings without a format = %+v, %v", older, err)
	}

	b, err := json.Marshal(RegisterRequest{Node: "node-a", CPUArch: "aarch64", RestartCount: 1})
	if err != nil || !strings.Contains(string(b), `"restartCount":1`) {
		t.Errorf("register body = %s, %v", b, err)
	}
	b, err = json.Marshal(Launch{Image: "i", ImageDigest: digest, Command: []string{"vllm"}, MaxNumSeqs: 256})
	if err != nil || !strings.Contains(string(b), `"maxNumSeqs":256`) {
		t.Errorf("launch = %s, %v", b, err)
	}
	if HeaderAgentInstanceID != "Hpcburst-Agent-Instance-Id" || http.CanonicalHeaderKey(HeaderAgentInstanceID) != HeaderAgentInstanceID {
		t.Errorf("header %q is not the canonical Hpcburst-Agent-Instance-Id", HeaderAgentInstanceID)
	}
}

// Every tag v1alpha1.JobTag produces must pass agent.json validation (HBEP-3
// fixes the tag's formula, so the validation follows it). The Allocation's name
// is cut to 40 characters, which can end in "-" or ".".
func TestJobTagsValidate(t *testing.T) {
	a := func(n int) string { return strings.Repeat("a", n) }
	// A 253-character DNS subdomain with dots and hyphens throughout.
	long := strings.Repeat("ab-cd.ef", 32)[:253]
	for _, c := range []struct {
		name     string
		alloc    string
		cutEndIn string // how the name's first 40 characters end, to prove the case is what it claims
	}{
		{"a 39-character provider name and its 5 random characters: the cut ends at the hyphen", a(39) + "-x7k2p", "-"},
		{"a name whose 40th character is a dot", a(39) + ".x7k2p", "."},
		{"a name whose 40th character is a hyphen, as a long deployment name", "example-model-deployment-for-long-conte-x1", "-"},
		{"a 40-character name", a(40), "a"},
		{"a 1-character name", "a", "a"},
		{"a 253-character name", long, ""},
		{"a 38-character provider name and its suffix: the cut ends after one random character", a(38) + "-x7k2p", "x"},
	} {
		t.Run(c.name, func(t *testing.T) {
			tag := v1alpha1.JobTag("default", c.alloc, "11111111-2222-3333-4444-555555555555")
			cfg := validConfig()
			cfg.Tag = tag
			if err := cfg.Validate(); err != nil {
				t.Errorf("tag %q (%d characters) was refused: %v", tag, len(tag), err)
			}
			if _, err := cfg.Encode(); err != nil {
				t.Errorf("agent.json for tag %q was not encoded: %v", tag, err)
			}
			if len(tag) > 56 {
				t.Errorf("tag %q is %d characters, JobTagLength is 56", tag, len(tag))
			}
			// The cut name, between "hb-" and "-<12 hex>", ends as the case says.
			cut := tag[len("hb-") : len(tag)-len("-")-12]
			if c.cutEndIn != "" && !strings.HasSuffix(cut, c.cutEndIn) {
				t.Errorf("the cut name %q does not end in %q: the case does not test what it says", cut, c.cutEndIn)
			}
		})
	}
}

func TestJobTagFormRefuses(t *testing.T) {
	h := "1bda24d00504"
	for name, tag := range map[string]string{
		"uppercase":                "hb-Helios-" + h,
		"no hash":                  "hb-helios-a",
		"an 11-hex hash":           "hb-helios-a-" + h[:11],
		"a 13-hex hash":            "hb-helios-a-" + h + "0",
		"an uppercase hash":        "hb-helios-a-1BDA24D00504",
		"a non-hex hash":           "hb-helios-a-1bda24d0050g",
		"a 57-character tag":       "hb-" + strings.Repeat("a", 41) + "-" + h,
		"a name starting with -":   "hb--helios-" + h,
		"a name starting with .":   "hb-.helios-" + h,
		"no hb prefix":             "xx-helios-" + h,
		"an empty name":            "hb--" + h,
		"a character outside a-z.": "hb-hel_ios-" + h,
		"a slash":                  "hb-hel/ios-" + h,
		"a trailing newline":       "hb-helios-" + h + "\n",
	} {
		cfg := validConfig()
		cfg.Tag = tag
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: tag %q was accepted", name, tag)
		}
	}
}

// The host layout's paths and port, written out by hand from
// docs/development.md (Runtime conventions, The host layout).
func TestHostLayout(t *testing.T) {
	h := Host{StateDir: "/home/example-user/.local/state/hpcburst", PortBase: 29220}
	for got, want := range map[string]string{
		h.CredentialsDir():    "/home/example-user/.local/state/hpcburst/credentials",
		h.AgentBuildsDir():    "/home/example-user/.local/state/hpcburst/agent-builds",
		h.AgentCertFile():     "/home/example-user/.local/state/hpcburst/agent-endpoint/tls.crt",
		h.AgentKeyFile():      "/home/example-user/.local/state/hpcburst/agent-endpoint/tls.key",
		h.AgentEndpointAddr(): "127.0.0.1:29223",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if p := h.AgentEndpointPort(); p != 29223 {
		t.Errorf("AgentEndpointPort = %d, want 29223 (base + 3)", p)
	}
	if got := AllocationCredentialPath(h.CredentialsDir(), "uid-1"); got != "/home/example-user/.local/state/hpcburst/credentials/allocations/uid-1" {
		t.Errorf("AllocationCredentialPath = %q", got)
	}
}

// The schema's bound on targetSessions is the protocol's (v1alpha1 states it
// in a kubebuilder marker, which cannot name a constant).
func TestMaxTargetSessionsAgreesWithTheAPI(t *testing.T) {
	if MaxTargetSessions != v1alpha1.MaxTargetSessions {
		t.Errorf("agentapi.MaxTargetSessions = %d, v1alpha1.MaxTargetSessions = %d", MaxTargetSessions, v1alpha1.MaxTargetSessions)
	}
}
