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
