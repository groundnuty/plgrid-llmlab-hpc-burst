package main

import (
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The frpc digest agent.json names is the one hack/fetch-frp.sh pins and
// checks the extracted frpc against, read from the script, not restated.
func TestFrpcDigestIsTheFetchScriptsPin(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "hack", "fetch-frp.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^FRPC_SHA=([0-9a-f]{64})$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("hack/fetch-frp.sh has no FRPC_SHA line of 64 hex characters")
	}
	if !strings.Contains(string(b), "linux_amd64") {
		t.Fatal("hack/fetch-frp.sh no longer names linux_amd64; the x86_64 entry may be for another build")
	}
	if got := frpcDigests["x86_64"]; got != string(m[1]) {
		t.Fatalf("frpcDigests[x86_64] = %q, want hack/fetch-frp.sh's FRPC_SHA %s", got, m[1])
	}
	if len(frpcDigests) != 1 {
		t.Fatalf("frpcDigests has %d entries; only linux_amd64's frpc is pinned", len(frpcDigests))
	}
}

// The Provider-manager scheme main uses passes; one without apps/v1, or
// without the framework's kinds, is refused with every missing kind named.
func TestCheckScheme(t *testing.T) {
	s, err := providerScheme()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkScheme(s); err != nil {
		t.Fatalf("main's scheme: %v", err)
	}
	bare := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, coordinationv1.AddToScheme} {
		if err := add(bare); err != nil {
			t.Fatal(err)
		}
	}
	err = checkScheme(bare)
	for _, want := range []string{"apps/v1, Kind=ControllerRevision", "Kind=Allocation (", "Kind=Provider (", "Kind=PlatformProfile ("} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("a scheme with neither apps/v1 nor %s: error %v, want one naming %q", v1alpha1.GroupVersion, err, want)
		}
	}
}

// The front settings follow the recorded front: none without one; in direct
// mode the endpoint, the certificate's pin and the frpc digests; in ssh mode
// also this user and the Host's two forwarded ports (base+3 and base+7).
func TestFrontSettings(t *testing.T) {
	state := t.TempDir()
	h := agentapi.Host{StateDir: state, PortBase: 29300}
	cert := writeEndpointCert(t, h)
	base := []string{"--state-dir", state, "--port-base", "29300", "--health-addr", "127.0.0.1:29306"}

	f, err := parse(base, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	if fs, err := frontSettings(f); err != nil || fs != nil {
		t.Fatalf("no front: %+v, %v; want nil, nil (nothing is submitted)", fs, err)
	}

	f, err = parse(append(append([]string{}, base...), "--front-mode", "direct", "--front-addr", "front.example.org:29443"), os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	fs, err := frontSettings(f)
	if err != nil {
		t.Fatal(err)
	}
	if fs.Mode != agentapi.DialHomeDirect || fs.Endpoint != "front.example.org:29443" || fs.CertPin != agentapi.PinOf(cert) || fs.SSHUser != "" {
		t.Fatalf("direct: %+v", fs)
	}
	fs.FrpcDigests["x86_64"] = "changed"
	if frpcDigests["x86_64"] == "changed" {
		t.Fatal("the settings share frpcDigests' map; a change to one changes the pin")
	}

	f, err = parse(append(append([]string{}, base...), "--front-mode", "ssh", "--front-addr", "cp.example.org:22"), os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	fs, err = frontSettings(f)
	if err != nil {
		t.Fatal(err)
	}
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if fs.Mode != agentapi.DialHomeSSH || fs.SSHUser != u.Username || fs.AgentPort != 29303 || fs.DialHomePort != 29307 {
		t.Fatalf("ssh: %+v; want user %s and ports 29303, 29307", fs, u.Username)
	}

	if err := os.Remove(h.AgentCertFile()); err != nil {
		t.Fatal(err)
	}
	if _, err := frontSettings(f); err == nil || !strings.Contains(err.Error(), "agent endpoint certificate") {
		t.Fatalf("no certificate: %v, want an error naming the agent endpoint certificate", err)
	}
}

// The ssh control directory stays short enough for sshrun's 90-byte socket
// path (a 30-byte name in it) wherever the state directory is.
func TestSSHControlDirIsShort(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1001")
	if got := sshControlDir(agentapi.Host{StateDir: "/" + strings.Repeat("s", 200), PortBase: 29300}); !strings.HasPrefix(got, "/run/user/1001/hpcburst-ssh-") || len(got)+31 > 90 {
		t.Fatalf("control directory %q: want one under XDG_RUNTIME_DIR that leaves room for a 30-byte socket name", got)
	}
	t.Setenv("XDG_RUNTIME_DIR", "relative")
	if got := sshControlDir(agentapi.Host{PortBase: 29300}); !filepath.IsAbs(got) {
		t.Fatalf("control directory %q with a relative XDG_RUNTIME_DIR: want an absolute one", got)
	}
}
