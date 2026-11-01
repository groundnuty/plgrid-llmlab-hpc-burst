package main

import (
	"bytes"
	"encoding/pem"
	"maps"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// pinnedFrpc reads frp-pins.txt as text, not through internal/launcher, and
// returns its frpc_sha256 column per release architecture.
func pinnedFrpc(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "internal", "launcher", "frp-pins.txt"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, m := range regexp.MustCompile(`(?m)^0\.71\.1\s+(\w+)\s+\S+\s+[0-9a-f]{64}\s+[0-9a-f]{64}\s+([0-9a-f]{64})\s*$`).FindAllSubmatch(b, -1) {
		out[string(m[1])] = string(m[2])
	}
	if len(out) != 2 {
		t.Fatalf("frp-pins.txt has frpc sums for %v; want amd64 and arm64 lines of six fields", out)
	}
	return out
}

// The frpc digests agent.json names are the pinned ones, per cpuArch: for
// x86_64 the one hack/fetch-frp.sh pins and checks the extracted frpc
// against, and for aarch64 (Helios GH200) frp-pins.txt's arm64 frpc, both read
// from the files as text, not restated (review of PR 132: B1).
func TestFrpcDigestsArePinned(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "hack", "fetch-frp.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^FRPC_SHA=([0-9a-f]{64})$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("hack/fetch-frp.sh has no FRPC_SHA line of 64 hex characters")
	}
	if !strings.Contains(string(b), "linux_amd64") {
		t.Fatal("hack/fetch-frp.sh no longer names linux_amd64; its FRPC_SHA may be for another build")
	}
	got, err := deps{}.digests()
	if err != nil {
		t.Fatal(err)
	}
	pins := pinnedFrpc(t)
	want := map[string]string{"x86_64": string(m[1]), "aarch64": pins["arm64"]}
	if !maps.Equal(got, want) {
		t.Fatalf("frpc digests %v, want %v (fetch-frp.sh's FRPC_SHA for x86_64, frp-pins.txt's arm64 frpc for aarch64)", got, want)
	}
	if pins["amd64"] != want["x86_64"] {
		t.Errorf("frp-pins.txt's amd64 frpc %s is not fetch-frp.sh's FRPC_SHA %s", pins["amd64"], want["x86_64"])
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
	for _, want := range []string{"apps/v1, Kind=ControllerRevision (", "Kind=Allocation (", "Kind=Provider (", "Kind=PlatformProfile ("} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("a scheme with neither apps/v1 nor %s: error %v, want one naming %q", v1alpha1.GroupVersion, err, want)
		}
	}

	// Every kind but none of the list kinds, which the typed informers need
	// (review of PR 132: N1).
	noLists := runtime.NewScheme()
	noLists.AddKnownTypes(appsv1.SchemeGroupVersion, &appsv1.ControllerRevision{})
	noLists.AddKnownTypes(v1alpha1.GroupVersion, &v1alpha1.Allocation{}, &v1alpha1.Provider{}, &v1alpha1.PlatformProfile{})
	err = checkScheme(noLists)
	for _, want := range []string{"Kind=ControllerRevisionList", "Kind=AllocationList", "Kind=ProviderList", "Kind=PlatformProfileList"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("a scheme without the list kinds: error %v, want one naming %q", err, want)
		}
	}
	for _, kind := range []string{"Kind=ControllerRevision (", "Kind=Allocation (", "Kind=Provider (", "Kind=PlatformProfile ("} {
		if err != nil && strings.Contains(err.Error(), kind) {
			t.Errorf("a scheme that has %s: error %v names it", kind, err)
		}
	}
}

// The front settings follow the recorded front: none without one; in direct
// mode the endpoint, the certificate's pin and the frpc digests; in ssh mode
// also the user, which may have capitals, and the Host's two forwarded ports
// (base+3 and base+7).
func TestFrontSettings(t *testing.T) {
	state := t.TempDir()
	h := agentapi.Host{StateDir: state, PortBase: 29300}
	cert := writeEndpointCert(t, h)
	base := []string{"--state-dir", state, "--port-base", "29300", "--health-addr", "127.0.0.1:29306"}

	shared := map[string]string{"x86_64": strings.Repeat("a", 64), "aarch64": strings.Repeat("b", 64)}
	d := deps{
		frpcDigests: func() (map[string]string, error) { return shared, nil },
		username:    func() (string, error) { return "ExampleUser", nil },
	}
	f, err := parse(base, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	if fs, err := frontSettings(f, d); err != nil || fs != nil {
		t.Fatalf("no front: %+v, %v; want nil, nil (nothing is submitted)", fs, err)
	}

	f, err = parse(append(append([]string{}, base...), "--front-mode", "direct", "--front-addr", "front.example.org:29443"), os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	fs, err := frontSettings(f, d)
	if err != nil {
		t.Fatal(err)
	}
	if fs.Mode != agentapi.DialHomeDirect || fs.Endpoint != "front.example.org:29443" || fs.CertPin != agentapi.PinOf(cert) || fs.SSHUser != "" ||
		!maps.Equal(fs.FrpcDigests, shared) {
		t.Fatalf("direct: %+v", fs)
	}
	// The settings keep the certificate's PEM, which Create stages as each
	// Allocation's dial-home.crt: the PEM of the launcher's certificate, as the
	// test encodes it from the certificate it wrote, and not another PEM of the
	// same key.
	if want := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); !bytes.Equal(fs.CertPEM, want) {
		t.Errorf("CertPEM = %q, want the PEM of the agent endpoint's certificate", fs.CertPEM)
	}
	fs.FrpcDigests["x86_64"] = "changed"
	if shared["x86_64"] == "changed" {
		t.Fatal("the settings share the digests' map; a change to one changes the pin")
	}

	f, err = parse(append(append([]string{}, base...), "--front-mode", "ssh", "--front-addr", "cp.example.org:22"), os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	fs, err = frontSettings(f, d)
	if err != nil {
		t.Fatal(err)
	}
	if fs.Mode != agentapi.DialHomeSSH || fs.SSHUser != "ExampleUser" || fs.AgentPort != 29303 || fs.DialHomePort != 29307 {
		t.Fatalf("ssh: %+v; want user ExampleUser and ports 29303, 29307", fs)
	}
	// Without a seam, the user is the one who runs the test.
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := (deps{}).user(); err != nil || got != u.Username {
		t.Fatalf("the default user: %q, %v; want %q", got, err, u.Username)
	}

	if err := os.Remove(h.AgentCertFile()); err != nil {
		t.Fatal(err)
	}
	if _, err := frontSettings(f, d); err == nil || !strings.Contains(err.Error(), "agent endpoint certificate") {
		t.Fatalf("no certificate: %v, want an error naming the agent endpoint certificate", err)
	}
}

// Front settings that agent.json would refuse are refused by frontSettings,
// with the field named, before anything starts (review of PR 132: B3): a
// front address without a usable port, a digest that is not 64 lowercase hex
// characters, a user agentapi refuses, and no digest at all.
func TestFrontSettingsRefusals(t *testing.T) {
	state := t.TempDir()
	h := agentapi.Host{StateDir: state, PortBase: 29300}
	writeEndpointCert(t, h)
	good := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name, mode, addr, user string
		digests                map[string]string
		want                   string
	}{
		{"a direct address with port 0", "direct", "front.example.org:0", "", map[string]string{"x86_64": good}, `endpoint "front.example.org:0"`},
		{"an ssh address with no port", "ssh", "cp.example.org", "example", map[string]string{"x86_64": good}, `endpoint "cp.example.org"`},
		{"an upper-case digest", "direct", "front.example.org:29443", "", map[string]string{"x86_64": good, "aarch64": strings.Repeat("A", 64)}, "cpuArch aarch64: frpcDigest"},
		{"a short digest", "direct", "front.example.org:29443", "", map[string]string{"x86_64": "abc"}, "cpuArch x86_64: frpcDigest"},
		{"a user with a leading hyphen", "ssh", "cp.example.org:22", "-oProxyCommand", map[string]string{"x86_64": good}, `ssh.user "-oProxyCommand"`},
		{"a user with an @", "ssh", "cp.example.org:22", "a@b", map[string]string{"x86_64": good}, `ssh.user "a@b"`},
		{"no digest", "direct", "front.example.org:29443", "", map[string]string{}, "no frpc digest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := parse([]string{"--state-dir", state, "--port-base", "29300", "--health-addr", "127.0.0.1:29306",
				"--front-mode", tc.mode, "--front-addr", tc.addr}, os.Stderr)
			if err != nil {
				t.Fatal(err)
			}
			d := deps{
				frpcDigests: func() (map[string]string, error) { return tc.digests, nil },
				username:    func() (string, error) { return tc.user, nil },
			}
			fs, err := frontSettings(f, d)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("frontSettings = %+v, %v; want a refusal naming %q", fs, err, tc.want)
			}
		})
	}
}

// The certificate file the settings keep is the one Create stages, and the agent
// refuses any but exactly one certificate (HBEP-6, Certificate checks), so a
// file of two stops the binary here, before anything listens, not every job.
func TestFrontSettingsRefusesAFileOfTwoCertificates(t *testing.T) {
	state := t.TempDir()
	h := agentapi.Host{StateDir: state, PortBase: 29300}
	cert := writeEndpointCert(t, h)
	one := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if err := os.WriteFile(h.AgentCertFile(), append(append([]byte{}, one...), one...), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := parse([]string{"--state-dir", state, "--port-base", "29300", "--health-addr", "127.0.0.1:29306",
		"--front-mode", "direct", "--front-addr", "front.example.org:29443"}, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	d := deps{frpcDigests: func() (map[string]string, error) { return map[string]string{"x86_64": strings.Repeat("a", 64)}, nil }}
	if fs, err := frontSettings(f, d); err == nil || !strings.Contains(err.Error(), "the dial-home certificate: it holds 2 certificates") {
		t.Fatalf("frontSettings = %+v, %v; want the agent's refusal of two certificates", fs, err)
	}
}

// The Provider managers' grace leaves the lifecycle manager's 30 s, which is
// also hpcburst down's wait before SIGKILL, a margin for the rest of the stop
// (review of PR 132: N3; run_test.go stops during a submission).
func TestProviderGraceFitsTheStop(t *testing.T) {
	const margin = 10 * time.Second
	if providerGrace+margin > launcher.DefaultGrace {
		t.Fatalf("providerGrace %s leaves less than %s of hpcburst down's %s", providerGrace, margin, launcher.DefaultGrace)
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
