package launcher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testSpec(t *testing.T, nice int) launchSpec {
	t.Helper()
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	return launchSpec{l: l, bin: Binary{Path: "/bin/k3s-test"}, ports: Ports{API: 7001, HTTPS: 7002, KCM: 7003, LB: 7006}, nice: nice}
}

func TestK3sArgs(t *testing.T) {
	s := testSpec(t, 0)
	args := strings.Join(s.k3sArgs(), " ")
	// Every flag here is one spike S3 ran with (spikes/s3 README, k3s section), and HBEP-11
	// keeps its disabled components disabled. A missing flag means a component that nothing
	// in the design needs would run and cost memory.
	for _, want := range []string{
		"server", "--rootless", "--disable-agent",
		"--data-dir " + s.l.dataDir(),
		"--write-kubeconfig-mode 0600",
		"--private-registry " + s.l.emptyRegistry(),
		"--bind-address 127.0.0.1",
		"--https-listen-port 7002", "--apiserver-port 7001", "--lb-server-port 7006",
		"--kube-controller-manager-arg=secure-port=7003",
		"--disable-scheduler", "--disable-cloud-controller", "--disable-kube-proxy",
		"--disable-network-policy", "--disable-helm-controller",
		"--disable=coredns", "--disable=servicelb", "--disable=traefik",
		"--disable=local-storage", "--disable=metrics-server", "--disable=runtimes",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("k3s arguments lack %q:\n%s", want, args)
		}
	}
	for _, forbidden := range []string{"--cluster-init", "--token", "/etc/", "6443", "6444", "10250"} {
		if strings.Contains(args, forbidden) {
			t.Errorf("k3s arguments contain %q", forbidden)
		}
	}
}

func TestK3sEnvInheritsNothing(t *testing.T) {
	// Poison the launcher's own environment: none of it may reach k3s.
	t.Setenv("K3S_TOKEN", "leak")
	t.Setenv("K3S_URL", "https://leak.example.test")
	t.Setenv("KUBECONFIG", "/etc/rancher/k3s/k3s.yaml")
	t.Setenv("HTTPS_PROXY", "http://leak.example.test")
	s := testSpec(t, 0)
	got := map[string]string{}
	for _, kv := range s.env() {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("malformed entry %q", kv)
		}
		got[k] = v
	}
	if len(got) != len(envKeys) {
		t.Errorf("environment has %d variables, want %d: %v", len(got), len(envKeys), got)
	}
	for _, k := range envKeys {
		if _, ok := got[k]; !ok {
			t.Errorf("%s is not set", k)
		}
	}
	inside := func(p string) bool { return strings.HasPrefix(p, s.l.dir+"/") }
	for _, k := range []string{"HOME", "XDG_RUNTIME_DIR", "KUBECONFIG", "K3S_CONFIG_FILE"} {
		if !inside(got[k]) {
			t.Errorf("%s=%s is outside the state directory %s", k, got[k], s.l.dir)
		}
	}
	if got["K3S_CONFIG_FILE"] != s.l.emptyConfig() {
		t.Errorf("K3S_CONFIG_FILE = %s", got["K3S_CONFIG_FILE"])
	}
}

func TestK3sArgvNice(t *testing.T) {
	if a := testSpec(t, 0).argv(); a[0] != "/bin/k3s-test" {
		t.Errorf("without nice the binary comes first: %v", a)
	}
	a := testSpec(t, 10).argv()
	if strings.Join(a[:4], " ") != "nice -n 10 /bin/k3s-test" {
		t.Errorf("with nice: %v", a[:4])
	}
}

func TestPrepareIsolationLeavesEmptyFiles(t *testing.T) {
	s := testSpec(t, 0)
	if err := os.MkdirAll(s.l.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// A config file somebody filled in must be emptied again at the next start.
	if err := os.WriteFile(s.l.emptyConfig(), []byte("token: leak\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepareIsolation(s.l); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{s.l.emptyConfig(), s.l.emptyRegistry()} {
		fi, err := os.Stat(f)
		if err != nil || fi.Size() != 0 {
			t.Errorf("%s: %v, %v", f, fi, err)
		}
	}
	for _, d := range []string{s.l.home(), s.l.runDir(), s.l.dataDir(), s.l.logDir()} {
		fi, err := os.Stat(d)
		if err != nil || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v, %v", d, fi, err)
		}
	}
}
