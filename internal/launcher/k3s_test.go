package launcher

import (
	"os"
	"path/filepath"
	"slices"
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
		// HBEP-11: the audit log at the Metadata level, inside the state directory.
		"--kube-apiserver-arg=audit-policy-file=" + s.l.dir + "/audit-policy.yaml",
		"--kube-apiserver-arg=audit-log-path=" + s.l.dir + "/logs/audit.log",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("k3s arguments lack %q:\n%s", want, args)
		}
	}
	for _, forbidden := range []string{"--cluster-init", "--token", "/etc/"} {
		if strings.Contains(args, forbidden) {
			t.Errorf("k3s arguments contain %q", forbidden)
		}
	}
	// The root k3s' ports are never used (checked as ports, not as substrings of the
	// state directory's random name: issue #382).
	if used := rootK3sPortsIn(s.k3sArgs(), s.l.dir); len(used) != 0 {
		t.Errorf("k3s arguments use the root k3s' ports %v:\n%s", used, args)
	}
}

// rootK3sPorts are the system k3s's ports (6443, 6444, 10250), which the launcher must never use.
var rootK3sPorts = []string{"6443", "6444", "10250"}

// rootK3sPortsIn returns which of the root k3s' ports the arguments use. The state directory's
// path is taken out of them first: its name is random digits (os.MkdirTemp), which can spell a
// port by chance (#382, found on a CI run whose directory held 6443). What is left is checked
// for each port as a whole number: bounded by anything but a digit, so "--https-listen-port
// 6443", "secure-port=6443" and "127.0.0.1:6443" are used, and 16443 and 64430 are other numbers.
func rootK3sPortsIn(args []string, stateDir string) []string {
	var used []string
	for _, port := range rootK3sPorts {
		for _, a := range args {
			if stateDir != "" {
				a = strings.ReplaceAll(a, stateDir, "<state>")
			}
			if hasNumber(a, port) {
				used = append(used, port)
				break
			}
		}
	}
	return used
}

// hasNumber reports whether s holds the digits n as a whole number: not preceded or followed by
// another digit.
func hasNumber(s, n string) bool {
	for from := 0; ; {
		i := strings.Index(s[from:], n)
		if i < 0 {
			return false
		}
		start, end := from+i, from+i+len(n)
		if (start == 0 || !isDigit(s[start-1])) && (end == len(s) || !isDigit(s[end])) {
			return true
		}
		from = start + 1
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// The check is of ports, not of substrings. A state directory whose random name holds a root
// k3s port passes (the real arguments, built for such a directory); a real use of one fails.
func TestTheRootK3sPortCheckLooksAtPortsNotSubstrings(t *testing.T) {
	for _, name := range []string{"6443", "6444", "10250", "x6443y", "a-6443-6444-10250", "164430"} {
		t.Run("a state directory named "+name, func(t *testing.T) {
			l, err := newLayout(filepath.Join(shortDir(t), name))
			if err != nil {
				t.Fatal(err)
			}
			s := launchSpec{l: l, bin: Binary{Path: "/bin/k3s-test"}, ports: Ports{API: 7001, HTTPS: 7002, KCM: 7003, LB: 7006}}
			if used := rootK3sPortsIn(s.k3sArgs(), s.l.dir); len(used) != 0 {
				t.Errorf("the directory's name was taken for ports %v:\n%s", used, strings.Join(s.k3sArgs(), " "))
			}
		})
	}
	// A real use, through the real arguments: each of the three numbers as a port the launcher
	// was given.
	for _, c := range []struct {
		name  string
		ports Ports
		want  string
	}{
		{"the apiserver port", Ports{API: 6443, HTTPS: 7002, KCM: 7003, LB: 7006}, "6443"},
		{"the https port", Ports{API: 7001, HTTPS: 10250, KCM: 7003, LB: 7006}, "10250"},
		{"the controller manager's port", Ports{API: 7001, HTTPS: 7002, KCM: 6444, LB: 7006}, "6444"},
		{"the load balancer's port", Ports{API: 7001, HTTPS: 7002, KCM: 7003, LB: 6443}, "6443"},
	} {
		t.Run("a real use: "+c.name, func(t *testing.T) {
			s := testSpec(t, 0)
			s.ports = c.ports
			if used := rootK3sPortsIn(s.k3sArgs(), s.l.dir); len(used) != 1 || used[0] != c.want {
				t.Errorf("the root k3s port %s as %s was reported as %v", c.want, c.name, used)
			}
		})
	}
	// The forms a port takes in an argument, and numbers that only contain one.
	for _, c := range []struct {
		args []string
		want []string
	}{
		{[]string{"--https-listen-port", "6443"}, []string{"6443"}},
		{[]string{"--https-listen-port=6443"}, []string{"6443"}},
		{[]string{"--bind", "127.0.0.1:6444"}, []string{"6444"}},
		{[]string{"--bind", ":10250"}, []string{"10250"}},
		{[]string{"--kube-controller-manager-arg=secure-port=6443"}, []string{"6443"}},
		{[]string{"6443"}, []string{"6443"}},
		{[]string{"--x", "16443:6443"}, []string{"6443"}}, // a longer number first does not hide the port after it
		{[]string{"--apiserver-port", "16443"}, nil},
		{[]string{"--apiserver-port", "64430"}, nil},
		{[]string{"--apiserver-port", "106443"}, nil},
		{[]string{"--apiserver-port", "7001"}, nil},
	} {
		if got := rootK3sPortsIn(c.args, ""); !slices.Equal(got, c.want) {
			t.Errorf("%q: reported %v, want %v", c.args, got, c.want)
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
	for _, k := range []string{"HOME", "XDG_RUNTIME_DIR", "KUBECONFIG", "K3S_CONFIG_FILE", "TMPDIR"} {
		if !inside(got[k]) {
			t.Errorf("%s=%s is outside the state directory %s", k, got[k], s.l.dir)
		}
	}
	if got["K3S_CONFIG_FILE"] != s.l.emptyConfig() {
		t.Errorf("K3S_CONFIG_FILE = %s", got["K3S_CONFIG_FILE"])
	}
}

func TestK3sArgvNice(t *testing.T) {
	// The child runs the descriptor of the verified binary (ExtraFiles[0] is fd 3), never a path,
	// and nice(1) by absolute path, so neither the working directory nor PATH can choose either.
	if a := testSpec(t, 0).argv(); a[0] != "/proc/self/fd/3" {
		t.Errorf("without nice the binary's descriptor comes first: %v", a)
	}
	a := testSpec(t, 10).argv()
	if strings.Join(a[:4], " ") != "/usr/bin/nice -n 10 /proc/self/fd/3" {
		t.Errorf("with nice: %v", a[:4])
	}
}

func TestLaunchNeedsAnOpenedBinary(t *testing.T) {
	if _, err := launch(testSpec(t, 0)); err == nil || !strings.Contains(err.Error(), "not opened") {
		t.Fatalf("launch with an unopened binary: %v", err)
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
	for _, d := range []string{s.l.home(), s.l.runDir(), s.l.dataDir(), s.l.logDir(), s.l.dir + "/tmp"} {
		fi, err := os.Stat(d)
		if err != nil || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v, %v", d, fi, err)
		}
	}
	// The audit policy: one rule, level Metadata, for everything (Kubernetes' audit policy format,
	// audit.k8s.io/v1). Any other level would log request or response bodies.
	b, err := os.ReadFile(s.l.dir + "/audit-policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	pol := string(b)
	if !strings.Contains(pol, "apiVersion: audit.k8s.io/v1") || !strings.Contains(pol, "kind: Policy") ||
		strings.Count(pol, "level:") != 1 || !strings.Contains(pol, "  - level: Metadata\n") {
		t.Errorf("audit policy:\n%s", pol)
	}
	if fi, _ := os.Stat(s.l.dir + "/audit-policy.yaml"); fi.Mode().Perm() != 0o600 {
		t.Errorf("audit policy mode %v", fi.Mode().Perm())
	}
}
