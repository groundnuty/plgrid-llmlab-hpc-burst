package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

// Ports are loopback only. 29101-29130 is this spike's block.
const (
	portEtcdClient = 29101
	portEtcdPeer   = 29102
	portAPIServer  = 29103
	portKCM        = 29104
	portKCP        = 29111
	portKCPEtcd    = 29112
	portKCPPeer    = 29113
	portKCPMetrics = 29114
	portK3sAPI     = 29120 // k3s: apiserver (internal listener)
	portK3sHTTPS   = 29121 // k3s: supervisor and external-facing apiserver
	portK3sKCM     = 29122 // k3s: embedded kube-controller-manager
	portK3sLB      = 29125 // k3s: agent-side load balancer; unused without an agent, kept free so it cannot hit the default
)

// hostPorts lists the ports a host binds. Listen ports must be free before every launch and
// must be held by the launched processes afterwards. Reserved ports must only be free.
type hostPorts struct{ Listen, Reserved []int }

// host is one control-plane host option.
type host interface {
	name() string
	versions() (oldV, newV string)
	ports() hostPorts
	// binaries maps a label to the path of every executable the host runs at version ver, so
	// a phase can record the identity (sha256) of what it measured.
	binaries(ver string) map[string]string
	// stateDir is the directory that holds the host's database inside a data directory.
	stateDir(dataDir string) string
	// negativeControl is true for a host that is expected to fail the cascade tests.
	negativeControl() bool
	start(ver, dataDir, tag string) (*running, error)
}

// running is a started host: its processes plus the knowledge of how to reach it.
type running struct {
	t0         time.Time
	procs      []*proc
	admin      string // admin kubeconfig (may appear only after start)
	ctxName    string
	afterReady func(ctx context.Context) error                            // e.g. start the controller manager
	prepare    func(ctx context.Context) (workKubeconfig string, e error) // e.g. kcp child workspace
	hostPorts  []int                                                      // set by startReady from host.ports()
	logs       []string                                                   // the host processes' log files
}

func (r *running) pids() []int {
	var p []int
	for _, x := range r.procs {
		p = append(p, x.pid())
	}
	return p
}

func (r *running) stopAll(grace time.Duration) (time.Duration, error) {
	start := time.Now()
	var first error
	for i := len(r.procs) - 1; i >= 0; i-- {
		if _, err := r.procs[i].stop(grace); err != nil && first == nil {
			first = err
		}
	}
	return time.Since(start), first
}

func waitTCP(port int, timeout time.Duration) error {
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("port %d not listening after %s", port, timeout)
}

func writeKubeconfig(path, server, token string) error {
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["c"] = &clientcmdapi.Cluster{Server: server, InsecureSkipTLSVerify: true}
	cfg.AuthInfos["admin"] = &clientcmdapi.AuthInfo{Token: token}
	cfg.Contexts["c"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "admin"}
	cfg.CurrentContext = "c"
	return clientcmd.WriteToFile(*cfg, path)
}

// ---- envtest binaries: kube-apiserver + etcd (+ kube-controller-manager for GC) ----

type envtestHost struct {
	scratch string
	gc      bool
}

func (h envtestHost) name() string {
	if h.gc {
		return "envtest-gc"
	}
	return "envtest-bare"
}
func (h envtestHost) versions() (string, string) { return "1.35.0", "1.36.2" }
func (h envtestHost) negativeControl() bool      { return !h.gc }
func (h envtestHost) stateDir(dir string) string { return filepath.Join(dir, "etcd") }

func (h envtestHost) ports() hostPorts {
	p := hostPorts{Listen: []int{portEtcdClient, portEtcdPeer, portAPIServer}}
	if h.gc {
		p.Listen = append(p.Listen, portKCM)
	}
	return p
}

func (h envtestHost) binaries(ver string) map[string]string {
	dir := filepath.Join(h.scratch, "bin/envtest/k8s", ver+"-linux-amd64")
	m := map[string]string{"etcd": filepath.Join(dir, "etcd"), "kube-apiserver": filepath.Join(dir, "kube-apiserver")}
	if h.gc {
		m["kube-controller-manager"] = filepath.Join(h.scratch, "bin", "kube-controller-manager-"+ver)
	}
	return m
}

func (h envtestHost) start(ver, dir, tag string) (*running, error) {
	bin := filepath.Join(h.scratch, "bin/envtest/k8s", ver+"-linux-amd64")
	logs := filepath.Join(h.scratch, "logs")
	if err := os.MkdirAll(filepath.Join(dir, "etcd"), 0o700); err != nil {
		return nil, err
	}
	token, saKey, err := ensureEnvtestSecrets(dir)
	if err != nil {
		return nil, err
	}
	admin := filepath.Join(dir, "admin.kubeconfig")
	if err := writeKubeconfig(admin, fmt.Sprintf("https://127.0.0.1:%d", portAPIServer), token); err != nil {
		return nil, err
	}
	r := &running{t0: time.Now(), admin: admin, ctxName: "c"}
	cl := fmt.Sprintf("http://127.0.0.1:%d", portEtcdClient)
	peer := fmt.Sprintf("http://127.0.0.1:%d", portEtcdPeer)
	etcd, err := startProc("etcd", filepath.Join(logs, tag+"-etcd.log"), nil, filepath.Join(bin, "etcd"),
		"--name", "default", "--data-dir", filepath.Join(dir, "etcd"),
		"--listen-client-urls", cl, "--advertise-client-urls", cl,
		"--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer, "--initial-cluster", "default="+peer)
	if err != nil {
		return nil, err
	}
	r.procs = append(r.procs, etcd)
	if err := waitTCP(portEtcdClient, 30*time.Second); err != nil {
		return r, err
	}
	api, err := startProc("kube-apiserver", filepath.Join(logs, tag+"-apiserver.log"), nil, filepath.Join(bin, "kube-apiserver"),
		"--etcd-servers", cl, "--secure-port", fmt.Sprint(portAPIServer), "--bind-address", "127.0.0.1",
		"--advertise-address", "127.0.0.1", "--cert-dir", filepath.Join(dir, "certs"),
		"--service-account-issuer", fmt.Sprintf("https://127.0.0.1:%d", portAPIServer),
		"--service-account-key-file", saKey, "--service-account-signing-key-file", saKey,
		"--service-cluster-ip-range", "10.0.0.0/24", "--authorization-mode", "RBAC",
		"--token-auth-file", filepath.Join(dir, "tokens.csv"), "--disable-admission-plugins", "ServiceAccount",
		"--allow-privileged=true", "--storage-media-type", "application/json")
	if err != nil {
		return r, err
	}
	r.procs = append(r.procs, api)
	if h.gc {
		r.afterReady = func(ctx context.Context) error {
			kcm, err := startProc("kube-controller-manager", filepath.Join(logs, tag+"-kcm.log"), nil,
				filepath.Join(h.scratch, "bin", "kube-controller-manager-"+ver),
				"--kubeconfig", admin, "--controllers", "garbage-collector-controller,namespace-controller",
				"--leader-elect=false", "--secure-port", fmt.Sprint(portKCM), "--bind-address", "127.0.0.1")
			if err != nil {
				return err
			}
			r.procs = append(r.procs, kcm)
			return waitHealthz(ctx, kcm, portKCM, 60*time.Second)
		}
	}
	return r, nil
}

// waitHealthz polls the controller manager's /healthz on its secure port until it answers 200.
// The process p that was launched for this run must be alive at every probe, and the port must
// be held by p: a 200 from a leftover or foreign server proves nothing about p.
func waitHealthz(ctx context.Context, p *proc, port int, timeout time.Duration) error {
	hc := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	end := time.Now().Add(timeout)
	for time.Now().Before(end) && ctx.Err() == nil {
		if !p.alive() {
			return fmt.Errorf("%s exited before /healthz on port %d answered", p.name, port)
		}
		resp, err := hc.Get(fmt.Sprintf("https://127.0.0.1:%d/healthz", port))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				if !p.alive() {
					return fmt.Errorf("%s exited while /healthz on port %d answered", p.name, port)
				}
				if err := checkOwned([]int{port}, listeners([]int{p.pid()}), allListeners()); err != nil {
					return fmt.Errorf("/healthz on port %d answered, but not from %s: %w", port, p.name, err)
				}
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("port %d /healthz not ok after %s", port, timeout)
}

// ensureEnvtestSecrets makes a static admin token and a service-account key once per data directory.
func ensureEnvtestSecrets(dir string) (token, saKey string, err error) {
	saKey = filepath.Join(dir, "sa.key")
	tokFile := filepath.Join(dir, "tokens.csv")
	if b, e := os.ReadFile(tokFile); e == nil {
		t, _, _ := strings.Cut(string(b), ",")
		if _, e := os.Stat(saKey); e == nil && t != "" {
			return t, saKey, nil
		}
	}
	raw := make([]byte, 16)
	if _, err = rand.Read(raw); err != nil {
		return
	}
	token = hex.EncodeToString(raw)
	if err = os.WriteFile(tokFile, []byte(fmt.Sprintf("%s,admin,uid-admin,\"system:masters\"\n", token)), 0o600); err != nil {
		return
	}
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	err = os.WriteFile(saKey, pemBytes, 0o600)
	return
}

// ---- kcp release binary ----

type kcpHost struct{ scratch string }

func (kcpHost) name() string               { return "kcp" }
func (kcpHost) versions() (string, string) { return "0.32.5", "0.33.3" }
func (kcpHost) negativeControl() bool      { return false }
func (kcpHost) stateDir(dir string) string { return filepath.Join(dir, "root", "etcd-server") }
func (kcpHost) ports() hostPorts {
	return hostPorts{Listen: []int{portKCP, portKCPEtcd, portKCPPeer, portKCPMetrics}}
}
func (h kcpHost) binaries(ver string) map[string]string {
	return map[string]string{"kcp": filepath.Join(h.scratch, "bin/kcp-"+ver+"/bin/kcp")}
}

func (h kcpHost) start(ver, dir, tag string) (*running, error) {
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	admin := filepath.Join(root, "admin.kubeconfig")
	// Do not delete a leftover admin.kubeconfig: kcp reads the shard-admin token back from it
	// on a restart and refuses to start without it. Nothing else listens on our port (checked
	// before every start), so a stale file cannot make a not-yet-started server look ready.
	r := &running{t0: time.Now(), admin: admin, ctxName: "root"}
	p, err := startProc("kcp", filepath.Join(h.scratch, "logs", tag+"-kcp.log"),
		[]string{"HOME=" + filepath.Join(h.scratch, "work/kcphome")},
		filepath.Join(h.scratch, "bin/kcp-"+ver+"/bin/kcp"), "start",
		"--root-directory", root, "--bind-address", "127.0.0.1", "--secure-port", fmt.Sprint(portKCP),
		"--embedded-etcd-directory", filepath.Join(root, "etcd-server"),
		"--embedded-etcd-client-port", fmt.Sprint(portKCPEtcd), "--embedded-etcd-peer-port", fmt.Sprint(portKCPPeer),
		"--embedded-etcd-listen-metrics-urls", fmt.Sprintf("http://127.0.0.1:%d", portKCPMetrics))
	if err != nil {
		return nil, err
	}
	r.procs = append(r.procs, p)
	r.prepare = func(ctx context.Context) (string, error) {
		return kit.PrepareKCPWorkspace(ctx, admin, filepath.Join(root, "work.kubeconfig"))
	}
	return r, nil
}

// ---- k3s release binary, agentless and rootless, SQLite ----

// k3sHost runs "k3s server --rootless --disable-agent" as the invoking user: one process that
// holds the apiserver, the embedded kube-controller-manager (garbage collection) and a SQLite
// store (kine). Nothing of the system's own k3s is read or written: the process runs with an
// empty environment, a scratch HOME and empty config and registry files, its data directory
// and kubeconfig are in scratch, and every port is a loopback port of this spike's block.
type k3sHost struct{ scratch string }

func (k3sHost) name() string               { return "k3s" }
func (k3sHost) versions() (string, string) { return "1.35.9", "1.36.5" }
func (k3sHost) negativeControl() bool      { return false }
func (k3sHost) stateDir(dir string) string { return filepath.Join(dir, "data", "server", "db") }
func (k3sHost) ports() hostPorts {
	return hostPorts{Listen: []int{portK3sAPI, portK3sHTTPS, portK3sKCM}, Reserved: []int{portK3sLB}}
}
func (h k3sHost) binaries(ver string) map[string]string {
	return map[string]string{"k3s": filepath.Join(h.scratch, "bin/k3s-"+ver+"/k3s")}
}

// k3sForbidden are the system paths of the root-owned k3s on the shared VM. A run fails if
// a k3s log of ours mentions one, or if one of them changes while we run.
var k3sForbidden = []string{"/etc/rancher", "/var/lib/rancher", "/run/k3s"}

func (h k3sHost) start(ver, dir, tag string) (*running, error) {
	home := filepath.Join(h.scratch, "k3s-home")
	run := filepath.Join(h.scratch, "k3s-run")
	for _, d := range []string{home, run} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}
	emptyCfg := filepath.Join(h.scratch, "k3s-empty-config.yaml")
	emptyReg := filepath.Join(h.scratch, "k3s-empty-registries.yaml")
	for _, f := range []string{emptyCfg, emptyReg} {
		if err := os.WriteFile(f, nil, 0o600); err != nil {
			return nil, err
		}
	}
	admin := filepath.Join(dir, "k3s.yaml")
	logPath := filepath.Join(h.scratch, "logs", tag+"-k3s.log")
	r := &running{t0: time.Now(), admin: admin, ctxName: "default", logs: []string{logPath}}
	kcm := portK3sKCM
	argv := []string{"env", "-i", "PATH=/usr/bin:/bin", "HOME=" + home, "XDG_RUNTIME_DIR=" + run,
		"KUBECONFIG=" + filepath.Join(home, "kubeconfig"), "K3S_CONFIG_FILE=" + emptyCfg,
		filepath.Join(h.scratch, "bin/k3s-"+ver+"/k3s"), "server", "--rootless", "--disable-agent",
		"--data-dir", filepath.Join(dir, "data"), "--write-kubeconfig", admin,
		"--private-registry", emptyReg, "--bind-address", "127.0.0.1",
		"--https-listen-port", fmt.Sprint(portK3sHTTPS), "--apiserver-port", fmt.Sprint(portK3sAPI),
		"--lb-server-port", fmt.Sprint(portK3sLB), "--kube-controller-manager-arg=secure-port=" + fmt.Sprint(kcm),
		"--disable-scheduler", "--disable-cloud-controller", "--disable-kube-proxy", "--disable-network-policy",
		"--disable-helm-controller", "--disable=coredns", "--disable=servicelb", "--disable=traefik",
		"--disable=local-storage", "--disable=metrics-server", "--disable=runtimes"}
	p, err := startProc("k3s", logPath, nil, argv...)
	if err != nil {
		return nil, err
	}
	r.procs = append(r.procs, p)
	// "Ready" for k3s means /readyz plus the embedded controller manager answering on its own
	// port, as for envtest-gc: garbage collection is part of what the host is for.
	r.afterReady = func(ctx context.Context) error { return waitHealthz(ctx, p, kcm, 60*time.Second) }
	return r, nil
}

// auditLogs fails if a k3s log mentions a system path of the root-owned k3s.
func auditLogs(logs []string, forbidden []string) error {
	for _, lp := range logs {
		bs, err := os.ReadFile(lp)
		if err != nil {
			return err
		}
		for _, l := range strings.Split(string(bs), "\n") {
			for _, f := range forbidden {
				if strings.Contains(l, f) {
					return fmt.Errorf("%s mentions %s: %.200s", filepath.Base(lp), f, l)
				}
			}
		}
	}
	return nil
}

// sysStamp is the modification time of each forbidden path ("absent" if it does not exist).
func sysStamp(paths []string) map[string]string {
	m := map[string]string{}
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil {
			m[p] = fi.ModTime().UTC().Format(time.RFC3339)
		} else {
			m[p] = "absent"
		}
	}
	return m
}
