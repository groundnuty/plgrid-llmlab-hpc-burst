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
)

// host is one control-plane host option.
type host interface {
	name() string
	versions() (oldV, newV string)
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
	hostPorts  []int
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
	r := &running{t0: time.Now(), admin: admin, ctxName: "c", hostPorts: []int{portEtcdClient, portEtcdPeer, portAPIServer}}
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
		r.hostPorts = append(r.hostPorts, portKCM)
		r.afterReady = func(ctx context.Context) error {
			kcm, err := startProc("kube-controller-manager", filepath.Join(logs, tag+"-kcm.log"), nil,
				filepath.Join(h.scratch, "bin", "kube-controller-manager-"+ver),
				"--kubeconfig", admin, "--controllers", "garbage-collector-controller,namespace-controller",
				"--leader-elect=false", "--secure-port", fmt.Sprint(portKCM), "--bind-address", "127.0.0.1")
			if err != nil {
				return err
			}
			r.procs = append(r.procs, kcm)
			return waitHealthz(ctx, portKCM, 60*time.Second)
		}
	}
	return r, nil
}

func waitHealthz(ctx context.Context, port int, timeout time.Duration) error {
	hc := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	end := time.Now().Add(timeout)
	for time.Now().Before(end) && ctx.Err() == nil {
		resp, err := hc.Get(fmt.Sprintf("https://127.0.0.1:%d/healthz", port))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
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

func (h kcpHost) start(ver, dir, tag string) (*running, error) {
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	admin := filepath.Join(root, "admin.kubeconfig")
	// A leftover admin.kubeconfig from a previous run must not make a not-yet-started server look ready.
	_ = os.Remove(admin)
	r := &running{t0: time.Now(), admin: admin, ctxName: "root", hostPorts: []int{portKCP, portKCPEtcd, portKCPPeer, portKCPMetrics}}
	p, err := startProc("kcp", filepath.Join(h.scratch, "logs", tag+"-kcp.log"),
		[]string{"HOME=" + filepath.Join(h.scratch, "work/home")},
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
