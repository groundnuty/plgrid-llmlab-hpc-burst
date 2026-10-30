package testenv

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// DefaultPortBase is the first port of the block the control plane listens on.
const DefaultPortBase = 29200

const envPortBase = "HPCBURST_TESTENV_PORT_BASE"

// systemPorts are ports of a server another user of the machine may run (the
// Kubernetes API server and the kubelet, as k3s uses them). A test run that
// starts and stops a control plane must leave the ones that listened before it
// listening.
var systemPorts = []int{6443, 10250}

// GuardSystemPorts records which system ports listen now and returns a check
// that reports one that listened then and does not now. Call the check after
// every test run that started anything.
func GuardSystemPorts() func() error {
	before := map[int]bool{}
	for _, p := range systemPorts {
		before[p] = listening(p)
	}
	return func() error {
		var errs []error
		for _, p := range systemPorts {
			if before[p] && !listening(p) {
				errs = append(errs, fmt.Errorf("port %d listened before the run and does not any more", p))
			}
		}
		return errors.Join(errs...)
	}
}

// ControlPlane is a running etcd, kube-apiserver and kube-controller-manager.
type ControlPlane struct {
	// Config reaches the API server as cluster admin.
	Config *rest.Config
	// Kubeconfig is the path of an admin kubeconfig for the same server.
	Kubeconfig string
	// Dir holds the data, certificates and logs.
	Dir string

	procs []*proc
	guard func() error
}

// Options tune Start.
type Options struct {
	// BeforeControllerManager runs once the API server is ready and before the
	// controller manager starts. A test installs its CRDs here, so that the
	// garbage collector's first discovery already knows the custom resources.
	// (A collector started earlier learns of new resources only on its next
	// discovery pass, up to 30 seconds later.)
	BeforeControllerManager func(ctx context.Context, cfg *rest.Config) error

	// WithoutControllerManager starts a bare API server and etcd, as envtest
	// does. It is a negative control only: a test that needs garbage
	// collection must fail on it, which shows that the test checks the
	// collector and not just the API server.
	WithoutControllerManager bool
}

// Start launches the control plane in dir, waits until the API server is ready
// and the controller manager answers, and returns it. On error everything
// already started is stopped.
func Start(ctx context.Context, dir string, opts Options) (*ControlPlane, error) {
	bins, err := FindBinaries()
	if err != nil {
		return nil, err
	}
	base := DefaultPortBase
	if v := os.Getenv(envPortBase); v != "" {
		if base, err = strconv.Atoi(v); err != nil {
			return nil, fmt.Errorf("%s: %w", envPortBase, err)
		}
	}
	ports := struct{ etcdClient, etcdPeer, api, kcm int }{base, base + 1, base + 2, base + 3}
	// A listener on our ports would answer health checks for a server that is not ours.
	for _, p := range []int{ports.etcdClient, ports.etcdPeer, ports.api, ports.kcm} {
		if listening(p) {
			return nil, fmt.Errorf("port %d is already in use: another control plane is running on this port block", p)
		}
	}
	cp := &ControlPlane{Dir: dir, guard: GuardSystemPorts()}
	if err := cp.start(ctx, bins, opts, ports.etcdClient, ports.etcdPeer, ports.api, ports.kcm); err != nil {
		_ = cp.Stop()
		return nil, err
	}
	return cp, nil
}

func (cp *ControlPlane) start(ctx context.Context, bins Binaries, opts Options, etcdClient, etcdPeer, api, kcm int) error {
	logs := filepath.Join(cp.Dir, "logs")
	for _, d := range []string{logs, filepath.Join(cp.Dir, "etcd"), filepath.Join(cp.Dir, "certs")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	token, saKey, err := writeSecrets(cp.Dir)
	if err != nil {
		return err
	}
	clientURL := fmt.Sprintf("http://127.0.0.1:%d", etcdClient)
	peerURL := fmt.Sprintf("http://127.0.0.1:%d", etcdPeer)
	apiURL := fmt.Sprintf("https://127.0.0.1:%d", api)

	etcd, err := startProc("etcd", filepath.Join(logs, "etcd.log"), bins.Etcd,
		"--name", "default", "--data-dir", filepath.Join(cp.Dir, "etcd"),
		"--listen-client-urls", clientURL, "--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL, "--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "default="+peerURL)
	if err != nil {
		return err
	}
	cp.procs = append(cp.procs, etcd)
	if err := waitTCP(ctx, etcd, etcdClient, 30*time.Second); err != nil {
		return err
	}

	apiserver, err := startProc("kube-apiserver", filepath.Join(logs, "kube-apiserver.log"), bins.KubeAPIServer,
		"--etcd-servers", clientURL, "--secure-port", strconv.Itoa(api), "--bind-address", "127.0.0.1",
		"--advertise-address", "127.0.0.1", "--cert-dir", filepath.Join(cp.Dir, "certs"),
		"--service-account-issuer", apiURL,
		"--service-account-key-file", saKey, "--service-account-signing-key-file", saKey,
		"--service-cluster-ip-range", "10.0.0.0/24", "--authorization-mode", "RBAC",
		"--token-auth-file", filepath.Join(cp.Dir, "tokens.csv"), "--disable-admission-plugins", "ServiceAccount",
		"--allow-privileged=true", "--storage-media-type", "application/json")
	if err != nil {
		return err
	}
	cp.procs = append(cp.procs, apiserver)

	cp.Kubeconfig = filepath.Join(cp.Dir, "admin.kubeconfig")
	if err := writeKubeconfig(cp.Kubeconfig, apiURL, token); err != nil {
		return err
	}
	if cp.Config, err = clientcmd.BuildConfigFromFlags("", cp.Kubeconfig); err != nil {
		return err
	}
	cp.Config.QPS, cp.Config.Burst = 200, 400
	if err := waitReady(ctx, apiserver, cp.Config, 60*time.Second); err != nil {
		return err
	}

	if opts.BeforeControllerManager != nil {
		if err := opts.BeforeControllerManager(ctx, cp.Config); err != nil {
			return fmt.Errorf("before the controller manager: %w", err)
		}
	}

	if opts.WithoutControllerManager {
		return nil
	}
	kcmProc, err := startProc("kube-controller-manager", filepath.Join(logs, "kube-controller-manager.log"), bins.KubeControllerManager,
		"--kubeconfig", cp.Kubeconfig,
		"--controllers", "garbage-collector-controller,namespace-controller",
		"--leader-elect=false", "--secure-port", strconv.Itoa(kcm), "--bind-address", "127.0.0.1")
	if err != nil {
		return err
	}
	cp.procs = append(cp.procs, kcmProc)
	return waitHealthz(ctx, kcmProc, kcm, 60*time.Second)
}

// PIDs returns the process IDs of the control plane's processes.
func (cp *ControlPlane) PIDs() []int {
	var pids []int
	for _, p := range cp.procs {
		pids = append(pids, p.pid())
	}
	return pids
}

// Stop stops the processes in reverse start order, by PID, and then checks
// that a server that was listening on a system port before Start still is.
func (cp *ControlPlane) Stop() error {
	var errs []error
	for i := len(cp.procs) - 1; i >= 0; i-- {
		if err := cp.procs[i].stop(20 * time.Second); err != nil {
			errs = append(errs, err)
		}
	}
	if cp.guard != nil {
		if err := cp.guard(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// AssertAllDead reports a process of the control plane that is still alive.
func (cp *ControlPlane) AssertAllDead() error {
	for _, p := range cp.procs {
		if p.alive() {
			return fmt.Errorf("%s (pid %d) is still running", p.name, p.pid())
		}
	}
	return nil
}

func listening(port int) bool {
	c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// waitTCP waits until port accepts connections while p stays alive.
func waitTCP(ctx context.Context, p *proc, port int, timeout time.Duration) error {
	end := time.Now().Add(timeout)
	for time.Now().Before(end) && ctx.Err() == nil {
		if !p.alive() {
			return fmt.Errorf("%s exited before port %d accepted connections (see its log)", p.name, port)
		}
		if listening(port) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("port %d not listening after %s", port, timeout)
}

// waitReady polls /readyz of the API server while its process stays alive.
func waitReady(ctx context.Context, p *proc, cfg *rest.Config, timeout time.Duration) error {
	cl, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return err
	}
	cl.Timeout = 2 * time.Second
	end := time.Now().Add(timeout)
	for time.Now().Before(end) && ctx.Err() == nil {
		if !p.alive() {
			return fmt.Errorf("%s exited before /readyz answered (see its log)", p.name)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, cfg.Host+"/readyz", nil)
		if resp, err := cl.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s /readyz not ok after %s", p.name, timeout)
}

// waitHealthz polls the controller manager's /healthz on its secure port. The
// process we started must be alive at every probe: a 200 from some other
// server on the port proves nothing about it.
func waitHealthz(ctx context.Context, p *proc, port int, timeout time.Duration) error {
	hc := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // loopback health probe of our own child
	end := time.Now().Add(timeout)
	for time.Now().Before(end) && ctx.Err() == nil {
		if !p.alive() {
			return fmt.Errorf("%s exited before /healthz on port %d answered (see its log)", p.name, port)
		}
		resp, err := hc.Get(fmt.Sprintf("https://127.0.0.1:%d/healthz", port))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && p.alive() {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("%s /healthz on port %d not ok after %s", p.name, port, timeout)
}

// writeSecrets makes a static admin token and a service-account key in dir.
func writeSecrets(dir string) (token, saKey string, err error) {
	raw := make([]byte, 16)
	if _, err = rand.Read(raw); err != nil {
		return
	}
	token = hex.EncodeToString(raw)
	if err = os.WriteFile(filepath.Join(dir, "tokens.csv"), []byte(fmt.Sprintf("%s,admin,uid-admin,\"system:masters\"\n", token)), 0o600); err != nil {
		return
	}
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return
	}
	saKey = filepath.Join(dir, "sa.key")
	err = os.WriteFile(saKey, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}), 0o600)
	return
}

// writeKubeconfig writes an admin kubeconfig. The API server's certificate is
// self-signed for loopback, so the client skips verification; nothing outside
// the machine can reach the server.
func writeKubeconfig(path, server, token string) error {
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["c"] = &clientcmdapi.Cluster{Server: server, InsecureSkipTLSVerify: true}
	cfg.AuthInfos["admin"] = &clientcmdapi.AuthInfo{Token: token}
	cfg.Contexts["c"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "admin"}
	cfg.CurrentContext = "c"
	return clientcmd.WriteToFile(*cfg, path)
}
