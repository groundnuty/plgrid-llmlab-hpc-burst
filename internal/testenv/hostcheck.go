package testenv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// The ports this package may use or reach. IMPL-COMMON gives issue #40 the
// block 29200-29219, and issue #41's launcher the block 29220-29239. A test run
// that reaches any other port on this machine could reach the root k3s.
const (
	// PortRangeFirst and PortRangeLast bound the block a control plane started
	// by Start listens on: 29200-29219.
	PortRangeFirst = 29200
	PortRangeLast  = 29219

	// ExternalPortLast is the last port an external control plane (the
	// launcher's user-level k3s) may listen on: its block ends at 29239.
	ExternalPortLast = 29239

	// portBlockSize is the number of ports Start uses: etcd client, etcd peer,
	// kube-apiserver, kube-controller-manager.
	portBlockSize = 4
)

// systemPorts are the ports of the root k3s on this machine: its API server,
// its hidden apiserver port, and its kubelet. A test run that starts and stops
// a control plane must leave the ones that listened before it listening.
var systemPorts = []int{6443, 6444, 10250}

// portBase parses the first port of Start's block from the value of
// HPCBURST_TESTENV_PORT_BASE (empty means DefaultPortBase). The whole block,
// base to base+3, must lie inside 29200-29219, so the base is 29200 to 29216.
func portBase(value string) (int, error) {
	if value == "" {
		return DefaultPortBase, nil
	}
	base, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a port number: %w", envPortBase, value, err)
	}
	if last := PortRangeLast - portBlockSize + 1; base < PortRangeFirst || base > last {
		return 0, fmt.Errorf("%s=%d: the block of %d ports from %d must lie inside %d-%d, so the base is %d to %d",
			envPortBase, base, portBlockSize, base, PortRangeFirst, PortRangeLast, PortRangeFirst, last)
	}
	return base, nil
}

// CheckServer refuses an API server address that is not loopback on a port of
// issue #40's or #41's blocks (29200-29239). It parses the address and never
// connects. The root k3s listens on 6443, 6444 and 10250; a kubeconfig that
// points there would let a test run create CRDs in the root cluster.
func CheckServer(server string) error {
	if server == "" {
		return errors.New("the kubeconfig names no server")
	}
	raw := server
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw // client-go reads host:port as https
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("server %q is not an address: %w", server, err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("server %q: scheme %q is not http or https", server, u.Scheme)
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("server %q is not on loopback: a test run only talks to a control plane on this machine's own loopback address", server)
	}
	if u.Port() == "" {
		return fmt.Errorf("server %q names no port; it must be one of %d-%d", server, PortRangeFirst, ExternalPortLast)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		return fmt.Errorf("server %q: port %q is not a number: %w", server, u.Port(), err)
	}
	if port < PortRangeFirst || port > ExternalPortLast {
		return fmt.Errorf("server %q: port %d is outside %d-%d, the ports issues #40 and #41 may use; the root k3s listens on 6443, 6444 and 10250",
			server, port, PortRangeFirst, ExternalPortLast)
	}
	return nil
}

// ExternalConfig reads the kubeconfig of an external control plane (the
// launcher's user-level k3s) and returns a client configuration for it. It
// checks the current context's server with CheckServer before anything is
// built from the file and before any request is made, and it refuses a proxy,
// which could carry a loopback address to another host. The configuration it
// returns is built from the same parsed file that was checked, and its Host is
// checked again.
func ExternalConfig(kubeconfig string) (*rest.Config, error) {
	raw, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("kubeconfig %s: %w", kubeconfig, err)
	}
	ctx, ok := raw.Contexts[raw.CurrentContext]
	if !ok {
		return nil, fmt.Errorf("kubeconfig %s: current context %q does not exist", kubeconfig, raw.CurrentContext)
	}
	cluster, ok := raw.Clusters[ctx.Cluster]
	if !ok {
		return nil, fmt.Errorf("kubeconfig %s: context %q names the cluster %q, which does not exist", kubeconfig, raw.CurrentContext, ctx.Cluster)
	}
	if err := CheckServer(cluster.Server); err != nil {
		return nil, fmt.Errorf("kubeconfig %s refused: %w", kubeconfig, err)
	}
	if cluster.ProxyURL != "" {
		return nil, fmt.Errorf("kubeconfig %s refused: cluster %q sets a proxy, which could carry the loopback address to another host", kubeconfig, ctx.Cluster)
	}
	cfg, err := clientcmd.NewNonInteractiveClientConfig(*raw, raw.CurrentContext, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig %s: %w", kubeconfig, err)
	}
	if err := CheckServer(cfg.Host); err != nil {
		return nil, fmt.Errorf("kubeconfig %s refused after loading: %w", kubeconfig, err)
	}
	return cfg, nil
}

// GuardSystemPorts records which system ports (the root k3s's) listen now and
// returns a check that reports one that listened then and does not now. It
// reads the kernel's table of listening sockets and never connects, so it
// cannot disturb the server it watches. If that table cannot be read the
// check fails: an unreadable table is unknown, not a pass. Call the check after
// every test run that started anything.
func GuardSystemPorts() func() error {
	before, err := listeningPorts()
	if err != nil {
		return func() error {
			return fmt.Errorf("the listening sockets could not be read, so the system ports %v are unchecked: %w", systemPorts, err)
		}
	}
	return func() error {
		after, err := listeningPorts()
		if err != nil {
			return fmt.Errorf("the listening sockets could not be read, so the system ports %v are unchecked: %w", systemPorts, err)
		}
		var errs []error
		for _, p := range systemPorts {
			if before[p] && !after[p] {
				errs = append(errs, fmt.Errorf("port %d listened before the run and does not any more", p))
			}
		}
		return errors.Join(errs...)
	}
}

// listeningPorts returns the TCP ports that have a socket in the LISTEN state,
// IPv4 and IPv6, on any address. IPv6 may be absent from a kernel, so a missing
// /proc/net/tcp6 is tolerated; a missing /proc/net/tcp is not.
func listeningPorts() (map[int]bool, error) {
	ports := map[int]bool{}
	for _, f := range []struct {
		path     string
		optional bool
	}{{"/proc/net/tcp", false}, {"/proc/net/tcp6", true}} {
		fh, err := os.Open(f.path)
		if err != nil {
			if f.optional && errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, err
		}
		err = parseListening(fh, ports)
		_ = fh.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.path, err)
		}
	}
	return ports, nil
}

// tcpListen is the state code of a listening socket in /proc/net/tcp.
const tcpListen = "0A"

// parseListening adds to ports the local port of every LISTEN socket in a
// /proc/net/tcp or /proc/net/tcp6 table. A line is
//
//	sl local_address rem_address st ...
//
// with local_address as hex address, a colon and the hex port.
func parseListening(r io.Reader, ports map[int]bool) error {
	sc := bufio.NewScanner(r)
	first := true
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if first {
			first = false
			if len(fields) == 0 || fields[0] != "sl" {
				return fmt.Errorf("unexpected header %q", sc.Text())
			}
			continue
		}
		if len(fields) < 4 {
			continue
		}
		if fields[3] != tcpListen {
			continue
		}
		i := strings.LastIndexByte(fields[1], ':')
		if i < 0 {
			return fmt.Errorf("local address %q has no port", fields[1])
		}
		port, err := strconv.ParseUint(fields[1][i+1:], 16, 16)
		if err != nil {
			return fmt.Errorf("local address %q: %w", fields[1], err)
		}
		ports[int(port)] = true
	}
	return sc.Err()
}
