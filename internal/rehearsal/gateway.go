package rehearsal

// New. The dial-home gateway: the control-plane host's ssh server for ssh mode
// (HBEP-6, Dial-home modes), a real sshd on the login node's machinery
// (sshd.go). The agent in a job opens two `ssh -N -L` forwards to it, with a
// key that carries HBEP-6's restricted line. The gateway runs no relay and no
// forced command of its own: the key's command is /bin/false, and the two
// forwards end at the loopback ports of the agent endpoint and the front's
// dial-home listener, which the caller serves.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// GatewayConfig configures a gateway.
type GatewayConfig struct {
	// Dir is an existing, private (mode 700) scratch directory for sshd's
	// files and the keys.
	Dir string
	// AgentPort is the agent endpoint's loopback port on this host (the
	// launcher's base+3): connection 1's target.
	AgentPort int
	// DialHomePort is the front's dial-home listener's loopback port on this
	// host (base+7): connection 2's target. It differs from AgentPort,
	// because the key permits exactly these two ports.
	DialHomePort int
	// SSHD is the sshd binary; "" means $HPCBURST_SSHD, then /usr/sbin/sshd.
	SSHD string
}

// Gateway is a running dial-home gateway.
type Gateway struct {
	cfg   GatewayConfig
	files sshdFiles
	user  string
	srv   *sshdServer

	closeOnce sync.Once
}

// gatewayConf is sshd_config for the gateway. Forwarding is left at sshd's
// default, as on a control-plane host that nobody configured for this: the
// restriction is the key's options, so a refusal in a test comes from them
// and from nothing else. The two ClientAlive lines are HBEP-6's operator
// setting (Dead nodes).
var gatewayConf = []string{
	"AllowTcpForwarding yes", "GatewayPorts no", "PermitUserEnvironment no",
	"ClientAliveInterval 10", "ClientAliveCountMax 3",
}

// dialHomeKeyOptions is what precedes the key in authorized_keys: HBEP-6's
// restricted line (Dial-home modes, The key's line, owner decision of 31 Oct
// 2026) for the two ports.
func dialHomeKeyOptions(agentPort, dialHomePort int) string {
	return fmt.Sprintf(`restrict,port-forwarding,permitopen="127.0.0.1:%d",permitopen="127.0.0.1:%d",permitlisten="127.0.0.1:1",command="/bin/false"`,
		agentPort, dialHomePort)
}

// StartGateway starts the sshd listener. It fails, rather than guesses, when
// a path is unsafe, the directory is not private, a port is not a port or a
// tool is missing.
func StartGateway(cfg GatewayConfig) (*Gateway, error) {
	if cfg.SSHD == "" {
		cfg.SSHD = SSHDPath()
	}
	if err := CheckTools(cfg.SSHD); err != nil {
		return nil, err
	}
	for _, p := range []struct {
		name string
		port int
	}{{"AgentPort", cfg.AgentPort}, {"DialHomePort", cfg.DialHomePort}} {
		if p.port < 1 || p.port > 65535 {
			return nil, fmt.Errorf("rehearsal: %s %d is not a port", p.name, p.port)
		}
	}
	if cfg.AgentPort == cfg.DialHomePort {
		return nil, errors.New("rehearsal: AgentPort and DialHomePort must differ: the key permits exactly two ports")
	}
	if err := checkDir(cfg.Dir, "it holds the gateway's host key and the client's private key"); err != nil {
		return nil, err
	}
	u, err := sshdUser()
	if err != nil {
		return nil, err
	}
	g := &Gateway{cfg: cfg, files: filesIn(cfg.Dir), user: u}
	spec := sshdSpec{user: u, options: dialHomeKeyOptions(cfg.AgentPort, cfg.DialHomePort), conf: gatewayConf}
	if g.srv, err = startSSHD(cfg.SSHD, g.files, spec); err != nil {
		return nil, err
	}
	return g, nil
}

// Addr is the sshd listener's address, 127.0.0.1:<kernel port>: agent.json's
// endpoint in ssh mode.
func (g *Gateway) Addr() string { return g.srv.ln.Addr().String() }

// User is the OS account that sshd logs in, read at run time: agent.json's
// ssh.user.
func (g *Gateway) User() string { return g.user }

// WriteDialHome puts the client key (mode 600) and known_hosts, whose one line
// is [127.0.0.1]:<port> <type> <base64>, into dir as the operator would for
// the cluster user: agentapi.DialHomeKeyFile and agentapi.DialHomeKnownHostsFile
// in the dial-home directory. It creates dir, mode 700, and refuses an
// existing one that is not private, since the key goes in it. Each file is
// replaced by a rename, so a reader never sees a partial one.
func (g *Gateway) WriteDialHome(dir string) error {
	if !filepath.IsAbs(dir) {
		return errors.New("rehearsal: the dial-home directory must be an absolute path")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() || fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("rehearsal: the dial-home directory %s must be a directory with mode 700: the key goes in it", dir)
	}
	for _, f := range []struct{ from, to string }{
		{g.files.clientKey, agentapi.DialHomeKeyFile},
		{g.files.knownHosts, agentapi.DialHomeKnownHostsFile},
	} {
		b, err := os.ReadFile(f.from)
		if err != nil {
			return err
		}
		if err := writeAtomic(filepath.Join(dir, f.to), b); err != nil {
			return err
		}
	}
	return nil
}

// Close stops the listener and every sshd child with the processes below
// them, and waits until they are gone. It is safe to call twice.
func (g *Gateway) Close() error {
	g.closeOnce.Do(func() {
		g.srv.stopListening()
		g.srv.stop(nil)
	})
	return nil
}
