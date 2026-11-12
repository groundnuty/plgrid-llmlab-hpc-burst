package front

// The dial-home listener's split by TLS server name, for the bulk path (beta; HBEP-6, The bulk
// path; HBEP-5, Listeners). With the bulk path on, the listener serves the launcher's second
// certificate to a client that names agentapi.BulkDialHomeServerName, and passes that client's
// GET /~!frp to the second frps's bind port; every other request under that name gets 404, so
// /agent/v1/ is never served to the bulk name. Under any other name, or none, the listener serves
// exactly as without the bulk path. Without the bulk path none of this runs: the listener's TLS
// configuration has no GetCertificate, and DialHome has no bulk handler.

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// withBulk sets the handler for requests whose TLS server name is the bulk path's: the second
// frps's /~!frp passthrough, which answers 404 to anything but frpc's websocket upgrade.
func (d *DialHome) withBulk(h http.Handler) { d.bulk = h }

// servesBulk reports whether r came under the bulk path's server name, with the bulk path on.
func (d *DialHome) servesBulk(r *http.Request) bool {
	return d.bulk != nil && r.TLS != nil && r.TLS.ServerName == agentapi.BulkDialHomeServerName
}

// withBulkCertificate returns a copy of cfg, the dial-home listener's TLS configuration, that
// presents the launcher's second certificate (agentapi.Host.BulkCertFile, with the agent endpoint's
// key) to a client naming agentapi.BulkDialHomeServerName, and cfg's own certificate to every
// other client: GetCertificate answers nil for them, and Go then takes Certificates (crypto/tls,
// Config.GetCertificate). The second certificate must load with the key, and name the bulk server.
func withBulkCertificate(cfg *tls.Config, host agentapi.Host) (*tls.Config, error) {
	if cfg == nil {
		return nil, errors.New("the bulk path needs the dial-home listener's TLS: no dial-home mode was recorded")
	}
	bulk, err := tls.LoadX509KeyPair(host.BulkCertFile(), host.AgentKeyFile())
	if err != nil {
		return nil, fmt.Errorf("the bulk path needs the launcher's second certificate with the agent endpoint's key: %w", err)
	}
	leaf := bulk.Leaf
	if leaf == nil {
		if leaf, err = x509.ParseCertificate(bulk.Certificate[0]); err != nil {
			return nil, fmt.Errorf("the bulk path's certificate %s: %w", host.BulkCertFile(), err)
		}
	}
	if leaf.VerifyHostname(agentapi.BulkDialHomeServerName) != nil {
		return nil, fmt.Errorf("the bulk path's certificate %s does not name %s", host.BulkCertFile(), agentapi.BulkDialHomeServerName)
	}
	out := cfg.Clone()
	out.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		if hello.ServerName == agentapi.BulkDialHomeServerName {
			return &bulk, nil
		}
		return nil, nil
	}
	return out, nil
}

// FrpsBulkAddrs are the second frps's bind and tcpmux addresses, for the bulk path (HBEP-5,
// Parameters: frpsBulkBindAddr, frpsBulkMuxAddr). Set in the configuration, each is used as
// given; unset, it is the IPv4 loopback address at the launcher's port base plus 13 or 14, where
// the launcher's frps-bulk listens.
func (c Config) FrpsBulkAddrs(host agentapi.Host) (bind, mux string, err error) {
	bind, mux = c.FrpsBulkBindAddr, c.FrpsBulkMuxAddr
	if bind != "" && mux != "" {
		return bind, mux, nil
	}
	if host.PortBase <= 0 {
		return "", "", errors.New("frpsBulkBindAddr or frpsBulkMuxAddr is unset and no launcher port base is known for its default")
	}
	if bind == "" {
		bind = host.FrpsBulkBindAddr()
	}
	if mux == "" {
		mux = host.FrpsBulkTCPMuxAddr()
	}
	return bind, mux, nil
}

// checkBulkMode refuses the bulk path where it has no meaning: with no dial-home mode recorded
// (the listener is plain, so it has no server name to split by) and in ssh mode, where the agent
// starts no bulk frpc (HBEP-6, The bulk path). The launcher refuses both first (#448, ruling S2).
func checkBulkMode(mode string) error {
	switch mode {
	case ModeDirect, ModeHostDialed:
		return nil
	case ModeSSH:
		return errors.New("the bulk path is for direct and host-dialed mode only: in ssh mode the agent starts no bulk frpc")
	}
	return errors.New("the bulk path needs a dial-home mode: the dial-home listener splits by TLS server name")
}
