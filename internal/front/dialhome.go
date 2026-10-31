package front

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Dial-home modes, as the launcher records them (HBEP-11, open question 8).
const (
	ModeDirect = "direct"
	ModeSSH    = "ssh"
)

// DialHome is the dial-home listener's handler (HBEP-5, Listeners). In direct
// mode it passes every request under /agent/v1/ through, unchanged, to the
// lifecycle controller's loopback agent endpoint and streams the answer back,
// long polls included; the controller authenticates the agent and the front
// adds nothing. In ssh mode the agent reaches that endpoint through its
// forward, and the passthrough is unused. Every other path belongs to
// HBEP-6's tunnel, which is not built yet: 404 until then.
type DialHome struct {
	mode   string
	agent  http.Handler
	tunnel http.Handler
}

// NewDialHome builds the handler. In direct mode the passthrough connects to
// host.AgentEndpointAddr() over TLS and accepts only the certificate at
// host.AgentCertFile(), by its pin. tunnel may be nil.
func NewDialHome(mode string, host agentapi.Host, tunnel http.Handler) (*DialHome, error) {
	d := &DialHome{mode: mode, tunnel: tunnel}
	if mode != ModeDirect {
		return d, nil
	}
	pin, err := certPin(host.AgentCertFile())
	if err != nil {
		return nil, fmt.Errorf("direct mode needs the agent endpoint's certificate: %w", err)
	}
	target := &url.URL{Scheme: "https", Host: host.AgentEndpointAddr()}
	d.agent = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host
		},
		// No compression of its own: Go's transport would otherwise add
		// Accept-Encoding to a request that has none and unpack the answer,
		// and the passthrough adds nothing.
		Transport:     &http.Transport{TLSClientConfig: agentapi.ClientTLSConfig(pin), ForceAttemptHTTP2: false, DisableCompression: true},
		FlushInterval: -1,
	}
	return d, nil
}

func (d *DialHome) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/agent/v1/") {
		if d.agent == nil {
			http.NotFound(w, r) // ssh mode: the agent uses its forward
			return
		}
		d.agent.ServeHTTP(w, r)
		return
	}
	if d.tunnel != nil {
		d.tunnel.ServeHTTP(w, r)
		return
	}
	http.NotFound(w, r)
}

// certPin reads a PEM certificate and returns its pin (agentapi.PinOf).
func certPin(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	block, _ := pem.Decode(b)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", errors.New(path + " holds no PEM certificate")
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", err
	}
	return agentapi.PinOf(c), nil
}

// dialHomeTLS is the dial-home listener's TLS configuration: it terminates
// TLS with the launcher's certificate for the agent endpoint
// (Host.AgentCertFile), so the agent's pin holds on both paths (HBEP-5,
// Listeners; @hpc-burst-science-agent, 31 Oct 2026). It is required in both
// dial-home modes; with no mode recorded, no job dials home, and the listener
// is plain when the certificate is absent.
func dialHomeTLS(mode string, host agentapi.Host) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(host.AgentCertFile(), host.AgentKeyFile())
	if err != nil {
		if mode == ModeDirect || mode == ModeSSH {
			return nil, fmt.Errorf("the dial-home listener needs the agent endpoint's certificate and key: %w", err)
		}
		return nil, nil
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}
