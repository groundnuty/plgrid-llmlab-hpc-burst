package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// runSetup writes one configuration's files: the spec, the dial-home
// certificate (where the front reads it), frps's configuration with both
// server plugins (the launcher's own rendering), the front's configuration
// and client keys, each Instance's key, and, in direct mode, every frpc's
// configuration (the agent's own rendering of HBEP-6's frpc block). In
// host-dialed mode frpc's server is the job side's serving listener, whose
// port the node chooses, so "s16 frpcconf" renders them once the node is up.
func runSetup(args []string) error {
	fs := flag.NewFlagSet("setup", flag.ContinueOnError)
	dir := fs.String("dir", "", "the configuration's directory (created, mode 700; keep it short: sockets live in it)")
	n := fs.Int("n", 1, "tunnels")
	mode := fs.String("mode", ModeDirect, "direct or host-dialed")
	base := fs.Int("base", 30130, "the launcher's port base (agentapi.Host)")
	server := fs.String("server", "127.0.0.1:30143", "where frpc dials home in direct mode: the link in front of the dial-home listener")
	clientListen := fs.String("client-listen", "127.0.0.1:30142", "the front's client listener")
	tokenDelay := fs.Duration("token-delay", 200*time.Millisecond, "the engine double's wait before each token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || !filepath.IsAbs(*dir) || *n < 1 {
		return errors.New("-dir (absolute) and -n >= 1 are required")
	}
	if *mode != ModeDirect && *mode != ModeHostDialed {
		return fmt.Errorf("-mode %q", *mode)
	}
	for _, a := range []string{*server, *clientListen} {
		if err := requireLoopback(a); err != nil {
			return err
		}
	}
	for _, d := range []string{*dir, filepath.Join(*dir, "s"), filepath.Join(*dir, "frpc"), filepath.Join(*dir, "a")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	host := agentapi.Host{StateDir: filepath.Join(*dir, "state"), PortBase: *base}
	if err := os.MkdirAll(host.StateDir, 0o700); err != nil {
		return err
	}
	cert, err := writeDialHomeCert(host)
	if err != nil {
		return err
	}
	pw, _, err := launcher.EnsureFrpsWebPassword(host)
	if err != nil {
		return err
	}
	health := net.JoinHostPort("127.0.0.1", strconv.Itoa(*base+4))
	frpsCfg, err := launcher.WriteFrpsConfigWithFront(host, pw, health)
	if err != nil {
		return err
	}
	clientKey, err := agentapi.NewKey()
	if err != nil {
		return err
	}
	keysFile := filepath.Join(*dir, "client-keys.json")
	if err := writeJSONFile(keysFile, map[string]any{"keys": []map[string]string{{"id": "s16-load", "key": clientKey}}}); err != nil {
		return err
	}
	frontCfg := map[string]any{"clientListen": *clientListen, "clientKeysFile": keysFile, "usageDir": filepath.Join(*dir, "usage")}
	if *mode == ModeDirect {
		frontCfg["dialHomeListen"] = host.DialHomeAddr()
	}
	frontCfgPath := filepath.Join(*dir, "front.json")
	if err := writeJSONFile(frontCfgPath, frontCfg); err != nil {
		return err
	}
	sp := Spec{Mode: *mode, Dir: *dir, StateDir: host.StateDir, PortBase: *base, FrpsConfig: frpsCfg, FrpsWebPass: pw,
		FrontConfig: frontCfgPath, ClientKey: clientKey, ClientListen: *clientListen, FrontHealth: health, CertFile: cert,
		DeploymentsNS: "s16", ProviderName: "example-a", ProfileName: "example-batch", TokenDelayMs: int(tokenDelay.Milliseconds())}
	if *mode == ModeDirect {
		sp.Server = *server
	}
	if err := os.MkdirAll(host.CredentialsDir(), 0o700); err != nil {
		return err
	}
	for i := 0; i < *n; i++ {
		t := Tunnel{Index: i, UID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), Allocation: fmt.Sprintf("10000000-0000-4000-8000-%012d", i+1),
			Model: fmt.Sprintf("s16-m%04d", i), Socket: filepath.Join(*dir, "s", strconv.Itoa(i)),
			Frpc: filepath.Join(*dir, "frpc", strconv.Itoa(i)+".toml"), FrpcLog: filepath.Join(*dir, "frpc", strconv.Itoa(i)+".log"),
			Dir: filepath.Join(*dir, "a", strconv.Itoa(i)), Tag: fmt.Sprintf("s16-%d", i), Job: ""}
		if t.Token, err = agentapi.NewKey(); err != nil {
			return err
		}
		if t.InstanceKey, err = agentapi.NewKey(); err != nil {
			return err
		}
		if err := agentapi.WriteKeyFile(agentapi.InstanceKeyPath(host.CredentialsDir(), t.UID), t.InstanceKey); err != nil {
			return err
		}
		sp.Tunnels = append(sp.Tunnels, t)
	}
	if err := sp.check(); err != nil {
		return err
	}
	if *mode == ModeDirect {
		h, p, _ := net.SplitHostPort(*server)
		port, _ := strconv.Atoi(p)
		for _, t := range sp.Tunnels {
			if err := writeFrpc(sp, t, h, port); err != nil {
				return err
			}
		}
	}
	return writeJSONFile(filepath.Join(*dir, "spec.json"), sp)
}

// runFrpcConf renders the frpc configurations of a host-dialed configuration,
// each to its tunnel's job side's serving port (the node's ready file).
func runFrpcConf(args []string) error {
	fs := flag.NewFlagSet("frpcconf", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the spec")
	readyPath := fs.String("ready", "", "the node's ready file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sp, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	var rd struct {
		ServingPorts []int `json:"serving_ports"`
	}
	if err := readJSONFile(*readyPath, &rd); err != nil {
		return err
	}
	if len(rd.ServingPorts) != len(sp.Tunnels) {
		return fmt.Errorf("the node's ready file has %d serving ports for %d tunnels", len(rd.ServingPorts), len(sp.Tunnels))
	}
	for i, t := range sp.Tunnels {
		if err := writeFrpc(sp, t, "127.0.0.1", rd.ServingPorts[i]); err != nil {
			return err
		}
	}
	return nil
}

// writeFrpc writes one tunnel's frpc configuration with the agent's own
// renderer (internal/agent.FrpcConfig, HBEP-6's frpc block).
func writeFrpc(sp Spec, t Tunnel, host string, port int) error {
	text, err := agent.FrpcConfig{ServerAddr: host, ServerPort: port, LogFile: t.FrpcLog, Token: t.Token,
		CAFile: sp.CertFile, ServerName: agentapi.DialHomeServerName, UID: t.UID, Socket: t.Socket}.Render()
	if err != nil {
		return fmt.Errorf("tunnel %d's frpc configuration: %w", t.Index, err)
	}
	return os.WriteFile(t.Frpc, text, 0o600)
}

// writeDialHomeCert writes a throwaway certificate for the dial-home listener
// where the front reads it (agentapi.Host.AgentCertFile), as
// internal/front/frprig_test.go does, and returns its path for frpc's
// trustedCaFile.
func writeDialHomeCert(host agentapi.Host) (string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", err
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: agentapi.DialHomeServerName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(48 * time.Hour), DNSNames: []string{agentapi.DialHomeServerName}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return "", err
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(host.AgentCertFile()), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(host.AgentCertFile(), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return "", err
	}
	if err := os.WriteFile(host.AgentKeyFile(), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		return "", err
	}
	return host.AgentCertFile(), nil
}

func readJSONFile(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}
