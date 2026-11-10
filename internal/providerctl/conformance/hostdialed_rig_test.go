package conformance

// The control-plane host's side of the reach-host-dialed rows (issue #305): what the batch Provider's controller in
// host-dialed mode dials, and the instruments the row reads. The relays themselves are the controller's own
// (providerctl.Options.Relays, so internal/hostdial's Owner, driven by the controller's observations as in production);
// this file gives them their two ports and their ssh.
//
//   - The control port: the real agent API server (internal/agentserver) with the launcher's kind of certificate, over a
//     fake Backend (pkg/agentapi/fake) that keeps the agent's registration and tells it to stop at the end. The agent's
//     registration crosses the control relay, so it is the control session's proof.
//   - The serving port: a responder that checks the 256 byte values the node sends, each in its place, and answers with
//     the 256 values in descending order (servingResponder).
//   - The relays' ssh: a wrapper that writes down the remote line it was given, then execs ssh with the same arguments,
//     so the relay's PID, its process group and its stop by PID are ssh's own (relayWrapper). The line is what the
//     srun -u and epoch checks read: what ssh got, not what the code meant to send.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agentserver"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The digests of the 256 byte values in ascending order (0x00 to 0xff) and in descending order (0xff to 0x00), held as
// literals so that neither side's check is built by the code that makes the bytes. Both were computed on 10 Nov 2026 with
// two tools that share nothing with this package: python3's hashlib (sha256(bytes(range(256))) and
// sha256(bytes(range(255, -1, -1)))) and, the same values, bash's printf of each byte's octal escape piped to coreutils'
// sha256sum.
const (
	ascendingSHA256  = "40aff2e9d2d8922e47afd4648e6967497158785fbd1da870e7110266bf944880"
	descendingSHA256 = "cd6816b77f68d70001fc3eaa4d42bdd67cb5973b3151cc5292ecc02a3daac6ab"
)

// servingExchangeWait bounds each read of the responder's one exchange: the node's 256 bytes, then the node's close.
const servingExchangeWait = 60 * time.Second

// servingConn is one connection the responder accepted: what came from the node and what it sent back.
type servingConn struct {
	// Received is what the node sent before the responder answered, up to 256 bytes; InPlace says whether it was the
	// 256 values each at its own index.
	Received []byte
	InPlace  bool
	// Wrote is how many bytes of the descending answer went out; Closed is whether the node then closed its end.
	Wrote  int
	Closed bool
}

// servingResponder is the serving channel's loopback port on the control-plane host (HBEP-6: the dial-home listener's
// place, base+7). The relay's control-plane end dials it for each stream the job opens.
type servingResponder struct {
	ln net.Listener
	wg sync.WaitGroup

	mu    sync.Mutex
	conns []servingConn
}

func startServingResponder(t testing.TB) *servingResponder {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &servingResponder{ln: ln}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			r.wg.Add(1)
			go func() { defer r.wg.Done(); r.serve(c) }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); r.wg.Wait() })
	return r
}

func (r *servingResponder) port() int { return r.ln.Addr().(*net.TCPAddr).Port }

// serve is one exchange: 256 bytes from the node, checked each at its index; the 256 values in descending order back;
// then the node's close.
func (r *servingResponder) serve(c net.Conn) {
	defer c.Close()
	var sc servingConn
	defer func() {
		r.mu.Lock()
		r.conns = append(r.conns, sc)
		r.mu.Unlock()
	}()
	_ = c.SetDeadline(time.Now().Add(servingExchangeWait))
	buf := make([]byte, 256)
	n, _ := io.ReadFull(c, buf)
	sc.Received = append([]byte(nil), buf[:n]...)
	sc.InPlace = n == 256
	for i := 0; i < n; i++ {
		if int(buf[i]) != i {
			sc.InPlace = false
		}
	}
	if n < 256 {
		return
	}
	answer := make([]byte, 256)
	for i := range answer {
		answer[i] = byte(255 - i)
	}
	sc.Wrote, _ = c.Write(answer)
	_ = c.SetDeadline(time.Now().Add(servingExchangeWait))
	extra, err := io.Copy(io.Discard, io.LimitReader(c, 4096))
	sc.Closed = err == nil && extra == 0
}

// exchanges are the connections the responder has finished, in order.
func (r *servingResponder) exchanges() []servingConn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]servingConn(nil), r.conns...)
}

// sha256Hex is the hex sha256 of b.
func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// writeEndpointCert writes a self-signed ECDSA P-256 certificate and key where the launcher puts the agent endpoint's
// (agentapi.Host.AgentCertFile), naming the dial-home server name that the agent and frpc check, and returns its PEM and
// pin: the Provider's front settings stage the PEM and write the pin into agent.json, so the agent's pinned HTTPS to
// the control port accepts exactly this server.
func writeEndpointCert(t testing.TB, host agentapi.Host) ([]byte, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "agent-endpoint"},
		DNSNames:     []string{agentapi.DialHomeServerName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(host.AgentCertFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(host.AgentCertFile(), certPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host.AgentKeyFile(), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPEM, agentapi.PinOf(cert)
}

// relayWrapperTemplate is the relays' ssh. It writes down the remote line, ssh's last argument, as ssh will get it, then
// execs ssh with every argument, so that the process the relay owner started, stops by its PID and reaps is ssh itself.
// @FAULT@ is empty in a live run; a dry run's fault changes the line or the process there (dryRelayFaults). The record is
// private (the line names the job and the paths on the cluster), mode 600 in a mode-700 directory.
const relayWrapperTemplate = `#!/bin/bash
args=("$@")
last=${args[-1]}
@FAULT@
(umask 077; printf '%s\n' "$last" >> '@REC@')
args[-1]=$last
exec ssh "${args[@]}"
`

// relayWrapper writes the wrapper and returns its path and its record's.
func relayWrapper(t testing.TB, fault string) (wrapper, record string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "hdw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	record = filepath.Join(dir, "lines")
	wrapper = filepath.Join(dir, "ssh")
	if strings.ContainsAny(record, `'\`) {
		t.Fatalf("the wrapper's record path holds a quote")
	}
	body := strings.NewReplacer("@FAULT@", fault, "@REC@", record).Replace(relayWrapperTemplate)
	if err := os.WriteFile(wrapper, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return wrapper, record
}

// relayLines reads the wrapper's record: the remote lines that were srun steps (the wrapper also sees ssh -G, which the
// relay owner runs to name the login host). A record that cannot be read is an error, not "no line".
func relayLines(record string) ([]string, error) {
	b, err := os.ReadFile(record)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // no relay was started
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "srun ") {
			out = append(out, l)
		}
	}
	return out, nil
}

// hostDialedRig is the control-plane host of one run: the two ports, the backend and the relays' settings.
type hostDialedRig struct {
	host     agentapi.Host
	certPEM  []byte
	pin      string
	control  net.Listener
	backend  *fake.Backend
	serving  *servingResponder
	wrapper  string
	record   string
	settings providerctl.RelaySettings
	// staleWrapper and staleRecord are the stale relays' own ssh and record, apart from the Owner's.
	staleWrapper, staleRecord string
	// stop is the backend the agent API serves, which records the stop's way to the agent.
	stop *stopWatch
}

// stopWatch is the agent API's backend as the row reads the stop (the review of PR 306, B2): the fake Backend, recording,
// once armed for the Allocation, whether the agent's own poll was served the stop, which then crossed the control session
// to the agent, and whether the agent reported after that: the last report it sends once it has stopped on the
// instruction (internal/agent, sendLastReport). A job that merely ended shows neither.
type stopWatch struct {
	*fake.Backend
	mu            sync.Mutex
	uid           string
	served, acked bool
}

func (s *stopWatch) arm(uid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uid, s.served, s.acked = uid, false, false
}

func (s *stopWatch) seen() (served, acked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.served, s.acked
}

func (s *stopWatch) Desired(ctx context.Context, c agentapi.Caller) (agentapi.DesiredState, error) {
	d, err := s.Backend.Desired(ctx, c)
	if err == nil && d.Action == agentapi.ActionStop {
		s.mu.Lock()
		if s.uid != "" && c.AllocationUID == s.uid {
			s.served = true
		}
		s.mu.Unlock()
	}
	return d, err
}

func (s *stopWatch) Report(ctx context.Context, c agentapi.Caller, r agentapi.Report) error {
	err := s.Backend.Report(ctx, c, r)
	if err == nil {
		s.mu.Lock()
		if s.served && c.AllocationUID == s.uid {
			s.acked = true
		}
		s.mu.Unlock()
	}
	return err
}

// newHostDialedRig makes the rig: the certificate, the control port's listener (served by startHostSide once the world
// has its credentials directory), the serving responder and the relays' ssh wrapper with the fault (empty live). The relay
// settings name both ports, the control directory and the fixed epoch; tune shortens a dry run's timings.
func newHostDialedRig(t testing.TB, controlDir, fault, staleFault string, tune func(*providerctl.RelaySettings)) *hostDialedRig {
	t.Helper()
	state, err := os.MkdirTemp("", "hds")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(state) })
	g := &hostDialedRig{host: agentapi.Host{StateDir: state}, backend: fake.NewBackend()}
	g.stop = &stopWatch{Backend: g.backend}
	g.certPEM, g.pin = writeEndpointCert(t, g.host)
	if g.control, err = net.Listen("tcp4", "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	g.serving = startServingResponder(t)
	g.wrapper, g.record = relayWrapper(t, fault)
	g.staleWrapper, g.staleRecord = relayWrapper(t, staleFault)
	g.settings = providerctl.RelaySettings{
		Ports: map[agentapi.RelayChannel]int{
			agentapi.RelayControl: g.control.Addr().(*net.TCPAddr).Port,
			agentapi.RelayServing: g.serving.port(),
		},
		ControlDir: controlDir, SSHBinary: g.wrapper,
		Epoch: func(context.Context) (int64, error) { return hostDialedEpoch, nil },
	}
	if tune != nil {
		tune(&g.settings)
	}
	return g
}

// startHostSide serves the agent API on the control port over the world's credentials and Allocations, and runs the
// controller's relay owner, until the test ends: its stop, which stops every relay by PID, runs before the world's own
// cleanup cancels the run's jobs (cleanups run last in, first out).
func (g *hostDialedRig) startHostSide(t testing.TB, w *world) {
	t.Helper()
	srv, err := agentserver.New(agentserver.Config{
		Lookup: agentserver.NewClientLookup(w.c), CredentialsDir: w.credDir, Backend: g.stop, Host: g.host,
		Logger: slog.New(slog.DiscardHandler), PollInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, g.control) }()
	relays := make(chan error, 1)
	go func() { relays <- w.ctl.RunRelays(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-relays; err != nil {
			w.p.log("the relay owner ended: %v", err)
		}
		if err := <-served; err != nil {
			w.p.log("the agent API server ended: %v", err)
		}
	})
}
