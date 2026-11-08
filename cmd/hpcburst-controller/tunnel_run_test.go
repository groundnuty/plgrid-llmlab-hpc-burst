package main

// The binary's run function with the tunnel (tunnel.go) against a real API server (envtest), the
// real frps that internal/launcher starts and the real frpc, both v0.71.1 from testenv.FrpBinaries
// (HPCBURST_FRP_DIR; HPCBURST_REQUIRE_FRP=1 makes a missing binary a failure). Allocations and
// Instances are created through the API with their credential files where the lifecycle controller
// writes them; no ModelDeployment, Provider or reconciler is involved, so what moves an Instance's
// TunnelConnected condition is the tunnel alone.
//
// Every expectation is hard-coded from HBEP-6 (Path state; Admission), reasons included. A
// condition computed with frpsplugin.BuildReport, or by the writer, would agree with the code
// whatever it did. The refusals are read from frpc's own log, which carries the plugin's reason, and
// from frps's own proxy list, which holds no proxy the plugin refused.
//
// Ports: the kernel chooses the agent endpoint's port, and freeTunnelPortBase holds the block: the
// plugin's (+8) for the binary, the metrics' (+12) until the binary starts, frps's three (+9, +10,
// +11) until frps starts; frps is started on that block, so the controller's plugin endpoint is the
// one frps posts to. Everything binds 127.0.0.1.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// What HBEP-6 says the condition is, as literals: the test does not borrow them from the code.
const (
	statusTrue    = "True"
	statusFalse   = "False"
	statusUnknown = "Unknown"

	reasonOnline     = "ProxyOnline"
	reasonOffline    = "ProxyOffline"
	reasonUnreadable = "ProxyListUnreadable"

	// The plugin's refusals, as handler.go words them, which reach frpc's log (HBEP-6: reject reasons
	// reach frpc).
	refusedCredential = "is not the credential of a live Allocation"
	refusedNotPlaced  = "is not the UID of an Instance placed on this Allocation"

	tunnelNS = "tunnel-ns"
)

// freeTunnelPortBase returns a port base whose block the tunnel uses is free, and holds it, as
// freePortBase does (held_test.go): the agent endpoint (+3, which the kernel chooses) and the plugin
// (+8) for the binary's own servers, the metrics (+12) until the start releases it, and frps's
// bind, tcpmux and web ports (+9 to +11) for frps. frps is a real process that binds those three
// itself and cannot take a listener, so they are held until startFrps releases them just before it
// starts frps: the residual window is that start, not the test's whole run. A stranger that takes
// one in it fails frps's start, and startFrps repeats that start on a fresh block (issue #261).
// The rig starts the binary with launchBinary, with no retry on a taken port: once frps runs, its
// block cannot change.
func freeTunnelPortBase(t *testing.T) int {
	t.Helper()
	return holdBlock(t, []int{agentapi.FrpsPluginPortOffset}, tunnelOtherOffsets)
}

// tunnelOtherOffsets are the tunnel block's offsets that are not the binary's own servers: the
// metrics, released at the binary's start, and frps's three, released at frps's.
var tunnelOtherOffsets = []int{agentapi.ControllerMetricsPortOffset, agentapi.FrpsBindPortOffset, agentapi.FrpsTCPMuxPortOffset, agentapi.FrpsWebPortOffset}

// condObs is one observed state of an Instance's TunnelConnected condition.
type condObs struct{ name, status, reason, rv string }

func (o condObs) cond() string { return o.status + "/" + o.reason }

// condWatch records, from a watch on the Instances, every change of an Instance's condition or
// resourceVersion, in order. A poll can miss a state that lasts a moment; a watch cannot.
type condWatch struct {
	mu   sync.Mutex
	last map[string]condObs
	hist []condObs
}

func startCondWatch(t *testing.T, api *apiServer) *condWatch {
	t.Helper()
	wc, err := client.NewWithWatch(api.env.Config, client.Options{Scheme: api.client.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w, err := wc.Watch(ctx, &v1alpha1.InstanceList{}, client.InNamespace(tunnelNS))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cw := &condWatch{last: map[string]condObs{}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range w.ResultChan() {
			in, ok := ev.Object.(*v1alpha1.Instance)
			if !ok || (ev.Type != watch.Added && ev.Type != watch.Modified) {
				continue
			}
			o := condObs{name: in.Name, rv: in.ResourceVersion}
			if c := meta.FindStatusCondition(in.Status.Conditions, v1alpha1.ConditionTunnelConnected); c != nil {
				o.status, o.reason = string(c.Status), c.Reason
			}
			cw.mu.Lock()
			if cw.last[o.name] != o {
				cw.last[o.name] = o
				cw.hist = append(cw.hist, o)
			}
			cw.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		cancel()
		w.Stop()
		<-done
	})
	return cw
}

// mark is a position in the history; events(name, mark) lists what the Instance did after it.
func (cw *condWatch) mark() int {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return len(cw.hist)
}

func (cw *condWatch) events(name string, from int) []condObs {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	var out []condObs
	for _, o := range cw.hist[from:] {
		if o.name == name {
			out = append(out, o)
		}
	}
	return out
}

// lastRV is the resourceVersion of the last state the watch has delivered for the Instance.
func (cw *condWatch) lastRV(name string) string {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.last[name].rv
}

// conds is the sequence of distinct conditions the Instance had after the mark, "" for none.
func (cw *condWatch) conds(name string, from int) []string {
	var out []string
	for _, o := range cw.events(name, from) {
		if c := o.cond(); len(out) == 0 || out[len(out)-1] != c {
			out = append(out, c)
		}
	}
	return out
}

// uid is the Instance's UID as frpc names its proxy.
func uid(in *v1alpha1.Instance) string { return string(in.UID) }

// tunnelRig is a control plane: the API server, the state directory of a copy of the binary, and the
// real frps and frpcs around it.
type tunnelRig struct {
	t        *testing.T
	api      *apiServer
	f        flags
	frpcPath string
	frpsBin  launcher.FrpsBinary
	frps     *launcher.FrpsProc
	frpsDone bool
	watch    *condWatch
	nfrpc    int
	// logf is where a retry of frps's start is logged: the test's log, or a test's own recorder.
	logf func(format string, args ...any)
	// tried is every block the rig left for a fresh one, in order: a fresh block shares no port with
	// any of them, not only with the one it replaces.
	tried []flags
}

func newTunnelRig(t *testing.T) *tunnelRig {
	t.Helper()
	frps, frpc := testenv.FrpBinaries(t)
	api := startAPIServer(t)
	ctx := context.Background()
	for _, ns := range []string{v1alpha1.ControllerNamespace, tunnelNS} {
		if err := api.client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	// No front: nothing is submitted, and the tunnel's own settings are the only ones under test.
	f, _, _ := prepareStateAt(t, api, freeTunnelPortBase(t), "", "")
	t.Cleanup(func() {
		if pids := processesNaming(f.host.StateDir); len(pids) > 0 {
			t.Errorf("processes %v still run with the state directory on their command line", pids)
		}
	})
	bin, err := launcher.ResolveFrpsFile(frps, launcher.DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bin.Close)
	return &tunnelRig{t: t, api: api, f: f, frpcPath: frpc, frpsBin: bin, watch: startCondWatch(t, api), logf: t.Logf}
}

// beforeFrps, when set, runs in each attempt to start frps, after the three ports frps binds are
// released and before frps starts: a test puts a stranger on one of them, or breaks the start
// some other way.
var beforeFrps func(t *testing.T, attempt int, f flags)

// errFrpsPortTaken is what an attempt to start frps returns when frps exited with status 1 and
// this attempt's output holds frps's bind-error line for one of its three ports ("listen tcp A:
// bind: address already in use", frpsBindFailure): a stranger took one of the ports between their
// release and frps's bind. It is the only failure of a start that is repeated.
var errFrpsPortTaken = errors.New("frps found one of its ports taken")

// startFrps starts frps with the launcher's own functions on the block of the binary's port base,
// as hpcburst up does, and waits until its web server answers. frps binds its three ports itself,
// so a stranger can take one between their release and the bind (issue #261); a start whose frps
// lost a port (errFrpsPortTaken) is repeated on a fresh block, at most maxBindRetries times, each
// logged (startWithRetries). Every port the binary and frps use is the port base plus an offset,
// so a fresh block is a fresh base for both: b, the binary running on the old one (nil if none is
// yet), is stopped and started again on the new one, and startFrps returns that binary. The
// caller reads r.f.host again afterwards. A start that fails any other way fails the test.
func (r *tunnelRig) startFrps(b *binary) *binary {
	r.t.Helper()
	err := startWithRetries(r.logf, r.tryFrps, func(int) { b = r.freshBlock(b) })
	if err != nil {
		r.t.Fatal(err)
	}
	return b
}

// startWithRetries runs attempt (numbered from 0) until it returns nil. An attempt that returns
// errFrpsPortTaken is repeated after retry(n), at most maxBindRetries times, and each repeat is
// logged with logf; any other error is returned at once, as it is.
func startWithRetries(logf func(format string, args ...any), attempt func(n int) error, retry func(n int)) error {
	for n := 0; ; n++ {
		err := attempt(n)
		if err == nil || !errors.Is(err, errFrpsPortTaken) {
			return err
		}
		if n == maxBindRetries {
			return fmt.Errorf("frps still found a port taken after %d retries: %w", maxBindRetries, err)
		}
		logf("frps's start %d found one of its ports taken (%v); picking a fresh block and starting again (retry %d of %d)",
			n+1, err, n+1, maxBindRetries)
		retry(n)
	}
}

// tryFrps is one attempt to start frps on the rig's block. It returns nil once frps's web server
// answers; errFrpsPortTaken when frps exited with a bind failure (frpsExitError: exit status 1 and
// a bind-error line in everything it wrote during this attempt, read from where the log ended
// before the start, since the log is appended to); and any other failure as an error that is not
// errFrpsPortTaken: the process exits for another reason, or its web server does not answer in 30 s.
func (r *tunnelRig) tryFrps(attempt int) error {
	r.t.Helper()
	host := r.f.host
	pw, _, err := launcher.EnsureFrpsWebPassword(host)
	if err != nil {
		r.t.Fatal(err)
	}
	cfg, err := launcher.WriteFrpsConfig(host, pw)
	if err != nil {
		r.t.Fatal(err)
	}
	logPath := launcher.FrpsLogFile(host)
	var from int64
	if fi, err := os.Stat(logPath); err == nil {
		from = fi.Size()
	}
	releaseForFrps(host)
	if beforeFrps != nil {
		beforeFrps(r.t, attempt, r.f)
	}
	proc, err := launcher.StartFrps(launcher.FrpsSpec{Bin: r.frpsBin, Config: cfg, Log: logPath, Dir: host.StateDir})
	if err != nil {
		r.t.Fatal(err)
	}
	hc := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case <-proc.Done():
			defer proc.Release() // exited: nothing to stop, and its pidfd is closed
			return frpsExitError(logFrom(logPath, from), proc.ExitStatus(),
				[]string{host.FrpsBindAddr(), host.FrpsTCPMuxAddr(), host.FrpsWebAddr()})
		default:
		}
		if resp, err := hc.Get("http://" + host.FrpsWebAddr() + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				r.frps, r.frpsDone = proc, false
				r.t.Cleanup(func() { r.stopFrpsIfRunning() })
				return nil
			}
		}
		if time.Now().After(deadline) {
			if _, err := proc.Stop(context.Background(), 5*time.Second); err != nil {
				r.t.Errorf("stop the frps whose web server never answered: %v", err)
			}
			return fmt.Errorf("frps's web server did not answer /healthz on %s within 30 s", host.FrpsWebAddr())
		}
		select {
		case <-proc.Done():
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// frpsBindErrorLine is frps 0.71.1's own report of a port it cannot bind, as a whole line of its
// output: "create server listener error, listen tcp A: bind: address already in use" for the bind
// and tcpmux ports, and the web server's "listen tcp A: bind: address already in use" for the web
// port (checked by hand with a stranger on each of the three). The match is of the whole line, so
// the phrase inside another complaint (a configuration key that spells it, a path) is not it, and
// A is the address, which the caller checks against frps's three.
var frpsBindErrorLine = regexp.MustCompile(`^(?:create server listener error, )?listen tcp (\S+): bind: address already in use$`)

// frpsBindExit is the exit status frps gives when it fails to bind (os/exec's text for status 1).
const frpsBindExit = "exit status 1"

// frpsBindFailure returns the line of out that is frps's bind error for one of the addresses in
// ports, and true, when frps ended with the exit status of a failed bind. out is everything frps
// wrote during the attempt, whole.
func frpsBindFailure(out, status string, ports []string) (string, bool) {
	if status != frpsBindExit {
		return "", false
	}
	for line := range strings.SplitSeq(out, "\n") {
		line = strings.TrimRight(line, "\r")
		if m := frpsBindErrorLine.FindStringSubmatch(line); m != nil && slices.Contains(ports, m[1]) {
			return line, true
		}
	}
	return "", false
}

// frpsExitError is the error of an attempt whose frps exited at its start: out is everything frps
// wrote during the attempt, status how it ended, ports its three addresses. It is errFrpsPortTaken
// (carrying the bind line) for a failed bind and an ordinary error otherwise. The attempt is
// classified on its whole output, and only the text of the error is cut to its last 1,500 bytes.
func frpsExitError(out, status string, ports []string) error {
	shown := out
	if len(shown) > 1500 {
		shown = shown[len(shown)-1500:]
	}
	if line, ok := frpsBindFailure(out, status, ports); ok {
		return fmt.Errorf("%w: frps exited at its start: %s: %s\n%s", errFrpsPortTaken, status, line, shown)
	}
	return fmt.Errorf("frps exited at its start: %s\n%s", status, shown)
}

// logFrom is what the file holds from offset on, "" if it cannot be read.
func logFrom(path string, offset int64) string {
	b, err := os.ReadFile(path)
	if err != nil || int64(len(b)) < offset {
		return ""
	}
	return string(b[offset:])
}

// freshBlock moves the rig to a fresh port block, held as the first was, for a start of frps that
// found a port of the old one taken. b, the binary running on the old block, is stopped and
// started again on the new one (r.start); freshBlock returns the new binary, nil if b was nil.
func (r *tunnelRig) freshBlock(b *binary) *binary {
	r.t.Helper()
	if b != nil {
		if err := b.stop(r.t); err != nil {
			r.t.Fatalf("run returned %v after its context ended, want nil", err)
		}
	}
	old := r.f
	r.tried = append(r.tried, old)
	r.f = repickBlock(r.t, old, tunnelOtherOffsets, r.tried...)
	dropOldBlock(old, r.f) // what the old block still held, except what the fresh one uses
	if b == nil {
		return nil
	}
	return r.start()
}

func (r *tunnelRig) stopFrpsIfRunning() {
	if r.frps == nil || r.frpsDone {
		return
	}
	r.frpsDone = true
	if _, err := r.frps.Stop(context.Background(), 5*time.Second); err != nil {
		r.t.Errorf("stop frps: %v", err)
	}
}

// allocation creates an Allocation in phase, with its credential file where the lifecycle
// controller keeps it, and returns it with the credential.
func (r *tunnelRig) allocation(name string, phase v1alpha1.AllocationPhase) (*v1alpha1.Allocation, string) {
	r.t.Helper()
	a := batchAllocation(name, tunnelNS, "example-provider", partX86, strings.Repeat("a", 64))
	if err := r.api.client.Create(context.Background(), a); err != nil {
		r.t.Fatal(err)
	}
	r.setAllocationPhase(a, phase)
	return a, writeCredential(r.t, r.f.host, a)
}

func (r *tunnelRig) setAllocationPhase(a *v1alpha1.Allocation, phase v1alpha1.AllocationPhase) {
	r.t.Helper()
	ctx := context.Background()
	if err := r.api.client.Get(ctx, client.ObjectKeyFromObject(a), a); err != nil {
		r.t.Fatal(err)
	}
	a.Status.AtProvider.Phase = phase
	if err := r.api.client.Status().Update(ctx, a); err != nil {
		r.t.Fatal(err)
	}
}

// instance creates an Instance placed on the named Allocation of the namespace, in phase if given.
func (r *tunnelRig) instance(name, allocation string, phase v1alpha1.InstancePhase) *v1alpha1.Instance {
	r.t.Helper()
	full := strings.Repeat("c", 64)
	in := &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tunnelNS},
		Spec: v1alpha1.InstanceSpec{
			DeploymentID: v1alpha1.FullDeploymentID(full), IdentityRecord: "id-" + full[:12],
			AllocationRef: v1alpha1.ObjectReference{Name: allocation},
			Launch: v1alpha1.Launch{
				Image: "docker.io/vllm/vllm-openai", ImageDigest: "sha256:" + strings.Repeat("b", 64),
				Command:    []string{"vllm", "serve", "/hpcburst/model", "--uds", "/hpcburst/run/worker-0.sock", "--max-num-seqs=256"},
				Checkpoint: v1alpha1.LaunchCheckpoint{Repository: "example-org/example-1b", Revision: strings.Repeat("a", 40)},
				GPUs:       1, MaxNumSeqs: 256,
			},
		},
	}
	ctx := context.Background()
	if err := r.api.client.Create(ctx, in); err != nil {
		r.t.Fatal(err)
	}
	if phase != "" {
		in.Status.Phase = phase
		if err := r.api.client.Status().Update(ctx, in); err != nil {
			r.t.Fatal(err)
		}
	}
	return in
}

// setInstancePhase sets an Instance's phase, as the lifecycle controller does when it ends one.
func (r *tunnelRig) setInstancePhase(in *v1alpha1.Instance, phase v1alpha1.InstancePhase) {
	r.t.Helper()
	ctx := context.Background()
	if err := r.api.client.Get(ctx, client.ObjectKeyFromObject(in), in); err != nil {
		r.t.Fatal(err)
	}
	in.Status.Phase = phase
	if err := r.api.client.Status().Update(ctx, in); err != nil {
		r.t.Fatal(err)
	}
}

// orphanEvents lists the TunnelOrphaned events in the API server as "action type kind name
// reportingController".
func (r *tunnelRig) orphanEvents() []string {
	r.t.Helper()
	var l eventsv1.EventList
	if err := r.api.client.List(context.Background(), &l, client.InNamespace(tunnelNS)); err != nil {
		r.t.Fatal(err)
	}
	var out []string
	for _, e := range l.Items {
		if e.Reason == v1alpha1.EventReasonTunnelOrphaned {
			out = append(out, fmt.Sprintf("%s %s %s %s %s", e.Action, e.Type, e.Regarding.Kind, e.Regarding.Name, e.ReportingController))
		}
	}
	return out
}

// markSettled returns a position in the watch's history once the watch has delivered every
// Instance's stored state. The watch runs behind the writes: a mark taken as soon as a poll sees a
// state would put that state's own event after the mark (a failure seen once in 15 loaded runs).
// The Instances must not be written meanwhile, which is so wherever this is used: the condition
// is stable, or the binary is down.
func (r *tunnelRig) markSettled(ins ...*v1alpha1.Instance) int {
	r.t.Helper()
	for _, in := range ins {
		_, _, rv := r.stored(in)
		waitUntil(r.t, 30*time.Second, "the watch to deliver "+in.Name+"'s stored state, resourceVersion "+rv, func() bool { return r.watch.lastRV(in.Name) == rv })
	}
	return r.watch.mark()
}

// stored reads the Instance's TunnelConnected condition from the API server, "" "" for none, with
// the resourceVersion.
func (r *tunnelRig) stored(in *v1alpha1.Instance) (status, reason, rv string) {
	r.t.Helper()
	got := &v1alpha1.Instance{}
	if err := r.api.client.Get(context.Background(), client.ObjectKeyFromObject(in), got); err != nil {
		r.t.Fatal(err)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionTunnelConnected); c != nil {
		return string(c.Status), c.Reason, got.ResourceVersion
	}
	return "", "", got.ResourceVersion
}

// waitCond waits, while the binary b runs, until the Instance's stored condition is status/reason.
func (r *tunnelRig) waitCond(b *binary, in *v1alpha1.Instance, status, reason string) {
	r.t.Helper()
	b.eventually(r.t, fmt.Sprintf("%s's TunnelConnected is %s/%s", in.Name, status, reason), 120*time.Second, func() bool {
		s, rs, _ := r.stored(in)
		return s == status && rs == reason
	})
}

// frpcProc is one running frpc.
type frpcProc struct {
	r    *tunnelRig
	log  string
	kill func()
}

// runFrpc starts an frpc as the agent would (HBEP-6, frpc in the job; plain tcp, since the front's
// wss listener is not here): the credential in metadatas.token, one tcpmux proxy per name. Its
// files are its own, so that several run at once.
func (r *tunnelRig) runFrpc(token string, names ...string) *frpcProc {
	r.t.Helper()
	r.nfrpc++
	dir := filepath.Join(r.f.host.StateDir, fmt.Sprintf("frpc-%d", r.nfrpc))
	if err := os.Mkdir(dir, 0o700); err != nil {
		r.t.Fatal(err)
	}
	logFile, cfgFile := filepath.Join(dir, "frpc.log"), filepath.Join(dir, "frpc.toml")
	sock := filepath.Join(dir, "engine.sock") // nothing listens: no work connection is made
	var b strings.Builder
	fmt.Fprintf(&b, `serverAddr = "127.0.0.1"
serverPort = %d
loginFailExit = false
log.to = %q
metadatas.token = %q
transport.protocol = "tcp"
transport.poolCount = 0
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatInterval = 10
transport.heartbeatTimeout = 25
`, r.f.host.FrpsBindPort(), logFile, token)
	for _, name := range names {
		fmt.Fprintf(&b, `
[[proxies]]
name = %q
type = "tcpmux"
multiplexer = "httpconnect"
customDomains = [%q]
[proxies.plugin]
type = "unix_domain_socket"
unixPath = %q
`, name, name, sock)
	}
	if err := os.WriteFile(cfgFile, []byte(b.String()), 0o600); err != nil {
		r.t.Fatal(err)
	}
	out, err := os.Create(cfgFile + ".out")
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { out.Close() })
	cmd := exec.Command(r.frpcPath, "-c", cfgFile)
	cmd.Env = []string{"PATH=/usr/bin:/bin"} // frp reads http_proxy for its connection to frps otherwise
	cmd.Stdout, cmd.Stderr = out, out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	killed := false
	p := &frpcProc{r: r, log: logFile}
	p.kill = func() {
		if killed {
			return
		}
		killed = true
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			r.t.Errorf("frpc (pid %d) did not exit after SIGKILL", cmd.Process.Pid)
		}
	}
	r.t.Cleanup(p.kill)
	return p
}

func (p *frpcProc) logText() string {
	b, _ := os.ReadFile(p.log)
	return string(b)
}

// refusedLine reports whether frpc's log has a line naming the proxy and the plugin's reason.
func (p *frpcProc) refusedLine(name, reason string) bool {
	for _, l := range strings.Split(p.logText(), "\n") {
		if strings.Contains(l, name) && strings.Contains(l, reason) {
			return true
		}
	}
	return false
}

// proxyList is frps's own list of tcpmux proxies, by name, with the status each has, read with the
// controller's reader and the web password the launcher wrote.
func (r *tunnelRig) proxyList() (map[string]string, error) {
	rd, err := frpsplugin.NewHostReader(r.f.host)
	if err != nil {
		r.t.Fatal(err)
	}
	list, err := rd.List(context.Background())
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, p := range list {
		out[p.Name] = p.Status
	}
	return out, nil
}

// pluginRefusesLogin posts a Login for the token to the plugin the binary mounted, as frps does.
func (r *tunnelRig) pluginRefusesLogin(token string) bool {
	r.t.Helper()
	body, err := json.Marshal(map[string]any{"version": "0.1.0", "op": "Login", "content": map[string]any{"metas": map[string]string{"token": token}}})
	if err != nil {
		r.t.Fatal(err)
	}
	hc := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, err := hc.Post("http://"+r.f.host.FrpsPluginAddr()+agentapi.FrpsPluginPath+"?op=Login", "application/json", bytes.NewReader(body))
	if err != nil {
		r.t.Fatalf("plugin Login: %v", err)
	}
	defer resp.Body.Close()
	var a struct {
		Reject bool `json:"reject"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		r.t.Fatalf("plugin Login: %v", err)
	}
	return a.Reject
}

// start runs a copy of the binary. Each copy has its own metrics registry: the collectors' names
// are registered once per registry, and a test that runs two copies in one process (one after the
// other, as the restart does) would fail on the second with controller-runtime's.
func (r *tunnelRig) start() *binary {
	return launchBinary(r.t, r.f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry()})
}

// TestTunnelWithRealFrpsAndFrpc is the path end to end: what the plugin refuses, what each state of
// frps and frpc does to each Instance's condition, and what a restart of the binary keeps.
func TestTunnelWithRealFrpsAndFrpc(t *testing.T) {
	setVars(t, &tunnelReadPeriod, 2*time.Second) // HBEP-6's 10 s, shortened; the trigger alone is the next test
	// TunnelOrphanGrace's 2 minutes, shortened to a few reads: the grace's own behaviour is tested
	// on pathState's clock (tunnel_orphan_test.go), and this test keeps the package's time.
	setVars(t, &tunnelOrphanGrace, 5*time.Second)
	r := newTunnelRig(t)
	from := len(logs.String())
	host := r.f.host

	// Two live Allocations and their Instances; one Instance of each kind that must never be
	// admitted or written: one that has ended.
	allocA, credA := r.allocation("al-a", v1alpha1.AllocationRunning)
	allocB, credB := r.allocation("al-b", v1alpha1.AllocationRunning)
	a1 := r.instance("in-a1", allocA.Name, "")
	a2 := r.instance("in-a2", allocA.Name, v1alpha1.InstanceStarting)
	b1 := r.instance("in-b1", allocB.Name, v1alpha1.InstanceReady)
	e1 := r.instance("in-e1", allocA.Name, v1alpha1.InstanceEnded)
	nobody := "00000000-0000-4000-8000-00000000dead" // the UID of no Instance

	// frps is not running: the binary starts and stays up, the plugin listens, and every Instance's
	// condition is Unknown, since the list cannot be read (HBEP-6: never False, never True).
	b := r.start()
	b.eventually(t, "the plugin endpoint is bound", 60*time.Second, func() bool { return heldAddrs.wasTaken(host.FrpsPluginAddr()) })
	for _, in := range []*v1alpha1.Instance{a1, a2, b1} {
		r.waitCond(b, in, statusUnknown, reasonUnreadable)
	}
	if s, rs, _ := r.stored(e1); s != "" || rs != "" {
		t.Errorf("the ended Instance has the condition %s/%s, want none", s, rs)
	}

	// frps comes up, with no proxy: the list reads, and every Instance is offline. A start that
	// found a port taken has moved the rig to a fresh block and started the binary on it again,
	// so b and the host are read again.
	b = r.startFrps(b)
	host = r.f.host
	for _, in := range []*v1alpha1.Instance{a1, a2, b1} {
		r.waitCond(b, in, statusFalse, reasonOffline)
	}

	// Refusals, through the plugin the binary mounted, with the valid proxy registering beside them
	// (the positive control: the refusals are not frps being down).
	//   - frpc A presents Allocation A's credential. Its proxy for a1 is placed on A and registers;
	//     the proxies named by b1 (an Instance of Allocation B), by e1 (an Instance of A that has
	//     ended) and by a UID nobody has are refused at NewProxy.
	frpcA := r.runFrpc(credA, uid(a1), uid(b1), uid(e1), nobody)
	r.waitCond(b, a1, statusTrue, reasonOnline)
	for _, refused := range []struct{ what, name string }{{"b1, another Allocation's Instance", uid(b1)}, {"e1, an ended Instance", uid(e1)}, {"a UID nobody has", nobody}} {
		b.eventually(t, "frpc's log shows the plugin's reason for refusing the proxy of "+refused.what, 60*time.Second,
			func() bool { return frpcA.refusedLine(refused.name, refusedNotPlaced) })
	}
	list, err := r.proxyList()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[uid(a1)] != "online" {
		t.Errorf("frps lists %v, want only %s online", list, a1.Name)
	}
	if s, rs, _ := r.stored(b1); s != statusFalse || rs != reasonOffline {
		t.Errorf("b1 is %s/%s although its proxy was refused, want False/ProxyOffline", s, rs)
	}

	//   - a token that is a key's form but no Allocation's: refused at Login, so nothing registers.
	wrong, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	frpcWrong := r.runFrpc(wrong, uid(a2))
	b.eventually(t, "frpc's log shows the plugin's reason for refusing the login of a wrong token", 60*time.Second,
		func() bool { return strings.Contains(frpcWrong.logText(), refusedCredential) })
	if list, err := r.proxyList(); err != nil || list[uid(a2)] != "" {
		t.Errorf("frps lists %v (%v) after a login with a wrong token, want no proxy for a2", list, err)
	}
	if s, rs, _ := r.stored(a2); s != statusFalse || rs != reasonOffline {
		t.Errorf("a2 is %s/%s after a login with a wrong token, want False/ProxyOffline", s, rs)
	}
	frpcWrong.kill()

	//   - an Allocation that has ended. B's credential works while B runs (b1 registers: the control),
	//     and after B has ended the same credential is refused. The wait for the plugin's own
	//     refusal comes before the new frpc starts, so that the refusal is of the ended Allocation
	//     and not a race with the index.
	frpcB := r.runFrpc(credB, uid(b1))
	r.waitCond(b, b1, statusTrue, reasonOnline)
	r.setAllocationPhase(allocB, v1alpha1.AllocationEnded)
	b.eventually(t, "the plugin refuses the credential of the Allocation that ended", 60*time.Second, func() bool { return r.pluginRefusesLogin(credB) })
	frpcB.kill()
	r.waitCond(b, b1, statusFalse, reasonOffline)
	endedAt := r.markSettled(b1)
	frpcB2 := r.runFrpc(credB, uid(b1))
	b.eventually(t, "frpc's log shows the plugin's reason for refusing the ended Allocation's token", 60*time.Second,
		func() bool { return strings.Contains(frpcB2.logText(), refusedCredential) })
	if list, err := r.proxyList(); err != nil || list[uid(b1)] == "online" {
		t.Errorf("frps lists %v (%v) after a login with an ended Allocation's token, want b1 not online", list, err)
	}
	if got := r.watch.conds(b1.Name, endedAt); len(got) != 0 {
		t.Errorf("b1's condition went %v after its Allocation ended and its frpc was refused, want no change", got)
	}
	frpcB2.kill()

	// A killed frpc is False/ProxyOffline; a new one is True again. A frpc serving two proxies
	// of its Allocation gets both online.
	frpcA.kill()
	r.waitCond(b, a1, statusFalse, reasonOffline)
	frpcA2 := r.runFrpc(credA, uid(a1), uid(a2))
	r.waitCond(b, a1, statusTrue, reasonOnline)
	r.waitCond(b, a2, statusTrue, reasonOnline)

	// A restart of the binary: the stop writes nothing (a1 is still True), frpc is killed while the
	// controller is down, and the new copy rebuilds from frps's list, so a1 goes False. Nothing else
	// is written first: no Unknown, and no True re-asserted from what was stored.
	if err := b.stop(t); err != nil {
		t.Fatalf("run returned %v after its context ended, want nil", err)
	}
	if s, rs, _ := r.stored(a1); s != statusTrue || rs != reasonOnline {
		t.Fatalf("after the stop a1 is %s/%s, want the True/ProxyOnline it had", s, rs)
	}
	frpcA2.kill()
	waitUntil(t, 60*time.Second, "frps to list a1 as offline", func() bool {
		l, err := r.proxyList()
		return err == nil && l[uid(a1)] == "offline"
	})
	if s, rs, _ := r.stored(a1); s != statusTrue || rs != reasonOnline {
		t.Fatalf("with the controller down a1 is %s/%s, want the stored True/ProxyOnline", s, rs)
	}
	restart := r.markSettled(a1, a2)
	b = r.start()
	for _, in := range []*v1alpha1.Instance{a1, a2} {
		r.waitCond(b, in, statusFalse, reasonOffline)
		if got := r.watch.conds(in.Name, restart); len(got) != 1 || got[0] != statusFalse+"/"+reasonOffline {
			t.Errorf("%s went %v after the restart, want exactly [False/ProxyOffline]: rebuilt from the list", in.Name, got)
		}
	}
	// The plugin is back: an frpc that logs in now registers.
	frpcA3 := r.runFrpc(credA, uid(a1))
	r.waitCond(b, a1, statusTrue, reasonOnline)
	_ = frpcA3

	// An orphan, through the whole binary (HBEP-6, The inventory; HBEP-8, Reasons in alpha): a3's
	// proxy registers through Allocation A's credential, then a3 ends while its frpc stays up, so
	// frps lists a proxy that is no current Instance's UID. The lifecycle controller's recorder
	// raises a degraded TunnelOrphaned regarding Allocation al-a, the one whose credential
	// admitted it. The test reads at least one event with its fields and no count: HBEP-8 says an
	// API-level test never counts events, since a repeat folds into a series on one object. The
	// hourly gate is pathState's, tested on its own clock (tunnel_orphan_test.go).
	a3 := r.instance("in-a3", allocA.Name, v1alpha1.InstanceStarting)
	r.runFrpc(credA, uid(a3))
	r.waitCond(b, a3, statusTrue, reasonOnline)
	if got := r.orphanEvents(); len(got) != 0 {
		t.Fatalf("TunnelOrphaned events %v while a3 lives, want none", got)
	}
	r.setInstancePhase(a3, v1alpha1.InstanceEnded)
	b.eventually(t, "the TunnelOrphaned event for a3's proxy", 60*time.Second, func() bool { return len(r.orphanEvents()) > 0 })
	for _, got := range r.orphanEvents() {
		if want := "degraded Warning Allocation al-a hpcburst-lifecycle"; got != want {
			t.Fatalf("a TunnelOrphaned event is %q, want %q (action, type, regarding, reporting controller)", got, want)
		}
	}

	// frps stops while a1 is True: Unknown, never False, and no write at all while it stays down.
	stopMark := r.markSettled(a1, a2)
	reads := tunnelReads.Load()
	r.stopFrpsIfRunning()
	r.waitCond(b, a1, statusUnknown, reasonUnreadable)
	r.waitCond(b, a2, statusUnknown, reasonUnreadable)
	b.eventually(t, "three more reads of the unreadable list", 60*time.Second, func() bool { return tunnelReads.Load() >= reads+3 })
	for _, in := range []*v1alpha1.Instance{a1, a2} {
		// Exactly one change since frps stopped, to Unknown: never False and never True meanwhile,
		// and no second write for the reads that followed, whose messages differ in the error's text.
		if got := r.watch.conds(in.Name, stopMark); len(got) != 1 || got[0] != statusUnknown+"/"+reasonUnreadable {
			t.Errorf("%s went %v after frps stopped, want exactly [Unknown/ProxyListUnreadable]", in.Name, got)
		}
		evs := r.watch.events(in.Name, stopMark)
		if len(evs) != 1 {
			t.Errorf("%s was written %d times after frps stopped (%+v), want once", in.Name, len(evs), evs)
		} else if _, _, rv := r.stored(in); rv != evs[0].rv {
			t.Errorf("%s's resourceVersion moved from %s to %s over reads that changed nothing", in.Name, evs[0].rv, rv)
		}
	}
	if s, rs, _ := r.stored(e1); s != "" || rs != "" {
		t.Errorf("the ended Instance has the condition %s/%s at the end, want none", s, rs)
	}

	if err := b.stop(t); err != nil {
		t.Fatalf("run returned %v after its context ended, want nil", err)
	}
	out := logs.String()[from:]
	for _, name := range []string{"frps-plugin", "tunnel-state"} {
		for _, msg := range []string{"component starting", "component stopped"} {
			if n := strings.Count(out, componentLine(msg, host.PortBase, name)); n != 2 {
				t.Errorf("the log has %d %q lines for %s, want 2 (the two runs)", n, msg, name)
			}
		}
	}
	if listening(host.FrpsPluginAddr()) {
		t.Errorf("the plugin endpoint %s still listens after the stop", host.FrpsPluginAddr())
	}
}

// With the read period an hour long, only the plugin's signal can make the writer read after a
// registration. The state tests above would pass on a broken trigger, since the periodic read
// covers it.
func TestTunnelTriggerCausesARead(t *testing.T) {
	setVars(t, &tunnelReadPeriod, time.Hour)
	r := newTunnelRig(t)
	allocA, credA := r.allocation("al-a", v1alpha1.AllocationRunning)
	a1 := r.instance("in-a1", allocA.Name, "")
	r.startFrps(nil) // before the binary, so that its read at the start reads the list
	before := tunnelReads.Load()
	b := r.start()
	// The read at the start says offline. Only now does frpc start, so that the start's read
	// cannot be what turns the condition True.
	//
	// tunnelReads counts a read once it has written its conditions, so the counter moves a moment
	// AFTER the condition a read wrote is visible: the baseline is taken only once the start's read
	// is counted, and after True the test waits for the trigger's read to be counted, not one load
	// (CI caught a load that came between the write and the count, 1 Nov 2026).
	r.waitCond(b, a1, statusFalse, reasonOffline)
	b.eventually(t, "the start's read is counted", 60*time.Second, func() bool { return tunnelReads.Load() >= before+1 })
	reads := tunnelReads.Load()
	r.runFrpc(credA, uid(a1))
	r.waitCond(b, a1, statusTrue, reasonOnline)
	b.eventually(t, "the read that turned the condition True is counted", 60*time.Second, func() bool { return tunnelReads.Load() >= reads+1 })
	if err := b.stop(t); err != nil {
		t.Fatalf("run returned %v after its context ended, want nil", err)
	}
}
