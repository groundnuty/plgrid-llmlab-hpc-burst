package front

// A mass re-login after the router's rebuilds are coalesced (#417): S16 (#412) found that after an frps or a front
// restart every tunnel is back within seconds, but the router rebuilt its whole view once per registration change
// under the lock routing needs, so at 300 and 600 tunnels the front's own probe reported its passes up to 10 s late and
// the first token of each tunnel's first request came 10-37 s after the cut. Here the front runs on the wall clock over
// the Tunnel to a tcpmux double that hands each tunnel's requests to one engine double, in S16's fixture shape (one
// served deployment of one Instance per tunnel, one model each). Every tunnel re-registers at once, as frps's plugin
// hears it on a restart; the re-probes run; a client per tunnel retries its model until a streamed request gets its
// first token.

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The bounds of the re-login, each well past what one coalesced rebuild costs at 600 tunnels (S16 measured 79 ms for
// one on the shared VM) and far below what one rebuild per change costs (600 in a row is 47 s there).
const (
	// reloginProbeBound: every re-probe's pass is reported this soon after the re-registrations.
	reloginProbeBound = 5 * time.Second
	// reloginTokenBound: every tunnel's first token comes this soon after its probe passed.
	reloginTokenBound = 5 * time.Second
	// reloginClientRetry is how long a client waits before it asks again after a refusal (S16's client retried every
	// 50-500 ms).
	reloginClientRetry = 100 * time.Millisecond
)

// passTimes records when each UID's probe first passed after it is armed.
type passTimes struct {
	mu    sync.Mutex
	armed bool
	at    map[types.UID]time.Time
}

func (p *passTimes) Probed(r ProbeResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.armed || !r.Passed || !r.Registered {
		return
	}
	if _, ok := p.at[r.UID]; !ok {
		p.at[r.UID] = time.Now()
	}
}

func (p *passTimes) PathProbeFailing(*v1alpha1.Instance, ProbeResult) {}

func (p *passTimes) arm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed, p.at = true, map[types.UID]time.Time{}
}

func (p *passTimes) of(u types.UID) (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.at[u]
	return t, ok
}

// newMuxFront is a front on the wall clock whose router and proxy run over the Tunnel to a forwarding tcpmux double,
// with n served deployments of one ready Instance each (model m-<i>), all sharing one Instance key and one engine
// double, and every one registered by a first probe round.
func newMuxFront(t *testing.T, n int) (*tunFront, []types.UID, *passTimes) {
	t.Helper()
	h := newHarnessReal(t, nil)
	key := newKey(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	engine := enginetest.Serve(t, ln, enginetest.Options{Key: key})
	m := startMuxDouble(t)
	m.forwardTo(engine.Addr())
	f := &tunFront{harness: h}
	f.log = slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	f.tun = NewTunnel(m.ln.Addr().String(), h.cfg.TunnelSetupTimeout.D())
	keys, err := ReadClientKeys(h.cfg.ClientKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	h.router = NewRouter(h.cfg, clock.Real{}, f.tun, h.rec, f.log, h.metrics)
	h.proxy = NewProxy(ProxyOptions{Config: h.cfg, Host: h.host, Clock: clock.Real{}, Router: h.router, Transports: f.tun, Keys: keys, Sink: h.sink, Metrics: h.metrics, Log: f.log})
	cln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := clientServer(h.cfg, h.proxy, h.base)
	go func() { _ = srv.Serve(cln) }()
	t.Cleanup(func() { _ = srv.Close() })
	h.url = "http://" + cln.Addr().String()
	passes := &passTimes{}
	f.prober = NewProber(h.cfg, f.tun, clock.Real{}, func() []v1alpha1.Instance { return h.snap.Instances }, passes, f.log)

	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	uids := make([]types.UID, n)
	for i := range uids {
		md := fmt.Sprintf("md-%d", i)
		h.servedMD(md, fmt.Sprintf("m-%d", i), 1, "helios-a")
		uids[i] = h.instance(md, md+"-0", "helios-a", nil, key, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
		in := h.inst(uids[i])
		in.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionTrue,
			Reason: v1alpha1.ReasonProxyOnline, LastTransitionTime: metav1.Now()}}
		m.set(uids[i], http.StatusOK)
	}
	h.apply()
	f.prober.Round(context.Background())
	waitRouterState(t, "every tunnel to be registered and routable", func() bool {
		for _, u := range uids {
			if _, eligible, _ := f.routing(u); !eligible {
				return false
			}
		}
		return true
	})
	return f, uids, passes
}

// firstToken sends model's streamed request until one gets a 200 and its first data line, or the deadline passes, and
// returns when the line came.
func firstToken(ctx context.Context, client *http.Client, url, clientKey, model string) (time.Time, error) {
	body := `{"model":"` + model + `","stream":true,"max_tokens":4,"messages":[{"role":"user","content":"hello there"}]}`
	var last string
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
		if err != nil {
			return time.Time{}, err
		}
		req.Header.Set("Authorization", "Bearer "+clientKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			sc := bufio.NewScanner(resp.Body)
			for sc.Scan() {
				if strings.HasPrefix(sc.Text(), "data: ") {
					at := time.Now()
					resp.Body.Close()
					return at, nil
				}
			}
			resp.Body.Close()
			last = "a 200 with no data line"
		} else if err == nil {
			last = resp.Status
			resp.Body.Close()
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
		case <-time.After(reloginClientRetry):
		}
	}
	return time.Time{}, fmt.Errorf("no first token before the deadline (last: %s)", last)
}

// TestAMassReloginRoutesAndStreamsWithinSecondsOfTheProbe at 300 and 600 tunnels: every re-probe's pass is reported
// within reloginProbeBound of the re-registrations, and every tunnel's first token comes within reloginTokenBound of
// its own pass. With one rebuild per registration change, the passes alone take tens of seconds at 600.
func TestAMassReloginRoutesAndStreamsWithinSecondsOfTheProbe(t *testing.T) {
	for _, n := range []int{300, 600} {
		t.Run(fmt.Sprintf("%d tunnels", n), func(t *testing.T) { massRelogin(t, n) })
	}
}

func massRelogin(t *testing.T, n int) {
	f, uids, passes := newMuxFront(t, n)
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: n, MaxConnsPerHost: 0}}
	t.Cleanup(client.CloseIdleConnections)

	passes.arm()
	cut := time.Now()
	for _, u := range uids {
		f.tun.Bump(u)
		f.prober.Reregistered(u)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tokens := make([]time.Time, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range uids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tokens[i], errs[i] = firstToken(ctx, client, f.url, f.clientKey, fmt.Sprintf("m-%d", i))
		}(i)
	}
	f.prober.Reprobe(context.Background())
	probed := time.Since(cut)
	wg.Wait()

	var toPass, toToken []time.Duration
	for i, u := range uids {
		p, ok := passes.of(u)
		if !ok {
			t.Fatalf("tunnel %d: no probe passed after the re-registration", i)
		}
		if errs[i] != nil {
			t.Fatalf("tunnel %d: %v", i, errs[i])
		}
		toPass = append(toPass, p.Sub(cut))
		toToken = append(toToken, tokens[i].Sub(p))
	}
	slices.Sort(toPass)
	slices.Sort(toToken)
	t.Logf("%d tunnels: the re-probes took %s; a pass after the re-registration p50 %s, max %s; the first token after its pass p50 %s, max %s",
		n, probed.Round(time.Millisecond), toPass[n/2].Round(time.Millisecond), toPass[n-1].Round(time.Millisecond),
		toToken[n/2].Round(time.Millisecond), toToken[n-1].Round(time.Millisecond))
	if toPass[n-1] > reloginProbeBound {
		t.Errorf("the last pass was reported %s after the re-registrations, want at most %s", toPass[n-1].Round(time.Millisecond), reloginProbeBound)
	}
	if toToken[n-1] > reloginTokenBound {
		t.Errorf("the slowest first token came %s after its pass, want at most %s", toToken[n-1].Round(time.Millisecond), reloginTokenBound)
	}
}
