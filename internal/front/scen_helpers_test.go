package front

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/balancer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The helpers of the HBEP-5 scenario tests (Deadlines, Drain, Choice, Bound
// and cap, Fault injection, Managed source). Their names start with sc so
// they cannot meet another file's.

const (
	scChat = "/v1/chat/completions"
	// scModel is the model name clients send and the ModelDeployment's
	// spec.model.
	scModel = "example-27b"
	// scDep is the served ModelDeployment's key, namespace/name.
	scDep = "ns/md"
)

// scOpts shape a scFix: one served ModelDeployment "md" of scModel on the
// batch Provider helios-a, with instances Instances md-0, md-1 and so on,
// each on its own engine double.
type scOpts struct {
	instances     int   // Instances created; default 1
	desired       int32 // spec.instances; default instances
	cfg           func(*Config)
	inst          func(i int, o *instanceOpts)
	md            func(md *v1alpha1.ModelDeployment)
	profileWindow string // the profile's drainWindow; "" keeps the default (2 h 10 min)
	// realClock builds the front on the wall clock (newHarnessReal); its
	// engines are then on the wall clock too.
	realClock bool
	// engineReal puts the engines of a fake-clock front on the wall clock, so
	// the test alone moves the fake clock (see scLockstep).
	engineReal bool
}

// scFix is a front with n Instances of one ModelDeployment.
type scFix struct {
	*harness
	engines []*enginetest.Engine
	uids    []types.UID
	names   []string // "ns/md-<i>": the Instances' names in the balancer and on the ring
}

func newScFix(t *testing.T, o scOpts) *scFix {
	t.Helper()
	if o.instances == 0 {
		o.instances = 1
	}
	if o.desired == 0 {
		o.desired = int32(o.instances)
	}
	var h *harness
	if o.realClock {
		h = newHarnessReal(t, o.cfg)
	} else {
		h = newHarness(t, o.cfg)
	}
	// An engine on the fake clock moves it between tokens. The interface is
	// left nil, not a nil *clock.Fake, when the engine runs on the wall clock.
	var engineClock clock.Clock
	if h.clk != nil && !o.engineReal {
		engineClock = h.clk
	}
	f := &scFix{harness: h}
	h.profile("helios-gh200", v1alpha1.PlatformBatch, o.profileWindow, "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", scModel, o.desired, "helios-a")
	if o.md != nil {
		o.md(h.md("md"))
	}
	for i := 0; i < o.instances; i++ {
		key := newKey(t)
		e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: engineClock})
		var iopts instanceOpts
		if o.inst != nil {
			o.inst(i, &iopts)
		}
		name := fmt.Sprintf("md-%d", i)
		f.engines = append(f.engines, e)
		f.uids = append(f.uids, h.instance("md", name, "helios-a", e, key, iopts))
		f.names = append(f.names, "ns/"+name)
	}
	h.apply()
	return f
}

// scNames are the names newScFix gives n Instances.
func scNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("ns/md-%d", i)
	}
	return out
}

// setAll sets the behaviour of every engine.
func (f *scFix) setAll(b enginetest.Behaviour) {
	for _, e := range f.engines {
		e.Set(b)
	}
}

// arrived is how many requests have reached the engines in all.
func (f *scFix) arrived() int {
	n := 0
	for _, e := range f.engines {
		n += len(e.Requests())
	}
	return n
}

// counts is how many requests each engine has seen.
func (f *scFix) counts() []int {
	out := make([]int, len(f.engines))
	for i, e := range f.engines {
		out[i] = len(e.Requests())
	}
	return out
}

// indexOf is the position of an Instance name in f.names.
func (f *scFix) indexOf(name string) int {
	for i, n := range f.names {
		if n == name {
			return i
		}
	}
	f.t.Fatalf("no Instance %q among %v", name, f.names)
	return -1
}

// scWalk is the order in which a ring over names visits them from key's
// position: the key's home first (balancer.NewRing, KubeAI's ring).
func scWalk(names []string, key string) []string { return balancer.NewRing(names).Walk(key) }

// scKeys returns n distinct session keys whose home on a ring of names is
// home.
func scKeys(t *testing.T, names []string, home string, n int) []string {
	t.Helper()
	ring := balancer.NewRing(names)
	var out []string
	for i := 0; len(out) < n && i < 100000; i++ {
		if k := fmt.Sprintf("conv-%d", i); ring.Home(k) == home {
			out = append(out, k)
		}
	}
	if len(out) < n {
		t.Fatalf("found only %d session keys homed on %s", len(out), home)
	}
	return out
}

// scGroup returns the balancer group of a served ModelDeployment.
func (h *harness) scGroup(dep string) *balancer.Group {
	h.t.Helper()
	h.router.mu.Lock()
	defer h.router.mu.Unlock()
	g := h.router.groups[dep]
	if g == nil {
		h.t.Fatalf("no balancer group for %s", dep)
	}
	return g
}

// scLoad is the in-flight count a group holds for one Instance.
func (h *harness) scLoad(dep, inst string) int64 {
	h.t.Helper()
	n, ok := h.scGroup(dep).InFlight(inst)
	if !ok {
		h.t.Fatalf("%s is not a member of %s", inst, dep)
	}
	return n
}

// scNoDrift fails the test if the front reported that its counts disagreed.
func (h *harness) scNoDrift() {
	h.t.Helper()
	if ev := h.rec.Find("degraded", "CountsDrift"); len(ev) != 0 {
		h.t.Errorf("the front reported a counts drift: %+v", ev)
	}
}

// scCall is a chat request running in its own goroutine. It never calls
// t.Fatal, which only the test's goroutine may.
type scCall struct {
	done chan struct{}
	resp *http.Response
	body string
	err  error
}

// scGo posts body to the chat endpoint with the client key and the extra
// headers, on a client that never reuses a connection (errClient).
func scGo(h *harness, body string, hdr ...string) *scCall {
	c := &scCall{done: make(chan struct{})}
	go func() {
		defer close(c.done)
		req, err := http.NewRequest(http.MethodPost, h.url+scChat, strings.NewReader(body))
		if err != nil {
			c.err = err
			return
		}
		req.Header.Set("Authorization", "Bearer "+h.clientKey)
		req.Header.Set("Content-Type", "application/json")
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		resp, err := errClient.Do(req)
		if err != nil {
			c.err = err
			return
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		c.resp, c.body = resp, string(b)
		if err != nil && err != io.ErrUnexpectedEOF {
			c.err = err
		}
	}()
	return c
}

// wait returns the response and its whole body.
func (c *scCall) wait(t *testing.T) (*http.Response, string) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(30 * time.Second):
		t.Fatal("a request did not finish within 30 s")
	}
	if c.err != nil {
		t.Fatalf("request: %v", c.err)
	}
	return c.resp, c.body
}

// scChatOnce sends one non-streamed chat request for scModel under the
// session key and returns its response and body.
func scChatOnce(t *testing.T, h *harness, session string) (*http.Response, string) {
	t.Helper()
	return scGo(h, errChatBody(scModel, false, "hello there"), "x-session-id", session).wait(t)
}

// scInstanceOf is the attribution header naming the Instance that answered.
func scInstanceOf(resp *http.Response) string { return resp.Header.Get("hpcburst-instance") }

// scEmitterOf is the attribution header naming who answered.
func scEmitterOf(resp *http.Response) string { return resp.Header.Get("hpcburst-emitter") }

// scReadEvent reads one SSE event (lines up to a blank line). At the end of
// the stream it returns what it has with the error.
func scReadEvent(br *bufio.Reader) (string, error) {
	var ev strings.Builder
	for {
		line, err := br.ReadString('\n')
		ev.WriteString(line)
		if err != nil {
			return ev.String(), err
		}
		if strings.TrimRight(line, "\r\n") == "" {
			return ev.String(), nil
		}
	}
}

// scIsContent reports whether an engine double's event carries a token; its
// content is "t0 ", "t1 " and so on, and the finish and usage chunks have none.
func scIsContent(ev string) bool { return strings.Contains(ev, `"content":"t`) }

// scContentEvents counts the token events in a stream's body.
func scContentEvents(body string) int {
	n := 0
	for _, e := range splitEvents(body) {
		if scIsContent(e) {
			n++
		}
	}
	return n
}

// scGoAheadLimit bounds scGated's wait for the engine to take one go-ahead.
// The engine reaches its gate as soon as it has written the token the client
// just read, so the bound is generous.
const scGoAheadLimit = 30 * time.Second

// scGated reads a stream event by event and, after each of the first n token
// events, sends the engine its go-ahead for the next token
// (enginetest.Behaviour.Gate). The client has then read the token, so the
// front has read it from the engine and reset its idle timer before the engine
// paces the next one: an engine on the fake clock never moves the clock ahead
// of the front's reads, whatever the speed of the machine. It returns the
// whole body.
func scGated(t *testing.T, resp *http.Response, gate chan<- struct{}, n int) string {
	t.Helper()
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	var body strings.Builder
	for sent := 0; ; {
		ev, err := scReadEvent(br)
		body.WriteString(ev)
		if scIsContent(ev) && sent < n {
			select {
			case gate <- struct{}{}:
				sent++
			case <-time.After(scGoAheadLimit):
				t.Fatalf("the engine did not take go-ahead %d of %d within %s; the body so far ends %q",
					sent+1, n, scGoAheadLimit, scTail(body.String()))
			}
		}
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				t.Fatalf("reading the stream after %d go-aheads: %v", sent, err)
			}
			return body.String()
		}
	}
}

// scLockstepGated is scLockstep for an engine that waits for a go-ahead
// before each token after the first (enginetest.Behaviour.Gate): after each
// token event it moves the fake clock by step, until it has moved it by
// total, and then, while it has not, sends the engine its go-ahead. The
// engine writes a token only after the client has read the one before and the
// clock has moved, so every move of the clock comes after the front's read of
// the token before it, and the front's idle timer is reset between any two
// moves. Without the gate an engine on the wall clock ran ahead: several
// buffered tokens reached the client after one read of the front, and the
// test's moves added up to the idle time-out. After the last move the engine
// is left waiting at its gate until the request ends. It returns the number of
// token events read and the whole body.
func scLockstepGated(t *testing.T, h *harness, resp *http.Response, gate chan<- struct{}, step, total time.Duration) (int, string) {
	t.Helper()
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	var body strings.Builder
	content := 0
	for moved := time.Duration(0); ; {
		ev, err := scReadEvent(br)
		body.WriteString(ev)
		if scIsContent(ev) {
			content++
			if moved < total {
				h.clk.Advance(step)
				moved += step
			}
			if moved < total {
				select {
				case gate <- struct{}{}:
				case <-time.After(scGoAheadLimit):
					t.Fatalf("the engine did not take its go-ahead after token event %d (clock moved by %v) within %s; the body so far ends %q",
						content, moved, scGoAheadLimit, scTail(body.String()))
				}
			}
		}
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				t.Fatalf("reading the stream: %v", err)
			}
			return content, body.String()
		}
	}
}

// scTail is the end of a body, for a failure message.
func scTail(s string) string {
	if len(s) > 300 {
		return s[len(s)-300:]
	}
	return s
}

// scLockstep reads a stream event by event and, after each token event, moves
// the fake clock by step, until the test has moved it by total. The engine of
// such a test runs on the wall clock (scOpts.engineReal), so the test is the
// only caller of Advance: the front's timers fall due exactly when the clock
// reaches them, whatever the speed of the machine, and the fake time a request
// spent is known by hand. Advance is never called by two goroutines, which
// could make the clock run backwards. It returns the number of token events
// read and the whole body.
func scLockstep(t *testing.T, h *harness, resp *http.Response, step, total time.Duration) (int, string) {
	t.Helper()
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	var body strings.Builder
	content := 0
	for moved := time.Duration(0); ; {
		ev, err := scReadEvent(br)
		body.WriteString(ev)
		if scIsContent(ev) {
			content++
			if moved < total {
				h.clk.Advance(step)
				moved += step
			}
		}
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				t.Fatalf("reading the stream: %v", err)
			}
			return content, body.String()
		}
	}
}

// scOneCut fails the test unless the body holds exactly one stream_cut event:
// checkStreamCut only looks at the last event, so a second one earlier in the
// stream would pass it.
func scOneCut(t *testing.T, body string) {
	t.Helper()
	if n := strings.Count(body, `"code":"stream_cut"`); n != 1 {
		t.Errorf("the stream holds %d stream_cut events, want exactly one", n)
	}
}
