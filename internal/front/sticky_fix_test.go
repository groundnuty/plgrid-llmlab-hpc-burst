package front

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// The fixture of the sticky-route rows (HBEP-5, Workloads, Sticky routes; HBEP-764 R9): a front with served ModelDeployments of scModel on one batch Provider
// and the Workload ns/alpha with sessionAffinity Route. Every ModelDeployment's status.currentIDs lists the deployment ID of its Instances, as the lifecycle
// controller writes it. A row sends its conversations by session key: a key's HOME is the Instance on the fork's ring over every Instance, so a request
// that lands elsewhere was moved by a decision (a spill, a pin), never by the ring alone. Every name here starts with sk.

// skOpts are the settings of an skFix. The zero value is md-a with a-0 and md-b with b-0, each at routing.instanceCap 1, and alpha with sessionAffinity
// Route, pinTTL 12 h and maxPins 100,000.
type skOpts struct {
	deps            []pwDep
	edit            func(*Config)
	noAffinity      bool   // alpha has no sessionAffinity
	pinTTL          string // alpha's pinTTL; "" is the default
	maxPins         int32  // alpha's maxPins; 0 is the default
	managed         bool   // md-llm, a managed-source ModelDeployment behind an LLMLab double, with alpha ManagedFirst and managedSessions 1
	stallManaged    bool   // the LLMLab double holds every request until unstall
	managedSessions int32  // with managed: alpha's managedSessions; default 1
	splitIDs        bool   // the Instances of a ModelDeployment have one deployment ID each, all current, as the classes of a ModelDeployment do
}

type skFix struct {
	*pwFix
	llm      *llmlabtest.Server
	splitIDs bool
}

const skAlphaKey = "llmlab-upstream-key-for-sticky-route-tests-8d12"

func newSkFix(t *testing.T, o skOpts) *skFix {
	t.Helper()
	if len(o.deps) == 0 {
		o.deps = []pwDep{{name: "md-a", instances: []string{"a-0"}, cap: 1}, {name: "md-b", instances: []string{"b-0"}, cap: 1}}
	}
	f := &skFix{pwFix: newPwFixOn(t, o.edit, o.deps...), splitIDs: o.splitIDs}
	if o.managed {
		f.llm = llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{skAlphaKey}, Models: []llmlabtest.Model{{ID: wrCatalog}}})
		f.profile("llmlab", v1alpha1.PlatformManaged, "", "")
		f.managedProvider("llm-a", "llmlab", "llmlab", f.llm.BaseURL(), skAlphaKey, wrCatalog)
		f.managedMD("md-llm", scModel, "llmlab", wrCatalog, "llm-a")
		if o.stallManaged {
			f.llm.Stall(true)
			t.Cleanup(func() { f.llm.Stall(false) })
		}
	}
	w := f.workload("ns", "alpha", scModel)
	w.Spec.Shape.ContextTokens.Typical, w.Spec.Shape.OutputTokens.Typical = 100, 25
	if !o.noAffinity {
		w.Spec.SessionAffinity = v1alpha1.SessionAffinityRoute
	}
	if o.pinTTL != "" {
		w.Spec.PinTTL = ptr.To(v1alpha1.Duration(o.pinTTL))
	}
	if o.maxPins > 0 {
		w.Spec.MaxPins = ptr.To(o.maxPins)
	}
	if o.managed {
		w.Spec.RouteOrder = v1alpha1.RouteManagedFirst
		w.Spec.ManagedSessions = ptr.To(int32(1))
		if o.managedSessions > 0 {
			w.Spec.ManagedSessions = ptr.To(o.managedSessions)
		}
	}
	f.setCurrentIDs()
	f.apply()
	return f
}

// setCurrentIDs gives every Instance of a served ModelDeployment the one deployment ID of that ModelDeployment, as its Instances have, and writes the
// ModelDeployment's status.currentIDs from it, as the lifecycle controller does.
func (f *skFix) setCurrentIDs() {
	n := 0
	for i := range f.snap.Deployments {
		md := &f.snap.Deployments[i]
		md.Status.CurrentIDs = nil
		id := v1alpha1.FullDeploymentID(fmt.Sprintf("%064x", 0xa0000+i))
		for j := range f.snap.Instances {
			if in := &f.snap.Instances[j]; len(in.OwnerReferences) == 1 && in.OwnerReferences[0].UID == md.UID {
				if f.splitIDs {
					n++
					id = v1alpha1.FullDeploymentID(fmt.Sprintf("%064x", 0xb0000+n))
					md.Status.CurrentIDs = append(md.Status.CurrentIDs, v1alpha1.ClassDeploymentID{ID: id})
				} else {
					md.Status.CurrentIDs = []v1alpha1.ClassDeploymentID{{ID: id}}
				}
				in.Spec.DeploymentID = id
			}
		}
	}
}

// alpha is the model name that names the Workload.
func (f *skFix) alpha() string { return scModel + "#alpha" }

// session returns a conversation key whose home is the Instance on the fork's ring over every Instance of the fixture. skip says how many such keys to pass,
// so a row can have several conversations with one home.
func (f *skFix) session(home string, skip int) string {
	f.t.Helper()
	all := pwNames(f.deps...)
	for _, s := range pwSessions(400) {
		if pwHome(f.t, all, s) == home {
			if skip == 0 {
				return s
			}
			skip--
		}
	}
	f.t.Fatalf("no key among 400 has the home %s", home)
	return ""
}

// start sends one chat request of alpha under the session key on its own goroutine, and returns it with the cancel that cuts it.
func (f *skFix) start(session string, hdr ...string) (*scCall, context.CancelFunc) {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.t.Cleanup(cancel)
	return tbCall(f.harness, ctx, errChatBody(f.alpha(), false, "hello there"), append([]string{"x-session-id", session}, hdr...)...), cancel
}

// hold makes the engine of inst hold every request until the returned release is called, which a cleanup also runs.
func (f *skFix) hold(inst string) (release func()) {
	ch := make(chan struct{})
	f.engines[inst].Set(enginetest.Behaviour{Hold: ch})
	done := false
	release = func() {
		if !done {
			done = true
			close(ch)
		}
	}
	f.t.Cleanup(release)
	return release
}

// waiting is the requests waiting at the front, in a group's queue or a Workload's FIFO.
func (f *skFix) waiting() int { return f.router.Status().Waiting }

// engineSaw is how many requests the engine of inst has received.
func (f *skFix) engineSaw(inst string) int { return len(f.engines[inst].Requests()) }

// totalSaw is how many requests every engine of the fixture has received.
func (f *skFix) totalSaw() int {
	n := 0
	for k := range f.engines {
		n += f.engineSaw(k)
	}
	return n
}

// alphaUID is the Workload's UID in the snapshot.
func (f *skFix) alphaUID() types.UID {
	for _, w := range f.snap.Workloads {
		if w.Name == "alpha" {
			return w.UID
		}
	}
	f.t.Fatal("no Workload alpha")
	return ""
}

// idOf is the full deployment ID of the Instance, as its snapshot object has it.
func (f *skFix) idOf(inst string) string { return string(f.inst(f.uids[inst]).Spec.DeploymentID) }

// mdUID is the ModelDeployment's UID in the snapshot.
func (f *skFix) mdUID(name string) types.UID { return f.md(name).UID }

// journalPath is the pin journal, in the usage directory.
func (f *skFix) journalPath() string { return filepath.Join(f.cfg.UsageDir, "sticky-pins.jsonl") }

// journal is the records in the pin journal now, in order, or none when there is no file.
func (f *skFix) journal() []pinRecord {
	f.t.Helper()
	b, err := os.ReadFile(f.journalPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		f.t.Fatal(err)
	}
	var out []pinRecord
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var r pinRecord
		if err := json.Unmarshal(line, &r); err != nil {
			f.t.Fatalf("a journal line is not a record: %v: %s", err, line)
		}
		out = append(out, r)
	}
	return out
}

// journalOf is the journal's records of one op, for one conversation's key (the hex of its SHA-256 by hand, never by the front's own function).
func (f *skFix) journalOf(op, session string) []pinRecord {
	f.t.Helper()
	var out []pinRecord
	for _, r := range f.journal() {
		if r.Op == op && r.Key == keyHashByHand(session) {
			out = append(out, r)
		}
	}
	return out
}

// keyHashByHand is the journal's name for a session key, the SHA-256 in hex, computed here and not by the front's own function.
func keyHashByHand(session string) string {
	sum := sha256.Sum256([]byte(session))
	return hex.EncodeToString(sum[:])
}

// setFailWrites makes every append to the pin journal fail with err (nil: none does).
func (f *skFix) setFailWrites(err error) {
	s := f.router.pins
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failWrites = err
}

// keyLockRefs is how many requests hold or wait for the conversation's key lock.
func (f *skFix) keyLockRefs(session string) int {
	s := f.router.pins
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.locks[string(f.alphaUID())+"/"+keyHash(session)]; l != nil {
		return l.refs
	}
	return 0
}

// chatAt sends one request of alpha and returns the Instance that answered it, or "" and the response when it was refused.
func (f *skFix) chatAt(session string) (string, *http.Response, string) {
	f.t.Helper()
	return f.chat(f.t, f.alpha(), session)
}

// postAt sends one request naming model under the session key and returns the Instance the response says answered it, with the response and its body. It
// does not count engines, so a request that was tried on two Instances is read from its answer.
func (f *skFix) postAt(session, model string) (string, *http.Response, string) {
	f.t.Helper()
	resp := f.errPost(scChat, errChatBody(model, false, "hello there"), "x-session-id", session)
	body := readAll(f.t, resp)
	return scInstanceOf(resp), resp, body
}

// advance moves the front's clock.
func (f *skFix) advance(d time.Duration) { f.clk.Advance(d) }

// restart stops the front's router and starts a new one over the same snapshot, the same engines and the same journal, as a restart does: the new router
// holds none of the old one's memory, and the Workload and every ModelDeployment keep their UIDs.
func (f *skFix) restart() {
	f.t.Helper()
	_ = f.srv.Close()
	f.router.closePins()
	keys, err := ReadClientKeys(f.cfg.ClientKeysFile)
	if err != nil {
		f.t.Fatal(err)
	}
	r := NewRouter(f.cfg, f.clk, f.trans, f.router.rec, f.router.log, f.metrics)
	r.Update(f.snap)
	p := NewProxy(ProxyOptions{Config: f.cfg, Host: f.host, Clock: f.clk, Router: r, Transports: f.trans, Keys: keys, Sink: f.sink, Metrics: f.metrics, Log: f.router.log, Records: f.recs})
	ln, err := enginetest.Listen(testPortLo, testPortHi)
	if err != nil {
		f.t.Fatal(err)
	}
	srv := clientServer(f.cfg, p, f.base)
	go func() { _ = srv.Serve(ln) }()
	f.t.Cleanup(func() { _ = srv.Close(); r.closePins() })
	f.router, f.proxy, f.srv, f.url = r, p, srv, "http://"+ln.Addr().String()
}
