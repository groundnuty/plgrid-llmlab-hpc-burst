package frpsplugin

// These tests run the real frps and frpc (v0.71.1) against the Handler and the Reader. What they
// check is what frps does with the plugin's answers, read from frps's own proxy list; a canned JSON
// shows none of that.

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Test credentials: keys' form, and nothing else.
const (
	tokA = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	tokB = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"
	tokC = "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3"
)

// newRealHandler returns a Handler over a MemIndex that is not yet synced, with the Notifier
// recording what it hears.
func newRealHandler(t *testing.T, snap Snapshot) (*MemIndex, *events, *Handler) {
	t.Helper()
	idx := &MemIndex{}
	if err := idx.Replace(snap); err != nil {
		t.Fatal(err)
	}
	ev := &events{}
	h, err := NewHandler(idx, Options{Notifier: ev})
	if err != nil {
		t.Fatal(err)
	}
	return idx, ev, h
}

func condOf(rep Report, uid string) (metav1.ConditionStatus, string) {
	c, ok := rep.Conditions[uid]
	if !ok {
		return "missing", ""
	}
	if c.Type != v1alpha1.ConditionTunnelConnected {
		return "wrong type " + metav1.ConditionStatus(c.Type), ""
	}
	return c.Status, c.Reason
}

// TestRealFrpsAdmissionAndPathState is the whole path: an frpc with a live Allocation's credential
// is refused at Login until the index has synced; then its proxy for an Instance placed on that
// Allocation registers, and its proxies for an Instance of another Allocation and for a UID nobody
// knows are refused at NewProxy; the reader reports online; killing frpc reaches the Notifier as
// CloseProxy and the reader reports offline; stopping frps makes the list unreadable.
func TestRealFrpsAdmissionAndPathState(t *testing.T) {
	const (
		uidA     = "uid-a-1111"    // placed on Allocation a, the one frpc presents
		uidB     = "uid-b-2222"    // placed on Allocation b: a real Instance, but not on frpc's Allocation
		uidAlone = "uid-nobody-33" // no Instance at all
	)
	// Instances of Allocation a whose proxies carry a field HBEP-6 forbids: each is placed, so
	// the field alone is what the plugin refuses. The value is the frpc setting that sends it,
	// and the key frps posts for it (frp pkg/msg/msg.go:102-134).
	forbidden := map[string]struct{ setting, wireKey string }{
		"uid-group-4444":    {"loadBalancer.group = \"g\"\nloadBalancer.groupKey = \"k\"", `"group"`},
		"uid-httpuser-5555": {"httpUser = \"u\"", `"http_user"`},
		"uid-sub-6666":      {"subdomain = \"s\"", `"subdomain"`},
		"uid-route-7777":    {"routeByHTTPUser = \"r\"", `"route_by_http_user"`},
	}
	placed := []string{uidA}
	extra := map[string]string{}
	for uid, f := range forbidden {
		placed = append(placed, uid)
		extra[uid] = f.setting
	}
	idx, ev, h := newRealHandler(t, Snapshot{Allocations: []AllocationEntry{
		{UID: "alloc-a", Credential: tokA, Instances: placed},
		{UID: "alloc-b", Credential: tokB, Instances: []string{uidB}},
	}})
	r := newRig(t, h)
	rd := r.reader()
	ctx := t.Context()
	uids := []string{uidA, uidB, uidAlone}

	served := append([]string{uidA, uidB, uidAlone}, placed[1:]...)
	frpcLog, killFrpc := r.runFrpc(frpcSpec{token: tokA, uids: served, extra: extra})

	// Not synced: frps asks, the plugin refuses the login, and nothing registers.
	waitFor(t, 20*time.Second, "frps never posted a Login to the plugin", func() bool { return len(r.cap.calls(OpLogin)) > 0 })
	for _, c := range r.cap.calls(OpNewProxy) {
		t.Fatalf("a NewProxy reached the plugin before the index synced: %s", c.Body)
	}
	if rep := rd.Read(ctx, uids); rep.Err != nil {
		t.Fatalf("reading the list: %v", rep.Err)
	} else if st, reason := condOf(rep, uidA); st != metav1.ConditionFalse || reason != v1alpha1.ReasonProxyOffline {
		t.Fatalf("before the sync %s is %s/%s, want False/ProxyOffline", uidA, st, reason)
	}

	// Synced: frpc's retry logs in, and registers the one proxy the plugin allows.
	idx.MarkSynced()
	waitFor(t, 60*time.Second, "frps never listed "+uidA+" online", func() bool {
		rep := rd.Read(ctx, uids)
		st, _ := condOf(rep, uidA)
		return rep.Err == nil && st == metav1.ConditionTrue
	})
	rep := rd.Read(ctx, uids)
	for _, c := range []struct {
		uid    string
		status metav1.ConditionStatus
		reason string
	}{
		{uidA, metav1.ConditionTrue, v1alpha1.ReasonProxyOnline},
		{uidB, metav1.ConditionFalse, v1alpha1.ReasonProxyOffline},
		{uidAlone, metav1.ConditionFalse, v1alpha1.ReasonProxyOffline},
	} {
		if st, reason := condOf(rep, c.uid); st != c.status || reason != c.reason {
			t.Errorf("%s: %s/%s, want %s/%s", c.uid, st, reason, c.status, c.reason)
		}
	}
	// The refused proxies are not in frps's own list at all, not even offline.
	list, err := rd.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range list {
		if p.Name != uidA {
			t.Errorf("frps lists %q (%s), but only %s may register", p.Name, p.Status, uidA)
		}
	}
	// Every proxy frpc sent reached the plugin (one NewProxy per name), and each forbidden field
	// arrived under the key the plugin reads, so the refusal was for that field.
	waitFor(t, 10*time.Second, "not every NewProxy reached the plugin", func() bool { return len(r.cap.calls(OpNewProxy)) >= len(served) })
	sent := map[string][]byte{}
	for _, c := range r.cap.calls(OpNewProxy) {
		var body struct {
			Content struct {
				ProxyName string `json:"proxy_name"`
			} `json:"content"`
		}
		if err := json.Unmarshal(c.Body, &body); err != nil {
			t.Fatal(err)
		}
		sent[body.Content.ProxyName] = c.Body
	}
	for uid, f := range forbidden {
		if b, ok := sent[uid]; !ok || !bytes.Contains(b, []byte(f.wireKey)) {
			t.Errorf("frps's NewProxy for %s lacks the key %s: %s", uid, f.wireKey, b)
		}
		if st, reason := condOf(rd.Read(ctx, []string{uid}), uid); st != metav1.ConditionFalse || reason != v1alpha1.ReasonProxyOffline {
			t.Errorf("%s: %s/%s, want False/ProxyOffline (refused for its forbidden field)", uid, st, reason)
		}
	}
	// The refusal reaches frpc with its rule (HBEP-6: reject reasons reach frpc).
	waitFor(t, 10*time.Second, "frpc's log does not show the plugin's reason for refusing "+uidB, func() bool {
		b, _ := os.ReadFile(frpcLog)
		return strings.Contains(string(b), reasonNotPlaced)
	})
	if got := ev.all(); len(got) != 1 || got[0] != "NewProxy "+uidA {
		t.Errorf("the Notifier heard %v, want only the accepted NewProxy for %s", got, uidA)
	}
	// The real frps's NewProxy carried the Allocation that admitted it: alloc-a holds tokA, the
	// token frpc logged in with.
	if got := ev.allocation("NewProxy " + uidA); got != "alloc-a" {
		t.Errorf("the Notifier heard Allocation %q with the real NewProxy of %s, want alloc-a", got, uidA)
	}

	// Orphans: the same list, read for an Instance set that does not include uidA.
	if rep := rd.Read(ctx, []string{uidB}); len(rep.Orphans) != 1 || rep.Orphans[0] != uidA {
		t.Errorf("orphans = %v, want [%s]", rep.Orphans, uidA)
	}

	// A crashed job: the kernel closes frpc's connection, frps drops the proxy and posts CloseProxy.
	killFrpc()
	waitFor(t, 20*time.Second, "the Notifier never heard CloseProxy for "+uidA+": "+strings.Join(ev.all(), ", "), func() bool { return ev.has("CloseProxy " + uidA) })
	waitFor(t, 20*time.Second, uidA+" never turned offline in frps's list", func() bool {
		rep := rd.Read(ctx, uids)
		st, reason := condOf(rep, uidA)
		return rep.Err == nil && st == metav1.ConditionFalse && reason == v1alpha1.ReasonProxyOffline
	})
	// An offline entry stays in frps's list, and still does not count as connected.
	if list, _ := rd.List(ctx); len(list) != 1 || list[0].Name != uidA || list[0].Status != "offline" {
		t.Errorf("after frpc was killed the list is %+v, want %s offline", list, uidA)
	}

	// frps is gone: the list cannot be read, which is Unknown and never the last value.
	r.stopFrps()
	rep = rd.Read(ctx, uids)
	if rep.Err == nil {
		t.Fatal("the list was read with frps stopped")
	}
	for _, uid := range uids {
		if st, reason := condOf(rep, uid); st != metav1.ConditionUnknown || reason != v1alpha1.ReasonProxyListUnreadable {
			t.Errorf("%s with frps stopped: %s/%s, want Unknown/ProxyListUnreadable", uid, st, reason)
		}
	}
	if len(rep.Orphans) != 0 {
		t.Errorf("orphans %v from a list that was not read", rep.Orphans)
	}
	r.checkFixtureShapes(t)
	r.dumpFixtures(t)
}

// TestRealFrpsRefusesAnUnknownCredential: a login with a credential that is a key's form but no
// Allocation's is refused with the rule, no session starts, and no NewProxy is ever posted.
func TestRealFrpsRefusesAnUnknownCredential(t *testing.T) {
	idx, ev, h := newRealHandler(t, Snapshot{Allocations: []AllocationEntry{
		{UID: "alloc-a", Credential: tokA, Instances: []string{"uid-a-1111"}},
	}})
	idx.MarkSynced()
	r := newRig(t, h)
	frpcLog, _ := r.runFrpc(frpcSpec{token: tokC, uids: []string{"uid-a-1111"}})

	waitFor(t, 20*time.Second, "frps never posted a Login", func() bool { return len(r.cap.calls(OpLogin)) > 0 })
	waitFor(t, 10*time.Second, "frpc's log does not show the plugin's reason for refusing the login", func() bool {
		b, _ := os.ReadFile(frpcLog)
		return strings.Contains(string(b), reasonCredential)
	})
	time.Sleep(2 * time.Second) // a refused login retries within 20 s at most; two seconds shows none got through
	if n := len(r.cap.calls(OpNewProxy)); n != 0 {
		t.Errorf("%d NewProxy calls after a refused login", n)
	}
	if got := ev.all(); len(got) != 0 {
		t.Errorf("the Notifier heard %v", got)
	}
	list, err := r.reader().List(t.Context())
	if err != nil || len(list) != 0 {
		t.Errorf("frps lists %+v (%v), want nothing", list, err)
	}
	for i, c := range r.cap.calls(OpLogin) {
		if strings.Contains(r.cap.out[i], tokC) {
			t.Errorf("the plugin's answer to %s contains the token", c.Op)
		}
	}
}

// TestRealReaderWithAWrongPasswordIsUnreadable: frps answers 401 to a password that is a key's form
// but not its own, and that is Unknown, never offline.
func TestRealReaderWithAWrongPasswordIsUnreadable(t *testing.T) {
	_, _, h := newRealHandler(t, Snapshot{})
	r := newRig(t, h)
	wrong := filepath.Join(r.state, "other-password")
	if err := os.WriteFile(wrong, []byte(strings.Repeat("0f", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	rd, err := NewReader(ReaderConfig{Addr: r.host.FrpsWebAddr(), User: "hpcburst", PasswordFile: wrong})
	if err != nil {
		t.Fatal(err)
	}
	rep := rd.Read(t.Context(), []string{"uid-x"})
	if rep.Err == nil || !strings.Contains(rep.Err.Error(), "401") {
		t.Fatalf("a wrong password gave %v, want an error naming 401", rep.Err)
	}
	if st, reason := condOf(rep, "uid-x"); st != metav1.ConditionUnknown || reason != v1alpha1.ReasonProxyListUnreadable {
		t.Errorf("a wrong password: %s/%s, want Unknown/ProxyListUnreadable", st, reason)
	}
	// The right one reads an empty list: online and offline are told apart from unreadable.
	if rep := r.reader().Read(t.Context(), []string{"uid-x"}); rep.Err != nil {
		t.Errorf("the right password: %v", rep.Err)
	} else if st, reason := condOf(rep, "uid-x"); st != metav1.ConditionFalse || reason != v1alpha1.ReasonProxyOffline {
		t.Errorf("an Instance frps does not list: %s/%s, want False/ProxyOffline", st, reason)
	}
}

// dumpFixtures writes the bodies frps posted to $FRPSPLUGIN_DUMP, when set: how testdata/ was captured.
func (r *rig) dumpFixtures(t *testing.T) {
	dir := os.Getenv("FRPSPLUGIN_DUMP")
	if dir == "" {
		return
	}
	for op, name := range map[string]string{OpLogin: "login.json", OpNewProxy: "newproxy.json", OpCloseProxy: "closeproxy.json"} {
		calls := r.cap.calls(op)
		if len(calls) == 0 {
			t.Errorf("no %s call to dump", op)
			continue
		}
		pick := calls[len(calls)-1]
		for _, c := range calls { // the accepted proxy's call, when there is one
			if bytes.Contains(c.Body, []byte(`"uid-a-1111"`)) {
				pick = c
			}
		}
		if err := os.WriteFile(filepath.Join(dir, name), pick.Body, 0o600); err != nil {
			t.Error(err)
		}
	}
}

// shapePaths are the JSON paths the handler reads, with the JSON type each has. testdata/ holds the
// bodies as frps v0.71.1 posted them (captured with FRPSPLUGIN_DUMP set, then scrubbed of the host
// name, address, privilege key and run ID; the tokens are test credentials), and the unit tests
// build their requests from them. The paths in the fixtures must be the ones the running frps
// posts, or the unit tests would pass on a format frps does not use.
var shapePaths = map[string][]string{
	OpLogin:      {"content.metas.token"},
	OpNewProxy:   {"content.user.metas.token", "content.proxy_name", "content.proxy_type", "content.custom_domains", "content.multiplexer"},
	OpCloseProxy: {"content.proxy_name"},
}

func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "missing"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return "other"
}

func jsonAt(t *testing.T, raw []byte, path string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%v in %s", err, raw)
	}
	for _, k := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}

// checkFixtureShapes compares, for each operation, the type at every path the handler reads in the
// body the running frps posted with the type in the fixture. It runs after the test's traffic, so
// every operation has been posted.
func (r *rig) checkFixtureShapes(t *testing.T) {
	t.Helper()
	for op, paths := range shapePaths {
		calls := r.cap.calls(op)
		if len(calls) == 0 {
			t.Errorf("frps posted no %s to compare with its fixture", op)
			continue
		}
		fixture, err := os.ReadFile(filepath.Join("testdata", strings.ToLower(op)+".json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range paths {
			want := jsonKind(jsonAt(t, fixture, p))
			if want == "missing" {
				t.Errorf("fixture for %s lacks %s", op, p)
			}
			for _, c := range calls {
				if got := jsonKind(jsonAt(t, c.Body, p)); got != want {
					t.Errorf("%s: %s is %s in frps's body, %s in testdata: %s", op, p, got, want, c.Body)
				}
			}
		}
	}
}
