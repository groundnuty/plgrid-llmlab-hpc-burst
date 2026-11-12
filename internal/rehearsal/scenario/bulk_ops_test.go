package scenario_test

// The bulk path's operations on the whole stack (HBEP-6, Operational readiness 2 and Test plan,
// Teardown; #448, part 7): the two behaviours no lower level covers. The rest of HBEP-6:657-661 is
// tested in parts 2 to 6, with the real frp where it matters.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// frpsBulkProxies lists the tcpmux proxies the second frps reports online, by name, from its web API
// (base+15, user hpcburst, the password in <state>/frps-bulk/web-password), as frpsProxies reads
// the first's.
func (r *rig) frpsBulkProxies() map[string]bool {
	r.t.Helper()
	pw, err := os.ReadFile(filepath.Join(r.h.State, "frps-bulk", "web-password"))
	if err != nil {
		r.t.Fatalf("frps-bulk's web password: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, r.ports.URL(15, "/api/proxy/tcpmux"), nil)
	req.SetBasicAuth("hpcburst", strings.TrimSpace(string(pw)))
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		r.t.Fatalf("frps-bulk's proxy list: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		r.t.Fatalf("frps-bulk's proxy list: %d %.200s", resp.StatusCode, body)
	}
	var list struct {
		Proxies []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"proxies"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		r.t.Fatalf("frps-bulk's proxy list: %v: %.200s", err, body)
	}
	out := map[string]bool{}
	for _, p := range list.Proxies {
		if p.Status == "online" {
			out[p.Name] = true
		}
	}
	return out
}

// sendSmall sends one small non-streamed chat request and returns its status.
func (r *rig) sendSmall(key string) int {
	r.t.Helper()
	return r.fc.Send("/v1/chat/completions", stack.ChatBody(stack.Model, 8, 0, false, ""), key).Status
}

// TestFrpsBulkRestartCutsOnlyBulk (HBEP-6, Operational readiness 2: "The second frps's restart drops
// only the bulk tunnels, and requests take the token tunnels meanwhile"): frps-bulk is killed through
// its pidfd while two token streams run. The supervisor starts it again; the streams finish uncut,
// the first frps keeps its process, a non-streamed request meanwhile gets its answer, and once the
// bulk frpc has logged in again and a probe passed, the next one takes the bulk transport again.
//
//livecheck:none a hosted throwaway runs it (IMPL-COMMON: rehearsals on hosted runners)
func TestFrpsBulkRestartCutsOnlyBulk(t *testing.T) {
	requireRehearsal(t)
	for _, c := range []struct {
		mode  string
		ports stack.Ports
	}{
		{"direct", portsBulkDirect},
		{string(agentapi.DialHomeHostDialed), portsBulkHostDialed},
	} {
		t.Run(c.mode, func(t *testing.T) {
			r := startRigWith(t, rigSetup{mode: c.mode, ports: c.ports, bulkPath: true})
			r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + bulkTokenDelay.String()}}
			in := r.startServing(true)
			key := r.fc.KeyFor(in)
			sup, err := pinSupervisor(t, r.h.State)
			if err != nil {
				t.Fatal(err)
			}
			_, firstEntry, err := pinEntry(t, r.h.State, "frps", sup)
			if err != nil {
				t.Fatal(err)
			}
			bulk, bulkEntry, err := pinEntry(t, r.h.State, "frps-bulk", sup)
			if err != nil {
				t.Fatal(err)
			}

			var dones []<-chan stack.Streamed
			for i := 0; i < 2; i++ {
				f, d := r.fc.StartStream(stack.ChatBody(stack.Model, 300, 300, true, ""), key)
				select {
				case <-f:
				case <-time.After(stack.WaitDeadline):
					t.Fatalf("stream %d sent no first token", i)
				}
				dones = append(dones, d)
			}
			if err := bulk.signal(syscall.SIGKILL); err != nil {
				t.Fatalf("SIGKILL to frps-bulk through its pidfd: %v", err)
			}
			bulk.waitExited(t, 30*time.Second)
			stack.WaitState(t, stack.WaitDeadline, "the supervisor to start frps-bulk again", func() (bool, string) {
				rec, err := readSupervisor(r.h.State)
				if err != nil {
					return false, err.Error()
				}
				e, ok := rec.entryOf("frps-bulk")
				return ok && e.Proc.PID != bulkEntry.Proc.PID, "not yet"
			})
			// Meanwhile requests take the token tunnel (HBEP-6, Operational readiness 2): the request is
			// answered, and it took the bulk transport only if that was registered again by then, which
			// the gauge read after it says.
			sentBefore := frontMetric(t, c.ports, `hpcburst_front_bulk_requests_total{path="bulk"}`)
			if code := r.sendSmall(key); code != http.StatusOK {
				t.Errorf("a non-streamed request while frps-bulk came back: %d, want 200", code)
			}
			if sent := frontMetric(t, c.ports, `hpcburst_front_bulk_requests_total{path="bulk"}`); sent > sentBefore &&
				frontMetric(t, c.ports, "hpcburst_front_bulk_registered") < 1 {
				t.Errorf("the request meanwhile was written to a bulk transport (bulk %v -> %v) that is not registered: it should have taken the token tunnel", sentBefore, sent)
			} else {
				t.Logf("the request meanwhile took the %s", map[bool]string{true: "bulk transport, registered again by then", false: "token tunnel"}[sent > sentBefore])
			}
			for i, d := range dones {
				s := <-d
				if done, finished := stack.SSEDone(s.Body); s.Err != nil || s.Status != http.StatusOK || !done || !finished {
					t.Errorf("token stream %d across frps-bulk's restart: %d, %v, done %v, finished %v: it was cut", i, s.Status, s.Err, done, finished)
				}
			}
			rec, err := readSupervisor(r.h.State)
			if err != nil {
				t.Fatal(err)
			}
			if e, ok := rec.entryOf("frps"); !ok || e.Proc.PID != firstEntry.Proc.PID || e.Proc.StartTicks != firstEntry.Proc.StartTicks {
				t.Errorf("the first frps changed across frps-bulk's restart: %+v", e.Proc)
			}
			stack.WaitState(t, stack.WaitDeadline, "the bulk transport to register again", func() (bool, string) {
				return frontMetric(t, c.ports, "hpcburst_front_bulk_registered") >= 1, "not registered"
			})
			before := frontMetric(t, c.ports, `hpcburst_front_bulk_requests_total{path="bulk"}`)
			if code := r.sendSmall(key); code != http.StatusOK {
				t.Fatalf("a non-streamed request after the bulk path came back: %d", code)
			}
			if after := frontMetric(t, c.ports, `hpcburst_front_bulk_requests_total{path="bulk"}`); after != before+1 {
				t.Errorf("after the re-registration the request did not take the bulk transport: bulk %v -> %v", before, after)
			}
		})
	}
}

// TestTeardownLeavesNoBulkProxy (HBEP-6, Test plan, Teardown, with the bulk path on), in direct and
// host-dialed mode: when the deployment scales to zero and its Instance ends, the second frps lists no
// online proxy for it, as the first doesn't. The control is the same list while the Instance serves: it
// lists the proxy, so the read can see one. Then down leaves nothing listening on the whole block,
// base..base+15, the bulk path's three ports included.
//
//livecheck:none a hosted throwaway runs it (IMPL-COMMON: rehearsals on hosted runners)
func TestTeardownLeavesNoBulkProxy(t *testing.T) {
	requireRehearsal(t)
	for _, c := range []struct {
		mode  string
		ports stack.Ports
	}{
		{"direct", portsBulkDirect},
		{string(agentapi.DialHomeHostDialed), portsBulkHostDialed},
	} {
		t.Run(c.mode, func(t *testing.T) {
			r := startRigWith(t, rigSetup{mode: c.mode, ports: c.ports, bulkPath: true})
			r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + bulkTokenDelay.String()}}
			in := r.startServing(true)
			if !r.frpsBulkProxies()[string(in.UID)] {
				t.Fatalf("frps-bulk does not list the serving Instance %s online: the check below could not see a proxy", in.UID)
			}
			r.scaleToZero()
			r.waitAllEnded([]types.UID{in.UID})
			stack.WaitState(t, stack.WaitDeadline, "frps-bulk to list the ended Instance's proxy offline", func() (bool, string) {
				return !r.frpsBulkProxies()[string(in.UID)], "still online"
			})
			if r.frpsProxies()[string(in.UID)] {
				t.Errorf("frps still lists the ended Instance %s online", in.UID)
			}
			block := r.h.Ports.All()
			if !slices.Contains(block, c.ports.Base+15) {
				t.Fatalf("the stack's block %v does not reach base+15: the check below would miss frps-bulk's ports", block)
			}
			r.h.Down()
			stack.WaitState(t, 30*time.Second, "nothing to listen on base..base+15 after down", func() (bool, string) {
				l := stack.Listening(t, block)
				return len(l) == 0, fmt.Sprintf("still listening: %v", l)
			})
		})
	}
}

// TestPreBulkAllocationKeepsItsTokenTunnel (HBEP-6:659 and :661): an Allocation staged before the bulk
// path was turned on keeps its token tunnel and takes no bulk transport. The host starts without the
// bulk path and its Instance serves; then down, and up --bulk-path on the same state. The launcher's
// first certificate, which the Allocation's frpc trusts, has the same bytes; its real agent, whose
// agent.json has no bulk block, still runs one frpc; the front, now with the bulk path, registers no
// bulk transport for it, and serves its non-streamed requests through the token tunnel, with neither
// bulk counter moving.
//
//livecheck:none a hosted throwaway runs it (IMPL-COMMON: rehearsals on hosted runners)
func TestPreBulkAllocationKeepsItsTokenTunnel(t *testing.T) {
	requireRehearsal(t)
	for _, c := range []struct {
		mode  string
		ports stack.Ports
	}{
		{"direct", portsBulkDirect},
		{string(agentapi.DialHomeHostDialed), portsBulkHostDialed},
	} {
		t.Run(c.mode, func(t *testing.T) {
			r := startRigWith(t, rigSetup{mode: c.mode, ports: c.ports})
			r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + bulkTokenDelay.String()}}
			in := r.startServing(false)
			key := r.fc.KeyFor(in)
			r.waitFrpcs(in, 1, 0)
			certPath := filepath.Join(r.h.State, "agent-endpoint", "tls.crt")
			before, err := os.ReadFile(certPath)
			if err != nil {
				t.Fatal(err)
			}
			if code := r.sendSmall(key); code != http.StatusOK {
				t.Fatalf("before the switch: %d", code)
			}

			r.h.Down()
			r.h.Ports.BulkPath, r.ports.BulkPath = true, true
			if out, err := r.h.UpAgain("--bulk-path"); err != nil {
				t.Fatalf("up --bulk-path on the same state: %v\n%s", err, out)
			}
			after, err := os.ReadFile(certPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("up --bulk-path changed the first certificate's bytes: the Allocation staged before would lose its token tunnel")
			}
			if _, err := os.Stat(filepath.Join(r.h.State, "agent-endpoint", "tls-bulk.crt")); err != nil {
				t.Fatalf("up --bulk-path issued no second certificate: %v", err)
			}
			stack.WaitState(t, stack.WaitDeadline, "the Instance's token transport to register again", func() (bool, string) {
				reg, _ := stack.Registered(r.ports, in)
				return reg, "not registered"
			})
			r.waitFrpcs(in, 1, 0)
			if code := r.sendSmall(key); code != http.StatusOK {
				t.Fatalf("after the switch: %d, want 200 through the token tunnel", code)
			}
			if reg := frontMetric(t, r.ports, "hpcburst_front_bulk_registered"); reg != 0 {
				t.Errorf("the front registered %v bulk transports for an Allocation staged before the bulk path", reg)
			}
			b := frontMetric(t, r.ports, `hpcburst_front_bulk_requests_total{path="bulk"}`)
			fb := frontMetric(t, r.ports, `hpcburst_front_bulk_requests_total{path="fallback"}`)
			if b != 0 || fb != 0 {
				t.Errorf("bulk counters %v and %v: a request of an Allocation staged before the bulk path tried a bulk transport", b, fb)
			}
			if r.frpsBulkProxies()[string(in.UID)] {
				t.Error("frps-bulk lists a proxy for an Allocation staged before the bulk path")
			}
		})
	}
}
