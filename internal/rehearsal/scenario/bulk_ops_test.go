package scenario_test

// The bulk path's operations on the whole stack (HBEP-6, Operational readiness 2 and Test plan,
// Teardown; #448, part 7): the two behaviours no lower level covers. The rest of HBEP-6:657-661 is
// tested in parts 2 to 6, with the real frp where it matters.

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
			if code := r.sendSmall(key); code != http.StatusOK {
				t.Errorf("a non-streamed request while the bulk path came back: %d, want 200 through the token tunnel", code)
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

// TestTeardownLeavesNoBulkProxy (HBEP-6, Test plan, Teardown, with the bulk path on): when the
// deployment scales to zero and its Instance ends, the second frps lists no online proxy for it, as
// the first doesn't. The control is the same list while the Instance serves: it lists the proxy, so
// the read can see one.
//
//livecheck:none a hosted throwaway runs it (IMPL-COMMON: rehearsals on hosted runners)
func TestTeardownLeavesNoBulkProxy(t *testing.T) {
	requireRehearsal(t)
	r := startRigWith(t, rigSetup{mode: "direct", ports: portsBulkDirect, bulkPath: true})
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
}
