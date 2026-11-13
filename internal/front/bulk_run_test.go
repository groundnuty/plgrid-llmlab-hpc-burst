package front

// The whole front with and without the bulk path (#448, part 6; S5: without it, nothing changes).

import (
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// countProbes counts the token probe's results.
type countProbes struct{ n atomic.Int64 }

func (c *countProbes) Probed(ProbeResult)                               { c.n.Add(1) }
func (c *countProbes) PathProbeFailing(*v1alpha1.Instance, ProbeResult) {}

// countingListener accepts connections on a kernel port, counts them and closes them.
func countingListener(t *testing.T) (addr string, accepted *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted = &atomic.Int64{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String(), accepted
}

// TestBulkOffRequestPathIsUnchanged: without the bulk path, the front never dials the second frps's
// tcpmux port, even with frpsBulkMuxAddr set, and serves no bulk plugin route (404). The control:
// with the bulk path, its probe dials that port, and the bulk plugin answers on the health address.
func TestBulkOffRequestPathIsUnchanged(t *testing.T) {
	for _, on := range []bool{false, true} {
		host := agentapi.Host{StateDir: t.TempDir(), PortBase: kernelPortBase(t)}
		bulkPair(t, host)
		mux, accepted := countingListener(t)
		inst := probeInstance("0a1b2c3d-0000-4000-8000-0000000000b1", v1alpha1.InstanceReady, metav1.ConditionTrue)
		probes := &countProbes{}
		f, stop := runProbeFront(t, startMuxDouble(t), func(o *Options) {
			o.PathEvents = probes
			o.Mode, o.Host, o.BulkPath = ModeHostDialed, host, on
			o.Config.FrpsBulkMuxAddr, o.Config.FrpsBulkBindAddr = mux, "127.0.0.1:1"
			o.Config.ProbeInterval = Duration(50 * time.Millisecond)
		}, inst)
		resp, err := http.Post("http://"+f.Health.String()+agentapi.FrontFrpsBulkPluginPath, "application/json",
			strings.NewReader(`{"version":"0.1.0","op":"Ping","content":{}}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if on {
			waitUntil(t, "the bulk probe to dial the second frps", func() bool { return accepted.Load() > 0 })
			if resp.StatusCode != http.StatusOK {
				t.Errorf("with the bulk path the bulk plugin answered %d", resp.StatusCode)
			}
			// A bulk NewProxy at the running front's bulk route moves the bulk epoch only.
			te, be := f.Tunnel.Epoch(inst.UID), f.bulk.tun.Epoch(inst.UID)
			r2, err := http.Post("http://"+f.Health.String()+agentapi.FrontFrpsBulkPluginPath, "application/json",
				strings.NewReader(`{"version":"0.1.0","op":"NewProxy","content":{"proxy_name":"`+string(inst.UID)+`"}}`))
			if err != nil {
				t.Fatal(err)
			}
			r2.Body.Close()
			if f.Tunnel.Epoch(inst.UID) != te || f.bulk.tun.Epoch(inst.UID) == be {
				t.Errorf("a bulk NewProxy at the front's bulk route: token epoch %d -> %d, bulk epoch %d -> %d; want only the bulk one moved",
					te, f.Tunnel.Epoch(inst.UID), be, f.bulk.tun.Epoch(inst.UID))
			}
		} else {
			// Several probe rounds of the token path pass meanwhile: waited on, not slept.
			waitUntil(t, "three token probes", func() bool { return probes.n.Load() >= 3 })
			if n := accepted.Load(); n != 0 {
				t.Errorf("without the bulk path the front dialled the second frps %d times", n)
			}
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("without the bulk path the bulk plugin route answered %d, want 404", resp.StatusCode)
			}
		}
		stop()
	}
}
