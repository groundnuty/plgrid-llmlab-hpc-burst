package agentapi

import "testing"

// The durable log's ports and files at #625's launcher base (HBEP-8, The durable log; HBEP-11): the
// log takes base+16 and base+17, above the bulk path's base+13 to base+15.
func TestEventLogHostLayout(t *testing.T) {
	h := Host{StateDir: "/home/example-user/.local/state/hpcburst", PortBase: 28480}
	for got, want := range map[string]string{
		h.EventLogAddr():        "127.0.0.1:28496",
		h.EventLogMonitorAddr(): "127.0.0.1:28497",
		h.EventLogDir():         "/home/example-user/.local/state/hpcburst/eventlog",
		h.EventLogTokenFile():   "/home/example-user/.local/state/hpcburst/eventlog/token",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if EventLogPortOffset != 16 || EventLogMonitorPortOffset != 17 {
		t.Errorf("offsets %d and %d, want 16 and 17", EventLogPortOffset, EventLogMonitorPortOffset)
	}
	if h.EventLogPort() != 28496 || h.EventLogMonitorPort() != 28497 {
		t.Errorf("ports %d and %d, want 28496 and 28497", h.EventLogPort(), h.EventLogMonitorPort())
	}
	for _, p := range []int{h.FrpsBulkBindPort(), h.FrpsBulkTCPMuxPort(), h.FrpsBulkWebPort()} {
		if p == h.EventLogPort() || p == h.EventLogMonitorPort() {
			t.Errorf("the log's port %d is the bulk path's", p)
		}
	}
}
