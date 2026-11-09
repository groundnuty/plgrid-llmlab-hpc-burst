package main

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// In host-dialed mode every batch Provider gets relay settings whose control
// relays dial base+3 and serving relays base+7 (HBEP-6, Dial-home modes,
// host-dialed mode), with the process's one set of slots; the other modes get
// none, so their controllers are unchanged.
func TestRelaySettingsOnlyInHostDialedMode(t *testing.T) {
	host := agentapi.Host{PortBase: 29780}
	for _, m := range []agentapi.DialHomeMode{agentapi.DialHomeDirect, agentapi.DialHomeSSH} {
		w := wiring{host: host, front: &providerctl.FrontSettings{Mode: m}}
		if s := w.relaySettings(); s != nil {
			t.Errorf("%s mode got relay settings %+v", m, s)
		}
	}
	if s := (wiring{host: host}).relaySettings(); s != nil {
		t.Errorf("no front got relay settings %+v", s)
	}
	s := wiring{host: host, front: &providerctl.FrontSettings{Mode: agentapi.DialHomeHostDialed}}.relaySettings()
	if s == nil {
		t.Fatal("host-dialed mode got no relay settings")
	}
	if s.Ports[agentapi.RelayControl] != 29783 || s.Ports[agentapi.RelayServing] != 29787 || len(s.Ports) != 2 {
		t.Errorf("relay ports %v, want control 29783 (base+3) and serving 29787 (base+7)", s.Ports)
	}
	if s.Slots != hostdial.SharedSlots || s.ControlDir == "" {
		t.Errorf("relay settings %+v: want the shared slots and a control directory", s)
	}
}
