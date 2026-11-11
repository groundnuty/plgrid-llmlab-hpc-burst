package main

// The relay keeper's opt-in (beta; HBEP-6, The relay keeper; #343, PR 4):
// --relay-keeper-socket unset is alpha, whose relay settings are exactly
// today's; set, in host-dialed mode only, it is keeper mode. No port is
// bound: the base only names the settings' ports.

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func keeperArgs(extra ...string) []string {
	return append([]string{"--state-dir", "/state", "--port-base", "30010", "--health-addr", "127.0.0.1:30016"}, extra...)
}

func TestTheRelayKeeperSocketIsOptInAndHostDialedOnly(t *testing.T) {
	f, err := parse(keeperArgs("--front-mode", "host-dialed"), io.Discard)
	if err != nil || f.relayKeeperSocket != "" {
		t.Fatalf("without the flag: socket %q, %v; want alpha's empty socket", f.relayKeeperSocket, err)
	}
	sock := "/state/relaykeeper.sock"
	f, err = parse(keeperArgs("--front-mode", "host-dialed", "--relay-keeper-socket", sock), io.Discard)
	if err != nil || f.relayKeeperSocket != sock {
		t.Fatalf("with the flag: socket %q, %v; want %q", f.relayKeeperSocket, err, sock)
	}
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"no front mode", keeperArgs("--relay-keeper-socket", sock), "host-dialed only"},
		{"direct mode", keeperArgs("--front-mode", "direct", "--front-addr", "front.example.org:30019", "--relay-keeper-socket", sock), "host-dialed only"},
		{"ssh mode", keeperArgs("--front-mode", "ssh", "--front-addr", "front.example.org:30019", "--relay-keeper-socket", sock), "host-dialed only"},
		{"relative path", keeperArgs("--front-mode", "host-dialed", "--relay-keeper-socket", "relaykeeper.sock"), "absolute"},
		{"too long", keeperArgs("--front-mode", "host-dialed", "--relay-keeper-socket", "/"+strings.Repeat("s", agentapi.MaxRelaySocketPath)), "at most 107"},
	} {
		if _, err := parse(c.args, io.Discard); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v; want an error saying %q", c.name, err, c.want)
		}
	}
	// 107 bytes exactly is allowed.
	edge := "/" + strings.Repeat("s", agentapi.MaxRelaySocketPath-1)
	if _, err := parse(keeperArgs("--front-mode", "host-dialed", "--relay-keeper-socket", edge), io.Discard); err != nil {
		t.Errorf("a 107-byte socket path is refused: %v", err)
	}
}

// Alpha's relay settings are today's, field for field: the agent endpoint
// (base+3) and the dial-home listener (base+7), the process's shared slots,
// and the ssh control directory, with nothing else set. Keeper mode, the
// positive control, names the ports and the socket only.
func TestRelaySettingsAreAlphasWithoutTheKeeperSocket(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/example")
	host := agentapi.Host{StateDir: "/state", PortBase: 30010}
	front := &providerctl.FrontSettings{Mode: agentapi.DialHomeHostDialed}
	ports := map[agentapi.RelayChannel]int{agentapi.RelayControl: 30013, agentapi.RelayServing: 30017}
	controlDir := filepath.Join("/run/user/example", "hpcburst-ssh-"+strconv.Itoa(os.Geteuid())+"-30010")

	alpha := wiring{host: host, front: front}.relaySettings()
	want := &providerctl.RelaySettings{Ports: ports, Slots: hostdial.SharedSlots, ControlDir: controlDir}
	if !reflect.DeepEqual(alpha, want) {
		t.Errorf("alpha's relay settings %+v, want %+v", alpha, want)
	}
	beta := wiring{host: host, front: front, keeperSocket: "/state/relaykeeper.sock"}.relaySettings()
	wantBeta := &providerctl.RelaySettings{Ports: ports, KeeperSocket: "/state/relaykeeper.sock"}
	if !reflect.DeepEqual(beta, wantBeta) {
		t.Errorf("keeper mode's relay settings %+v, want %+v", beta, wantBeta)
	}
	if direct := (wiring{host: host, front: &providerctl.FrontSettings{Mode: agentapi.DialHomeDirect}, keeperSocket: "/state/relaykeeper.sock"}).relaySettings(); direct != nil {
		t.Errorf("direct mode has relay settings: %+v", direct)
	}
}
