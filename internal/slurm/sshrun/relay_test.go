package sshrun

import (
	"slices"
	"strings"
	"testing"
)

// The relay's ssh is on a connection of its own (HBEP-6, Dial-home modes,
// host-dialed mode: "never the Provider's shared master"), in batch mode,
// with ServerAliveInterval=10 and ServerAliveCountMax=3, and the line is one
// argument after "--".
func TestRelayArgs(t *testing.T) {
	args := RelayArgs("/c/relay.conf", "srun -u --overlap --jobid=1 --ntasks=1 /s/hb-agent relay control")
	joined := strings.Join(args, " ")
	for _, want := range []string{"BatchMode=yes", "ServerAliveInterval=10", "ServerAliveCountMax=3", "ControlMaster=no", "ControlPath=none",
		"ClearAllForwardings=yes", "IdentityAgent=none", "StrictHostKeyChecking=yes", "EscapeChar=none"} {
		if !slices.Contains(args, want) {
			t.Errorf("the relay's ssh lacks -o %s: %s", want, joined)
		}
	}
	if args[0] != "-F" || args[1] != "/c/relay.conf" {
		t.Errorf("the relay's ssh does not start with its private -F file: %s", joined)
	}
	if strings.Contains(joined, "ProxyCommand") || strings.Contains(joined, "ControlMaster=yes") || strings.Contains(joined, "ControlPath=/") {
		t.Errorf("the relay's ssh would use or make a master: %s", joined)
	}
	if n := len(args); args[n-2] != "--" || args[n-1] != "srun -u --overlap --jobid=1 --ntasks=1 /s/hb-agent relay control" || args[n-3] != alias || args[n-4] != "-T" {
		t.Errorf("the relay's ssh does not end with -T, the host, -- and the line: %s", joined)
	}
}

// Classify passes on the relay step's own fixed line and nothing else of
// standard error; ssh's own failures are named by kind.
func TestClassifyARelayLine(t *testing.T) {
	cmd, err := (&Relay{o: RelayOptions{Binary: "ssh"}, config: "/nonexistent"}).command("x")
	if err != nil {
		t.Fatal(err)
	}
	l := cmd.Stderr.(*limited)
	l.w.WriteString("srun: warning: example\nhb-relay: no agent socket for job 7 on this node\nhb-agent relay control: /scratch/example/x\n")
	if got := Classify(cmd); got != "hb-relay: no agent socket for job 7 on this node" {
		t.Errorf("Classify = %q", got)
	}
	l.w.Reset()
	l.w.WriteString("example@login.example: Permission denied (publickey).\n")
	if got := Classify(cmd); got != "authentication was refused" {
		t.Errorf("Classify = %q, want the kind only", got)
	}
}
