package sshrun

import (
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
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

// The login host is ssh -G's hostname through the relays' own -F file (HBEP-6,
// Capacity): ssh writes host names in lower case, so two Providers that spell
// one host differently, with any user, are one login host. When ssh -G
// cannot run, it is the configured HostName as given.
func TestRelayLoginIsSSHGsHostName(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client on this host")
	}
	relay := func(name, host, user, bin string) *Relay {
		dir, err := os.MkdirTemp("", "rl")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		r, err := NewRelay(RelayOptions{Name: name, Binary: bin, ControlDir: dir,
			Config: slurm.SSHConfig{HostName: host, Port: 2222, User: user, IdentityFile: "/nonexistent/key", UserKnownHostsFile: "/nonexistent/known_hosts"}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	login := func(r *Relay) string {
		t.Helper()
		host, err := r.Login()
		if err != nil {
			t.Fatalf("Login: %v", err)
		}
		return host
	}
	a := relay("helios-a", "LOGIN01.Example.org", "example-user", "ssh")
	b := relay("helios-b", "login01.example.org", "other-user", "ssh")
	if got := login(a); got != "login01.example.org" {
		t.Errorf("Login = %q, want ssh -G's hostname login01.example.org", got)
	}
	if login(a) != login(b) {
		t.Errorf("one login host spelt two ways gives two keys: %q and %q", login(a), login(b))
	}
	if got := login(relay("helios-c", "LOGIN01.Example.org", "example-user", "/bin/false")); got != "LOGIN01.Example.org" {
		t.Errorf("with no ssh -G, Login = %q, want the configured HostName", got)
	}
}
