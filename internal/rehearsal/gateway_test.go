package rehearsal

// New. The dial-home gateway driven by the real ssh client with the agent's
// own arguments (internal/agent.SSHForward.Args) and the files WriteDialHome
// wrote. The expectations come from HBEP-6 (Dial-home modes, Test plan: "With
// the key's line, the two forwards work. A forward to a third port, a remote
// forward and a command are refused") and from ssh and sshd's documentation;
// none is read back from the gateway's own output or log.
//
// Every refusal has a control in the same test: the identical operation with a
// key that has no restriction (openDir) succeeds, so the refusal comes from
// the key's line and not from a port that cannot be bound, a listener that is
// not there or a login that fails. The gateway's sshd leaves forwarding at its
// default, so the key's options are all that restrict.

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// openDir is a second dial-home directory whose key the gateway accepts with
// no restriction but "restrict,port-forwarding": any port, remote forwards and
// commands. The gateway's sshd reads authorized_keys at each connection.
func (r *gwRig) openDir() string {
	r.t.Helper()
	dir := filepath.Join(r.base, "open")
	if err := os.Mkdir(dir, 0o700); err != nil {
		r.t.Fatal(err)
	}
	typ, body, err := keygen(filepath.Join(dir, agentapi.DialHomeKeyFile))
	if err != nil {
		r.t.Fatal(err)
	}
	f, err := os.OpenFile(r.gw.files.authorized, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		r.t.Fatal(err)
	}
	_, werr := fmt.Fprintf(f, "restrict,port-forwarding %s %s\n", typ, body)
	if cerr := f.Close(); werr != nil || cerr != nil {
		r.t.Fatalf("append the unrestricted key: %v, %v", werr, cerr)
	}
	known, err := os.ReadFile(filepath.Join(r.dh, agentapi.DialHomeKnownHostsFile))
	if err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, agentapi.DialHomeKnownHostsFile), known, 0o600); err != nil {
		r.t.Fatal(err)
	}
	return dir
}

// withoutForward is args without its "-L spec" pair, and the spec. The agent's
// vector ends with the destination.
func withoutForward(t *testing.T, args []string) (rest []string, spec string) {
	t.Helper()
	i := slices.Index(args, "-L")
	if i < 0 || i+1 >= len(args) || !strings.Contains(args[len(args)-1], "@") {
		t.Fatalf("the agent's ssh arguments %q have no -L pair or no destination at the end", args)
	}
	return slices.Concat(args[:i], args[i+2:]), args[i+1]
}

func marker(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// With the key's line the two forwards work (HBEP-6): connection 1 reaches
// the agent endpoint's port and connection 2 the dial-home listener's, each
// carries a request one way and the reply the other, and the replies show
// that neither crossed to the other port.
func TestTwoForwardsCarryBytesBothWays(t *testing.T) {
	r := newGatewayRig(t)
	control := r.forward(r.dh, r.agent.port)
	tunnel := r.forward(r.dh, r.dial.port)
	for _, c := range []struct {
		name string
		f    *runningForward
		srv  *echoServer
	}{{"control", control, r.agent}, {"tunnel", tunnel, r.dial}} {
		payload := c.name + "-" + marker(t)
		reply, err := exchange(c.f.Dial, payload)
		if want := c.srv.tag + payload + "\n"; err != nil || reply != want {
			t.Errorf("%s forward: reply %q, error %v; want %q from the %s server", c.name, reply, err, want, c.srv.tag)
		}
		if got := c.srv.received(); len(got) != 1 || got[0] != payload+"\n" {
			t.Errorf("%s forward: the %s server received %q, want exactly %q", c.name, c.srv.tag, got, payload+"\n")
		}
	}
}

// A forward to a third port is refused. The third port has a live listener, so
// a refusal cannot be a connect that failed; the unrestricted key reaches it
// by the identical forward.
func TestThirdPortIsRefused(t *testing.T) {
	r := newGatewayRig(t)
	third := startEcho(t, "third:")

	open := r.forward(r.openDir(), third.port)
	payload := "open-" + marker(t)
	if reply, err := exchange(open.Dial, payload); err != nil || reply != "third:"+payload+"\n" {
		t.Fatalf("control: the unrestricted key's forward to the third port: reply %q, error %v", reply, err)
	}
	open.stop()
	before := third.accepted()

	// ssh -N binds the local port whatever the server will allow; the refusal
	// comes when a connection is forwarded.
	f := r.forward(r.dh, third.port)
	payload = "refused-" + marker(t)
	reply, _ := exchange(f.Dial, payload) // EOF or a reset: both mean refused
	if reply != "" {
		t.Errorf("a forward to a third port answered %q, want nothing", reply)
	}
	if n := third.accepted(); n != before {
		t.Errorf("the third port's listener accepted %d connection(s) through the restricted key, want none", n-before)
	}
	for _, line := range third.received() {
		if strings.Contains(line, payload) {
			t.Errorf("the third port's listener received %q through the restricted key", line)
		}
	}
	// The permitted ports still work with the same key and a new connection.
	if reply, err := exchange(r.forward(r.dh, r.agent.port).Dial, "after-"+payload); err != nil || reply != "agent:after-"+payload+"\n" {
		t.Errorf("the permitted forward after a refusal: reply %q, error %v", reply, err)
	}
}

// A remote forward (-R) is refused, and ssh, which the agent starts with
// ExitOnForwardFailure=yes, exits by itself. The unrestricted key's identical
// -R stays up and carries bytes from the remote port to a local listener.
func TestRemoteForwardIsRefused(t *testing.T) {
	r := newGatewayRig(t)
	remoteArgs := func(dir string, port int) []string {
		rest, _ := withoutForward(t, r.args(dir, r.agent.port))
		spec := "127.0.0.1:" + strconv.Itoa(port) + ":127.0.0.1:" + strconv.Itoa(r.agent.port)
		return slices.Concat(rest[:len(rest)-1], []string{"-R", spec}, rest[len(rest)-1:])
	}

	port := freePort(t)
	open := r.startSSH(remoteArgs(r.openDir(), port))
	payload := "remote-" + marker(t)
	waitFor(t, "the unrestricted key's remote forward to carry bytes", func() bool {
		if open.exited() {
			t.Fatalf("control: the unrestricted key's ssh -R exited with %d: %s", open.code, open.out)
		}
		reply, err := exchange(dialPort(port), payload)
		return err == nil && reply == "agent:"+payload+"\n"
	})

	refused := r.startSSH(remoteArgs(r.dh, freePort(t)))
	if code := refused.wait("ssh -R to exit by itself"); code != 255 {
		t.Errorf("ssh -R with the restricted key exited with %d, want 255 (ExitOnForwardFailure)", code)
	}
	if !strings.Contains(refused.out.String(), "remote port forwarding failed") {
		t.Errorf("ssh -R printed %q, want its remote port forwarding failure", refused.out)
	}
}

// A command is refused: ssh gets the forced /bin/false's status, never ssh's
// own 255 (which would be a login that failed), and the command's output is
// absent. The unrestricted key runs the same command.
func TestCommandIsRefused(t *testing.T) {
	r := newGatewayRig(t)
	commandArgs := func(dir, command string) []string {
		rest, _ := withoutForward(t, r.args(dir, r.agent.port))
		rest = slices.DeleteFunc(rest, func(a string) bool { return a == "-N" })
		return append(rest, command)
	}
	word := marker(t)

	open := r.startSSH(commandArgs(r.openDir(), "echo "+word))
	if code := open.wait("the unrestricted key's command"); code != 0 || !strings.Contains(open.out.String(), word) {
		t.Fatalf("control: the unrestricted key's command exited %d with output %q, want 0 and %q", code, open.out, word)
	}

	p := r.startSSH(commandArgs(r.dh, "echo "+word))
	code := p.wait("the restricted key's command")
	if code == 0 || code == 255 {
		t.Errorf("a command with the restricted key exited %d, want the forced command's failure (not 0, not ssh's own 255)", code)
	}
	if strings.Contains(p.out.String(), word) {
		t.Errorf("the refused command's output %q appeared", word)
	}
}

// known_hosts pins the host key: another key on the same [127.0.0.1]:port line
// ends ssh, under the agent's BatchMode and StrictHostKeyChecking=yes, with
// 255 and no forward; the right line (the first forward) works.
func TestHostKeyIsPinned(t *testing.T) {
	r := newGatewayRig(t)
	payload := "pinned-" + marker(t)
	if reply, err := exchange(r.forward(r.dh, r.agent.port).Dial, payload); err != nil || reply != "agent:"+payload+"\n" {
		t.Fatalf("control: the forward with the right known_hosts: reply %q, error %v", reply, err)
	}

	wrong := filepath.Join(r.base, "wrong")
	if err := r.gw.WriteDialHome(wrong); err != nil {
		t.Fatal(err)
	}
	typ, body, err := keygen(filepath.Join(r.base, "other"))
	if err != nil {
		t.Fatal(err)
	}
	line := fmt.Sprintf("[127.0.0.1]:%d %s %s\n", r.gw.srv.port(), typ, body)
	if err := os.WriteFile(filepath.Join(wrong, agentapi.DialHomeKnownHostsFile), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	p := r.startSSH(r.args(wrong, r.agent.port))
	if code := p.wait("ssh to refuse the other host key"); code != 255 {
		t.Errorf("ssh with another host key exited %d, want 255", code)
	}
	if !strings.Contains(p.out.String(), "Host key verification failed") {
		t.Errorf("ssh printed %q, want its host key verification failure", p.out)
	}
}

// Close stops every sshd, with a forward established, and waits for them: at
// once after it returns no sshd is below this process and none names the
// gateway's directory. The forward's ssh sees the connection go and exits by
// itself, and a second Close does nothing.
func TestCloseLeavesNoProcess(t *testing.T) {
	r := newGatewayRig(t)
	f := r.forward(r.dh, r.agent.port)
	payload := "close-" + marker(t)
	if reply, err := exchange(f.Dial, payload); err != nil || reply != "agent:"+payload+"\n" {
		t.Fatalf("before Close the forward: reply %q, error %v", reply, err)
	}
	if len(r.gwProcs()) == 0 {
		t.Fatal("no sshd is alive before Close: the check after it proves nothing")
	}
	if err := r.gw.Close(); err != nil {
		t.Fatal(err)
	}
	if left := r.gwProcs(); len(left) != 0 {
		t.Errorf("processes of the gateway left when Close returned: %s", strings.Join(left, ", "))
	}
	f.waitState("the forward's ssh to see the gateway go", func(s agent.ForwardState) bool { return !s.Listening })
	f.stop()
	for _, p := range below(os.Getpid()) {
		if b, _ := os.ReadFile("/proc/" + strconv.Itoa(p.pid) + "/comm"); strings.HasPrefix(string(b), "ssh") {
			t.Errorf("process %d (%s) is left below the test after Close and the forward's stop", p.pid, strings.TrimSpace(string(b)))
		}
	}
	if err := r.gw.Close(); err != nil {
		t.Errorf("a second Close: %v", err)
	}
}

// WriteDialHome makes the directory private, and the files the agent and the
// provider's check look for, with the modes ssh needs: the key 600, a
// known_hosts of one line naming the listener's address.
func TestWriteDialHomeLayout(t *testing.T) {
	r := newGatewayRig(t)
	r.watch.wantSSH = false // no ssh runs here
	nested := filepath.Join(r.base, "home", ".hpcburst", "dial-home")
	if err := r.gw.WriteDialHome(nested); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(nested); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("dial-home directory: %v, %v; want mode 700", fi, err)
	}
	if fi, err := os.Stat(filepath.Join(nested, "key")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("key: %v, %v; want mode 600", fi, err)
	}
	b, err := os.ReadFile(filepath.Join(nested, "known_hosts"))
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(string(b))
	_, port, _ := strings.Cut(r.gw.Addr(), ":")
	if len(f) != 3 || strings.Count(string(b), "\n") != 1 || f[0] != "[127.0.0.1]:"+port || f[1] != "ssh-ed25519" {
		t.Errorf("known_hosts %q: want one line \"[127.0.0.1]:%s ssh-ed25519 <base64>\"", b, port)
	}
	// Writing again replaces both files and keeps the modes.
	if err := r.gw.WriteDialHome(nested); err != nil {
		t.Errorf("a second WriteDialHome: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(nested, "key")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("rewritten key: %v, %v; want mode 600", fi, err)
	}
	loose := filepath.Join(r.base, "loose")
	if err := os.Mkdir(loose, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(loose, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := r.gw.WriteDialHome(loose); err == nil {
		t.Error("WriteDialHome into a directory with mode 755 succeeded: the key would be readable by others")
	}
	if err := r.gw.WriteDialHome("relative/dir"); err == nil {
		t.Error("WriteDialHome with a relative path succeeded")
	}
}

// StartGateway refuses what would give a gateway that proves nothing.
func TestStartGatewayRefusesBadConfig(t *testing.T) {
	RequireTools(t)
	loose := t.TempDir()
	if err := os.Chmod(loose, 0o755); err != nil {
		t.Fatal(err)
	}
	private := t.TempDir()
	for _, c := range []struct {
		name string
		cfg  GatewayConfig
		want string
	}{
		{"no agent port", GatewayConfig{Dir: private, AgentPort: 0, DialHomePort: 2}, "AgentPort 0 is not a port"},
		{"port above 65535", GatewayConfig{Dir: private, AgentPort: 1, DialHomePort: 65536}, "DialHomePort 65536 is not a port"},
		{"equal ports", GatewayConfig{Dir: private, AgentPort: 5, DialHomePort: 5}, "must differ"},
		{"loose directory", GatewayConfig{Dir: loose, AgentPort: 1, DialHomePort: 2}, "mode 700"},
		{"relative directory", GatewayConfig{Dir: "g", AgentPort: 1, DialHomePort: 2}, "absolute"},
	} {
		g, err := StartGateway(c.cfg)
		if err == nil {
			g.Close()
			t.Errorf("%s: StartGateway succeeded", c.name)
		} else if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q, want it to say %q", c.name, err, c.want)
		}
	}
}

// The key's line is the one in HBEP-6 (Dial-home modes, The key's line). This
// is a regression guard on the text; the tests above are what show that the
// line restricts.
func TestKeyLineIsHBEP6s(t *testing.T) {
	got := dialHomeKeyOptions(29523, 29527)
	want := `restrict,port-forwarding,permitopen="127.0.0.1:29523",permitopen="127.0.0.1:29527",permitlisten="127.0.0.1:1",command="/bin/false"`
	if got != want {
		t.Errorf("key options\n got %s\nwant %s", got, want)
	}
}
