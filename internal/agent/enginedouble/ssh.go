package enginedouble

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// EnvSSHDieAfter, when set to a duration, makes the fake ssh drop its forward
// and exit 255 after that long, as ssh does when the connection is lost, so a
// test can exercise the agent's reconnect loop.
const EnvSSHDieAfter = "HB_FAKE_SSH_DIE_AFTER"

// requiredSSHOptions are the options the dial-home forward must carry
// (HBEP-7, Registration): no prompt, no unpinned host, no stray identity.
var requiredSSHOptions = map[string]string{
	"batchmode":             "yes",
	"exitonforwardfailure":  "yes",
	"stricthostkeychecking": "yes",
	"identitiesonly":        "yes",
}

// sshArgs are the OpenSSH arguments the fake understands.
type sshArgs struct {
	noCommand   bool
	options     map[string]string // lower-cased name -> value
	identity    string
	port        string
	forwards    []string
	destination string
}

// parseSSHArgs accepts "-N", "-o K=V" (or "-oK=V"), "-i file", "-p port",
// "-L spec" and the destination, and refuses any other option.
func parseSSHArgs(args []string) (sshArgs, error) {
	a := sshArgs{options: map[string]string{}}
	need := func(i *int, flag string) (string, error) {
		if *i+1 >= len(args) {
			return "", fmt.Errorf("option requires an argument -- %s", flag)
		}
		*i++
		return args[*i], nil
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			if a.destination != "" {
				return a, fmt.Errorf("a command after the destination %q: the fake runs only forwards (-N)", a.destination)
			}
			a.destination = arg
			continue
		}
		flag := arg[:2]
		val := arg[2:]
		var err error
		switch flag {
		case "-N":
			a.noCommand = true
		case "-T", "-n", "-q", "-v":
		case "-o", "-i", "-p", "-L":
			if val == "" {
				if val, err = need(&i, flag); err != nil {
					return a, err
				}
			}
			switch flag {
			case "-o":
				name, v, ok := strings.Cut(val, "=")
				if !ok {
					return a, fmt.Errorf("bad configuration option %q: the fake takes -o Name=value", val)
				}
				a.options[strings.ToLower(name)] = v
			case "-i":
				a.identity = val
			case "-p":
				a.port = val
			case "-L":
				a.forwards = append(a.forwards, val)
			}
		default:
			return a, fmt.Errorf("unknown option %q", arg)
		}
	}
	return a, nil
}

// validate says what is wrong with the arguments, or nil.
func (a sshArgs) validate() error {
	if !a.noCommand {
		return fmt.Errorf("no -N: the fake only forwards")
	}
	for name, want := range requiredSSHOptions {
		if got := a.options[name]; !strings.EqualFold(got, want) {
			return fmt.Errorf("option %s must be %s, got %q", name, want, got)
		}
	}
	kh := a.options["userknownhostsfile"]
	if kh == "" {
		return fmt.Errorf("option UserKnownHostsFile is missing")
	}
	if _, err := os.Stat(kh); err != nil {
		return fmt.Errorf("UserKnownHostsFile %q: %v", kh, err)
	}
	if a.identity == "" {
		return fmt.Errorf("no -i identity file")
	}
	if _, err := os.Stat(a.identity); err != nil {
		return fmt.Errorf("identity file %q: %v", a.identity, err)
	}
	if len(a.forwards) != 1 {
		return fmt.Errorf("want exactly one -L, got %d", len(a.forwards))
	}
	if !strings.Contains(a.destination, "@") {
		return fmt.Errorf("destination %q is not user@host", a.destination)
	}
	return nil
}

// parseForward reads "-L <local-socket-path>:<host>:<port>".
func parseForward(spec string) (socket, host, port string, err error) {
	i := strings.LastIndex(spec, ":")
	if i < 0 {
		return "", "", "", fmt.Errorf("-L %q is not <socket>:<host>:<port>", spec)
	}
	port, rest := spec[i+1:], spec[:i]
	j := strings.LastIndex(rest, ":")
	if j < 1 {
		return "", "", "", fmt.Errorf("-L %q is not <socket>:<host>:<port>", spec)
	}
	socket, host = rest[:j], rest[j+1:]
	if n, perr := strconv.Atoi(port); perr != nil || n < 1 || n > 65535 || host == "" {
		return "", "", "", fmt.Errorf("-L %q has no valid host and port", spec)
	}
	return socket, host, port, nil
}

// SSHMain is the fake ssh: it records its arguments, validates them, then
// listens on the -L socket and forwards each connection to the TCP address
// <host>:<port>, as "ssh -N -L" does. args are the arguments after the program
// name. It exits 255 on a refusal, and when EnvSSHDieAfter ends; on TERM it
// removes the socket and returns 0.
func SSHMain(args []string) int {
	if p := os.Getenv(EnvSSHLog); p != "" {
		if err := (&logFile{path: p}).write(map[string]any{"argv": args, "pid": os.Getpid()}); err != nil {
			fmt.Fprintln(os.Stderr, "enginedouble: log:", err)
		}
	}
	refuse := func(err error) int {
		fmt.Fprintln(os.Stderr, "ssh:", err)
		return 255
	}
	a, err := parseSSHArgs(args)
	if err == nil {
		err = a.validate()
	}
	if err != nil {
		return refuse(err)
	}
	socket, host, port, err := parseForward(a.forwards[0])
	if err != nil {
		return refuse(err)
	}
	var die <-chan time.Time
	if v := os.Getenv(EnvSSHDieAfter); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return refuse(fmt.Errorf("%s: %w", EnvSSHDieAfter, err))
		}
		die = time.After(d)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)

	_ = os.Remove(socket) // a stale socket, as with StreamLocalBindUnlink
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return refuse(fmt.Errorf("Could not request local forwarding: %w", err))
	}
	f := &forwarder{target: net.JoinHostPort(host, port), conns: map[net.Conn]struct{}{}}
	go f.serve(ln)
	defer func() { _ = os.Remove(socket) }()

	select {
	case <-sigs:
		_ = ln.Close()
		f.closeAll()
		return 0
	case <-die:
		_ = ln.Close()
		f.closeAll()
		fmt.Fprintln(os.Stderr, "ssh: Connection closed by remote host")
		return 255
	}
}

// forwarder copies each accepted connection to the target and back.
type forwarder struct {
	target string
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
}

func (f *forwarder) serve(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go f.forward(c)
	}
}

func (f *forwarder) track(c net.Conn) {
	f.mu.Lock()
	f.conns[c] = struct{}{}
	f.mu.Unlock()
}

func (f *forwarder) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.conns {
		_ = c.Close()
	}
}

func (f *forwarder) forward(local net.Conn) {
	defer local.Close()
	remote, err := net.DialTimeout("tcp", f.target, 5*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ssh: channel open failed:", err)
		return
	}
	defer remote.Close()
	f.track(local)
	f.track(remote)
	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
		done <- struct{}{}
	}
	go pipe(remote, local)
	go pipe(local, remote)
	<-done
	<-done
}
