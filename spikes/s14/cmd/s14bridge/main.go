// Command s14bridge stands in for the relay's ssh client on the control-plane
// host (HBEP-6, Dial-home modes, host-dialed mode). The relay owner
// (internal/hostdial.Owner) starts it as it would start ssh, with pipes for
// its standard input and output, and runs the session's host end over them.
//
// It dials the node's stand-in for "the login node's sshd plus srun" over TCP,
// through the link emulator, sends the relay line the owner built (one line),
// then copies standard input to the connection and the connection to standard
// output until the node side closes. It carries none of ssh's encryption, so
// its CPU is a lower bound of a real relay's ssh client.
//
// Standard library only, so its resident memory is close to a Go runtime's
// minimum: the owner starts two per tunnel.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", "", "the node's relay acceptor, through the link emulator (127.0.0.1:port)")
	line := flag.String("line", "", "the relay line the owner built (agentapi.RelayLine)")
	flag.Parse()
	if err := check(*addr, *line); err != nil {
		fmt.Fprintln(os.Stderr, "s14bridge:", err)
		os.Exit(2)
	}
	c, err := net.DialTimeout("tcp4", *addr, 10*time.Second)
	if err != nil {
		// ssh's own exit code for a connection that fails.
		fmt.Fprintln(os.Stderr, "s14bridge: connect:", err)
		os.Exit(255)
	}
	conn := c.(*net.TCPConn)
	_ = conn.SetNoDelay(true)
	if _, err := io.WriteString(conn, *line+"\n"); err != nil {
		fmt.Fprintln(os.Stderr, "s14bridge: the relay line:", err)
		os.Exit(255)
	}
	go func() {
		_, _ = io.Copy(conn, os.Stdin)
		_ = conn.CloseWrite()
	}()
	_, _ = io.Copy(os.Stdout, conn)
	os.Exit(0)
}

// check refuses an address off loopback and a line that is not one line.
func check(addr, line string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("-addr %q: %w", addr, err)
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("-addr %q is not on 127.0.0.1", addr)
	}
	if line == "" || strings.ContainsAny(line, "\r\n") {
		return fmt.Errorf("-line must be one non-empty line")
	}
	return nil
}
