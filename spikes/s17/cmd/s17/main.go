// Command s17 is spike S17's harness (issue #413, HBEP-6 open question 12):
// bulk transfers beside token streams in one Instance's tunnel, after spike S14's harness.
//
//	s17 certs    -dir D                 a throwaway certificate for the dial-home listener
//	s17 listener -listen A -frps B ...  the dial-home listener: TLS, then the /~!frp passthrough
//	s17 owner    -spec F ...            the relay owner (internal/hostdial.Owner) for N Allocations
//	s17 load     -spec F ...            token streams beside bulk transfers, through the front's tcpmux dials
//	s17 many     -spec F ...            many Instances, each with token streams and a bulk request every interval
//
// The node side (engine doubles, the agents' job sides and the relay steps) is
// the test binary of ./node, because the engine double may be linked only by
// _test.go files (internal/agent/enginedouble's import guard).
package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: s17 certs|listener|owner|load|many [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "certs":
		err = runCerts(os.Args[2:])
	case "listener":
		err = runListener(os.Args[2:])
	case "owner":
		err = runOwner(os.Args[2:])
	case "load":
		err = runLoad(os.Args[2:])
	case "many":
		err = runMany(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "s17:", err)
		os.Exit(1)
	}
}

// requireLoopback refuses any listen or target address that is not on 127.0.0.1.
func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("address %q: %w", addr, err)
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("address %q is not on 127.0.0.1 (loopback only)", addr)
	}
	return nil
}
