// Command s16 is spike S16's harness (issue #412, HBEP-6 open question 11):
// open question 11's four items that need no GPU, on one host.
//
//	s16 setup    -dir D -n N ...   one configuration's files (spec, certificate, frps's and frpc's configurations)
//	s16 frpcconf -spec F -ready R  host-dialed mode: frpc's configurations, once the job sides listen
//	s16 admit    -spec F ...       the lifecycle controller's frps plugin (frpsplugin, unchanged) on a fixture Index
//	s16 frps     -spec F ...       frps, started and stopped the launcher's way; SIGUSR1 restarts it
//	s16 front    -spec F ...       the front (front.Run, unchanged) over a fake API holding the fixture
//	s16 load     -spec F ...       the closed-loop client, through the front or straight to frps's tcpmux port
//	s16 filler   -role R ...       bulk bytes through the capped link
//	s16 owner    -spec F ...       host-dialed mode: the relay owner (hostdial.Owner) with real ssh relays
//
// The node side (engine doubles, job sides, relay steps) is the test binary of
// ./node, and the login node (internal/rehearsal) that of ./login, because
// internal/agent/enginedouble and internal/rehearsal may be linked only by
// _test.go files.
package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: s16 setup|frpcconf|admit|frps|front|load|filler|owner [flags]")
		os.Exit(2)
	}
	cmds := map[string]func([]string) error{
		"setup": runSetup, "frpcconf": runFrpcConf, "admit": runAdmit, "frps": runFrps, "front": runFront,
		"load": runLoad, "filler": runFiller, "owner": runOwner,
	}
	run, ok := cmds[os.Args[1]]
	if !ok {
		fmt.Fprintf(os.Stderr, "s16: unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "s16:", err)
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
