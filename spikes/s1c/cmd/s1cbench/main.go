// Command s1cbench is the measurement tool of spike S1c (issue #66): tunnel capacity, head-of-line
// blocking and dead-tunnel detection for HBEP-6. Go standard library only.
//
//	s1cbench load    closed-loop streaming clients against an OpenAI-compatible engine, direct or
//	                 through frps's tcpmux HTTP CONNECT port; writes a JSON summary per cell
//	s1cbench hol     fast token streams plus one bulk (or slowly read) response on the same path;
//	                 reports the fast streams' inter-token gaps before, during and after the bulk
//	s1cbench bulk    the bulk engine: GET /bulk?bytes=N answers N bytes as fast as the path takes them
//	s1cbench probe   opens a new connection through the tunnel every interval and polls frps's admin
//	                 API; one CSV line per probe and per API state change (heartbeat measurements)
//	s1cbench shaper  a TCP proxy with ONE rate limit shared by all its connections (a shared
//	                 bottleneck), for the head-of-line control where streams use separate connections
//
// Every listener binds 127.0.0.1 only; a flag naming any other address is refused.
package main

import (
	"fmt"
	"os"
)

func main() {
	cmds := map[string]func([]string) error{
		"load":   runLoad,
		"hol":    runHOL,
		"bulk":   runBulk,
		"probe":  runProbe,
		"shaper": runShaper,
	}
	if len(os.Args) < 2 || cmds[os.Args[1]] == nil {
		fmt.Fprintln(os.Stderr, "usage: s1cbench load|hol|bulk|probe|shaper [flags]")
		os.Exit(2)
	}
	if err := cmds[os.Args[1]](os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "s1cbench %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}
