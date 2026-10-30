// Command s2probe is the test client of spike S2 (router inside an Instance).
//
//	s2probe stream     one streamed completion; records every inter-token gap
//	s2probe placement  multi-turn sessions through a router; where did each turn land
//	s2probe removal    remove a worker from discovery while a stream is in flight
//
// Worker placement is read from each simulator's own /metrics, never from the router.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: s2probe stream|placement|removal [flags]")
		os.Exit(64)
	}
	var code int
	switch os.Args[1] {
	case "stream":
		code = runStream(os.Args[2:])
	case "placement":
		code = runPlacement(os.Args[2:])
	case "removal":
		code = runRemoval(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand", os.Args[1])
		code = 64
	}
	os.Exit(code)
}
