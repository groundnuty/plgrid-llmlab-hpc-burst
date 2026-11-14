// Command s2crouter starts spike S2c's processes the way the allocation agent starts them (issue #62):
//
//	s2crouter worker ...  one vLLM worker under Apptainer, through agent.WorkerExec (worker.go)
//	s2crouter envoy ...   one Envoy arm from the agent's own configuration (envoy.go)
//	s2crouter relay ...   loopback TCP to Unix-socket relays for the vllm-router arms (relay.go)
//	s2crouter probe ...   the dead-worker probe behind the bound Envoy arm (probe.go)
//	s2crouter signal|stop the job's signals, through verified pidfds only (proc.go)
//
// It imports internal/agent and pkg/agentapi and changes nothing in them: every product behaviour it
// relies on is called, not copied, except the few steps of agent.StartEnvoy that an edited
// configuration cannot go through (envoy.go says which, and why).
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var code int
	switch os.Args[1] {
	case "worker":
		code = workerMain(os.Args[2:])
	case "envoy":
		code = envoyMain(os.Args[2:])
	case "relay":
		code = relayMain(os.Args[2:])
	case "probe":
		code = probeMain(os.Args[2:])
	case "signal":
		code = signalMain(os.Args[2:])
	case "stop":
		code = stopMain(os.Args[2:])
	default:
		usage()
	}
	os.Exit(code)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: s2crouter worker|envoy|relay|probe|signal|stop [flags]")
	os.Exit(2)
}

// fail prints an error and returns exit status 1.
func fail(format string, a ...any) int {
	fmt.Fprintf(os.Stderr, "s2crouter: "+format+"\n", a...)
	return 1
}
