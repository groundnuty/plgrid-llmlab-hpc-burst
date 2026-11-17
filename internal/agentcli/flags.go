package agentcli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"
)

// options are the flags every command takes, and the ones of the three that create an Operation.
type options struct {
	kubeconfig string
	namespace  string
	asJSON     bool

	key     string
	purpose string
	timeout time.Duration
}

// defaultTimeout is how long scale, end and release wait for the plan.
const defaultTimeout = 2 * time.Minute

// newFlags makes a flag set that reports on stderr and never exits. create adds the flags of the commands
// that create an Operation.
func newFlags(name string, stderr io.Writer, create bool) (*flag.FlagSet, *options) {
	fs := flag.NewFlagSet("hpcburst "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	o := &options{}
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "the caller's kubeconfig (default $KUBECONFIG; never ~/.kube/config)")
	fs.StringVar(&o.namespace, "n", "", "the namespace (default: the kubeconfig's context namespace, else default)")
	fs.StringVar(&o.namespace, "namespace", "", "the namespace (same as -n)")
	fs.BoolVar(&o.asJSON, "json", false, "print the result as JSON (docs/agent-cli.md)")
	if create {
		fs.StringVar(&o.key, "key", "", "the Operation's name, its idempotency key (default: op- and 10 random characters)")
		fs.StringVar(&o.purpose, "purpose", "", "why, as free text of at most 256 characters")
		fs.DurationVar(&o.timeout, "timeout", defaultTimeout, "how long to wait for the plan")
	}
	return fs, o
}

// parseArgs parses the flags of a command whose positional arguments may come before, between or after
// them, which the flag package alone does not allow: it stops at the first argument that is not a flag.
// done is true when the command is to stop with code (help, or a usage error).
func parseArgs(fs *flag.FlagSet, args []string) (pos []string, code int, done bool) {
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, exitOK, true
			}
			return nil, exitUsage, true
		}
		if fs.NArg() == 0 {
			return pos, 0, false
		}
		pos = append(pos, fs.Arg(0))
		rest = fs.Args()[1:]
	}
}

// oneArg parses a command that takes exactly one positional argument, named what in the message.
func oneArg(fs *flag.FlagSet, args []string, what string) (arg string, code int, done bool) {
	pos, code, done := parseArgs(fs, args)
	if done {
		return "", code, true
	}
	if len(pos) != 1 {
		fmt.Fprintf(fs.Output(), "%s: exactly one %s is required\n", fs.Name(), what)
		return "", exitUsage, true
	}
	return pos[0], 0, false
}
