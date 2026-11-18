package main

// hpcburst identity add|renew NAME: the agents' identities (HBEP-10, Identities and roles; issue #589,
// PR 2). The work is internal/launcher/identity.go; this file is the command line, the usage and the
// report, and the status lines about the agents' roles and identities.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// identityUsage is the help of hpcburst identity.
const identityUsage = `usage: hpcburst identity add NAME [--state-dir DIR] [--json]
       hpcburst identity renew NAME [--state-dir DIR] [--json]

Makes an agent's identity (add) or gives it a new token (renew). An identity is a ServiceAccount called
NAME in the namespace hpcburst-agents, and its credential is a token that the API server issues for 30
days, written into <state-dir>/identities/NAME.kubeconfig (mode 600, in a directory of mode 700) with this
host's loopback server and CA. Every ServiceAccount of that namespace holds the agent roles through the
group system:serviceaccounts:hpcburst-agents, so adding one makes no binding. NAME must be a DNS-1123 label.
add refuses a name that exists (use renew), and renew one that does not. Both print the kubeconfig's path
and the token's expiry as the API server answered, never the token, and need the host running. A token
cannot be revoked before it expires other than by deleting the ServiceAccount, so renew leaves the old one
valid until then.`

func cmdIdentity(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && (args[0] == "-h" || args[0] == "-help" || args[0] == "--help") {
		args = []string{"add", "-h"} // the flags of add and renew, which are the same, and exit 0, as guard does
	}
	if len(args) == 0 || (args[0] != "add" && args[0] != "renew") {
		fmt.Fprintln(stderr, identityUsage)
		return exitUsage
	}
	verb := args[0]
	fs := flags("identity "+verb, stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, identityUsage)
		fs.PrintDefaults()
	}
	var sd string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.BoolVar(&asJSON, "json", false, "print the result as JSON")
	name, code, done := parseWithName(fs, args[1:])
	if done {
		return code
	}
	if name == "" {
		fmt.Fprintf(stderr, "hpcburst identity %s: the identity's NAME is required\n", verb)
		return exitUsage
	}
	if err := launcher.ValidIdentityName(name); err != nil {
		fmt.Fprintf(stderr, "hpcburst identity %s: %v\n", verb, err)
		return exitUsage
	}
	o := launcher.IdentityOptions{Name: name, Log: stderr}
	var err error
	if o.StateDir, err = stateDir(sd); err != nil {
		return fail(stderr, err)
	}
	add := launcher.AddIdentity
	if verb == "renew" {
		add = launcher.RenewIdentity
	}
	res, err := add(ctx, o)
	if err != nil {
		return fail(stderr, err)
	}
	if asJSON {
		emitJSON(stdout, res)
		return exitOK
	}
	verbDone := "added"
	if res.Renewed {
		verbDone = "renewed"
	}
	fmt.Fprintf(stdout, "identity %s %s (%s)\nkubeconfig: %s\ntoken expires: %s\n", name, verbDone, res.Username, res.Kubeconfig, res.Expires.Format(time.RFC3339))
	return exitOK
}

// parseWithName parses the flags of a command that takes one positional NAME, which may come before
// the flags or after them (the flag package stops at the first argument that is not a flag). A second
// positional argument is a usage error.
func parseWithName(fs *flag.FlagSet, args []string) (name string, code int, done bool) {
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return "", exitOK, true
			}
			return "", exitUsage, true
		}
		if fs.NArg() == 0 {
			return name, 0, false
		}
		if name != "" {
			fmt.Fprintf(fs.Output(), "%s: unexpected argument %q\n", fs.Name(), fs.Arg(0))
			return "", exitUsage, true
		}
		name, rest = fs.Arg(0), fs.Args()[1:]
	}
}

// printAgents prints the status lines about the agents' roles and identities. Both are states, not
// faults: roles that differ are reset by the next start, and a rejected token by renew.
func printAgents(w io.Writer, r *launcher.StatusReport) {
	switch rs := r.AgentRBAC; {
	case rs == nil:
		return // not asked, or could not say: the reasons name the failure
	case rs.Present && rs.Writes:
		fmt.Fprintf(w, "agent roles: present (hpcburst-agent-read, hpcburst-agent and hpcburst-operator, in the manifest's form; the first two bound to the group %s)\n", launcher.AgentsGroup)
	case rs.Present:
		fmt.Fprintf(w, "agent roles: present, WRITES OFF (%s)\n", rs.Detail)
	default:
		fmt.Fprintf(w, "agent roles: DIFFER FROM THE MANIFEST (%s): what an agent may do is not what the framework installs; the next hpcburst up resets them\n", rs.Detail)
	}
	if len(r.Identities) == 0 {
		fmt.Fprintln(w, "agent identities: none (hpcburst identity add NAME makes one)")
	}
	for _, id := range r.Identities {
		exp := "expiry unknown"
		if id.Expires != nil {
			exp = "expires " + id.Expires.Format(time.RFC3339)
		}
		if id.Valid {
			fmt.Fprintf(w, "agent identity %s: valid, %s (%s)\n", id.Name, exp, id.Kubeconfig)
		} else {
			fmt.Fprintf(w, "agent identity %s: NOT VALID, %s (%s)\n", id.Name, exp, id.Detail)
		}
	}
}

// printControllerIdentities prints the status text of the controllers' own identities (HBEP-10, Audit; #660):
// one line each, with the token's expiry, and a WARNING line under one whose token has 30 days or fewer left.
// An expiring or rejected token is a state, not a fault: the next down and up issue new ones.
func printControllerIdentities(w io.Writer, r *launcher.StatusReport) {
	for _, s := range r.ControllerIdentities {
		fmt.Fprintln(w, launcher.ControllerIdentityLine(s))
	}
}
