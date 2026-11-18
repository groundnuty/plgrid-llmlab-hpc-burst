package agentcli

import (
	"context"
	"flag"
	"io"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/inventory"
)

// cmdInventory is hpcburst inventory [-n NS | -A] [--owner USER] [--json] (HBEP-10, Inventory; issue #662).
// It is one read-only query: the lister of internal/inventory reads through the caller's client, which is
// the one every command here uses, so the credential rules are the same, and the printer writes the rows.
// The command's clock is read once, after the caller is known, and handed to the lister.
func (rt *cli) cmdInventory(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, o := newFlags("inventory", stderr, false)
	all := fs.Bool("A", false, "every namespace the caller can list (one cluster-wide list per kind)")
	fs.BoolVar(all, "all-namespaces", false, "every namespace (same as -A)")
	owner := fs.String("owner", "", "only the objects of this owner: the user name as the API server reports it")
	pos, code, done := parseArgs(fs, args)
	if done {
		return code
	}
	if len(pos) > 0 {
		return failf(stderr, exitUsage, "inventory: unexpected argument %q", pos[0])
	}
	// An empty --owner would list every owner's objects under a flag that says it selects one: a script whose
	// variable is empty must fail, not widen.
	ownerGiven := false
	fs.Visit(func(f *flag.Flag) { ownerGiven = ownerGiven || f.Name == "owner" })
	if ownerGiven && *owner == "" {
		return failf(stderr, exitUsage, "inventory: --owner needs a user name")
	}
	if *all && o.namespace != "" {
		return failf(stderr, exitUsage, "inventory: -n and -A together: give a namespace or every namespace, not both")
	}
	cl, ns, code := rt.open(ctx, o, stderr)
	if code != exitOK {
		return code
	}
	inv, err := inventory.List(ctx, cl.c, inventory.Options{Namespace: ns, AllNamespaces: *all, Owner: *owner}, rt.now())
	if err != nil {
		return failf(stderr, exitFail, "%v", err)
	}
	if o.asJSON {
		return emitCode(stderr, inventory.WriteJSON(stdout, inv))
	}
	return emitCode(stderr, inventory.WriteText(stdout, inv))
}
