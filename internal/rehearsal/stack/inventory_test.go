package stack

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// inventoryT keeps what the inventory reports.
type inventoryT struct {
	testing.TB
	errs []string
}

func (i *inventoryT) Errorf(format string, args ...any) {
	i.errs = append(i.errs, fmt.Sprintf(format, args...))
}

// The inventory, on a package of the test's own, so that each thing it must catch
// is a fixture here and not only a mutant of the real tables.
func TestCheckWatchInventory(t *testing.T) {
	const src = `package x

func guarded(r *rig) {
	r.syncWatch(stack.KindInstances|stack.KindAllocations, "x")
	r.watch.First(uid, at, match)
}

func withContext(ctx context.Context, r *rig) {
	r.syncWatch(ctx, stack.KindInstances, "x")
	r.watch.Of(uid)
}

func methodValue(r *rig) { check(r.watch.Allocations) }

func starts(r *rig) { r.watch = stack.StartWatch(t, state, ns, nil) }
`
	full := map[string]WatchClass{
		"guarded":     {Syncs: []string{"KindAllocations|KindInstances"}},
		"withContext": {Syncs: []string{"KindInstances"}},
		"methodValue": {Why: "waits itself"},
	}
	without := func(name string) map[string]WatchClass {
		out := map[string]WatchClass{}
		for k, v := range full {
			if k != name {
				out[k] = v
			}
		}
		return out
	}
	for name, c := range map[string]struct {
		extra string
		table map[string]WatchClass
		ctx   string
		says  string // empty: nothing reported
	}{
		"all classed": {table: full},
		"a reader with no class, by a method value": {table: without("methodValue"), says: "methodValue reads the watch"},
		"a removed catch-up": {table: map[string]WatchClass{
			"guarded":     {Syncs: []string{"KindAllocations|KindInstances"}},
			"withContext": {Syncs: []string{"KindInstances"}},
			"methodValue": {Syncs: []string{"KindInstances"}},
		}, says: "methodValue makes the catch-ups"},
		"other kinds than named": {table: map[string]WatchClass{
			"guarded":     {Syncs: []string{"KindInstances"}},
			"withContext": {Syncs: []string{"KindInstances"}},
			"methodValue": {Why: "waits itself"},
		}, says: "guarded makes the catch-ups"},
		"no reason given": {table: map[string]WatchClass{
			"guarded":     {Syncs: []string{"KindAllocations|KindInstances"}},
			"withContext": {Syncs: []string{"KindInstances"}},
			"methodValue": {},
		}, says: "does not say why"},
		"a stale entry": {table: map[string]WatchClass{
			"guarded":     {Syncs: []string{"KindAllocations|KindInstances"}},
			"withContext": {Syncs: []string{"KindInstances"}},
			"methodValue": {Why: "waits itself"},
			"gone":        {Why: "waits itself"},
		}, says: "gone, which no longer reads"},
		"a copy of the watch":  {extra: "func copies(r *rig) { w := r.watch; use(w) }\n", table: full, says: "cannot class"},
		"a context not passed": {table: full, ctx: "ctx", says: `with "" as its context`},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "x.go"), []byte(src+c.extra), 0o600); err != nil {
				t.Fatal(err)
			}
			rt := &inventoryT{TB: t}
			CheckWatchInventory(rt, dir, c.table, c.ctx)
			switch got := strings.Join(rt.errs, "\n"); {
			case c.says == "" && got != "":
				t.Errorf("reported %q, want nothing", got)
			case !strings.Contains(got, c.says):
				t.Errorf("reported %q, want it to say %q", got, c.says)
			}
		})
	}
}
