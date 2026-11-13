package quickstart

// The operator's guide's section on the router and several workers (issue #519) is held to what it
// describes. The existing drift tests cover its commands (TestOperationsCommands), its capitalised
// names (TestOperationsNamesAreConstants), its events (TestOperationsGuideFollowsTheDriftGuard) and
// its links; this file covers what they cannot see:
//   - the fields it names, lowercase ones the names test skips, are properties of the CRDs' stored
//     schemas, so a renamed or misspelt one fails; every property of an Instance's workers[] entry is
//     named, so a field added to the entry is not left undocumented;
//   - the numbers it states, the count's bounds and the entries status.restarts keeps, are the ones in
//     the schemas, which the guide's text is read against, never the other way round;
//   - the refusal messages it quotes are in the code that raises them;
//   - its turn-on and turn-off blocks differ in the one flag, and the turn-on block is the quickstart's
//     router block, so "the same flags as before" is what the blocks say.
// Each check is run on the real guide, which must pass, and on a copy with one change that it must
// reject, so none of them passes by finding nothing.

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const routerHeading = "### The router and several workers (beta)\n"

// routerSectionOf is the text of the procedure section of a copy of the guide, up to the next heading.
func routerSectionOf(doc string) string {
	i := strings.Index(doc, "\n"+routerHeading)
	if i < 0 {
		return ""
	}
	rest := doc[i+1+len(routerHeading):]
	if j := regexp.MustCompile(`(?m)^#{1,3} `).FindStringIndex(rest); j != nil {
		rest = rest[:j[0]]
	}
	return rest
}

// schemaAt walks a CRD schema along a dotted path, where "[]" is a list's items. It returns nil when
// a part is not a property.
func schemaAt(root map[string]any, path string) map[string]any {
	s := root
	for _, part := range strings.Split(path, ".") {
		if part == "workers[]" || part == "restarts[]" {
			name := strings.TrimSuffix(part, "[]")
			props, _ := s["properties"].(map[string]any)
			list, ok := props[name].(map[string]any)
			if !ok {
				return nil
			}
			if s, ok = list["items"].(map[string]any); !ok {
				return nil
			}
			continue
		}
		props, _ := s["properties"].(map[string]any)
		next, ok := props[part].(map[string]any)
		if !ok {
			return nil
		}
		s = next
	}
	return s
}

// routerFields are the fields the section must name, each with the kind whose schema holds it.
var routerFields = []struct{ path, kind string }{
	{"spec.workersPerInstance", "modeldeployment"},
	{"spec.launch.router", "instance"},
	{"spec.launch.workers", "instance"},
	{"spec.launch.maxNumSeqs", "instance"},
	{"status.healthyWorkers", "instance"},
	{"status.workers[]", "instance"},
	{"status.restarts", "instance"},
}

// routerQuotes are the refusal messages the section quotes. doc is how the section writes one, with a placeholder where the
// message has a number or a name; code is the fragment of the source file that raises it, the message's format as written there.
// A refusal that names a limit by its field has the field's constant spliced into both, so the word the guide uses is the constant's.
var routerQuotes = []struct{ doc, code, file string }{
	{"needs the lifecycle controller's --instance-router envoy", "needs the lifecycle controller's --instance-router envoy", "internal/lifecycle/reconcile.go"},
	{"(workers + 2) x attempts", "(workers + 2) x attempts", "internal/lifecycle/topology.go"},
	{"above the <m> per node of partition", "above the %d per node of partition", "internal/lifecycle/topology.go"},
	{fmt.Sprintf("above the %s <q> of QOS <name> on Provider <provider>", v1alpha1.LimitMaxGPUsPerUser), "above the %s %d of QOS %s on Provider %s", "internal/lifecycle/topology.go"},
	{"Envoy cannot run bare here", "Envoy cannot run bare here", "internal/agent/envoyglibc.go"},
	{"router did not start", "router did not start", "internal/agent/router.go"},
}

// routerProblems sets a copy of the guide against the CRDs, the code and the quickstart's router block,
// and returns one line for each difference.
func routerProblems(t *testing.T, doc string, kinds crdKinds, quickstartUp []string) []string {
	t.Helper()
	sec := routerSectionOf(doc)
	if sec == "" {
		return []string{"the guide has no section " + strings.TrimSpace(routerHeading)}
	}
	var out []string
	span := regexp.MustCompile("`([^`\n]+)`")
	named := map[string]bool{}
	for _, m := range span.FindAllStringSubmatch(sec, -1) {
		named[m[1]] = true
	}
	// A dotted path the section names is a property of its kind's schema; the ones it must name are
	// there.
	for _, f := range routerFields {
		k := kinds[f.kind]
		if k == nil {
			out = append(out, "config/crd has no "+f.kind)
			continue
		}
		if !named[f.path] {
			out = append(out, fmt.Sprintf("the section does not name %s", f.path))
		}
		if schemaAt(k.schema, f.path) == nil {
			out = append(out, fmt.Sprintf("%s is not in the %s schema", f.path, f.kind))
		}
	}
	// Every dotted spec or status path the section names resolves in the ModelDeployment's or the
	// Instance's schema, so a misspelt one, in whatever part of the path, is a problem of its own.
	fieldSpan := regexp.MustCompile(`^(spec|status)\.[A-Za-z0-9_.\[\]]+$`)
	for n := range named {
		if !fieldSpan.MatchString(n) {
			continue
		}
		found := false
		for _, kind := range []string{"modeldeployment", "instance"} {
			if k := kinds[kind]; k != nil && schemaAt(k.schema, n) != nil {
				found = true
			}
		}
		if !found {
			out = append(out, fmt.Sprintf("the section names %s, which is in neither the ModelDeployment's nor the Instance's schema", n))
		}
	}
	// Every property of a worker's entry is named, and each name is one.
	if w := kinds["instance"]; w != nil {
		if items := schemaAt(w.schema, "status.workers[]"); items != nil {
			props, _ := items["properties"].(map[string]any)
			for name := range props {
				if !named[name] {
					out = append(out, fmt.Sprintf("the section does not name %s, a property of an Instance's workers[] entry", name))
				}
			}
		}
	}
	// The count's bounds are the schema's.
	if md := kinds["modeldeployment"]; md != nil {
		if f := schemaAt(md.schema, "spec.workersPerInstance"); f != nil {
			lo, hi := numberOf(f["minimum"]), numberOf(f["maximum"])
			if want := fmt.Sprintf("from %d to %d", lo, hi); !strings.Contains(sec, want) {
				out = append(out, fmt.Sprintf("the section does not say workersPerInstance is %q, as its schema bounds it", want))
			}
		}
	}
	// The restarts status keeps are the schema's.
	if in := kinds["instance"]; in != nil {
		if f := schemaAt(in.schema, "status.restarts"); f != nil {
			if want := fmt.Sprintf("which holds %d entries", numberOf(f["maxItems"])); !strings.Contains(sec, want) {
				out = append(out, fmt.Sprintf("the section does not say status.restarts %q, as its schema bounds it", want))
			}
		}
	}
	// The quoted refusals are the code's.
	for _, q := range routerQuotes {
		if !strings.Contains(sec, q.doc) {
			out = append(out, fmt.Sprintf("the section does not quote %q", q.doc))
		}
		if !strings.Contains(readRepoFile(t, q.file), q.code) {
			out = append(out, fmt.Sprintf("%s does not hold %q, which the section's %q stands for", q.file, q.code, q.doc))
		}
	}
	return append(out, routerBlockProblems(sec, quickstartUp)...)
}

// numberOf reads a schema number, which the CRD's YAML gives as a float.
func numberOf(v any) int {
	f, _ := v.(float64)
	return int(f)
}

// routerBlockProblems checks the section's blocks: the turn-on block fetches Envoy's build, stops the
// host and starts it with the flag, as the quickstart's router block does; the turn-off block stops
// it and starts it again without the flag, with every other word the same.
func routerBlockProblems(sec string, quickstartUp []string) []string {
	var ups [][]string
	var fetch, downs int
	for _, m := range opsFence.FindAllStringSubmatch(sec, -1) {
		for _, l := range joinContinuations(m[3]) {
			l = strings.TrimSpace(l)
			switch {
			case strings.HasPrefix(l, "hack/fetch-envoy.sh --agent-builds"):
				fetch++
			case strings.HasPrefix(l, "bin/hpcburst down"):
				downs++
			case strings.HasPrefix(l, "bin/hpcburst up"):
				ups = append(ups, strings.Fields(l))
			}
		}
	}
	var out []string
	if len(ups) != 2 || fetch != 1 || downs != 2 {
		return []string{fmt.Sprintf("the section holds %d up lines, %d fetch lines and %d down lines, want 2, 1 and 2", len(ups), fetch, downs)}
	}
	on, off := ups[0], ups[1]
	if i := slices.Index(on, "--instance-router"); i < 0 || i+1 >= len(on) || on[i+1] != "envoy" {
		out = append(out, "the turn-on up line has no --instance-router envoy")
	}
	if slices.Contains(off, "--instance-router") {
		out = append(out, "the turn-off up line still has --instance-router")
	}
	if !slices.Equal(withoutFlag(on), withoutFlag(off)) {
		out = append(out, fmt.Sprintf("the turn-on and turn-off up lines differ in more than the flag:\n%q\n%q", on, off))
	}
	if !slices.Equal(on, quickstartUp) {
		out = append(out, fmt.Sprintf("the turn-on up line is not the quickstart's router block's:\n%q\n%q", on, quickstartUp))
	}
	return out
}

// withoutFlag drops --instance-router and its value from an up line.
func withoutFlag(words []string) []string {
	var out []string
	for i := 0; i < len(words); i++ {
		if words[i] == "--instance-router" {
			i++
			continue
		}
		out = append(out, words[i])
	}
	return out
}

// quickstartRouterUp is the up line of the quickstart's router block, as words.
func quickstartRouterUp(t *testing.T) []string {
	t.Helper()
	bl := fence.FindAllStringSubmatch(routerSection(t), -1)
	if len(bl) != 1 {
		t.Fatalf("the quickstart's router section holds %d blocks, want one", len(bl))
	}
	for _, l := range joinContinuations(bl[0][2]) {
		if l = strings.TrimSpace(l); strings.HasPrefix(l, "bin/hpcburst up") {
			return strings.Fields(l)
		}
	}
	t.Fatal("the quickstart's router block has no up line")
	return nil
}

func TestOperationsRouterSectionFollowsTheSchemasAndTheCode(t *testing.T) {
	doc := readRepoFile(t, opsGuide)
	if problems := routerProblems(t, doc, readCRDNames(t), quickstartRouterUp(t)); len(problems) > 0 {
		t.Errorf("the router section differs from what it describes:\n%s", strings.Join(problems, "\n"))
	}
}

// Each check rejects its own wrong copy: one change to the guide, and the problem it must name.
func TestOperationsRouterChecksFailOnAMutant(t *testing.T) {
	doc := readRepoFile(t, opsGuide)
	kinds, up := readCRDNames(t), quickstartRouterUp(t)
	for _, c := range []struct{ name, old, new, want string }{
		{"a misspelt count field", "`spec.workersPerInstance`, from 1 to 8", "`spec.workersPerInstanse`, from 1 to 8", "workersPerInstanse"},
		{"a field renamed away", "`status.healthyWorkers`: how many", "`status.healthyWorker`: how many", "status.healthyWorker"},
		{"a worker property left out", "`routerAdmittedGeneration`", "`routerAdmitted`", "routerAdmittedGeneration"},
		{"a wrong bound", "from 1 to 8", "from 1 to 9", "from 1 to 8"},
		{"a wrong restart bound", "which holds 32 entries", "which holds 16 entries", "which holds 32 entries"},
		{"a refusal no code raises", "Envoy cannot run bare here", "Envoy cannot run here", "Envoy cannot run bare here"},
		{"a QOS refusal worded as no code words it", "of QOS <name> on Provider <provider>", "of the QOS <name> on Provider <provider>", "above the maxGpusPerUser <q> of QOS <name> on Provider <provider>"},
		{"a turn-off that keeps the flag", "    --front-mode host-dialed\n  ```\n\n  - New Instances", "    --front-mode host-dialed --instance-router envoy\n  ```\n\n  - New Instances", "still has --instance-router"},
		{"a turn-on that is not the quickstart's", "--front-mode host-dialed --instance-router envoy", "--front-mode ssh --instance-router envoy", "not the quickstart's"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if strings.Count(doc, c.old) != 1 {
				t.Fatalf("the mutant's text occurs %d times in the guide, want once: the mutant would say nothing", strings.Count(doc, c.old))
			}
			problems := routerProblems(t, strings.Replace(doc, c.old, c.new, 1), kinds, up)
			if !strings.Contains(strings.Join(problems, "\n"), c.want) {
				t.Errorf("the check did not name %q for this change; it said %q", c.want, problems)
			}
		})
	}
}
