package testenv

// Issue #559: on the development VM the ports 30000-32767 are the owner's root k3s's NodePort range. Its kube-proxy claims a NodePort there on
// every interface through iptables, with no listening process, so a test that binds or dials a fixed port there can meet a service that `ss`
// does not show (127.0.0.1:30444 answered HTTPS on 14 Nov 2026, and a front failed its health check on it). Every fixed test port block lies
// below 30000 (the science agent's ruling on #559). This test fails if a test or rehearsal port, base or block end, written in a port's syntax,
// lies in that range again.

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The host k3s's NodePort range: no test port may lie in it.
const nodePortFirst, nodePortLast = 30000, 32767

// portSyntax are the ways the repo's tests and scripts write a port, each capturing the number: an address, an identifier with port or base in
// its name given a value, the launcher's --port-base flag (a literal or strconv.Itoa of one) and PortsFromBase, strconv.Itoa of a number on a
// line that names a port, an expectation that names a port, and a block written as a range. A map of ports and a tuple assignment are read by
// portMapHits and tupleHits (#562's review).
var portSyntax = []*regexp.Regexp{
	regexp.MustCompile(`(?:127\.0\.0\.1|localhost|0\.0\.0\.0|\[::1\]):(\d{5})\b`),
	regexp.MustCompile(`(?i)\w*(?:port|base)\w*"?\s*(?::=|=|:)\s*(?:float64\()?"?(\d{5})\b`),
	regexp.MustCompile(`(?i)port-base"?,?\s*(?:strconv\.Itoa\()?"?(\d{5})\b`),
	regexp.MustCompile(`PortsFromBase\((\d{5})\)`),
	regexp.MustCompile(`(?i)\bports?\b.*strconv\.Itoa\((\d{5})\)|strconv\.Itoa\((\d{5})\).*\bports?\b`),
	regexp.MustCompile(`(?i)port\w*\s+is\s+(\d{5})\b`),
	blockRange,
}

// blockRange is a block written as a range. The range 30000-32767 itself is the hazard's own name ("the NodePort range (30000-32767)"),
// not a block of ours, and is not read as one (#562's review on main with #560); any other range is.
var blockRange = regexp.MustCompile(`\b(\d{5})-(\d{5})\b`)

// portMap is a map literal of ints on a line that names ports ("Ports: map[agentapi.RelayChannel]int{...}"), and mapValue each value in it.
var (
	portMap  = regexp.MustCompile(`(?i)\bports?\b.*map\[[^\]]*\]int\{([^}]*)\}`)
	mapValue = regexp.MustCompile(`:\s*(\d{5})\b`)
	// tuple is an assignment of several values at once ("c.o.StateDir, c.o.PortBase = l.dir, 28360").
	tuple = regexp.MustCompile(`^\s*([\w.]+(?:\s*,\s*[\w.]+)+)\s*:?=\s*(.+)$`)
	// portName is an identifier with port or base in its name.
	portName = regexp.MustCompile(`(?i)(?:port|base)`)
	// fiveDigits is a value that is a five-digit number and nothing else.
	fiveDigits = regexp.MustCompile(`^(\d{5})$`)
)

// portMapHits are the values of a map of ports on the line.
func portMapHits(line string) []string {
	var out []string
	for _, m := range portMap.FindAllStringSubmatch(line, -1) {
		for _, v := range mapValue.FindAllStringSubmatch(m[1], -1) {
			out = append(out, v[1])
		}
	}
	return out
}

// tupleHits are the values a tuple assignment gives the identifiers with port or base in their names, matched by position.
func tupleHits(line string) []string {
	m := tuple.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	names, values := strings.Split(m[1], ","), strings.Split(m[2], ",")
	if len(names) != len(values) {
		return nil
	}
	var out []string
	for i, n := range names {
		if v := fiveDigits.FindStringSubmatch(strings.TrimSpace(values[i])); v != nil && portName.MatchString(n) {
			out = append(out, v[1])
		}
	}
	return out
}

// nodePortHits are the numbers on a line, written as ports, that lie in the NodePort range.
func nodePortHits(line string) []int {
	var found []string
	for _, re := range portSyntax {
		for _, m := range re.FindAllStringSubmatch(line, -1) {
			if re == blockRange && m[1] == strconv.Itoa(nodePortFirst) && m[2] == strconv.Itoa(nodePortLast) {
				continue
			}
			found = append(found, m[1:]...)
		}
	}
	found = append(append(found, portMapHits(line)...), tupleHits(line)...)
	// Each value once per line: two syntaxes can read the same number ("--port-base", strconv.Itoa(N)).
	var hits []int
	seen := map[int]bool{}
	for _, g := range found {
		if n, err := strconv.Atoi(g); err == nil && n >= nodePortFirst && n <= nodePortLast && !seen[n] {
			seen[n] = true
			hits = append(hits, n)
		}
	}
	return hits
}

// scanned is whether the guard reads a file: the root module's test files, the hack scripts, and spike S2c, whose dry run binds a block. The
// other spikes are closed modules whose records name the ports they ran on (spikes/s16 binds 30130-30159; its rerun is the science agent's to
// plan, #559), and recorded results are data.
func scanned(rel string) bool {
	switch {
	case strings.Contains(rel, "/results/"), strings.HasPrefix(rel, "third_party/"):
		return false
	case strings.HasPrefix(rel, "spikes/s2c/"):
		return strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, ".sh") || strings.HasSuffix(rel, ".py")
	case strings.HasPrefix(rel, "spikes/"):
		return false
	case strings.HasPrefix(rel, "hack/") && strings.HasSuffix(rel, ".sh"):
		return true
	}
	return strings.HasSuffix(rel, "_test.go")
}

func TestNoTestPortLiesInTheHostK3sNodePortRange(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("no go.mod at %s: %v", root, err)
	}
	self := filepath.Join("internal", "testenv", "nodeport_test.go")
	files := 0
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == self || !scanned(filepath.ToSlash(rel)) {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files++
		for i, line := range strings.Split(string(b), "\n") {
			if hits := nodePortHits(line); len(hits) > 0 {
				t.Errorf("%s:%d: port %v lies in 30000-32767, the host k3s's NodePort range: move the block below 30000 (#559)", rel, i+1, hits)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 100 {
		t.Fatalf("the guard read %d files, too few for the repo's tests: it reads the wrong tree", files)
	}
}

// Each syntax is caught at the range's edges, and a number that is no port, or a port below the range, is not.
func TestTheNodePortGuardCatchesEachPortSyntax(t *testing.T) {
	for _, c := range []struct {
		line string
		hits int
	}{
		{`const frontPluginFixedAddr = "127.0.0.1:30050"`, 1},
		{`	bulkUpBase       = 30000`, 1},
		{`BASE_PORT=32767`, 1},
		{`PORT_BASE=${PORT_BASE:-30420}`, 0}, // a default written as a shell expansion is reported by hand, not by syntax (spikes/s2c's job.sh)
		{`"check port": float64(30160)`, 1},
		{`	o.PortBase = 30320`, 1},
		{`[]string{"--port-base", "30010"}`, 1},
		{`hpcburst up --port-base 31000`, 1},
		{`ports, err := PortsFromBase(30020)`, 1},
		{`}, "port_value is 30161"},`, 1},
		{`// #448's block (30280-30359, IMPL-COMMON)`, 2},
		{`// #448's block (28280-28359, IMPL-COMMON)`, 0},
		// The hazard's own name is not a block; a block inside it still is, and so is a range that only starts or ends at its edge.
		{`// below the NodePort range (30000-32767), whose ports the host's k3s can take`, 0},
		{`// as a Kubernetes NodePort (30000-32767) of the host's own k3s does`, 0},
		{`// a block of ours at 30040-30049 in a comment`, 2},
		{`// 30000-30019, the old keeper block`, 2},
		{`// 31000-32767`, 2},
		{`	bulkUpBase       = 29999`, 0},
		{`	bulkUpBase       = 32768`, 0},
		{`exec sleep 31337`, 0},
		{`// 32256 / 161 / 2 = 100.17 s`, 0},
		{`want := []int32{2221, 35078, 31724, 88788}`, 0},
		// #562's review: a map of ports, a tuple assignment, and strconv.Itoa.
		{`	o.Ports = map[agentapi.RelayChannel]int{control: 30000, serving: 30001}`, 2},
		{`		Ports:  map[agentapi.RelayChannel]int{agentapi.RelayControl: 28002, agentapi.RelayServing: 28003},`, 0},
		{`	counts := map[string]int{"tokens": 30000}`, 0},
		{`			c.o.StateDir, c.o.PortBase = l.dir, 30360`, 1},
		{`			c.o.StateDir, c.o.Tokens = l.dir, 30360`, 0},
		{`	r := cli(t, "up", "--state-dir", dir, "--port-base", strconv.Itoa(30020), "--controller-bin", bin,`, 1},
		{`	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(30500)) // the port`, 1},
		{`	s := strconv.Itoa(30500) // a count of tokens`, 0},
	} {
		if got := nodePortHits(c.line); len(got) != c.hits {
			t.Errorf("%q: hits %v, want %d", c.line, got, c.hits)
		}
	}
	// The scanned files: tests, the hack scripts and S2c, never results, other spikes or the vendored code.
	for _, c := range []struct {
		rel  string
		want bool
	}{
		{"internal/rehearsal/scenario/workers_test.go", true},
		{"internal/launcher/ports.go", false},
		{"hack/rehearsal.sh", true},
		{"spikes/s2c/scripts/test_job_status.sh", true},
		{"spikes/s2c/results/run1/listeners.txt", false},
		{"spikes/s16/cmd/s16/setup.go", false},
		{"third_party/forked/x/a_test.go", false},
	} {
		if got := scanned(c.rel); got != c.want {
			t.Errorf("scanned(%s) = %t, want %t", c.rel, got, c.want)
		}
	}
}
