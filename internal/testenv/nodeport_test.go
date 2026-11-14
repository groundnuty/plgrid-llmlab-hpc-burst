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
// its name given a value, the launcher's --port-base flag and PortsFromBase, an expectation that names a port, and a block written as a range.
var portSyntax = []*regexp.Regexp{
	regexp.MustCompile(`(?:127\.0\.0\.1|localhost|0\.0\.0\.0|\[::1\]):(\d{5})\b`),
	regexp.MustCompile(`(?i)\w*(?:port|base)\w*"?\s*(?::=|=|:)\s*(?:float64\()?"?(\d{5})\b`),
	regexp.MustCompile(`(?i)port-base"?,?\s*"?(\d{5})\b`),
	regexp.MustCompile(`PortsFromBase\((\d{5})\)`),
	regexp.MustCompile(`(?i)port\w*\s+is\s+(\d{5})\b`),
	regexp.MustCompile(`\b(\d{5})-(\d{5})\b`),
}

// nodePortHits are the numbers on a line, written as ports, that lie in the NodePort range.
func nodePortHits(line string) []int {
	var hits []int
	for _, re := range portSyntax {
		for _, m := range re.FindAllStringSubmatch(line, -1) {
			for _, g := range m[1:] {
				if n, err := strconv.Atoi(g); err == nil && n >= nodePortFirst && n <= nodePortLast {
					hits = append(hits, n)
				}
			}
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
		{`	bulkUpBase       = 29999`, 0},
		{`	bulkUpBase       = 32768`, 0},
		{`exec sleep 31337`, 0},
		{`// 32256 / 161 / 2 = 100.17 s`, 0},
		{`want := []int32{2221, 35078, 31724, 88788}`, 0},
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
