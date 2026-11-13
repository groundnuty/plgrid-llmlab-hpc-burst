package main

import (
	"bytes"
	"path/filepath"
	"slices"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// The expected values here come from HBEP-5 (bounded loads at hash_balance_factor 125, the field's
// place in Envoy's v3 Cluster: common_lb_config.consistent_hashing_lb_config) and the harness contract
// (one field changed, nothing else), not from the code under test.

// render4 is the agent's own rendering for four workers.
func render4(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	c := agent.EnvoyConfig{RunDir: dir, ReadinessPort: 40000}
	for _, n := range []string{"worker-0", "worker-1", "worker-2", "worker-3"} {
		c.Workers = append(c.Workers, agent.EnvoyWorker{Name: n, Socket: filepath.Join(dir, n+".sock")})
	}
	b, err := agent.RenderEnvoyConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.CheckEnvoyConfig(b, c); err != nil {
		t.Fatalf("the agent's guard refuses its own rendering: %v", err)
	}
	return b
}

// withFactor is a hand-written bootstrap of the workers' cluster that sets the bound, as #518's render
// will, and one without.
const withFactor = `{"static_resources":{"clusters":[{"name":"workers","lb_policy":"RING_HASH","common_lb_config":{"healthy_panic_threshold":{"value":0},"consistent_hashing_lb_config":{"hash_balance_factor":125}}}]}}`
const withoutFactor = `{"static_resources":{"clusters":[{"name":"workers","lb_policy":"RING_HASH","common_lb_config":{"healthy_panic_threshold":{"value":0}}}]}}`

func TestBoundOnARenderThatSetsIt(t *testing.T) {
	// The arm's bound equals the render's: the product's own bytes, unchanged.
	out, changed, err := applyBound([]byte(withFactor), 125, false)
	if err != nil || changed || !bytes.Equal(out, []byte(withFactor)) {
		t.Fatalf("bound 125 on a render of 125: changed=%v err=%v, want the render's own bytes", changed, err)
	}
	// No bound: exactly the one field deleted.
	out, changed, err = applyBound([]byte(withFactor), 0, true)
	if err != nil || !changed {
		t.Fatalf("none on a render of 125: changed=%v err=%v", changed, err)
	}
	if f, err := factorOf(out); err != nil || f != nil {
		t.Fatalf("none: factor %v err %v, want none", f, err)
	}
	if d, _ := diffPaths([]byte(withFactor), out); !slices.Equal(d, []string{boundPath}) {
		t.Fatalf("none: diff %q, want only %s", d, boundPath)
	}
	// Another factor: the one field set.
	out, changed, _ = applyBound([]byte(withFactor), 150, false)
	if f, _ := factorOf(out); !changed || f == nil || *f != 150 {
		t.Fatalf("150 on a render of 125: changed=%v factor=%v", changed, f)
	}
}

func TestBoundOnARenderWithout(t *testing.T) {
	out, changed, err := applyBound([]byte(withoutFactor), 0, true)
	if err != nil || changed || !bytes.Equal(out, []byte(withoutFactor)) {
		t.Fatalf("none on a render with no bound: changed=%v err=%v, want the render's own bytes", changed, err)
	}
	out, changed, err = applyBound([]byte(withoutFactor), 125, false)
	if err != nil || !changed {
		t.Fatalf("125 on a render with no bound: changed=%v err=%v", changed, err)
	}
	if f, _ := factorOf(out); f == nil || *f != 125 {
		t.Fatalf("125: factor %v", f)
	}
	// The parent object did not exist: the diff still names the one leaf added.
	d, _ := diffPaths([]byte(withoutFactor), out)
	if !slices.Equal(d, []string{boundPath}) {
		t.Fatalf("125 on no parent: diff %q", d)
	}
}

// The agent's real rendering, whatever main holds: before #518 it sets no bound, after it 125
// (HBEP-5). Either way the bound arm and the no-bound arm differ from it in that one place only, and the
// arm that matches the render runs the product's bytes unchanged.
func TestBoundOnTheAgentsRendering(t *testing.T) {
	r := render4(t)
	cur, err := factorOf(r)
	if err != nil {
		t.Fatal(err)
	}
	if cur != nil && *cur != 125 {
		t.Fatalf("the agent renders hash_balance_factor %d; HBEP-5 says 125", *cur)
	}
	b125, changed125, err := applyBound(r, 125, false)
	if err != nil {
		t.Fatal(err)
	}
	none, changedNone, err := applyBound(r, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if cur != nil {
		// #518 is in: the bound arm is the product's rendering, byte for byte.
		if changed125 || !bytes.Equal(b125, r) {
			t.Fatal("the agent renders the bound, yet the bound arm's bytes differ from the rendering")
		}
		if !changedNone {
			t.Fatal("the no-bound arm left the bound in place")
		}
	} else if changedNone || !changed125 {
		t.Fatalf("before #518: none changed=%v (want false), 125 changed=%v (want true)", changedNone, changed125)
	}
	for name, b := range map[string][]byte{"bound": b125, "none": none} {
		d, err := diffPaths(r, b)
		if err != nil {
			t.Fatal(err)
		}
		if len(d) > 1 || (len(d) == 1 && d[0] != boundPath) {
			t.Errorf("%s arm: diff from the agent's rendering %q, want at most the bound's path", name, d)
		}
	}
}

func TestDiffPaths(t *testing.T) {
	cases := []struct {
		a, b string
		want []string
	}{
		{`{"x":1,"y":{"z":[1,2]}}`, `{"x":1,"y":{"z":[1,2]}}`, []string{}},
		{`{"x":1}`, `{"x":2}`, []string{"x"}},
		{`{"x":1}`, `{"x":1,"w":0}`, []string{"w"}},
		{`{"y":{"z":[1,2]}}`, `{"y":{"z":[1,3]}}`, []string{"y.z[1]"}},
		{`{"y":{"z":[1,2]}}`, `{"y":{"z":[1]}}`, []string{"y.z[1]"}},
		{`{"y":{}}`, `{"y":{"k":"v"}}`, []string{"y.k"}},
		{`{}`, `{"y":{"k":{"j":1}}}`, []string{"y.k.j"}},
		{`{"y":{"k":{}}}`, `{}`, []string{"y.k"}},
	}
	for _, c := range cases {
		got, err := diffPaths([]byte(c.a), []byte(c.b))
		if err != nil || !slices.Equal(got, c.want) || got == nil {
			t.Errorf("diff(%s, %s) = %q, %v; want %q", c.a, c.b, got, err, c.want)
		}
	}
}

func TestParseBound(t *testing.T) {
	if f, none, err := parseBound("125"); err != nil || none || f != 125 {
		t.Errorf("125: %d %v %v", f, none, err)
	}
	if _, none, err := parseBound("none"); err != nil || !none {
		t.Errorf("none: %v %v", none, err)
	}
	for _, bad := range []string{"99", "0", "", "1.25", "abc"} {
		if _, _, err := parseBound(bad); err == nil {
			t.Errorf("%q accepted: Envoy wants 100 or more", bad)
		}
	}
}
