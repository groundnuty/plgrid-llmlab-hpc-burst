package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
)

// The bound: ring_hash's bounded loads, the cluster's common_lb_config.consistent_hashing_lb_config.
// hash_balance_factor (HBEP-5; #518 adds it to the agent's configuration at 125). The arm's
// configuration is the agent's rendering with at most this one field changed: set to the arm's factor,
// or deleted for the arm with no bound. Nothing else is touched, and diffPaths proves it.

// boundPath is the one path an arm may change, in the form diffPaths reports.
const boundPath = "static_resources.clusters[0].common_lb_config.consistent_hashing_lb_config.hash_balance_factor"

// parseBound reads the -bound flag: a factor in percent (100 or more, as Envoy requires), or "none".
func parseBound(s string) (factor int, none bool, err error) {
	if s == "none" {
		return 0, true, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 100 {
		return 0, false, fmt.Errorf("-bound %q: want a factor of 100 or more, or none", s)
	}
	return n, false, nil
}

// decodeConfig decodes a bootstrap keeping numbers exact.
func decodeConfig(b []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var m map[string]any
	if err := d.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// workersCluster is the workers' cluster, the first of static_resources.clusters.
func workersCluster(m map[string]any) (map[string]any, error) {
	sr, _ := m["static_resources"].(map[string]any)
	cs, _ := sr["clusters"].([]any)
	if len(cs) == 0 {
		return nil, fmt.Errorf("the bootstrap has no cluster")
	}
	c, _ := cs[0].(map[string]any)
	if c == nil || c["name"] != "workers" {
		return nil, fmt.Errorf("the first cluster is not the workers' cluster")
	}
	return c, nil
}

// factorOf is the configuration's hash_balance_factor, or nil when it sets none.
func factorOf(b []byte) (*int, error) {
	m, err := decodeConfig(b)
	if err != nil {
		return nil, err
	}
	c, err := workersCluster(m)
	if err != nil {
		return nil, err
	}
	lb, _ := c["common_lb_config"].(map[string]any)
	ch, _ := lb["consistent_hashing_lb_config"].(map[string]any)
	v, ok := ch["hash_balance_factor"]
	if !ok {
		return nil, nil
	}
	n, err := strconv.Atoi(fmt.Sprint(v))
	if err != nil {
		return nil, fmt.Errorf("hash_balance_factor %v is not an integer", v)
	}
	return &n, nil
}

// applyBound returns the arm's configuration: rendered itself when it already carries the arm's bound
// (changed false: the product's own bytes), else the rendering with the one field set or deleted.
func applyBound(rendered []byte, factor int, none bool) (out []byte, changed bool, err error) {
	cur, err := factorOf(rendered)
	if err != nil {
		return nil, false, err
	}
	if (none && cur == nil) || (!none && cur != nil && *cur == factor) {
		return rendered, false, nil
	}
	m, err := decodeConfig(rendered)
	if err != nil {
		return nil, false, err
	}
	c, err := workersCluster(m)
	if err != nil {
		return nil, false, err
	}
	lb, _ := c["common_lb_config"].(map[string]any)
	if lb == nil {
		lb = map[string]any{}
		c["common_lb_config"] = lb
	}
	ch, _ := lb["consistent_hashing_lb_config"].(map[string]any)
	if none {
		delete(ch, "hash_balance_factor") // the parent stays, possibly empty: Envoy's default is no bound
	} else {
		if ch == nil {
			ch = map[string]any{}
			lb["consistent_hashing_lb_config"] = ch
		}
		ch["hash_balance_factor"] = json.Number(strconv.Itoa(factor))
	}
	out, err = json.MarshalIndent(m, "", "  ")
	return out, true, err
}

// diffPaths lists the leaf paths at which two bootstraps differ: a leaf whose value differs, or a leaf
// only one of them has (an object only one side has is listed by its leaves; an empty one by its own
// path).
func diffPaths(a, b []byte) ([]string, error) {
	ma, err := decodeConfig(a)
	if err != nil {
		return nil, err
	}
	mb, err := decodeConfig(b)
	if err != nil {
		return nil, err
	}
	var out []string
	walkDiff("", ma, mb, &out)
	sort.Strings(out)
	return out, nil
}

func walkDiff(path string, a, b any, out *[]string) {
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		keys := map[string]bool{}
		for k := range am {
			keys[k] = true
		}
		for k := range bm {
			keys[k] = true
		}
		for k := range keys {
			p := k
			if path != "" {
				p = path + "." + k
			}
			av, ain := am[k]
			bv, bin := bm[k]
			if !ain {
				leaves(p, bv, out)
				continue
			}
			if !bin {
				leaves(p, av, out)
				continue
			}
			walkDiff(p, av, bv, out)
		}
		return
	}
	al, aok := a.([]any)
	bl, bok := b.([]any)
	if aok && bok {
		for i := 0; i < len(al) || i < len(bl); i++ {
			p := fmt.Sprintf("%s[%d]", path, i)
			if i >= len(al) || i >= len(bl) {
				*out = append(*out, p)
				continue
			}
			walkDiff(p, al[i], bl[i], out)
		}
		return
	}
	if !reflect.DeepEqual(a, b) {
		*out = append(*out, path)
	}
}

// leaves lists the leaf paths under v at path: an object's leaves, or path itself for anything else
// (an empty object included).
func leaves(path string, v any, out *[]string) {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		*out = append(*out, path)
		return
	}
	for k, x := range m {
		leaves(path+"."+k, x, out)
	}
}
