package fakeslurm

// Ported from spikes/s4/fakeslurm/args.go. Changed: an option that is not in
// the command's table is an error (S4 ignored it), so that an option the
// plugin should never send cannot pass silently.

import (
	"fmt"
	"strings"
)

type valueMode int

const (
	valNone valueMode = iota
	valRequired
)

// optDef describes one option.
type optDef struct {
	name  string // long name without dashes
	short byte   // 0 when none
	value valueMode
}

type opt struct {
	Name   string
	Val    string
	HasVal bool
}

// parsed is the outcome of parseOpts.
type parsed struct {
	opts []opt
	rest []string
}

// get returns the value of the last occurrence of the named option.
func (p parsed) get(name string) (string, bool) {
	v, ok := "", false
	for _, o := range p.opts {
		if o.Name == name && o.HasVal {
			v, ok = o.Val, true
		}
	}
	return v, ok
}

// has reports whether the named option appeared at all.
func (p parsed) has(name string) bool {
	for _, o := range p.opts {
		if o.Name == name {
			return true
		}
	}
	return false
}

// parseOpts understands --long, --long=v, --long v, -s v, -sv, clustered short
// booleans and "--". An option not in defs is an error.
func parseOpts(args []string, defs []optDef) (parsed, error) {
	byLong := map[string]optDef{}
	byShort := map[byte]optDef{}
	for _, d := range defs {
		byLong[d.name] = d
		if d.short != 0 {
			byShort[d.short] = d
		}
	}
	var p parsed
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			p.rest = append(p.rest, args[i+1:]...)
			return p, nil
		case strings.HasPrefix(a, "--"):
			name, val, hasEq := strings.Cut(a[2:], "=")
			d, known := byLong[name]
			if !known {
				return p, fmt.Errorf("unrecognized option '--%s'", name)
			}
			if d.value != valNone && !hasEq {
				if i+1 >= len(args) {
					return p, fmt.Errorf("option '--%s' requires an argument", name)
				}
				i++
				val, hasEq = args[i], true
			}
			p.opts = append(p.opts, opt{Name: name, Val: val, HasVal: hasEq})
		case len(a) > 1 && a[0] == '-':
			n, err := parseShort(a, args, i, byShort, &p)
			if err != nil {
				return p, err
			}
			i += n
		default:
			p.rest = append(p.rest, a)
		}
	}
	return p, nil
}

// parseShort reads one cluster of short options at args[i]. It returns how
// many following arguments it consumed as values.
func parseShort(a string, args []string, i int, byShort map[byte]optDef, p *parsed) (int, error) {
	for j := 1; j < len(a); j++ {
		d, known := byShort[a[j]]
		switch {
		case !known:
			return 0, fmt.Errorf("invalid option -- '%c'", a[j])
		case d.value == valNone:
			p.opts = append(p.opts, opt{Name: d.name})
		case a[j+1:] != "":
			p.opts = append(p.opts, opt{Name: d.name, Val: a[j+1:], HasVal: true})
			return 0, nil
		case i+1 >= len(args):
			return 0, fmt.Errorf("option requires an argument -- '%c'", a[j])
		default:
			p.opts = append(p.opts, opt{Name: d.name, Val: args[i+1], HasVal: true})
			return 1, nil
		}
	}
	return 0, nil
}

// splitList splits a comma separated list, dropping empty items.
func splitList(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}
