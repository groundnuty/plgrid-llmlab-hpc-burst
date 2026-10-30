package fakeslurm

import (
	"fmt"
	"strings"
)

type valueMode int

const (
	valNone valueMode = iota
	valRequired
)

// optDef describes one option. Unknown options are ignored by the parser.
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

// last returns the name of the last option out of names that appeared, or "".
func (p parsed) last(names ...string) string {
	r := ""
	for _, o := range p.opts {
		for _, n := range names {
			if o.Name == n {
				r = n
			}
		}
	}
	return r
}

// parseOpts understands --long, --long=v, --long v, -s v, -sv, clustered
// short booleans and "--". Options not in defs are recorded under their own
// name and otherwise ignored; an unknown long option without '=' never
// swallows the next argument.
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
			if known && d.value != valNone && !hasEq {
				if i+1 >= len(args) {
					return p, fmt.Errorf("option '--%s' requires an argument", name)
				}
				i++
				val, hasEq = args[i], true
			}
			p.opts = append(p.opts, opt{Name: name, Val: val, HasVal: hasEq})
		case len(a) > 1 && a[0] == '-':
			for j := 1; j < len(a); j++ {
				d, known := byShort[a[j]]
				if !known {
					p.opts = append(p.opts, opt{Name: string(a[j])})
					continue
				}
				if d.value == valNone {
					p.opts = append(p.opts, opt{Name: d.name})
					continue
				}
				if rest := a[j+1:]; rest != "" {
					p.opts = append(p.opts, opt{Name: d.name, Val: rest, HasVal: true})
					break
				}
				if i+1 >= len(args) {
					return p, fmt.Errorf("option requires an argument -- '%c'", a[j])
				}
				i++
				p.opts = append(p.opts, opt{Name: d.name, Val: args[i], HasVal: true})
				break
			}
		default:
			p.rest = append(p.rest, a)
		}
	}
	return p, nil
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
