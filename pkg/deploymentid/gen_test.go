package deploymentid

import (
	"math/rand"
	"slices"
	"strings"
)

// Generators for the property tests. They are hand-written and seeded with a
// fixed value, so a failure reproduces. They deliberately produce what a
// careless generator would not: unknown flags, repeated flags, empty values,
// an empty env, and non-ASCII and awkward paths and values. Flag and variable
// names are ASCII, as the scheme requires; the non-ASCII keys that tell UTF-16
// order from bytewise order are tested on the serialiser alone (jcs_test.go).

// textPieces are the building blocks of generated strings: ASCII, characters
// JSON escapes, and non-ASCII from the BMP and from the supplementary planes
// (U+FB33, U+FF5E, U+1F600, U+2028).
var textPieces = []string{
	"a", "b", "c", "Z", "0", "7", "-", "_", ".", " ", "\"", "\\", "\n", "\t", "<", "&", "/",
	"\x00", "\x7f", "\xc3\xa9", "\xc3\xb6", "\xe6\x97\xa5", "\xef\xac\xb3", "\xf0\x9f\x98\x80", "\xe2\x80\xa8", "\xef\xbd\x9e",
}

// flagNamePieces and envNamePieces are the pieces of a generated flag or
// variable name. A flag name has no underscore, which resolveFlag would read
// as a dash, so every generated name is its own resolved form.
var (
	flagNamePieces = []string{"a", "b", "c", "z", "0", "7", "-", "x1", "dtype", "no", "-no-"}
	envNamePieces  = []string{"A", "B", "C", "Z", "0", "7", "_", "X1", "VLLM", "NO", "_NO_"}
)

type gen struct{ r *rand.Rand }

func newGen(seed int64) *gen { return &gen{r: rand.New(rand.NewSource(seed))} }

func (g *gen) pick(pieces []string) string { return pieces[g.r.Intn(len(pieces))] }

// text returns a string of 0..max pieces, so it is often empty.
func (g *gen) text(max int) string {
	var b strings.Builder
	for n := g.r.Intn(max + 1); n > 0; n-- {
		b.WriteString(g.pick(textPieces))
	}
	return b.String()
}

func (g *gen) nonEmptyText() string {
	for {
		if s := g.text(4); s != "" {
			return s
		}
	}
}

func (g *gen) hex(n int) string {
	const digits = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = digits[g.r.Intn(16)]
	}
	return string(b)
}

// unknownName returns a valid flag (or, with env, variable) name that the vLLM
// table does not list and that no refusal rule touches: it is accepted as it
// is, and an input. Single letters such as "a" and "c" are prefixes of refused
// flags, so resolveFlag refuses them and they are not generated.
func (g *gen) unknownName(env bool) string {
	// A bound turns a table that lists every generated name into a failure
	// rather than a hang.
	for tries := 0; tries < 10000; tries++ {
		var b strings.Builder
		pieces := flagNamePieces
		if env {
			pieces = envNamePieces
		}
		for n := 1 + g.r.Intn(4); n > 0; n-- {
			b.WriteString(g.pick(pieces))
		}
		name := b.String()
		tab := tables[engineVLLM]
		if env {
			if _, listed := tab.env[name]; !listed && checkName(name, true) == nil {
				return name
			}
		} else if resolved, err := tab.resolveFlag(name); err == nil && resolved == name {
			if _, listed := tab.flags[name]; !listed {
				return name
			}
		}
	}
	panic("gen: no unlisted name found in 10000 tries")
}

// pathPieces are textPieces without the separator.
var pathPieces = slices.DeleteFunc(slices.Clone(textPieces), func(s string) bool { return s == "/" })

// path returns a clean relative path of 1..3 segments, often with awkward characters.
func (g *gen) path() string {
	var segs []string
	for n := 1 + g.r.Intn(3); n > 0; n-- {
		var b strings.Builder
		for k := 1 + g.r.Intn(3); k > 0; k-- {
			b.WriteString(g.pick(pathPieces))
		}
		if seg := b.String(); seg != "." && seg != ".." {
			segs = append(segs, seg)
		}
	}
	if len(segs) == 0 {
		return g.path()
	}
	return strings.Join(segs, "/")
}

func (g *gen) files() []File {
	seen := map[string]bool{}
	var files []File
	for n := 1 + g.r.Intn(5); n > 0; n-- {
		p := g.path()
		if !seen[p] {
			seen[p] = true
			files = append(files, File{p, g.hex(64)})
		}
	}
	return files
}

// rulesOf returns the names of the table's rules of one class, flags or variables.
func rulesOf(c class, env bool) []string {
	var names []string
	for _, r := range vllmV0320 {
		if r.class == c && r.env == env {
			names = append(names, r.name)
		}
	}
	return names
}

// names picks k distinct names: from the table's rules of class c, or, with
// probability 1/2 each, an unknown name.
func (g *gen) names(k int, c class, env bool) []string {
	table := rulesOf(c, env)
	seen := map[string]bool{}
	var out []string
	for ; k > 0; k-- {
		var n string
		if c == classInput && g.r.Intn(2) == 0 {
			n = g.unknownName(env)
		} else if len(table) > 0 {
			n = table[g.r.Intn(len(table))]
		} else {
			continue
		}
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// interleave merges per-name value lists in a random order that keeps the
// order within each name: the only order HBEP-3 says matters.
func (g *gen) interleave(lists [][]Flag) []Flag {
	lists = slices.Clone(lists)
	var out []Flag
	for len(lists) > 0 {
		i := g.r.Intn(len(lists))
		out = append(out, lists[i][0])
		if lists[i] = lists[i][1:]; len(lists[i]) == 0 {
			lists = slices.Delete(lists, i, i+1)
		}
	}
	return out
}

// flags returns input flags (table and unknown ones, some repeated) mixed
// with non-input flags, in an arbitrary interleaving.
func (g *gen) flags() []Flag {
	var lists [][]Flag
	for _, c := range []class{classInput, classNotInput} {
		for _, name := range g.names(g.r.Intn(5), c, false) {
			var l []Flag
			for n := 1 + g.r.Intn(3); n > 0; n-- {
				l = append(l, Flag{name, g.text(3)})
			}
			lists = append(lists, l)
		}
	}
	return g.interleave(lists)
}

// env returns input and non-input variables with distinct names, possibly none.
func (g *gen) env() []EnvVar {
	var env []EnvVar
	for _, c := range []class{classInput, classNotInput} {
		for _, name := range g.names(g.r.Intn(3), c, true) {
			env = append(env, EnvVar{name, g.text(3)})
		}
	}
	g.r.Shuffle(len(env), func(i, j int) { env[i], env[j] = env[j], env[i] })
	return env
}

func (g *gen) spec() (ServingSpec, Accelerator, []File) {
	spec := ServingSpec{
		Checkpoint:         Checkpoint{Repository: g.nonEmptyText(), Revision: g.hex(40)},
		Engine:             Engine{Name: "vllm", Version: g.nonEmptyText(), ImageDigest: "sha256:" + g.hex(64)},
		Flags:              g.flags(),
		Env:                g.env(),
		ChatTemplateSHA256: g.hex(64),
	}
	return spec, Accelerator{GPUModel: g.nonEmptyText(), CPUArch: g.nonEmptyText()}, g.files()
}
