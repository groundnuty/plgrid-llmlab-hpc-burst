package deploymentid

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
)

// iterations is the number of random cases per property. The seeds are fixed,
// so a failure reproduces; the failing case is printed with %q.
const iterations = 400

func cloneInputs(spec ServingSpec, files []File) (ServingSpec, []File) {
	spec.Flags = slices.Clone(spec.Flags)
	spec.Env = slices.Clone(spec.Env)
	return spec, slices.Clone(files)
}

// flipHex changes the last hex digit.
func flipHex(s string) string {
	last := s[len(s)-1]
	if last == '0' {
		return s[:len(s)-1] + "1"
	}
	return s[:len(s)-1] + "0"
}

// regroup returns flags in a random order that keeps the order of each name's
// values.
func (g *gen) regroup(flags []Flag) []Flag {
	var names []string
	by := map[string][]Flag{}
	for _, f := range flags {
		if _, ok := by[f.Name]; !ok {
			names = append(names, f.Name)
		}
		by[f.Name] = append(by[f.Name], f)
	}
	lists := make([][]Flag, 0, len(names))
	for _, n := range names {
		lists = append(lists, by[n])
	}
	return g.interleave(lists)
}

// Reordering what HBEP-3 says is unordered gives the same ID: flags of
// different names, environment variables and files.
func TestPropertyReorderingKeepsID(t *testing.T) {
	g := newGen(1)
	reordered := 0
	for i := 0; i < iterations; i++ {
		spec, accel, files := g.spec()
		want := mustCompute(t, spec, accel, files)
		s2, f2 := cloneInputs(spec, files)
		s2.Flags = g.regroup(spec.Flags)
		g.r.Shuffle(len(s2.Env), func(a, b int) { s2.Env[a], s2.Env[b] = s2.Env[b], s2.Env[a] })
		g.r.Shuffle(len(f2), func(a, b int) { f2[a], f2[b] = f2[b], f2[a] })
		got := mustCompute(t, s2, accel, f2)
		if !bytes.Equal(got.Canonical, want.Canonical) {
			t.Fatalf("iteration %d: reordering changed the ID\n spec  %q\n files %q\n again %q\n files %q",
				i, spec.Flags, files, s2.Flags, f2)
		}
		if !slices.Equal(s2.Flags, spec.Flags) || !slices.Equal(s2.Env, spec.Env) || !slices.Equal(f2, files) {
			reordered++
		}
	}
	if reordered < iterations/4 {
		t.Errorf("only %d of %d cases changed an order; the generator does not exercise the property", reordered, iterations)
	}
}

// The order of one flag's repeated values is meaningful: reversing different
// values changes the ID.
func TestPropertyRepeatedFlagOrderMatters(t *testing.T) {
	g := newGen(2)
	for i := 0; i < iterations; i++ {
		spec, accel, files := g.spec()
		name := g.unknownName(false)
		a, b := g.text(3), g.text(3)+"x"
		spec.Flags = append(spec.Flags, Flag{name, a}, Flag{name, b})
		s2, f2 := cloneInputs(spec, files)
		s2.Flags[len(s2.Flags)-2], s2.Flags[len(s2.Flags)-1] = Flag{name, b}, Flag{name, a}
		if mustCompute(t, spec, accel, files).SHA256 == mustCompute(t, s2, accel, f2).SHA256 {
			t.Fatalf("iteration %d: swapping the values %q and %q of %q kept the ID", i, a, b, name)
		}
	}
}

// Changing any one input alone changes the ID: each scalar, each file's hash
// and path, a file added or removed, and each input flag or variable changed,
// removed, repeated or added, an unknown one included.
func TestPropertyEachInputChangesID(t *testing.T) {
	g := newGen(3)
	checked := 0
	for i := 0; i < iterations; i++ {
		spec, accel, files := g.spec()
		base := mustCompute(t, spec, accel, files).SHA256
		for _, v := range inputVariants(g, spec, accel, files) {
			got, err := Compute(v.spec, v.accel, v.files)
			if err != nil {
				t.Fatalf("iteration %d, %s: %v", i, v.name, err)
			}
			if got.SHA256 == base {
				t.Fatalf("iteration %d: changing %s alone kept the ID\n flags %q\n env %q", i, v.name, spec.Flags, spec.Env)
			}
			checked++
		}
	}
	if checked < 10*iterations {
		t.Errorf("only %d single-input changes were tried", checked)
	}
}

// Every flag and variable of the table that is an input changes the ID when
// it is added to a spec that lacks it, and a different value changes it again.
func TestEveryTableInputChangesID(t *testing.T) {
	spec, accel, files := baseSpec()
	spec.Flags, spec.Env = nil, nil
	base := mustCompute(t, spec, accel, files).SHA256
	for _, r := range vllmV0320 {
		if r.class != classInput {
			continue
		}
		with := func(val string) string {
			s, f := cloneInputs(spec, files)
			if r.env {
				s.Env = []EnvVar{{r.name, val}}
			} else {
				s.Flags = []Flag{{r.name, val}}
			}
			return mustCompute(t, s, accel, f).SHA256
		}
		a, b := with("one"), with("two")
		if a == base || b == base || a == b {
			t.Errorf("%s: adding it or changing its value does not change the ID (%v %v)", r.name, a == base, a == b)
		}
	}
}

// strip removes the flags and variables that the table marks as not an input.
func strip(spec ServingSpec) ServingSpec {
	spec.Flags = slices.DeleteFunc(slices.Clone(spec.Flags), func(f Flag) bool { return className(f.Name, false) == classNotInput })
	spec.Env = slices.DeleteFunc(slices.Clone(spec.Env), func(e EnvVar) bool { return className(e.Name, true) == classNotInput })
	return spec
}

// Non-inputs left out, changed or added alone leave the ID as it was. The
// four flags and two variables named by name below are written out from
// HBEP-3 and the issue, not taken from the table under test.
func TestPropertyNonInputsDoNotChangeID(t *testing.T) {
	g := newGen(4)
	named := []Flag{{"port", ""}, {"host", ""}, {"max-num-seqs", ""}, {"gpu-memory-utilization", ""}}
	namedEnv := []EnvVar{{"HF_HOME", ""}, {"CUDA_VISIBLE_DEVICES", ""}}
	for i := 0; i < iterations; i++ {
		spec, accel, files := g.spec()
		clean := strip(spec)
		want := mustCompute(t, clean, accel, files).SHA256
		if got := mustCompute(t, spec, accel, files).SHA256; got != want {
			t.Fatalf("iteration %d: the non-input flags and variables of %q %q changed the ID", i, spec.Flags, spec.Env)
		}
		for _, r := range vllmV0320 {
			if r.class != classNotInput {
				continue
			}
			s, f := cloneInputs(clean, files)
			s.Flags, s.Env = g.insertNonInput(s.Flags, s.Env, r)
			if got := mustCompute(t, s, accel, f).SHA256; got != want {
				t.Fatalf("iteration %d: adding the non-input %s changed the ID", i, r.name)
			}
		}
		s, f := cloneInputs(clean, files)
		for _, n := range named {
			s.Flags = append(s.Flags, Flag{n.Name, g.text(3)})
		}
		for _, n := range namedEnv {
			s.Env = append(s.Env, EnvVar{n.Name, g.text(3)})
		}
		if got := mustCompute(t, s, accel, f).SHA256; got != want {
			t.Fatalf("iteration %d: port, host, max-num-seqs, gpu-memory-utilization, HF_HOME or CUDA_VISIBLE_DEVICES changed the ID", i)
		}
	}
}

// insertNonInput adds the rule's flag (1..3 times, with different values) or
// variable at a random position.
func (g *gen) insertNonInput(flags []Flag, env []EnvVar, r rule) ([]Flag, []EnvVar) {
	if r.env {
		env = slices.Insert(env, g.r.Intn(len(env)+1), EnvVar{r.name, g.text(3)})
		return flags, env
	}
	for n := 1 + g.r.Intn(3); n > 0; n-- {
		flags = slices.Insert(flags, g.r.Intn(len(flags)+1), Flag{r.name, g.text(3)})
	}
	return flags, env
}

// A key, a short alias, or a setting the framework passes is refused wherever
// it appears among the flags or variables, and the error does not repeat the
// value.
func TestPropertyRefusedInputsAreRefused(t *testing.T) {
	g := newGen(5)
	for i := 0; i < iterations; i++ {
		spec, accel, files := g.spec()
		for _, r := range vllmV0320 {
			want := map[class]error{classNever: ErrSecret, classFrameworkPassed: ErrFrameworkPassed}[r.class]
			if want != nil {
				g.expectRefused(t, spec, accel, files, r.name, r.env, want)
			}
		}
		alias := "-" + g.pick([]string{"t", "q", "O", "p", "tp", "dp", "x3"})
		g.expectRefused(t, spec, accel, files, alias, false, ErrShortAlias)
	}
}

func (g *gen) expectRefused(t *testing.T, spec ServingSpec, accel Accelerator, files []File, name string, env bool, want error) {
	t.Helper()
	const secret = "VALUE-THAT-MUST-NOT-LEAK"
	s, f := cloneInputs(spec, files)
	if env {
		s.Env = slices.DeleteFunc(s.Env, func(e EnvVar) bool { return e.Name == name })
		s.Env = slices.Insert(s.Env, g.r.Intn(len(s.Env)+1), EnvVar{name, secret})
	} else {
		s.Flags = slices.Insert(s.Flags, g.r.Intn(len(s.Flags)+1), Flag{name, secret})
	}
	_, err := Compute(s, accel, f)
	if !errors.Is(err, want) {
		t.Fatalf("%q (env %v): got %v, want %v", name, env, err, want)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("%q: the error repeats the value: %v", name, err)
	}
}

// A flag the table does not list is an input: adding it changes the ID, and
// so does changing its value. This is the case a generator that only produced
// table flags could not reach.
func TestPropertyUnknownFlagsAreInputs(t *testing.T) {
	g := newGen(6)
	for i := 0; i < iterations; i++ {
		spec, accel, files := g.spec()
		name := g.unknownName(false)
		if className(name, false) != classInput {
			t.Fatalf("generator produced a listed name %q", name)
		}
		base := mustCompute(t, spec, accel, files).SHA256
		withA, fA := cloneInputs(spec, files)
		withA.Flags = append(withA.Flags, Flag{name, "a"})
		withB, fB := cloneInputs(spec, files)
		withB.Flags = append(withB.Flags, Flag{name, "b"})
		a, b := mustCompute(t, withA, accel, fA).SHA256, mustCompute(t, withB, accel, fB).SHA256
		if a == base || b == base || a == b {
			t.Fatalf("iteration %d: unknown flag %q is not treated as an input", i, name)
		}
	}
}
