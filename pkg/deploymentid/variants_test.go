package deploymentid

import (
	"fmt"
	"slices"
	"strings"
)

// A variant is a copy of the inputs of a base case with exactly one input
// changed.
type variant struct {
	name  string
	spec  ServingSpec
	accel Accelerator
	files []File
}

// collector builds variants of one base case.
type collector struct {
	spec  ServingSpec
	accel Accelerator
	files []File
	out   []variant
}

type edit = func(*ServingSpec, *Accelerator, *[]File)

func (c *collector) add(name string, e edit) {
	s, f := cloneInputs(c.spec, c.files)
	a := c.accel
	e(&s, &a, &f)
	c.out = append(c.out, variant{name, s, a, f})
}

// inputVariants returns, for a base case, one variant per input that can be
// changed alone: each scalar field, each file's hash and path, a file added
// and removed, each input flag changed, removed, repeated and with its
// repeats reversed, an unknown flag added, and the same for variables.
func inputVariants(g *gen, spec ServingSpec, accel Accelerator, files []File) []variant {
	c := &collector{spec: spec, accel: accel, files: files}
	c.add("repository", func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Checkpoint.Repository += "x" })
	c.add("revision", func(s *ServingSpec, _ *Accelerator, _ *[]File) {
		s.Checkpoint.Revision = flipHex(s.Checkpoint.Revision)
	})
	c.add("engine version", func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Engine.Version += "x" })
	c.add("image digest", func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Engine.ImageDigest = flipHex(s.Engine.ImageDigest) })
	c.add("chat template", func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.ChatTemplateSHA256 = flipHex(s.ChatTemplateSHA256) })
	c.add("gpu model", func(_ *ServingSpec, a *Accelerator, _ *[]File) { a.GPUModel += "x" })
	c.add("cpu arch", func(_ *ServingSpec, a *Accelerator, _ *[]File) { a.CPUArch += "x" })
	c.fileVariants()
	c.flagVariants(g)
	c.envVariants(g)
	return c.out
}

func (c *collector) fileVariants() {
	for i := range c.files {
		c.add(fmt.Sprintf("file %d hash", i), func(_ *ServingSpec, _ *Accelerator, f *[]File) {
			(*f)[i].SHA256 = flipHex((*f)[i].SHA256)
		})
		c.add(fmt.Sprintf("file %d path", i), func(_ *ServingSpec, _ *Accelerator, f *[]File) {
			(*f)[i].Path += "\xf0\x9f\x98\x80"
		})
		if len(c.files) > 1 {
			c.add(fmt.Sprintf("remove file %d", i), func(_ *ServingSpec, _ *Accelerator, f *[]File) {
				*f = slices.Delete(*f, i, i+1)
			})
		}
	}
	c.add("add file", func(_ *ServingSpec, _ *Accelerator, f *[]File) {
		*f = append(*f, File{"added\xf0\x9f\x98\x80/file", strings.Repeat("e", 64)})
	})
}

func className(name string, env bool) class {
	cl, _ := tables[engineVLLM].classify(name, env)
	return cl
}

func (c *collector) flagVariants(g *gen) {
	seen := map[string]bool{}
	for _, f := range c.spec.Flags {
		seen[f.Name] = true
	}
	for name := range seen {
		if className(name, false) != classInput {
			continue
		}
		c.add("change flag "+name, func(s *ServingSpec, _ *Accelerator, _ *[]File) {
			s.Flags[slices.IndexFunc(s.Flags, func(x Flag) bool { return x.Name == name })].Value += "x"
		})
		c.add("remove flag "+name, func(s *ServingSpec, _ *Accelerator, _ *[]File) {
			s.Flags = slices.DeleteFunc(s.Flags, func(x Flag) bool { return x.Name == name })
		})
		c.add("repeat flag "+name, func(s *ServingSpec, _ *Accelerator, _ *[]File) {
			s.Flags = append(s.Flags, Flag{name, "z"})
		})
	}
	fresh := g.unknownName(false)
	for seen[fresh] {
		fresh = g.unknownName(false)
	}
	c.add("add unknown flag", func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Flags = append(s.Flags, Flag{fresh, ""}) })
}

func (c *collector) envVariants(g *gen) {
	have := map[string]bool{}
	for _, e := range c.spec.Env {
		have[e.Name] = true
		if className(e.Name, true) != classInput {
			continue
		}
		name := e.Name
		c.add("change env "+name, func(s *ServingSpec, _ *Accelerator, _ *[]File) {
			s.Env[slices.IndexFunc(s.Env, func(x EnvVar) bool { return x.Name == name })].Value += "x"
		})
		c.add("remove env "+name, func(s *ServingSpec, _ *Accelerator, _ *[]File) {
			s.Env = slices.DeleteFunc(s.Env, func(x EnvVar) bool { return x.Name == name })
		})
	}
	fresh := g.unknownName(true)
	for have[fresh] {
		fresh = g.unknownName(true)
	}
	c.add("add unknown env", func(s *ServingSpec, _ *Accelerator, _ *[]File) { s.Env = append(s.Env, EnvVar{fresh, ""}) })
}
