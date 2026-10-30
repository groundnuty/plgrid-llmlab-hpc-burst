// Command mutate-cel lists the CEL admission rules in a directory of CRD
// manifests and writes a copy of the manifests with one rule neutralised. It is
// the helper of hack/mutate-cel.sh, which runs the e2e suite against each copy
// and asserts that the rule's test fails.
//
//	mutate-cel list DIR
//	mutate-cel neutralise DIR OUTDIR ID    ID is one printed by list; "-" neutralises nothing
//
// A rule is addressed by the CRD's plural name, its path in the schema and its
// index among the rules at that path, for example
//
//	allocations:$.spec#2
//	modeldeployments:$.spec.serving.flags[]#5
//
// where $ is the root of the schema and [] is an array's item schema.
// Neutralising replaces the rule's expression with "true", so it admits
// everything. The manifests are edited as parsed objects, never line by line:
// the YAML emitter wraps long strings across lines.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mutate-cel:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	switch {
	case len(args) == 2 && args[0] == "list":
		crds, err := load(args[1])
		if err != nil {
			return err
		}
		for _, r := range rules(crds) {
			fmt.Printf("%s\t%s\n", r.id, oneLine(r.message, 90))
		}
		return nil
	case len(args) == 4 && args[0] == "neutralise":
		return neutralise(args[1], args[2], args[3])
	}
	return fmt.Errorf("usage: mutate-cel list DIR | mutate-cel neutralise DIR OUTDIR ID")
}

type namedCRD struct {
	file string
	crd  *apiextensionsv1.CustomResourceDefinition
}

func load(dir string) ([]namedCRD, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no CRD manifests in %s", dir)
	}
	var out []namedCRD
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.UnmarshalStrict(raw, crd); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		out = append(out, namedCRD{file: filepath.Base(f), crd: crd})
	}
	return out, nil
}

// rule is a pointer to one validation rule inside a loaded CRD.
type rule struct {
	id      string
	message string
	v       *apiextensionsv1.ValidationRule
}

func rules(crds []namedCRD) []rule {
	var out []rule
	for _, n := range crds {
		for i := range n.crd.Spec.Versions {
			ver := &n.crd.Spec.Versions[i]
			if ver.Schema == nil || ver.Schema.OpenAPIV3Schema == nil {
				continue
			}
			walk(n.crd.Spec.Names.Plural, "$", ver.Schema.OpenAPIV3Schema, &out)
		}
	}
	return out
}

func walk(plural, path string, s *apiextensionsv1.JSONSchemaProps, out *[]rule) {
	for i := range s.XValidations {
		v := &s.XValidations[i]
		*out = append(*out, rule{id: fmt.Sprintf("%s:%s#%d", plural, path, i), message: v.Message, v: v})
	}
	keys := make([]string, 0, len(s.Properties))
	for k := range s.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		p := s.Properties[k]
		walk(plural, path+"."+k, &p, out)
		s.Properties[k] = p
	}
	if s.Items != nil && s.Items.Schema != nil {
		walk(plural, path+"[]", s.Items.Schema, out)
	}
	if s.AdditionalProperties != nil && s.AdditionalProperties.Schema != nil {
		walk(plural, path+".*", s.AdditionalProperties.Schema, out)
	}
}

func neutralise(dir, outDir, id string) error {
	crds, err := load(dir)
	if err != nil {
		return err
	}
	if id != "-" {
		var hit *rule
		all := rules(crds)
		for i := range all {
			if all[i].id == id {
				hit = &all[i]
			}
		}
		if hit == nil {
			return fmt.Errorf("no rule %q; mutate-cel list shows the ids", id)
		}
		if hit.v.Rule == "true" {
			return fmt.Errorf("rule %q is already \"true\"", id)
		}
		hit.v.Rule = "true"
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	for _, n := range crds {
		raw, err := yaml.Marshal(n.crd)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(outDir, n.file), raw, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func oneLine(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > max {
		return s[:max-3] + "..."
	}
	return s
}
