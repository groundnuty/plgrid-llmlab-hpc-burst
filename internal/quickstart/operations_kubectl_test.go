package quickstart

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The guide's kubectl lines (hbk) are checked without a kubectl: CI's go job has none (envtest's
// assets hold no kubectl, and k3s's own unpacks a data directory first). Each verb the guide uses
// has the flags it may take, the flags it must take and the arguments it needs, as kubectl
// v1.35's `<verb> --help` gives them, and each value is checked against what the framework
// defines: kinds and schemas against config/crd, labels, annotations and the namespace against the
// constants. A verb, a flag or a value the table does not hold fails.

// crdKind is one kind of config/crd: whether it has the scale subresource, and its schema.
type crdKind struct {
	scalable bool
	schema   map[string]any
}

// crdKinds maps every name kubectl takes for a kind of config/crd (plural, singular and short
// names) to the kind.
type crdKinds map[string]*crdKind

func readCRDNames(t *testing.T) crdKinds {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "config", "crd", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("config/crd has no CRD (%v)", err)
	}
	out := crdKinds{}
	for _, f := range files {
		var crd struct {
			Spec struct {
				Names struct {
					Plural     string   `json:"plural"`
					Singular   string   `json:"singular"`
					ShortNames []string `json:"shortNames"`
				} `json:"names"`
				Versions []struct {
					Storage      bool           `json:"storage"`
					Subresources map[string]any `json:"subresources"`
					Schema       struct {
						OpenAPIV3Schema map[string]any `json:"openAPIV3Schema"`
					} `json:"schema"`
				} `json:"versions"`
			} `json:"spec"`
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal(b, &crd); err != nil || crd.Spec.Names.Plural == "" {
			t.Fatalf("%s: no CRD names (%v)", f, err)
		}
		k := &crdKind{}
		for _, v := range crd.Spec.Versions {
			if v.Storage {
				_, k.scalable = v.Subresources["scale"]
				k.schema = v.Schema.OpenAPIV3Schema
			}
		}
		if k.schema == nil {
			t.Fatalf("%s: no stored version with a schema", f)
		}
		for _, n := range append([]string{crd.Spec.Names.Plural, crd.Spec.Names.Singular}, crd.Spec.Names.ShortNames...) {
			out[n] = k
		}
	}
	return out
}

// builtinKinds are the Kubernetes kinds the guide reads besides the framework's own: the records
// are ControllerRevisions (HBEP-11, Ownership, records and deletion), the controllers' Leases live
// in hpcburst-system (HBEP-11, Controllers), and Events are HBEP-8's.
var builtinKinds = map[string]bool{
	"controllerrevision": true, "controllerrevisions": true,
	"lease": true, "leases": true,
	"event": true, "events": true,
}

// A flag's value check; nil for a flag that takes no value.
type valueCheck func(string) error

func oneOf(vs ...string) valueCheck {
	return func(v string) error {
		if !slices.Contains(vs, v) {
			return fmt.Errorf("want one of %q", vs)
		}
		return nil
	}
}

var (
	namespaceValue = oneOf(v1alpha1.ControllerNamespace)
	selectorValue  = func(v string) error {
		k, val, ok := strings.Cut(v, "=")
		switch {
		case !ok:
			return fmt.Errorf("want key=value")
		case k == v1alpha1.LabelRecord:
			return oneOf(v1alpha1.RecordAccounting, v1alpha1.RecordVerification, v1alpha1.RecordIdentity)(val)
		case k == v1alpha1.LabelProvider:
			return nil
		}
		return fmt.Errorf("%s is not a label the framework sets on what the guide lists", k)
	}
	outputValue = func(v string) error {
		if tmpl, ok := strings.CutPrefix(v, "jsonpath="); ok {
			return checkJSONPath(tmpl)
		}
		return oneOf("yaml", "json", "wide", "name")(v)
	}
)

// kubectlVerb is what one kubectl verb takes, as the guide uses it.
type kubectlVerb struct {
	flags    map[string]valueCheck
	required []string
	// args checks the positional arguments: the kind first, where there is one.
	args func(pos []string, crds crdKinds, flags map[string]string) error
}

// kubectlVerbs are the verbs the guide uses, from kubectl v1.35's help for each: describe has no
// -o, scale requires --replicas, apply and patch require their -f and -p. annotate is given
// --overwrite always: the Provider's controller leaves abandon-attempt in place after acting on it
// (internal/providerctl/external.go), so a later attempt's value is refused without it.
var kubectlVerbs = map[string]kubectlVerb{
	"get": {
		flags: map[string]valueCheck{"-o": outputValue, "-l": selectorValue, "-n": namespaceValue, "--sort-by": prefixed("."), "-w": nil},
		args:  kindAnd(0, 1),
	},
	// wait takes one kind/name, and --for the Ready condition or the object's deletion. The
	// quickstart uses it on a ModelDeployment; --timeout is a Go duration, as kubectl reads it.
	"wait": {
		flags: map[string]valueCheck{
			"--for":     oneOf("condition="+v1alpha1.ConditionReady, "delete"),
			"--timeout": positiveDuration,
			"-n":        namespaceValue,
		},
		required: []string{"--for"},
		args: func(pos []string, crds crdKinds, _ map[string]string) error {
			if len(pos) != 1 {
				return fmt.Errorf("want one kind/name argument, got %q", pos)
			}
			kind, name, ok := strings.Cut(pos[0], "/")
			if !ok || name == "" {
				return fmt.Errorf("want kind/name, got %q", pos[0])
			}
			if crds[kind] == nil {
				return fmt.Errorf("%s is not a kind of config/crd", kind)
			}
			return nil
		},
	},
	"describe": {
		flags: map[string]valueCheck{"-l": selectorValue, "-n": namespaceValue},
		args:  kindAnd(0, 1),
	},
	"scale": {
		flags: map[string]valueCheck{"--replicas": func(v string) error {
			if n, err := strconv.Atoi(v); err != nil || n < 0 {
				return fmt.Errorf("want a count")
			}
			return nil
		}, "-n": namespaceValue},
		required: []string{"--replicas"},
		args: func(pos []string, crds crdKinds, f map[string]string) error {
			if err := kindAnd(1, 1)(pos, crds, f); err != nil {
				return err
			}
			if k := crds[pos[0]]; k == nil || !k.scalable {
				return fmt.Errorf("%s has no scale subresource", pos[0])
			}
			return nil
		},
	},
	"annotate": {
		flags:    map[string]valueCheck{"--overwrite": nil, "-n": namespaceValue},
		required: []string{"--overwrite"},
		args: func(pos []string, crds crdKinds, f map[string]string) error {
			if err := kindAnd(2, 2)(pos, crds, f); err != nil {
				return err
			}
			if k, _, _ := strings.Cut(pos[2], "="); k != v1alpha1.AnnotationAbandonAttempt {
				return fmt.Errorf("%s is not the annotation an operator sets (HBEP-11, The Allocation's record)", k)
			}
			return nil
		},
	},
	"delete": {
		flags: map[string]valueCheck{"-n": namespaceValue},
		args:  kindAnd(1, 1),
	},
	"apply": {
		flags: map[string]valueCheck{"-f": func(v string) error {
			if fi, err := os.Stat(filepath.Join(root, v)); filepath.IsAbs(v) || err != nil || !fi.Mode().IsRegular() {
				return fmt.Errorf("want a file of this repository")
			}
			return nil
		}},
		required: []string{"-f"},
		args: func(pos []string, _ crdKinds, _ map[string]string) error {
			if len(pos) != 0 {
				return fmt.Errorf("apply -f takes no kind")
			}
			return nil
		},
	},
	"patch": {
		flags:    map[string]valueCheck{"--type": oneOf("merge"), "-p": func(string) error { return nil }, "-n": namespaceValue},
		required: []string{"--type", "-p"},
		args: func(pos []string, crds crdKinds, f map[string]string) error {
			if err := kindAnd(1, 1)(pos, crds, f); err != nil {
				return err
			}
			k := crds[pos[0]]
			if k == nil {
				return fmt.Errorf("the guide patches only kinds of config/crd")
			}
			var body any
			if err := json.Unmarshal([]byte(f["-p"]), &body); err != nil {
				return fmt.Errorf("-p is not JSON: %v", err)
			}
			return checkPatch(k.schema, body, "")
		},
	},
}

// positiveDuration accepts a Go duration above zero, which is how kubectl reads --timeout.
func positiveDuration(v string) error {
	if d, err := time.ParseDuration(v); err != nil || d <= 0 {
		return fmt.Errorf("want a positive duration such as 3h")
	}
	return nil
}

func prefixed(p string) valueCheck {
	return func(v string) error {
		if !strings.HasPrefix(v, p) {
			return fmt.Errorf("want a value that starts with %s", p)
		}
		return nil
	}
}

// kindAnd checks a kind, or kinds joined by commas, of config/crd or builtinKinds, followed by
// between lo and hi more arguments.
func kindAnd(lo, hi int) func([]string, crdKinds, map[string]string) error {
	return func(pos []string, crds crdKinds, _ map[string]string) error {
		if len(pos) < 1+lo || len(pos) > 1+hi {
			return fmt.Errorf("want a kind and %d to %d more arguments, got %q", lo, hi, pos)
		}
		for _, r := range strings.Split(pos[0], ",") {
			if crds[r] == nil && !builtinKinds[r] {
				return fmt.Errorf("%s is neither a kind of config/crd nor one the guide is known to read", r)
			}
		}
		return nil
	}
}

// checkJSONPath accepts the guide's one template shape: a range over .items[*] that prints each
// object's name or its tag, the external-name annotation crossplane keeps it in.
func checkJSONPath(tmpl string) error {
	tag := ".metadata.annotations." + strings.ReplaceAll(meta.AnnotationKeyExternalName, ".", `\.`)
	for _, m := range regexp.MustCompile(`\{([^}]*)\}`).FindAllStringSubmatch(tmpl, -1) {
		switch f := m[1]; {
		case f == "range .items[*]", f == "end", strings.HasPrefix(f, `"`):
		case f == ".metadata.name", f == tag:
		default:
			return fmt.Errorf("the template reads %s, which the test does not know", f)
		}
	}
	return nil
}

// checkPatch walks a merge patch along a CRD's schema: each key must be a property, a null removes
// it, and a string must satisfy the property's enum and pattern.
func checkPatch(schema map[string]any, body any, path string) error {
	switch v := body.(type) {
	case nil:
		return nil
	case map[string]any:
		props, _ := schema["properties"].(map[string]any)
		for k, sub := range v {
			ps, ok := props[k].(map[string]any)
			if !ok {
				return fmt.Errorf("%s.%s is not in the schema", path, k)
			}
			if err := checkPatch(ps, sub, path+"."+k); err != nil {
				return err
			}
		}
	case string:
		if enum, ok := schema["enum"].([]any); ok && !slices.Contains(enum, any(v)) {
			return fmt.Errorf("%s: %q is not one of %v", path, v, enum)
		}
		if p, ok := schema["pattern"].(string); ok && !regexp.MustCompile(p).MatchString(v) {
			return fmt.Errorf("%s: %q does not match %s", path, v, p)
		}
	}
	return nil
}

// checkHbk checks one kubectl line of the guide, its words as the shell gives them.
func checkHbk(t *testing.T, line string, words []string, crds crdKinds) {
	t.Helper()
	if len(words) < 2 {
		t.Errorf("%q names no verb", line)
		return
	}
	verb, ok := kubectlVerbs[words[1]]
	if !ok {
		t.Errorf("%q: the test does not know the kubectl verb %s", line, words[1])
		return
	}
	var pos []string
	given := map[string]string{}
	for i := 2; i < len(words); i++ {
		w := words[i]
		if !strings.HasPrefix(w, "-") {
			pos = append(pos, w)
			continue
		}
		name, val, inline := strings.Cut(w, "=")
		check, known := verb.flags[name]
		switch {
		case !known:
			t.Errorf("%q: kubectl %s takes no flag %s here", line, words[1], name)
			continue
		case check == nil && inline:
			t.Errorf("%q: %s takes no value", line, name)
			continue
		case check != nil && !inline:
			if i+1 == len(words) {
				t.Errorf("%q: %s has no value", line, name)
				continue
			}
			i++
			val = words[i]
		}
		given[name] = val
		if check != nil {
			if err := check(val); err != nil {
				t.Errorf("%q: %s %q: %v", line, name, val, err)
			}
		}
	}
	for _, r := range verb.required {
		if _, ok := given[r]; !ok {
			t.Errorf("%q: kubectl %s needs %s", line, words[1], r)
		}
	}
	if err := verb.args(pos, crds, given); err != nil {
		t.Errorf("%q: %v", line, err)
	}
}
