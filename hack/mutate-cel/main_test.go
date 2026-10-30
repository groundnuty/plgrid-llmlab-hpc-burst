package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A small CRD with three rules: two at the spec, one on the items of a list.
const fixture = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.invalid
spec:
  group: example.invalid
  names: {kind: Widget, plural: widgets, singular: widget, listKind: WidgetList}
  scope: Namespaced
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            x-kubernetes-validations:
            - {rule: "self.a != 'x'", message: "a is not x"}
            - {rule: "self.a != 'y'", message: "a is not y"}
            properties:
              a: {type: string}
              list:
                type: array
                items:
                  type: object
                  x-kubernetes-validations:
                  - {rule: "self.n > 0", message: "n is positive"}
                  properties:
                    n: {type: integer}
`

func writeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "widgets.yaml"), []byte(fixture), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func ruleMap(t *testing.T, dir string) map[string]string {
	t.Helper()
	crds, err := load(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]string{}
	for _, r := range rules(crds) {
		m[r.id] = r.v.Rule
	}
	return m
}

func TestRulesAreAddressedByPathAndIndex(t *testing.T) {
	got := ruleMap(t, writeFixture(t))
	want := map[string]string{
		"widgets:$.spec#0":        "self.a != 'x'",
		"widgets:$.spec#1":        "self.a != 'y'",
		"widgets:$.spec.list[]#0": "self.n > 0",
	}
	if len(got) != len(want) {
		t.Fatalf("rules = %v, want %v", got, want)
	}
	for id, rule := range want {
		if got[id] != rule {
			t.Errorf("rule %s = %q, want %q", id, got[id], rule)
		}
	}
}

func TestNeutraliseChangesExactlyTheNamedRule(t *testing.T) {
	src, out := writeFixture(t), filepath.Join(t.TempDir(), "out")
	if err := neutralise(src, out, "widgets:$.spec#1"); err != nil {
		t.Fatal(err)
	}
	before, after := ruleMap(t, src), ruleMap(t, out)
	for id, rule := range before {
		want := rule
		if id == "widgets:$.spec#1" {
			want = "true"
		}
		if after[id] != want {
			t.Errorf("after neutralising #1, rule %s = %q, want %q", id, after[id], want)
		}
	}
	// The source is untouched.
	if ruleMap(t, src)["widgets:$.spec#1"] != "self.a != 'y'" {
		t.Error("the source manifests were written")
	}
	// "-" rewrites the manifests and changes no rule.
	same := filepath.Join(t.TempDir(), "same")
	if err := neutralise(src, same, "-"); err != nil {
		t.Fatal(err)
	}
	for id, rule := range before {
		if ruleMap(t, same)[id] != rule {
			t.Errorf("rule %s changed under \"-\"", id)
		}
	}
}

func TestNeutraliseRefusesWhatItCannotChange(t *testing.T) {
	src := writeFixture(t)
	if err := neutralise(src, filepath.Join(t.TempDir(), "o"), "widgets:$.spec#9"); err == nil || !strings.Contains(err.Error(), "no rule") {
		t.Errorf("an unknown rule: %v", err)
	}
	once := filepath.Join(t.TempDir(), "once")
	if err := neutralise(src, once, "widgets:$.spec#0"); err != nil {
		t.Fatal(err)
	}
	// Neutralising a rule that is already "true" would change nothing and look like a kill.
	if err := neutralise(once, filepath.Join(t.TempDir(), "twice"), "widgets:$.spec#0"); err == nil {
		t.Error("a rule that is already \"true\" was neutralised again")
	}
	if err := neutralise(t.TempDir(), filepath.Join(t.TempDir(), "o"), "-"); err == nil {
		t.Error("an empty directory was accepted")
	}
}
