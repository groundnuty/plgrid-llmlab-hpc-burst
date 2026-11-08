package quickstart

import (
	"regexp"
	"strings"
	"testing"
)

// For a reasoning model that exhausts its budget, the guide tells the operator to declare the
// workload's outputTokens.typical as the typical total output, reasoning included (issue #266).
// A field the guide names as spec.workload.<path> must be a property of the ModelDeployment's
// stored schema in config/crd, which is generated from the Go types, so a renamed or misspelt
// field fails here.
//
// The test reads the guide's path and the schema, and nothing of what the field does: its effect
// is checked by reading the code (internal/agent/engine/qualify.go, pkg/agentapi/checks.go,
// internal/lifecycle/spec.go).
func TestOperationsWorkloadFieldsAreInTheSchema(t *testing.T) {
	kind := readCRDNames(t)["modeldeployment"]
	if kind == nil {
		t.Fatal("config/crd has no ModelDeployment")
	}
	path := regexp.MustCompile("`(spec\\.workload(?:\\.[A-Za-z]+)*)`")
	found := map[string]bool{}
	for _, s := range opsSegments(t) {
		if s.fenced {
			continue
		}
		for _, m := range path.FindAllStringSubmatch(s.text, -1) {
			found[m[1]] = true
			schema := kind.schema
			for _, part := range strings.Split(m[1], ".") {
				props, _ := schema["properties"].(map[string]any)
				next, ok := props[part].(map[string]any)
				if !ok {
					t.Errorf("the guide names %s, but %q is not a property of the ModelDeployment's schema at that point", m[1], part)
					break
				}
				schema = next
			}
		}
	}
	if !found["spec.workload.outputTokens.typical"] {
		t.Errorf("the guide no longer names spec.workload.outputTokens.typical, the figure to declare for a reasoning model; it names %v", found)
	}
}
