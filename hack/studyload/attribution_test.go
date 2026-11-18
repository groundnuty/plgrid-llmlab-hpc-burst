package main

import (
	"net/http"
	"os"
	"regexp"
	"testing"
)

// The names above are copied from internal/front/errors.go, which a load tool cannot import without the Kubernetes client. This reads that
// file and fails on drift, so a rename there cannot leave the tool recording empty routes. A missing file fails; it is not skipped.
func TestAttributionNamesMatchTheFront(t *testing.T) {
	src, err := os.ReadFile("../../internal/front/errors.go")
	if err != nil {
		t.Fatal(err)
	}
	for constant, value := range map[string]string{
		"HeaderRequestID": headerRequestID, "HeaderEmitter": headerEmitter, "HeaderProvider": headerProvider,
		"HeaderDeploymentID": headerDeploymentID, "HeaderInstance": headerInstance,
	} {
		if !regexp.MustCompile(`\b` + constant + `\s*=\s*"` + regexp.QuoteMeta(value) + `"`).Match(src) {
			t.Errorf("internal/front/errors.go no longer has %s = %q", constant, value)
		}
	}
}

func TestAttributionIsReadFromTheResponseHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Hpcburst-Request-Id", "r1")
	h.Set("Hpcburst-Emitter", "front")
	h.Set("Hpcburst-Provider", "p")
	h.Set("Hpcburst-Deployment-Id", "d")
	h.Set("Hpcburst-Instance", "ns/i")
	a := attributionOf(h)
	if a != (Attribution{"r1", "front", "p", "d", "ns/i"}) || a.route() != (Route{"p", "d", "ns/i"}) {
		t.Errorf("attribution %+v route %+v", a, a.route())
	}
	if (Route{}).String() != "(unattributed)" {
		t.Errorf("an empty route is %q", Route{}.String())
	}
}
