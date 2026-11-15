package front

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

// The record fetch's errors (#585; HBEP-5's Errors table, as PR #586 amends it): 404 deployment_not_found,
// 401 invalid_api_key, 409 deployment_ambiguous and 503 records_unavailable, for a failed read and for a
// record that cannot be read. The fixtures and helpers are records_test.go's.

// TestDeploymentRecordsUnknownID: an ID no identity record holds answers HBEP-5's 404
// deployment_not_found, with only the front's attribution. That covers a display form with no record, a
// full ID whose display form another ID's record holds, a served ID in a namespace the front does not
// watch, and anything that is not 12 or 64 lowercase hex.
func TestDeploymentRecordsUnknownID(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	h := newHarness(t, func(c *Config) { c.Namespaces = []string{"ns-a"} })
	h.addRecords(servedRecord(t, "ns-a", g), servedRecord(t, "ns-z", readGolden(t, "07-bytewise-paths-no-gpu")))
	seven := readGolden(t, "07-bytewise-paths-no-gpu").sha256
	for _, id := range []string{
		"abcdef012345",    // a display form with no record
		rfForeign,         // vector 01's display form, another full ID
		seven, seven[:12], // a record in ns-z, which the front does not watch
		strings.ToUpper(rfDisplay01), rfDisplay01[:11], rfDisplay01 + "0", // not 12 or 64 lowercase hex
		rfDisplay01 + "%2Fx", // decoded once, a slash
		"",
	} {
		resp, body := h.getRecords(id)
		checkError(t, resp, body, wantError{status: http.StatusNotFound, code: "deployment_not_found"})
		checkAttribution(t, resp, efFrontOnly)
	}
}

// TestDeploymentRecordsNeedAClientKey: the route is authenticated like any request (HBEP-5, Request path
// step 1): no key, a wrong one or another scheme answers 401 invalid_api_key, and the front reads no
// record for it.
func TestDeploymentRecordsNeedAClientKey(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	h := newHarness(t, nil)
	h.addRecords(servedRecord(t, "ns-a", g))
	for _, auth := range []string{"", "Bearer wrong-key", "Basic " + h.clientKey, "Bearer "} {
		req, err := http.NewRequest(http.MethodGet, h.url+"/hpcburst/v1/deployments/"+g.sha256, nil)
		if err != nil {
			t.Fatal(err)
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := errClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body := readAll(t, resp)
		checkError(t, resp, body, wantError{status: http.StatusUnauthorized, code: "invalid_api_key"})
		checkAttribution(t, resp, efFrontOnly)
	}
	if a := h.recs.Actions(); len(a) != 0 {
		t.Errorf("the front called the API for unauthenticated requests: %v", a)
	}
}

// TestDeploymentRecordsAmbiguousDisplayForm: a display form that two identity records hold with different
// full IDs, as records in two namespaces may (HBEP-3's collision check is per namespace), answers 409
// deployment_ambiguous, its message naming both full IDs; each full ID still answers its own record. The
// second record is injected, as HBEP-3's test plan does for a collision, since a real one cannot be
// produced: it says it holds rfForeign, and holds vector 07's object.
func TestDeploymentRecordsAmbiguousDisplayForm(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	seven := readGolden(t, "07-bytewise-paths-no-gpu")
	h := newHarness(t, nil)
	injected := servedRecord(t, "ns-b", golden{canonical: seven.canonical, sha256: rfForeign})
	h.addRecords(servedRecord(t, "ns-a", g), injected)
	resp, body := h.getRecords(rfDisplay01)
	checkError(t, resp, body, wantError{status: http.StatusConflict, code: "deployment_ambiguous"})
	checkAttribution(t, resp, efFrontOnly)
	for _, id := range []string{g.sha256, rfForeign} {
		if !strings.Contains(body, id) {
			t.Errorf("the 409's message does not name %s: %s", id, body)
		}
	}
	for _, c := range []struct{ id, ns string }{{g.sha256, "ns-a"}, {rfForeign, "ns-b"}} {
		resp, body := h.getRecords(c.id)
		checkOK(t, h, resp, body)
		if !strings.Contains(body, `"id":"`+c.id+`"`) || !strings.Contains(body, `"identityRecords":[{"namespace":"`+c.ns+`","name":"id-`+rfDisplay01+`"}]`) {
			t.Errorf("GET %s: %s, want its own record in %s alone", c.id, body, c.ns)
		}
	}
}

// TestDeploymentRecordsUnavailable: a failed read of the records answers 503 records_unavailable with
// Retry-After: 5, never a 404 (HBEP-9, the record fetch).
func TestDeploymentRecordsUnavailable(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	h := newHarness(t, nil)
	h.addRecords(servedRecord(t, "ns-a", g))
	h.recs.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("the API server is unreachable")
	})
	for _, id := range []string{g.sha256, rfDisplay01, "abcdef012345"} {
		resp, body := h.getRecords(id)
		checkError(t, resp, body, wantError{status: http.StatusServiceUnavailable, code: "records_unavailable", retryAfter: "5"})
		checkAttribution(t, resp, efFrontOnly)
	}
}

// TestDeploymentRecordsCorruptRecord: a record found under the ID's display form that cannot be read or
// re-canonicalised answers 503 records_unavailable with Retry-After: 5, alone or beside a good record, by
// either form of the ID: never 200, which would vouch for an answer beside it, and never 404, which would
// say no such deployment exists (HBEP-9, the record fetch, as PR #586 amends it).
func TestDeploymentRecordsCorruptRecord(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	// numberObject is a canonical object of no scheme v1 type: RFC 8785 text cannot be written for it.
	numberObject := golden{canonical: []byte(`{"a":1}`), sha256: g.sha256}
	notJSON := func(rec *appsv1.ControllerRevision) *appsv1.ControllerRevision {
		rec.Data.Raw = []byte("not JSON")
		return rec
	}
	for _, c := range []struct {
		name    string
		records func(t *testing.T) []*appsv1.ControllerRevision
		corrupt string // the corrupt record's name, which the message names
	}{
		{"an identity record that cannot be re-canonicalised, alone", func(t *testing.T) []*appsv1.ControllerRevision {
			return []*appsv1.ControllerRevision{servedRecord(t, "ns-a", numberObject)}
		}, "ns-a/id-" + rfDisplay01},
		{"an identity record whose data is not JSON, alone", func(t *testing.T) []*appsv1.ControllerRevision {
			return []*appsv1.ControllerRevision{notJSON(servedRecord(t, "ns-a", g))}
		}, "ns-a/id-" + rfDisplay01},
		{"a second identity record that cannot be re-canonicalised, beside a good one", func(t *testing.T) []*appsv1.ControllerRevision {
			return []*appsv1.ControllerRevision{servedRecord(t, "ns-a", g), servedRecord(t, "ns-b", numberObject)}
		}, "ns-b/id-" + rfDisplay01},
		{"a verification record with a verdict neither pass nor fail, beside a good identity record", func(t *testing.T) []*appsv1.ControllerRevision {
			return []*appsv1.ControllerRevision{servedRecord(t, "ns-a", g),
				verificationRecord(t, "ns-a", g.sha256, rfUIDA, 1, verificationData("md-ns-a-0", g.sha256, 1, "maybe", ""))}
		}, "ns-a/ver-" + rfUIDA + "-1"},
		{"a verification record whose data is not JSON, beside a good identity record", func(t *testing.T) []*appsv1.ControllerRevision {
			return []*appsv1.ControllerRevision{servedRecord(t, "ns-a", g),
				notJSON(verificationRecord(t, "ns-b", rfForeign, rfUIDC, 1, verificationData("md-ns-b-0", rfForeign, 1, "pass", "")))}
		}, "ns-b/ver-" + rfUIDC + "-1"},
		{"a verification record that names no full ID, beside a good identity record", func(t *testing.T) []*appsv1.ControllerRevision {
			return []*appsv1.ControllerRevision{servedRecord(t, "ns-a", g),
				verificationRecord(t, "ns-a", g.sha256, rfUIDA, 1, verificationData("md-ns-a-0", "", 1, "pass", ""))}
		}, "ns-a/ver-" + rfUIDA + "-1"},
		{"a verification record whose label and name disagree, beside a good identity record", func(t *testing.T) []*appsv1.ControllerRevision {
			rec := verificationRecord(t, "ns-a", g.sha256, rfUIDA, 1, verificationData("md-ns-a-0", g.sha256, 1, "pass", ""))
			rec.Labels["hpcburst.groundnuty.github.io/instance-uid"] = rfUIDB
			return []*appsv1.ControllerRevision{servedRecord(t, "ns-a", g), rec}
		}, "ns-a/ver-" + rfUIDA + "-1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.addRecords(c.records(t)...)
			for _, id := range []string{g.sha256, rfDisplay01} {
				resp, body := h.getRecords(id)
				checkError(t, resp, body, wantError{status: http.StatusServiceUnavailable, code: "records_unavailable", retryAfter: "5"})
				checkAttribution(t, resp, efFrontOnly)
				if !strings.Contains(body, c.corrupt) {
					t.Errorf("GET %s: the 503's message does not name the record %s: %s", id, c.corrupt, body)
				}
			}
		})
	}
}
