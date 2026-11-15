package front

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The tests of GET /hpcburst/v1/deployments/{id} (#585; HBEP-9, the record fetch; HBEP-5, Contract and
// Errors). The records they serve hold HBEP-3's committed golden vectors (pkg/deploymentid/testdata), and
// every expected body is a literal here or a vector's file: none comes from records.go, whose own types
// would agree with whatever it renders. Paths, codes and labels are literals from the HBEPs, not the
// front's constants, so a misspelt constant cannot pass its own test.

// golden is one of HBEP-3's committed golden vectors.
type golden struct {
	canonical []byte // canonical.json: the RFC 8785 bytes of the v1 object
	sha256    string // from sha256.txt: their SHA-256, the full deployment ID
}

func readGolden(t *testing.T, dir string) golden {
	t.Helper()
	base := filepath.Join("..", "..", "pkg", "deploymentid", "testdata", "vectors", dir)
	canon, err := os.ReadFile(filepath.Join(base, "canonical.json"))
	if err != nil {
		t.Fatal(err)
	}
	sum, err := os.ReadFile(filepath.Join(base, "sha256.txt"))
	if err != nil {
		t.Fatal(err)
	}
	return golden{canonical: canon, sha256: strings.Fields(string(sum))[0]}
}

// The vectors' IDs, and their display forms written out from HBEP-3 and the vector files.
const (
	rfDisplay01 = "0e1dd69efe8a" // HBEP-3's worked example: "Its display form is 0e1dd69efe8a"
	rfDisplay03 = "8a2051bec23d"
	// rfForeign shares vector 01's display form and is another full ID, as a record in another
	// namespace may hold (HBEP-3's collision rule is per namespace).
	rfForeign = "0e1dd69efe8a" + "ffffffffffffffffffffffffffffffffffffffffffffffffffff"
	// The Instances of the verification tests: lowercase UUIDs, as the API server gives.
	rfUIDA = "00000000-0000-4000-8000-00000000000a"
	rfUIDB = "00000000-0000-4000-8000-00000000000b"
	rfUIDC = "00000000-0000-4000-8000-00000000000c"
)

// writtenIdentity is an identity record's data as both writers marshal it (internal/lifecycle's
// identityData, identity.go:406; pkg/provider/managedrecord's data): json.Marshal over the canonical
// bytes as a RawMessage. It is the writers' shape, not the reader's.
type writtenIdentity struct {
	Scheme    string          `json:"scheme"`
	SHA256    string          `json:"sha256"`
	Canonical json.RawMessage `json:"canonical"`
}

func identityData(t *testing.T, g golden) []byte {
	t.Helper()
	b, err := json.Marshal(writtenIdentity{Scheme: "v1", SHA256: g.sha256, Canonical: json.RawMessage(g.canonical)})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// servedRecord is a served ID's identity record, from the lifecycle controller's constructor.
func servedRecord(t *testing.T, ns string, g golden) *appsv1.ControllerRevision {
	t.Helper()
	rec, err := v1alpha1.NewIdentityRecord(ns, "md-"+ns, g.sha256, identityData(t, g))
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// verificationData is a verification record's data, written by hand in the shape of internal/lifecycle's
// verificationData: the fields named here are a sample of it, enough to show the record passes whole.
func verificationData(instance, deploymentID string, gen int, verdict, cause string) string {
	d := `{"instance":"` + instance + `","deploymentID":"` + deploymentID + `","generation":` + strconv.Itoa(gen) +
		`,"restart":"start","settings":{"dtype":"bfloat16","max-model-len":"32768"},"resolved":{"tensor-parallel-size":"1"},` +
		`"rehash":{"checked":3},"verdict":"` + verdict + `"`
	if cause != "" {
		d += `,"cause":"` + cause + `"`
	}
	return d + "}"
}

func verificationRecord(t *testing.T, ns, deploymentID, uid string, gen int64, data string) *appsv1.ControllerRevision {
	t.Helper()
	rec, err := v1alpha1.NewVerificationRecord(ns, "md-"+ns, deploymentID, types.UID(uid), gen, []byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// addRecords puts records in the harness's API, as the controllers' writes would, without counting as the
// front's calls.
func (h *harness) addRecords(recs ...*appsv1.ControllerRevision) {
	h.t.Helper()
	for _, r := range recs {
		if err := h.recs.Tracker().Add(r); err != nil {
			h.t.Fatal(err)
		}
	}
}

// getRecords asks for the records of id with the client key and returns the response and its body.
func (h *harness) getRecords(id string) (*http.Response, string) {
	h.t.Helper()
	resp := h.errGet("/hpcburst/v1/deployments/" + id)
	return resp, readAll(h.t, resp)
}

// jsonEqual fails unless got and want are the same JSON value.
func jsonEqual(t *testing.T, what, got, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("%s: the body is not JSON: %v: %s", what, err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("%s: the expected body is not JSON: %v", what, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s: body\n%s\nwant\n%s", what, got, want)
	}
}

// checkOK asserts a 200 with a JSON body and only the front's attribution (HBEP-5, Attribution headers: no
// target is chosen), recorded under its request ID with the tests' client key.
func checkOK(t *testing.T, h *harness, resp *http.Response, body string) {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type %q, want application/json", ct)
	}
	id := checkAttribution(t, resp, efFrontOnly)
	if r := h.record(id); r.Status != http.StatusOK || r.ClientKeyID != "tester" || r.Path != resp.Request.URL.Path {
		t.Errorf("usage record %+v: want status 200, client key tester, path %s", r, resp.Request.URL.Path)
	}
}

// servedCanonical is the canonical member of an answer, as its bytes arrived.
func servedCanonical(t *testing.T, body string) []byte {
	t.Helper()
	var v struct {
		Canonical json.RawMessage `json:"canonical"`
	}
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatal(err)
	}
	return v.Canonical
}

func hexSHA256(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// TestDeploymentRecordsServedByFullAndDisplayForm: a served deployment's ID, asked for by its full form
// and by its display form, answers its identity record: the full ID, the display form, the scheme and the
// canonical object, here byte for byte the vector's RFC 8785 bytes (vector 01 holds none of the characters
// json.Marshal escapes), so they hash to the ID.
func TestDeploymentRecordsServedByFullAndDisplayForm(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	h := newHarness(t, nil)
	h.addRecords(servedRecord(t, "ns-a", g))
	want := `{"id":"` + g.sha256 + `","displayID":"` + rfDisplay01 + `","scheme":"v1","canonical":` + string(g.canonical) +
		`,"identityRecords":[{"namespace":"ns-a","name":"id-` + rfDisplay01 + `"}],"verifications":[]}`
	for _, form := range []struct{ name, id string }{{"full", g.sha256}, {"display", rfDisplay01}} {
		t.Run(form.name, func(t *testing.T) {
			resp, body := h.getRecords(form.id)
			checkOK(t, h, resp, body)
			jsonEqual(t, "GET "+form.id, body, want)
			c := servedCanonical(t, body)
			if !bytes.Equal(c, g.canonical) {
				t.Errorf("canonical bytes\n%s\nwant canonical.json's\n%s", c, g.canonical)
			}
			if sum := hexSHA256(c); sum != g.sha256 {
				t.Errorf("the served canonical object hashes to %s, want the ID %s", sum, g.sha256)
			}
		})
	}
}

// TestDeploymentRecordsCanonicalAsStored: the canonical object passes as the record stores it. Both writers
// store it through json.Marshal, which escapes <, > and & and U+2028 inside it, so for vector 05, which
// holds them, the answer is the same JSON value as canonical.json, not its bytes: a client verifies an ID
// by re-serialising the object per RFC 8785 before hashing.
func TestDeploymentRecordsCanonicalAsStored(t *testing.T) {
	g := readGolden(t, "05-unicode-order")
	h := newHarness(t, nil)
	rec := servedRecord(t, "ns-a", g)
	h.addRecords(rec)
	resp, body := h.getRecords(g.sha256)
	checkOK(t, h, resp, body)
	c := servedCanonical(t, body)
	jsonEqual(t, "the canonical object", string(c), string(g.canonical))
	var stored writtenIdentity
	if err := json.Unmarshal(rec.Data.Raw, &stored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(c, stored.Canonical) {
		t.Errorf("canonical bytes\n%s\nwant the record's as stored\n%s", c, stored.Canonical)
	}
	if bytes.Equal(stored.Canonical, g.canonical) {
		t.Fatal("vector 05's stored canonical object is canonical.json's bytes: the escaping this test is about is gone; revisit its contract sentence")
	}
}

// TestDeploymentRecordsManaged: a managed catalog entry's ID answers its identity record in hpcburst-system
// (#574, PR #578), by full and display form, with no verification records. The front watches ns-a only,
// so the record is reached only because the front reads hpcburst-system too.
func TestDeploymentRecordsManaged(t *testing.T) {
	g := readGolden(t, "03-managed-no-revision-unicode")
	for _, watched := range [][]string{{"ns-a"}, nil} {
		t.Run("watching "+strings.Join(watched, ",")+"(all if empty)", func(t *testing.T) {
			h := newHarness(t, func(c *Config) { c.Namespaces = watched })
			// The platform and catalog name of vector 03's input.json.
			rec, err := v1alpha1.NewManagedIdentityRecord("hpcburst-system", "llmlab", "example-ü-😀-27b", g.sha256, identityData(t, g))
			if err != nil {
				t.Fatal(err)
			}
			h.addRecords(rec)
			want := `{"id":"` + g.sha256 + `","displayID":"` + rfDisplay03 + `","scheme":"v1","canonical":` + string(g.canonical) +
				`,"identityRecords":[{"namespace":"hpcburst-system","name":"id-` + rfDisplay03 + `"}],"verifications":[]}`
			for _, id := range []string{g.sha256, rfDisplay03} {
				resp, body := h.getRecords(id)
				checkOK(t, h, resp, body)
				jsonEqual(t, "GET "+id, body, want)
				if c := servedCanonical(t, body); !bytes.Equal(c, g.canonical) {
					t.Errorf("canonical bytes %s, want canonical.json's %s", c, g.canonical)
				}
			}
		})
	}
}

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

// TestDeploymentRecordsTwoInstances: the verification records of an ID that span two Instances, one of them
// of two workers, are each answered with its Instance UID, its worker, its generation, its outcome and its
// data as stored, sorted. A record in another namespace that carries the same display form but names
// another full ID is not the ID's, and is left out.
func TestDeploymentRecordsTwoInstances(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	h := newHarness(t, nil)
	a1 := verificationData("md-ns-a-0", g.sha256, 1, "fail", "rehash")
	a2 := verificationData("md-ns-a-0", g.sha256, 2, "pass", "")
	b1 := verificationData("md-ns-a-1", g.sha256, 1, "pass", "")
	bw := verificationData("md-ns-a-1", g.sha256, 1, "pass", "")
	worker, err := v1alpha1.NewWorkerVerificationRecord("ns-a", "md-ns-a", g.sha256, rfUIDB, "worker-1", 1, []byte(bw))
	if err != nil {
		t.Fatal(err)
	}
	h.addRecords(servedRecord(t, "ns-a", g),
		verificationRecord(t, "ns-a", g.sha256, rfUIDA, 2, a2), verificationRecord(t, "ns-a", g.sha256, rfUIDB, 1, b1),
		worker, verificationRecord(t, "ns-a", g.sha256, rfUIDA, 1, a1),
		verificationRecord(t, "ns-b", rfForeign, rfUIDC, 1, verificationData("md-ns-b-0", rfForeign, 1, "pass", "")))
	want := `{"id":"` + g.sha256 + `","displayID":"` + rfDisplay01 + `","scheme":"v1","canonical":` + string(g.canonical) +
		`,"identityRecords":[{"namespace":"ns-a","name":"id-` + rfDisplay01 + `"}],"verifications":[` +
		`{"namespace":"ns-a","name":"ver-` + rfUIDA + `-1","instanceUID":"` + rfUIDA + `","generation":1,"outcome":"fail","record":` + a1 + `},` +
		`{"namespace":"ns-a","name":"ver-` + rfUIDA + `-2","instanceUID":"` + rfUIDA + `","generation":2,"outcome":"pass","record":` + a2 + `},` +
		`{"namespace":"ns-a","name":"ver-` + rfUIDB + `-1","instanceUID":"` + rfUIDB + `","generation":1,"outcome":"pass","record":` + b1 + `},` +
		`{"namespace":"ns-a","name":"ver-` + rfUIDB + `-worker-1-1","instanceUID":"` + rfUIDB + `","worker":"worker-1","generation":1,"outcome":"pass","record":` + bw + `}]}`
	for _, id := range []string{g.sha256, rfDisplay01} {
		resp, body := h.getRecords(id)
		checkOK(t, h, resp, body)
		jsonEqual(t, "GET "+id, body, want)
	}
}

// TestDeploymentRecordsSameIDInTwoNamespaces: one full ID served in two namespaces has a record in each;
// the answer names both, and the verification records of both.
func TestDeploymentRecordsSameIDInTwoNamespaces(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	h := newHarness(t, nil)
	a := verificationData("md-ns-a-0", g.sha256, 1, "pass", "")
	b := verificationData("md-ns-b-0", g.sha256, 1, "fail", "contradiction")
	h.addRecords(servedRecord(t, "ns-b", g), servedRecord(t, "ns-a", g),
		verificationRecord(t, "ns-b", g.sha256, rfUIDB, 1, b), verificationRecord(t, "ns-a", g.sha256, rfUIDA, 1, a))
	want := `{"id":"` + g.sha256 + `","displayID":"` + rfDisplay01 + `","scheme":"v1","canonical":` + string(g.canonical) +
		`,"identityRecords":[{"namespace":"ns-a","name":"id-` + rfDisplay01 + `"},{"namespace":"ns-b","name":"id-` + rfDisplay01 + `"}],"verifications":[` +
		`{"namespace":"ns-a","name":"ver-` + rfUIDA + `-1","instanceUID":"` + rfUIDA + `","generation":1,"outcome":"pass","record":` + a + `},` +
		`{"namespace":"ns-b","name":"ver-` + rfUIDB + `-1","instanceUID":"` + rfUIDB + `","generation":1,"outcome":"fail","record":` + b + `}]}`
	resp, body := h.getRecords(rfDisplay01)
	checkOK(t, h, resp, body)
	jsonEqual(t, "GET "+rfDisplay01, body, want)
}
