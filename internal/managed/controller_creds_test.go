package managed

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The credentials file (spec.credentialsRef.file), read at every pass, and
// what the gateway's answers to its key do to the Provider's conditions.

func TestCredentialsFileProblems(t *testing.T) {
	fixFile := func(t *testing.T, e *env) { writeCreds(t, e.path, testKey, e.gw.BaseURL(), "") }
	fixRef := func(t *testing.T, e *env) {
		fixFile(t, e)
		e.setCredentialsRef(&v1alpha1.CredentialsReference{File: e.path})
	}
	for _, tc := range []struct {
		name    string
		opts    []envOption
		breakIt func(t *testing.T, e *env)
		fix     func(t *testing.T, e *env)
		reason  string
		message string // a part of the condition's message
	}{
		{"mode 0644", nil,
			func(t *testing.T, e *env) {
				if err := os.Chmod(e.path, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			func(t *testing.T, e *env) {
				if err := os.Chmod(e.path, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			"FileRefused", "0644"},
		{"a missing file", nil,
			func(t *testing.T, e *env) {
				if err := os.Remove(e.path); err != nil {
					t.Fatal(err)
				}
			},
			fixFile, "FileRefused", "no such file"},
		{"malformed JSON", nil,
			func(t *testing.T, e *env) { writeFile(t, e.path, `{"key": `) },
			fixFile, "FileRefused", ""},
		{"a baseURL without /v1", nil,
			func(t *testing.T, e *env) { writeCreds(t, e.path, testKey, e.gw.URL(), "") },
			fixFile, "FileRefused", "/v1"},
		// HBEP-4, Test plan, "Managed credentials": an unknown member, http to
		// a host that is not loopback, an expiry that is not RFC 3339.
		{"an unknown member", nil,
			func(t *testing.T, e *env) {
				writeFile(t, e.path, `{"key":"`+testKey+`","baseURL":"`+e.gw.BaseURL()+`","expires":"2027-01-31T00:00:00Z"}`)
			},
			fixFile, "FileRefused", `unknown member "expires"`},
		{"http to a host that is not loopback", nil,
			func(t *testing.T, e *env) {
				writeFile(t, e.path, `{"key":"`+testKey+`","baseURL":"http://gateway.example/v1"}`)
			},
			fixFile, "FileRefused", "want https"},
		{"an expiry that is not RFC 3339", nil,
			func(t *testing.T, e *env) {
				writeFile(t, e.path, `{"key":"`+testKey+`","baseURL":"`+e.gw.BaseURL()+`","expiry":"2027-01-31"}`)
			},
			fixFile, "FileRefused", "RFC 3339"},
		{"no credentialsRef",
			[]envOption{withCredentialsRef(func(string) *v1alpha1.CredentialsReference { return nil })},
			nil, fixRef, "NoCredentialsFile", "names no credentials file"},
		{"a secretRef",
			[]envOption{withCredentialsRef(func(string) *v1alpha1.CredentialsReference {
				return &v1alpha1.CredentialsReference{SecretRef: &v1alpha1.SecretKeyReference{Name: "example", Namespace: "example", Key: "key"}}
			})},
			nil, fixRef, "NoCredentialsFile", "names no credentials file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, append([]envOption{withModels(model("example-a"))}, tc.opts...)...)
			if tc.breakIt != nil {
				tc.breakIt(t, e)
			}
			e.syncAt(at(12, 0, 0))

			c := e.wantCondition(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionTrue, tc.reason)
			if !strings.Contains(c.Message, provider.ErrCredentials.Error()) || !strings.Contains(c.Message, tc.message) {
				t.Fatalf("message = %q, want it to hold %q and %q", c.Message, provider.ErrCredentials.Error(), tc.message)
			}
			// Not a single request reached the gateway, of any path.
			if n := len(e.gw.Requests()); n != 0 {
				t.Fatalf("the gateway logged %d requests, want 0", n)
			}
			prov := e.provider()
			if len(prov.Status.Catalog) != 0 || prov.Status.LastCatalogRead != nil {
				t.Fatalf("status = catalog %v, lastCatalogRead %v, want none: nothing was read", prov.Status.Catalog, prov.Status.LastCatalogRead)
			}
			wantMT(t, "lastReconcileTime", prov.Status.LastReconcileTime, at(12, 0, 0))
			for _, typ := range []string{v1alpha1.ConditionProviderReady, v1alpha1.ConditionCredentialsRejected, v1alpha1.ConditionCredentialsExpiring} {
				e.wantNoCondition(typ)
			}

			// The pass before the stored floor (lastReconcileTime plus one
			// second plus a minute) writes nothing and has one second to go.
			before := e.provider()
			if wait := e.syncAt(at(12, 1, 0)); wait != time.Second {
				t.Fatalf("Sync at 12:01:00 waited %v, want 1s", wait)
			}
			if e.provider().ResourceVersion != before.ResourceVersion || len(e.gw.Requests()) != 0 {
				t.Fatal("a pass before the floor wrote the Provider or read")
			}

			// Fixed, the next allowed pass reads and clears the condition.
			tc.fix(t, e)
			e.syncAt(at(12, 1, 1))
			e.wantCondition(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionFalse, "FileRead")
			if n := e.gets(); n != 1 {
				t.Fatalf("the gateway logged %d reads after the fix, want 1", n)
			}
			wantEntry(t, e.entry("example-a"), true, "", at(12, 1, 1))
			e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
		})
	}
}

// A file that goes bad after a read leaves the catalog and Ready as they were;
// only CredentialsInvalid and the reconcile time change.
func TestCredentialsFileBreakingAfterAReadKeepsTheCatalog(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	e.syncAt(at(12, 0, 0))
	if err := os.Chmod(e.path, 0o644); err != nil {
		t.Fatal(err)
	}
	e.syncAt(at(12, 1, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionTrue, "FileRefused")
	if n := e.gets(); n != 1 {
		t.Fatalf("the gateway logged %d reads, want 1: none for the refused file", n)
	}
	prov := e.provider()
	wantEntry(t, e.entry("example-a"), true, "", at(12, 0, 0))
	wantMT(t, "lastCatalogRead", prov.Status.LastCatalogRead, at(12, 0, 0))
	wantMT(t, "lastReconcileTime", prov.Status.LastReconcileTime, at(12, 1, 0))
	ready := e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
	wantMT(t, "Ready's lastTransitionTime", &ready.LastTransitionTime, at(12, 0, 0))

	// No read happened since 12:00:00, so a fixed file is read a minute on.
	if err := os.Chmod(e.path, 0o600); err != nil {
		t.Fatal(err)
	}
	e.syncAt(at(12, 1, 10))
	e.wantCondition(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionFalse, "FileRead")
	if n := e.gets(); n != 2 {
		t.Fatalf("the gateway logged %d reads after the fix, want 2", n)
	}
}

// The file is read at every pass: the key is never kept between passes.
func TestKeyIsReadFromTheFileAtEveryPass(t *testing.T) {
	const keyB, unknown = "example-key-0002", "example-key-unknown"
	e := newEnv(t, withModels(model("example-a")), withExtraKeys(keyB))
	e.syncAt(at(12, 0, 0))
	writeCreds(t, e.path, keyB, e.gw.BaseURL(), "")
	e.syncAt(at(12, 1, 0))
	writeCreds(t, e.path, unknown, e.gw.BaseURL(), "")
	e.syncAt(at(12, 2, 0))

	reqs := e.gw.Requests()
	if len(reqs) != 3 {
		t.Fatalf("the gateway logged %d requests, want 3", len(reqs))
	}
	for i, want := range []string{"Bearer " + testKey, "Bearer " + keyB, "Bearer " + unknown} {
		if got := reqs[i].Header.Get("Authorization"); got != want {
			t.Fatalf("request %d sent Authorization %q, want %q", i, got, want)
		}
	}
	// The key the gateway does not know is refused, and the condition says so.
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
}

// A key the gateway rejects (HBEP-4: 401 or 403) sets CredentialsRejected and
// leaves the catalog, lastCatalogRead and Ready as they were.
func TestRejectedKeySetsTheConditionAndLeavesTheCatalog(t *testing.T) {
	for _, status := range []int{401, 403} {
		t.Run("status "+strconv.Itoa(status), func(t *testing.T) {
			e := newEnv(t, withModels(model("example-a")))
			e.syncAt(at(12, 0, 0))

			e.gw.RejectKey(testKey, status)
			e.gw.SetModels(model("example-a"), model("example-d")) // hidden behind the rejection
			e.syncAt(at(12, 5, 0))
			c := e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
			wantMT(t, "condition time", &c.LastTransitionTime, at(12, 5, 0))
			if !strings.Contains(c.Message, strconv.Itoa(status)) {
				t.Fatalf("message = %q, want it to carry the status %d", c.Message, status)
			}
			if got := e.names(); len(got) != 1 || got[0] != "example-a" {
				t.Fatalf("catalog = %v, want it untouched by a rejected read", got)
			}
			wantEntry(t, e.entry("example-a"), true, "", at(12, 0, 0))
			prov := e.provider()
			wantMT(t, "lastCatalogRead", prov.Status.LastCatalogRead, at(12, 0, 0)) // not advanced
			wantMT(t, "lastReconcileTime", prov.Status.LastReconcileTime, at(12, 5, 0))
			ready := e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead") // as it was
			wantMT(t, "Ready's lastTransitionTime", &ready.LastTransitionTime, at(12, 0, 0))
			e.wantCondition(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionFalse, "FileRead")

			// The condition does not churn while the key stays rejected.
			e.syncAt(at(12, 6, 0))
			c = e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
			wantMT(t, "condition time", &c.LastTransitionTime, at(12, 5, 0))
			wantMT(t, "lastReconcileTime", e.provider().Status.LastReconcileTime, at(12, 6, 0))

			e.gw.RejectKey(testKey, 0)
			e.syncAt(at(12, 7, 0))
			c = e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, "KeyAccepted")
			wantMT(t, "condition time", &c.LastTransitionTime, at(12, 7, 0))
			if got := e.names(); len(got) != 2 || got[1] != "example-d" {
				t.Fatalf("catalog = %v, want example-d once a read succeeds", got)
			}
			wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 7, 0))
			if n := e.gets(); n != 4 {
				t.Fatalf("the gateway logged %d reads, want 4", n)
			}
		})
	}
}

func TestRejectedKeyFromTheStartWritesNoCatalog(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	e.gw.RejectKey(testKey, 401)
	e.syncAt(at(12, 0, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
	prov := e.provider()
	if len(prov.Status.Catalog) != 0 || prov.Status.LastCatalogRead != nil {
		t.Fatalf("status = catalog %v, lastCatalogRead %v, want none: nothing was ever read", prov.Status.Catalog, prov.Status.LastCatalogRead)
	}
	wantMT(t, "lastReconcileTime", prov.Status.LastReconcileTime, at(12, 0, 0))
	e.wantNoCondition(v1alpha1.ConditionProviderReady)
}

// A key the gateway lets expire answers 401, as a rejected one does.
func TestExpiredKeyIsRejectedLikeAnyOther(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	e.syncAt(at(12, 0, 0))

	e.gw.ExpireKeyAt(testKey, time.Now().Add(-time.Hour)) // the double keeps real time
	e.gw.SetModels(model("example-a"), model("example-d"))
	e.syncAt(at(12, 1, 0))
	c := e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
	if !strings.Contains(c.Message, "401") {
		t.Fatalf("message = %q, want it to carry 401", c.Message)
	}
	if got := e.names(); len(got) != 1 {
		t.Fatalf("catalog = %v, want it untouched by an expired key", got)
	}
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 0, 0))
	wantMT(t, "lastReconcileTime", e.provider().Status.LastReconcileTime, at(12, 1, 0))
	e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")

	e.gw.ExpireKeyAt(testKey, time.Time{}) // a renewed key
	e.syncAt(at(12, 2, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, "KeyAccepted")
	if got := e.names(); len(got) != 2 {
		t.Fatalf("catalog = %v, want example-d once the key works", got)
	}
}
