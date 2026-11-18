package front

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// HBEP-5, Workloads (beta, proposed), Naming it: a request names a Workload by the header workloadHeader sets
// (x-hpcburst-workload) or by the "#<workload>" suffix of its model name; the front finds it by name among the Workloads whose spec.model is
// the request's model; and each of three namings is a 400 invalid_model_name, never a fallback to no Workload: no Workload of that name
// (unknown_workload), one in more than one namespace (workload_ambiguous), and a header and a suffix that name different Workloads
// (workload_conflict). The errors come in the order the science agent ruled on #764 (Q9): grammar, unknown_provider, those three, then not_attached
// and no_ready_target. Every status, code and detail below is typed from the HBEP, never read from the front's constants.

const wlHeader = "x-hpcburst-workload"

// wlFront is a front with one served ModelDeployment of scModel, md-a, with one Instance, a-0, and the Workloads of each test.
func wlFront(t *testing.T) *pwFix {
	t.Helper()
	return newPwFix(t, pwDep{name: "md-a", instances: []string{"a-0"}})
}

// say posts a chat request for model with the extra headers.
func (f *pwFix) say(model string, hdr ...string) (*http.Response, string) {
	f.t.Helper()
	resp := f.errPost(scChat, errChatBody(model, false, "hello there"), hdr...)
	return resp, readAll(f.t, resp)
}

// refused asserts a 400 invalid_model_name with the detail, raised by the front before any target, and that no engine saw the request. It returns
// the request's usage record.
func (f *pwFix) refused(what, model string, detail string, hdr ...string) usage.Record {
	f.t.Helper()
	before := f.seen()
	resp, body := f.say(model, hdr...)
	f.checkFrontError(f.t, resp, body, wantError{status: 400, code: "invalid_model_name", detail: detail})
	for k, n := range f.seen() {
		if n != before[k] {
			f.t.Errorf("%s: the engine of %s saw a request that was refused", what, k)
		}
	}
	return f.record(resp.Header.Get("hpcburst-request-id"))
}

// served asserts a 200 from the Instance and returns the request's usage record.
func (f *pwFix) served(what, inst, model string, hdr ...string) usage.Record {
	f.t.Helper()
	resp, body := f.say(model, hdr...)
	if resp.StatusCode != 200 || scInstanceOf(resp) != inst {
		f.t.Fatalf("%s: status %d from %q, want 200 from %s: %s", what, resp.StatusCode, scInstanceOf(resp), inst, body)
	}
	return f.record(resp.Header.Get("hpcburst-request-id"))
}

func wantWorkload(t *testing.T, what string, rec usage.Record, want *string) {
	t.Helper()
	switch {
	case want == nil && rec.Workload != nil:
		t.Errorf("%s: the record's workload is %q, want null", what, *rec.Workload)
	case want != nil && (rec.Workload == nil || *rec.Workload != *want):
		t.Errorf("%s: the record's workload is %v, want %q", what, rec.Workload, *want)
	}
}

func str(s string) *string { return &s }

// A Workload is named by the suffix, by the header, or by both naming the same one, and each is served from the Workload's deployment. The record
// carries the resolved name; a request that names none carries null, in the JSON as null and not absent.
func TestAWorkloadIsNamedBySuffixByHeaderOrByBoth(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.apply()
	for name, c := range map[string]struct {
		model string
		hdr   []string
	}{
		"the suffix":                        {scModel + "#study-a", nil},
		"the header":                        {scModel, []string{wlHeader, "study-a"}},
		"the header's name, in capitals":    {scModel, []string{"X-HPCBURST-WORKLOAD", "study-a"}},
		"both, naming the same Workload":    {scModel + "#study-a", []string{wlHeader, "study-a"}},
		"the suffix and an empty header":    {scModel + "#study-a", []string{wlHeader, ""}},
		"the suffix, at the provider level": {scModel + "@helios-a#study-a", nil},
	} {
		rec := f.served(name, "ns/a-0", c.model, c.hdr...)
		wantWorkload(t, name, rec, str("study-a"))
		if rec.RequestedModel != c.model {
			t.Errorf("%s: requestedModel %q, want what was sent, %q", name, rec.RequestedModel, c.model)
		}
	}
	// No Workload: null in the JSON, not absent and not "".
	rec := f.served("no Workload", "ns/a-0", scModel)
	wantWorkload(t, "no Workload", rec, nil)
	if !bytes.Contains(readUsageFiles(t, f.cfg.UsageDir), []byte(`"workload":null`)) {
		t.Errorf("no usage line has %q: a request that names no Workload is recorded with a null workload", `"workload":null`)
	}
	// An empty header names nothing, as an empty session header does.
	wantWorkload(t, "an empty header", f.served("an empty header", "ns/a-0", scModel, wlHeader, ""), nil)
}

// Row: the name or the header names no Workload of the model. A Workload of another model by that name is no Workload of this one; a header that is
// no DNS label can name none. Each is refused, with a null workload in the record, and none reaches an engine.
func TestAnUnknownWorkloadIsRefused(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.workload("ns", "other-model-wl", "example-other-7b")
	f.apply()
	for name, c := range map[string]struct {
		model string
		hdr   []string
	}{
		"a suffix no Workload has":                        {scModel + "#nope", nil},
		"a header no Workload has":                        {scModel, []string{wlHeader, "nope"}},
		"a header that is no DNS label":                   {scModel, []string{wlHeader, "Bad_Name"}},
		"a Workload of another model":                     {scModel + "#other-model-wl", nil},
		"a header naming a Workload of another":           {scModel, []string{wlHeader, "other-model-wl"}},
		"a header of two names in one line":               {scModel, []string{wlHeader, "study-a, nope"}},
		"a suffix with a header that exists":              {scModel + "#nope", []string{wlHeader, "study-a"}},
		"the header's capitalisation is no name":          {scModel, []string{wlHeader, "Study-A"}},
		"a suffix no Workload has, at the provider level": {scModel + "@helios-a#nope", nil},
	} {
		rec := f.refused(name, c.model, "unknown_workload", c.hdr...)
		wantWorkload(t, name, rec, nil)
	}
}

// Row: one Workload in more than one namespace. The same name in two namespaces is ambiguous for a model that both name, and its twin, the same
// name for two different models, is not.
func TestAWorkloadInTwoNamespacesIsAmbiguousUnlessTheirModelsDiffer(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "dup", scModel)
	f.workload("ns-two", "dup", scModel)
	f.workload("ns", "twin", scModel)
	f.workload("ns-two", "twin", "example-other-7b") // the twin: not a Workload of scModel
	f.workload("ns", "solo", scModel)
	f.apply()
	for name, c := range map[string]struct {
		model string
		hdr   []string
	}{
		"by the suffix": {scModel + "#dup", nil},
		"by the header": {scModel, []string{wlHeader, "dup"}},
		"by both":       {scModel + "#dup", []string{wlHeader, "dup"}},
	} {
		rec := f.refused(name, c.model, "workload_ambiguous", c.hdr...)
		wantWorkload(t, name, rec, nil)
	}
	wantWorkload(t, "the twin", f.served("the twin: the same name for another model", "ns/a-0", scModel+"#twin"), str("twin"))
	wantWorkload(t, "a Workload in one namespace", f.served("a Workload in one namespace", "ns/a-0", scModel+"#solo"), str("solo"))
}

// Row: a header and a suffix that name different Workloads. Two names that are the same Workload are accepted (the first test above).
func TestAHeaderAndASuffixNamingDifferentWorkloadsConflict(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.workload("ns", "study-b", scModel)
	f.apply()
	rec := f.refused("the suffix a, the header b", scModel+"#study-a", "workload_conflict", wlHeader, "study-b")
	wantWorkload(t, "the suffix a, the header b", rec, nil)
	f.refused("the suffix b, the header a", scModel+"#study-b", "workload_conflict", wlHeader, "study-a")
	// Neither name is refused alone.
	wantWorkload(t, "a alone", f.served("a alone", "ns/a-0", scModel+"#study-a"), str("study-a"))
	wantWorkload(t, "b alone", f.served("b alone", "ns/a-0", scModel, wlHeader, "study-b"), str("study-b"))
}

// The order of the errors (Q9), one case for each adjacent pair, each with both faults in the one request: grammar before unknown_provider,
// unknown_provider before the Workload's three, those three in the order HBEP-5 lists them, and each before not_attached; not_attached before
// no_ready_target.
func TestTheErrorsComeInTheOrderTheScienceAgentRuled(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.workload("ns", "study-b", scModel)
	f.workload("ns", "dup", scModel)
	f.workload("ns-two", "dup", scModel)
	f.workload("ns", "orphan-wl", "example-orphan-1b")       // a model no ModelDeployment names
	f.workload("ns", "orphan-wl-2", "example-orphan-1b")     //
	f.workload("ns", "lonely", scModel)                      // listed by no deployment: its serving set is empty
	f.md("md-a").Spec.Workloads = []string{"study-a", "dup"} // so md-a serves study-a and dup, and not study-b or lonely
	f.apply()
	cases := []struct {
		name   string
		model  string
		hdr    []string
		detail string // of a 400
		inact  string // of a 503
	}{
		// grammar, then unknown_provider: a name with both faults is a grammar error.
		{"grammar before unknown_provider", scModel + "@no-such-provider#", nil, "grammar", ""},
		{"grammar before unknown_provider, two hashes", scModel + "@no-such-provider#a#b", nil, "grammar", ""},
		// unknown_provider, then the Workload's: an unknown provider with a Workload no one has.
		{"unknown_provider before unknown_workload", scModel + "@no-such-provider#nope", nil, "unknown_provider", ""},
		{"unknown_provider before workload_conflict", scModel + "@no-such-provider#study-a", []string{wlHeader, "study-b"}, "unknown_provider", ""},
		// unknown_workload, then workload_ambiguous: a header that is ambiguous and a suffix that is unknown.
		{"unknown_workload before workload_ambiguous", scModel + "#nope", []string{wlHeader, "dup"}, "unknown_workload", ""},
		// workload_ambiguous, then workload_conflict: an ambiguous header and a suffix that is another, existing Workload.
		{"workload_ambiguous before workload_conflict", scModel + "#study-a", []string{wlHeader, "dup"}, "workload_ambiguous", ""},
		// the Workload's three before not_attached: a Workload of a model no deployment names, with a conflict and with an unknown name.
		{"workload_conflict before not_attached", "example-orphan-1b#orphan-wl", []string{wlHeader, "orphan-wl-2"}, "workload_conflict", ""},
		{"unknown_workload before not_attached", "example-orphan-1b#nope", nil, "unknown_workload", ""},
		// not_attached: the Workload resolves, and its model has no deployment; or its serving set is empty.
		{"not_attached for a model no deployment names", "example-orphan-1b#orphan-wl", nil, "", "not_attached"},
		{"not_attached for an empty serving set", scModel + "#lonely", nil, "", "not_attached"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, body := f.say(c.model, c.hdr...)
			if c.detail != "" {
				f.checkFrontError(t, resp, body, wantError{status: 400, code: "invalid_model_name", detail: c.detail})
			} else {
				f.checkFrontError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: c.inact, retryAfter: "30"})
			}
		})
	}

	// not_attached before no_ready_target: serving deployments with no eligible target are no_ready_target. Both are 503 model_inactive; the
	// detail tells them apart, and a Workload whose serving deployment has no Instance ready is the second.
	f.inst(f.uids["ns/a-0"]).Status.Phase = v1alpha1.InstanceVerifying
	f.apply()
	resp, body := f.say(scModel + "#study-a")
	f.checkFrontError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
	resp, body = f.say(scModel + "#lonely")
	f.checkFrontError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "not_attached", retryAfter: "30"})
}

// A refusal after the Workload resolved carries it in the record, and a refusal before does not: not_attached and no_ready_target name the
// Workload the request resolved to, the three 400s never do.
func TestTheRecordNamesTheWorkloadOnlyOnceItResolved(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.workload("ns", "lonely", scModel)
	f.md("md-a").Spec.Workloads = []string{"study-a"}
	f.apply()
	f.inst(f.uids["ns/a-0"]).Status.Phase = v1alpha1.InstanceVerifying
	f.apply()

	resp, body := f.say(scModel + "#study-a")
	f.checkFrontError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
	wantWorkload(t, "no_ready_target", f.record(resp.Header.Get("hpcburst-request-id")), str("study-a"))

	resp, body = f.say(scModel + "#lonely")
	f.checkFrontError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "not_attached", retryAfter: "30"})
	wantWorkload(t, "not_attached", f.record(resp.Header.Get("hpcburst-request-id")), str("lonely"))

	wantWorkload(t, "unknown_workload", f.refused("unknown", scModel+"#nope", "unknown_workload"), nil)
	// A Workload header of a name no Workload can have is never written to the record, whatever the client sent.
	f.refused("raw text", scModel, "unknown_workload", wlHeader, "<script>raw header text</script>")
	if bytes.Contains(readUsageFiles(t, f.cfg.UsageDir), []byte("raw header text")) {
		t.Error("a usage record holds the raw text of a Workload header that named no Workload")
	}
}

// An edit applies to the requests that arrive after the front sees it (HBEP-5, Workloads, Edits; the science agent's ruling Q15, the part that
// is routing): a request that names a Workload once it is deleted gets unknown_workload, and one in flight is not touched. The FIFO and the
// counts of the edit are a later part of the build.
func TestADeletedWorkloadIsUnknownToNewRequestsAndInFlightOnesFinish(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.apply()
	wantWorkload(t, "before", f.served("before", "ns/a-0", scModel+"#study-a"), str("study-a"))

	hold := make(chan struct{})
	f.engines["ns/a-0"].Set(enginetest.Behaviour{Hold: hold})
	held := scGo(f.harness, errChatBody(scModel+"#study-a", false, "hello there"))
	errWaitFor(t, "the request to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })

	f.dropWorkload("ns", "study-a")
	f.apply()
	f.engines["ns/a-0"].Set(enginetest.Behaviour{})
	f.refused("after the delete", scModel+"#study-a", "unknown_workload")
	f.refused("after the delete, by header", scModel, "unknown_workload", wlHeader, "study-a")

	close(hold)
	resp, body := held.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
		t.Errorf("the request in flight when the Workload was deleted: status %d from %q, want 200 from ns/a-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	if !strings.Contains(string(readUsageFiles(t, f.cfg.UsageDir)), `"workload":"study-a"`) {
		t.Error("no record carries the Workload of the request that was in flight")
	}
}

// A header sent on two lines with two different names names no Workload that exists (it reads as the combined list, HBEP-5's grammar has no
// such name), and on two lines with one name it is that name. net/http's Header.Set would send one line, so the test adds both.
func TestAWorkloadHeaderSentTwiceReadsAsItsCombinedList(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.apply()
	for name, c := range map[string]struct {
		lines  []string
		detail string // "" is served
	}{
		"two lines, one name":      {[]string{"study-a", "study-a"}, ""},
		"two lines, two names":     {[]string{"study-a", "nope"}, "unknown_workload"},
		"an empty line and a name": {[]string{"", "study-a"}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, f.url+scChat, strings.NewReader(errChatBody(scModel, false, "hello there")))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+f.clientKey)
			for _, l := range c.lines {
				req.Header.Add(wlHeader, l)
			}
			resp, err := errClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body := readAll(t, resp)
			if c.detail == "" {
				if resp.StatusCode != 200 {
					t.Fatalf("status %d, want 200: %s", resp.StatusCode, body)
				}
				return
			}
			f.checkFrontError(t, resp, body, wantError{status: 400, code: "invalid_model_name", detail: c.detail})
		})
	}
}
