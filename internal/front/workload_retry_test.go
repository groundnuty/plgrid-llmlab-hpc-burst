package front

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Workloads, Retries (the science agent's ruling on #764, Q13, the part that is the serving set): a request's retries re-choose with the
// name it was parsed to, so the Workload's serving set holds on every retry. The proxy chooses again at three sites: the dial refused on a
// target, which goes to the next target at once (the first alpha resend), the dial refused after the request was written to another Instance
// (the same, among Instances only), and a request written and lost before any response byte, retried to another Instance (beta's retry).
//
// The layout is md-a (a-0, lists alpha), md-b (b-0, lists beta) and md-c (c-0, lists alpha). The session is homed on a-0, and the fork's ring puts
// b-0 next on the walk over all three, then c-0, so without the Workload a retry goes to b-0, and with it, only to c-0: b-0 does not serve alpha.

func retryPinFix(t *testing.T) (*pwFix, string) {
	t.Helper()
	f := newPwFix(t,
		pwDep{name: "md-a", instances: []string{"a-0"}, workloads: []string{"alpha"}},
		pwDep{name: "md-b", instances: []string{"b-0"}, workloads: []string{"beta"}},
		pwDep{name: "md-c", instances: []string{"c-0"}, workloads: []string{"alpha"}})
	f.workload("ns", "alpha", scModel)
	f.workload("ns", "beta", scModel)
	f.apply()
	all := pwNames(f.deps...)
	for i := 0; i < 500; i++ {
		s := "retry-key-" + string(rune('a'+i%26)) + string(rune('a'+i/26%26))
		if pwHome(t, all, s) == "ns/a-0" && pwHome(t, []string{"ns/b-0", "ns/c-0"}, s) == "ns/b-0" {
			return f, s
		}
	}
	t.Fatal("no session is home on a-0 with b-0 next on the walk")
	return nil, ""
}

// wantRetried asserts the answer came from inst and the engines saw this many requests each: a-0, b-0, c-0.
func wantRetried(t *testing.T, what string, f *pwFix, inst string, a, b, c int) {
	t.Helper()
	got := map[string]int{}
	for k, n := range f.seen() {
		got[k] = n
	}
	if got["ns/a-0"] != a || got["ns/b-0"] != b || got["ns/c-0"] != c {
		t.Errorf("%s: the engines saw a-0 %d, b-0 %d, c-0 %d requests, want %d, %d, %d (the answer came from %s)", what, got["ns/a-0"], got["ns/b-0"], got["ns/c-0"], a, b, c, inst)
	}
}

// Control: with no Workload the retry's next target is b-0, so the layout makes the filter the only reason the tests below do not reach it.
func TestWithNoWorkloadTheRetryGoesToTheNextInstanceOnTheWalk(t *testing.T) {
	f, s := retryPinFix(t)
	f.engines["ns/a-0"].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	resp := f.errPost(scChat, errChatBody(scModel, false, "hello there"), "x-session-id", s)
	body := readAll(t, resp)
	wantWhole(t, resp, body, "ns/b-0")
	wantRetried(t, "no Workload", f, "ns/b-0", 1, 1, 0)
}

// The retry after a request was written and lost keeps the serving set, by suffix and by header.
func TestARetryAfterALostRequestKeepsTheWorkloadsServingSet(t *testing.T) {
	for name, c := range map[string]struct {
		model string
		hdr   []string
	}{
		"named by the suffix": {scModel + "#alpha", nil},
		"named by the header": {scModel, []string{wlHeader, "alpha"}},
		"named by both":       {scModel + "#alpha", []string{wlHeader, "alpha"}},
	} {
		t.Run(name, func(t *testing.T) {
			f, s := retryPinFix(t)
			f.engines["ns/a-0"].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
			resp := f.errPost(scChat, errChatBody(c.model, false, "hello there"), append([]string{"x-session-id", s}, c.hdr...)...)
			body := readAll(t, resp)
			wantWhole(t, resp, body, "ns/c-0")
			wantRetried(t, name, f, "ns/c-0", 1, 0, 1)
			wantWorkload(t, name, f.record(resp.Header.Get("hpcburst-request-id")), str("alpha"))
		})
	}
}

// The resend after a dial refused keeps it too: a-0's engine is gone, the request was written nowhere, and it goes to the next serving target.
func TestAResendAfterARefusedDialKeepsTheWorkloadsServingSet(t *testing.T) {
	for name, c := range map[string]struct {
		model string
		hdr   []string
	}{
		"named by the suffix": {scModel + "#alpha", nil},
		"named by the header": {scModel, []string{wlHeader, "alpha"}},
	} {
		t.Run(name, func(t *testing.T) {
			f, s := retryPinFix(t)
			f.engines["ns/a-0"].Close()
			resp := f.errPost(scChat, errChatBody(c.model, false, "hello there"), append([]string{"x-session-id", s}, c.hdr...)...)
			body := readAll(t, resp)
			wantWhole(t, resp, body, "ns/c-0")
			wantRetried(t, name, f, "ns/c-0", 0, 0, 1)
			wantWorkload(t, name, f.record(resp.Header.Get("hpcburst-request-id")), str("alpha"))
		})
	}
}

// The control of the two above: with no Workload, the same refused dial goes to b-0, which is why they prove the serving set held.
func TestWithNoWorkloadTheResendAfterARefusedDialGoesToTheNextInstanceOnTheWalk(t *testing.T) {
	f, s := retryPinFix(t)
	f.engines["ns/a-0"].Close()
	resp := f.errPost(scChat, errChatBody(scModel, false, "hello there"), "x-session-id", s)
	body := readAll(t, resp)
	wantWhole(t, resp, body, "ns/b-0")
	wantRetried(t, "no Workload", f, "ns/b-0", 0, 1, 0)
}
