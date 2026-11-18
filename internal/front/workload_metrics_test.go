package front

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-8, Front metrics (beta): hpcburst_front_requests_total, hpcburst_front_cuts_total and hpcburst_front_time_to_first_token_seconds gain a
// workload label, the request's resolved Workload name, empty for a request that names none or is refused before one resolves, so the label is
// bounded by the Workload objects. The series are pre-created only with an empty workload: a Workload object adds none, its first request does.
// Label names and values are typed from the HBEP, one test per series, each with a request that names no Workload as the control.

func metricsFront(t *testing.T) *pwFix {
	t.Helper()
	f := wlFront(t)
	f.workload("ns", "alpha", scModel)
	f.workload("ns", "lonely", scModel)
	f.md("md-a").Spec.Workloads = []string{"alpha"} // so "lonely" has no serving deployment
	f.apply()
	return f
}

func TestTheRequestsSeriesCarriesTheResolvedWorkload(t *testing.T) {
	f := metricsFront(t)
	served := func(model string, hdr ...string) {
		t.Helper()
		resp, body := f.say(model, hdr...)
		if resp.StatusCode != 200 {
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	}
	served(scModel + "#alpha")
	served(scModel, wlHeader, "alpha")
	served(scModel)
	// Refused before a Workload resolved: the label is empty. A grammar error and an unknown Workload.
	for _, model := range []string{scModel + "##x", scModel + "#nope"} {
		resp, _ := f.say(model)
		f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	}
	// Refused after the Workload resolved: it has no serving deployment, and the label names it.
	resp, _ := f.say(scModel + "#lonely")
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))

	ss := scrape(t, f.metrics)
	wantSample(t, ss, 2, mRequests, "code", "200", "endpoint", "chat", "provider", "helios-a", "model_deployment", "ns/md-a", "workload", "alpha")
	wantSample(t, ss, 1, mRequests, "code", "200", "endpoint", "chat", "provider", "helios-a", "model_deployment", "ns/md-a", "workload", "")
	wantSample(t, ss, 2, mRequests, "code", "400", "endpoint", "chat", "provider", "", "model_deployment", "", "workload", "")
	wantSample(t, ss, 1, mRequests, "code", "503", "endpoint", "chat", "provider", "", "model_deployment", "", "workload", "lonely")
	wantNoSample(t, ss, mRequests, "workload", "nope")
	wantNoSample(t, ss, mRequests, "workload", "##x")
}

func TestTheCutsSeriesCarriesTheResolvedWorkloadAndAWorkloadObjectAddsNone(t *testing.T) {
	f := metricsFront(t)
	// Before any request: every cut series is a real zero with an empty workload, for the target and for a request that had none, and a Workload
	// object that exists adds no series of its own.
	zeros := scrape(t, f.metrics)
	for _, by := range []string{"front_deadline", "front_idle", "front_headers", "tunnel", "upstream", "lease_end", "client", "front_shutdown"} {
		wantSample(t, zeros, 0, mCuts, "cut_by", by, "provider", "helios-a", "model_deployment", "ns/md-a", "workload", "")
		wantSample(t, zeros, 0, mCuts, "cut_by", by, "provider", "", "model_deployment", "", "workload", "")
	}
	wantNoSample(t, zeros, mCuts, "workload", "alpha")
	wantNoSample(t, zeros, mCuts, "workload", "lonely")

	// A Workload's stream the engine kills after two tokens is cut by the upstream, under its Workload.
	f.engines["ns/a-0"].Set(enginetest.Behaviour{Tokens: 6, KillAfter: 2})
	resp := f.errPost(scChat, errChatBody(scModel+"#alpha", true, "hello"))
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	rec := f.record(resp.Header.Get("hpcburst-request-id"))
	if rec.CutBy == nil || *rec.CutBy != "upstream" {
		t.Fatalf("the stream's record: cutBy %v, want upstream", rec.CutBy)
	}
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, mCuts, "cut_by", "upstream", "provider", "helios-a", "model_deployment", "ns/md-a", "workload", "alpha")
	wantSample(t, ss, 0, mCuts, "cut_by", "upstream", "provider", "helios-a", "model_deployment", "ns/md-a", "workload", "") // the control: it is not counted there too

	// The same cut for a request that names none is the empty-workload series.
	resp = f.errPost(scChat, errChatBody(scModel, true, "hello"))
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 1, mCuts, "cut_by", "upstream", "provider", "helios-a", "model_deployment", "ns/md-a", "workload", "")
	wantSample(t, ss, 1, mCuts, "cut_by", "upstream", "provider", "helios-a", "model_deployment", "ns/md-a", "workload", "alpha")
}

func TestTheFirstTokenSeriesCarriesTheResolvedWorkload(t *testing.T) {
	f := metricsFront(t)
	f.engines["ns/a-0"].Set(enginetest.Behaviour{Tokens: 3, FirstToken: 3_000_000_000})
	resp := f.errPost(scChat, errChatBody(scModel, true, "hi"), wlHeader, "alpha")
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	ss := scrape(t, f.metrics)
	labels := []string{"provider", "helios-a", "model_deployment", "ns/md-a", "endpoint", "chat", "workload", "alpha"}
	wantSample(t, ss, 1, mTTFT+"_count", labels...)
	wantSample(t, ss, 3, mTTFT+"_sum", labels...)
	wantNoSample(t, ss, mTTFT+"_count", "workload", "") // no request without a Workload yet: the series for none is not there

	resp = f.errPost(scChat, errChatBody(scModel, true, "hi"))
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 1, mTTFT+"_count", "provider", "helios-a", "model_deployment", "ns/md-a", "endpoint", "chat", "workload", "")
	wantSample(t, ss, 1, mTTFT+"_count", labels...)
}
