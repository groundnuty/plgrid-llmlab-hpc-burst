package front

// The upload of a request's body in its usage record (issue #224, the upload
// direction, front to node): the bytes the front sent upstream and the
// interval in which it wrote them. The size expected is what the engine double
// received, not what the front says it sent; the interval must lie inside the
// request's own times, start before end.

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

func TestTheUsageRecordHoldsTheBodysUpload(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	s.e.Set(enginetest.Behaviour{Tokens: 3})
	// A body of about 200 KB, the size of the tunnel hour's large requests.
	resp := s.post("/v1/chat/completions", errChatBody(efModel, false, strings.Repeat("upload ", 30_000)))
	readAll(t, resp)
	id := resp.Header.Get("Hpcburst-Request-Id")
	rec := s.record(id)
	reqs := s.e.Requests()
	if len(reqs) == 0 {
		t.Fatal("the engine double received no request")
	}
	got := reqs[len(reqs)-1]
	if rec.BodyBytes == nil || *rec.BodyBytes != int64(len(got.Body)) || len(got.Body) < 200_000 {
		t.Fatalf("bodyBytes = %v, want the %d bytes the engine received", ptrStr(rec.BodyBytes), len(got.Body))
	}
	parse := func(name, v string) time.Time {
		at, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t.Fatalf("%s %q: %v", name, v, err)
		}
		return at
	}
	start, end := parse("startTime", rec.StartTime), parse("endTime", rec.EndTime)
	bs, be := parse("bodyStartTime", rec.BodyStartTime), parse("bodyEndTime", rec.BodyEndTime)
	// The request's times are to the millisecond, the body's to the microsecond: compare at the millisecond.
	if bs.Truncate(time.Millisecond).Before(start) || be.Before(bs) || end.Before(be.Truncate(time.Millisecond)) {
		t.Errorf("the upload %s to %s is not inside the request's %s to %s, start before end", rec.BodyStartTime, rec.BodyEndTime, rec.StartTime, rec.EndTime)
	}
	if !strings.HasSuffix(rec.BodyStartTime, "Z") || len(rec.BodyStartTime) != len("2006-01-02T15:04:05.000000Z") {
		t.Errorf("bodyStartTime %q is not UTC RFC 3339 to the microsecond", rec.BodyStartTime)
	}
}

// A request the front never wrote upstream (refused before a target) has no upload in its record.
func TestARequestNeverSentUpstreamHasNoUpload(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	resp := s.post("/v1/chat/completions", errChatBody("no-such-model", false, "hi"))
	readAll(t, resp)
	rec := s.record(resp.Header.Get("Hpcburst-Request-Id"))
	if rec.BodyBytes != nil || rec.BodyStartTime != "" || rec.BodyEndTime != "" {
		t.Errorf("a request refused before a target has an upload: %v %q %q", ptrStr(rec.BodyBytes), rec.BodyStartTime, rec.BodyEndTime)
	}
}
