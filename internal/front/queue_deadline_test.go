package front

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// TestQueueWaitCutByTheFrontDeadline (PR #95 review: Astra B10): with
// maxRequestDuration (5 s) below queueTimeout (60 s), a request waiting for
// a full Instance meets the front's own deadline. It gets an attributed 504
// request_timeout with detail deadline and cutBy front_deadline, not the
// empty 200 of a client that went away.
func TestQueueWaitCutByTheFrontDeadline(t *testing.T) {
	one := int32(1)
	f := newScFix(t, scOpts{
		cfg: func(c *Config) { c.MaxRequestDuration = Duration(5 * time.Second) },
		md:  func(md *v1alpha1.ModelDeployment) { md.Spec.Routing = &v1alpha1.Routing{InstanceCap: &one} },
	})
	hold := make(chan struct{})
	defer close(hold)
	f.engines[0].Set(enginetest.Behaviour{Hold: hold})
	first := scGo(f.harness, errChatBody(scModel, false, "first"))
	errWaitFor(t, "the first request to hold the Instance's one slot", func() bool { return f.engines[0].Open() == 1 })
	second := scGo(f.harness, errChatBody(scModel, false, "second"))
	errWaitFor(t, "the second request to wait at the front", func() bool { return f.router.Status().Waiting == 1 })
	f.clk.Advance(5 * time.Second)
	resp, body := second.wait(t)
	if resp.StatusCode != 504 {
		t.Fatalf("status %d body %q, want 504 request_timeout", resp.StatusCode, body)
	}
	checkErrorBody(t, body, codeRequestTimeout, "deadline")
	if resp.Header.Get("hpcburst-request-id") == "" || scEmitterOf(resp) != emitterFront {
		t.Errorf("the 504 is not attributed: request id %q emitter %q", resp.Header.Get("hpcburst-request-id"), scEmitterOf(resp))
	}
	if rec := f.record(resp.Header.Get("hpcburst-request-id")); rec.CutBy == nil || *rec.CutBy != cutFrontDeadline || rec.Status != 504 {
		t.Errorf("record status %d cutBy %v, want 504 and front_deadline", rec.Status, rec.CutBy)
	}
	_ = first
}
