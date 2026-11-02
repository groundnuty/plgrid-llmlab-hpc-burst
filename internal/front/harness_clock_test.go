package front

import (
	"net/http"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// An Instance added with no leaseEnd must be routable on whatever clock the
// harness starts on. The default was t0 + 48 h on every clock, so on the wall
// clock it ended on 2 Nov 2026 at 12:00 UTC and every real-clock front found
// "no ready target" from then on (issue #226).
//
// The harness starts here on an injected clock 72 h after t0, past the old
// default's end: the same position the wall clock has now. The test does not
// depend on the date it runs on.
func TestHarnessDefaultLeaseEndFollowsTheClockItStartsOn(t *testing.T) {
	start := t0.Add(72 * time.Hour)
	h := newHarnessOn(t, clock.NewFake(start), nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", scModel, 1, "helios-a")
	uid := h.instance("md", "md-0", "helios-a", e, key, instanceOpts{})
	h.apply()

	le := h.inst(uid).Status.LeaseEnd
	if le == nil || !le.Time.Equal(start.Add(48*time.Hour)) {
		t.Errorf("the default lease end is %v, want 48 h after the clock's start, %s", le, start.Add(48*time.Hour))
	}
	resp, _ := postBounded(t, h, scChat, errChatBody(scModel, false, "hello there"))
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200 from the Instance whose lease ends 48 h after the clock's start: %s", resp.StatusCode, body)
	}
	if n := len(e.Requests()); n != 1 {
		t.Errorf("the engine saw %d requests, want 1", n)
	}
}

// The fake-clock harness keeps its documented default, t0 + 48 h, which the
// drain and lease-end scenarios are written against.
func TestHarnessDefaultLeaseEndOnTheFakeClockIsT0Plus48h(t *testing.T) {
	h := newHarness(t, nil)
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", scModel, 1, "helios-a")
	uid := h.instance("md", "md-0", "helios-a", nil, "", instanceOpts{})

	// The clock having moved before the Instance is added changes nothing.
	h.clk.Advance(6 * time.Hour)
	later := h.instance("md", "md-1", "helios-a", nil, "", instanceOpts{})
	for _, id := range []types.UID{uid, later} {
		le := h.inst(id).Status.LeaseEnd
		if le == nil || !le.Time.Equal(t0.Add(48*time.Hour)) {
			t.Errorf("Instance %s: lease end %v, want t0 + 48 h, %s", id, le, t0.Add(48*time.Hour))
		}
	}
}
