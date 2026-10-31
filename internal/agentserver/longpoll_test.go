package agentserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// pollRig is a registered agent whose Allocation has a desired state, on a
// server with the given timing.
func pollRig(t *testing.T, interval, timeout time.Duration) (*rig, agentapi.DesiredState) {
	t.Helper()
	x := newRig(t, func(c *Config) { c.PollInterval, c.LongPollTimeout = interval, timeout })
	x.register(x.good())
	d := agentapi.DesiredState{Action: agentapi.ActionIdle, Settings: agentapi.AgentSettings{ProbeInterval: agentapi.Duration(time.Second)}}.WithVersion()
	x.backend.SetDesired(uidA, d)
	return x, d
}

func since(v string) string { return agentapi.PathDesired + "?" + agentapi.QuerySince + "=" + v }

// poll runs a long poll in a goroutine and returns what it ends with.
func (r *rig) poll(ctx context.Context, target string) <-chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- r.doCtx(ctx, http.MethodGet, target, r.good(), nil) }()
	return done
}

func versionOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", rec.Code, rec.Body)
	}
	var d agentapi.DesiredState
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	return d.Version
}

func TestAnEmptySinceReturnsAtOnce(t *testing.T) {
	x, d := pollRig(t, 20*time.Millisecond, 10*time.Second)
	start := time.Now()
	rec := x.do(http.MethodGet, agentapi.PathDesired, x.good(), nil)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %v, want an immediate answer", elapsed)
	}
	if v := versionOf(t, rec); v != d.Version {
		t.Errorf("version = %q, want %q", v, d.Version)
	}
	if n := x.backend.Calls(fake.MethodDesired); n != 1 {
		t.Errorf("Backend.Desired calls = %d, want 1", n)
	}
}

func TestADifferentVersionReturnsAtOnce(t *testing.T) {
	x, d := pollRig(t, 20*time.Millisecond, 10*time.Second)
	start := time.Now()
	rec := x.do(http.MethodGet, since("an-older-version"), x.good(), nil)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %v, want an immediate answer", elapsed)
	}
	if v := versionOf(t, rec); v != d.Version {
		t.Errorf("version = %q, want %q", v, d.Version)
	}
}

func TestTheSameVersionWaitsForTheTimeoutThenReturnsTheSameVersion(t *testing.T) {
	x, d := pollRig(t, 20*time.Millisecond, 300*time.Millisecond)
	start := time.Now()
	rec := x.do(http.MethodGet, since(d.Version), x.good(), nil)
	elapsed := time.Since(start)
	if elapsed < 280*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("took %v, want about 300 ms", elapsed)
	}
	if v := versionOf(t, rec); v != d.Version {
		t.Errorf("version = %q, want the unchanged %q", v, d.Version)
	}
	// It asked again while it waited: one call to start and several polls.
	if n := x.backend.Calls(fake.MethodDesired); n < 4 {
		t.Errorf("Backend.Desired calls = %d, want the first and at least three polls", n)
	}
}

func TestAChangeDuringTheWaitReturnsTheNewStateBeforeTheTimeout(t *testing.T) {
	x, d := pollRig(t, 20*time.Millisecond, 10*time.Second)
	start := time.Now()
	done := x.poll(context.Background(), since(d.Version))

	time.Sleep(100 * time.Millisecond)
	changed := agentapi.DesiredState{Action: agentapi.ActionIdle, Settings: agentapi.AgentSettings{ProbeInterval: agentapi.Duration(9 * time.Second)}}.WithVersion()
	x.backend.SetDesired(uidA, changed)

	select {
	case rec := <-done:
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("took %v, want well before the 10 s timeout", elapsed)
		}
		if v := versionOf(t, rec); v != changed.Version || v == d.Version {
			t.Errorf("version = %q, want the new %q", v, changed.Version)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("the poll did not return after the state changed")
	}
}

func TestACancelledRequestEndsThePollAtOnceAndStopsAskingTheBackend(t *testing.T) {
	x, d := pollRig(t, 20*time.Millisecond, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	done := x.poll(ctx, since(d.Version))
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the poll still runs 2 s after its request was cancelled")
	}
	calls := x.backend.Calls(fake.MethodDesired)
	time.Sleep(150 * time.Millisecond) // several poll intervals
	if n := x.backend.Calls(fake.MethodDesired); n != calls {
		t.Errorf("Backend.Desired calls rose from %d to %d after the poll ended", calls, n)
	}
}

func TestABackendErrorDuringThePollIsReturnedToTheAgent(t *testing.T) {
	x, d := pollRig(t, 20*time.Millisecond, 10*time.Second)
	done := x.poll(context.Background(), since(d.Version))

	// Wait until the poll has made its first call and a poll, then fail the next.
	deadline := time.Now().Add(5 * time.Second)
	for x.backend.Calls(fake.MethodDesired) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	x.backend.FailNext(fake.MethodDesired, agentapi.Errorf(agentapi.CodeUnavailable, "caches are not synced"))

	select {
	case rec := <-done:
		wantError(t, rec, agentapi.CodeUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("the poll did not return the Backend's error")
	}
}

func TestTheDefaultsAreAOneSecondIntervalAndTheProtocolsTimeout(t *testing.T) {
	x := newRig(t)
	if x.srv.pollInterval != time.Second {
		t.Errorf("PollInterval default = %v, want 1 s", x.srv.pollInterval)
	}
	if x.srv.longPoll != agentapi.LongPollTimeout {
		t.Errorf("LongPollTimeout default = %v, want %v", x.srv.longPoll, agentapi.LongPollTimeout)
	}
}
