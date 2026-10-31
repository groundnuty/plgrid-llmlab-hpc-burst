package fake_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// A round at the admission bounds holds 4 fixed shapes, 1 tool call and
// MaxTargetSessions concurrent requests (agentapi.MaxTargetSessions).
const fullRoundRequests = 4 + 1 + agentapi.MaxTargetSessions

func warmUpOf(sessions int32, toolCalls bool) func(*agentapi.DesiredInstance) {
	return warmUpChecks(agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: toolCalls, TargetSessions: sessions})
}

func TestAFullWarmUpAtMaxTargetSessionsRunsAllFiveRoundsAndEveryReportIsAccepted(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(warmUpOf(agentapi.MaxTargetSessions, true))))
	r.steps(1 + agentapi.MaxWarmUpRounds) // the worker starts; then one round per Step

	eq(t, "reports accepted by the Backend", len(r.reports()), 6)
	w := checksOf(t, r).WarmUp
	eq(t, "rounds", len(w.Rounds), 5)
	eq(t, "Done", w.Done, true)
	for n, round := range w.Rounds {
		eq(t, fmt.Sprintf("round %d: requests", n+1), len(round.Requests), fullRoundRequests)
	}
	body, err := json.Marshal(r.last())
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > agentapi.MaxBodyBytes {
		t.Errorf("the last report is %d bytes, above the %d-byte limit the Agent accepted it under", len(body), agentapi.MaxBodyBytes)
	}
}

func TestAReportAboveTheBodyLimitFailsTheStepBeforeAnythingIsSent(t *testing.T) {
	// 100 copies of a full round: more than 10 MB of timings in the first round.
	hundredfold := func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
		round := fake.DefaultWarmUpRound(n, req)
		one := round.Requests
		for range 99 {
			round.Requests = append(round.Requests, one...)
		}
		return round
	}
	r := newRig(t, fake.Engine{WarmUpRound: hundredfold})
	r.set(run(instance(warmUpOf(agentapi.MaxTargetSessions, true))))
	r.step() // the worker starts: a small report
	reports := r.b.Calls(fake.MethodReport)
	first, _ := r.a.LastReport()

	err := r.a.Step(bgndCtx) // round 1 is 102,900 timings
	if err == nil {
		t.Fatal("Step: no error for a report above the body limit")
	}
	var e *agentapi.Error
	if !errors.As(err, &e) || e.Code != agentapi.CodeBadRequest {
		t.Errorf("Step: err = %v, want one wrapping an *agentapi.Error of CodeBadRequest", err)
	}
	eq(t, "Report calls: the Agent refused before it sent", r.b.Calls(fake.MethodReport), reports)
	eq(t, "reports the Backend holds", len(r.reports()), 1)
	got, _ := r.a.LastReport()
	eq(t, "the Agent's last report is still the earlier one", got, first)
}

func TestWarmUpErrorsAreCutToMaxTimingErrorBytesInEveryRoundOfTheReport(t *testing.T) {
	// "€" is 3 bytes: 21 of them are 63 bytes, and a 22nd would end at 66.
	cases := []struct {
		name string
		unit string
		n    int // repetitions: about 10 KB
		want string
	}{
		{"multi-byte characters", "€", 3400, strings.Repeat("€", 21)},
		{"ASCII", "e", 10240, strings.Repeat("e", 64)},
	}
	for _, c := range cases {
		for _, sessions := range []int32{2, agentapi.MaxTargetSessions} {
			t.Run(fmt.Sprintf("%s, %d sessions", c.name, sessions), func(t *testing.T) {
				long := strings.Repeat(c.unit, c.n)
				failing := func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
					round := fake.DefaultWarmUpRound(n, req)
					for i := range round.Requests {
						round.Requests[i].Error = long
					}
					return round
				}
				r := newRig(t, fake.Engine{WarmUpRound: failing})
				r.set(run(instance(warmUpOf(sessions, false))))
				r.steps(3) // the worker starts; rounds 1 and 2
				w := checksOf(t, r).WarmUp
				eq(t, "rounds", len(w.Rounds), 2)
				for n, round := range w.Rounds {
					for i, rq := range round.Requests {
						if rq.Error != c.want {
							t.Fatalf("round %d request %d: Error is %d bytes (%.20q...), want the %d-byte cut", n+1, i, len(rq.Error), rq.Error, len(c.want))
						}
					}
				}
			})
		}
	}
}

func TestMessagesAreCutToMaxMessageBytesInTheReport(t *testing.T) {
	// 85 "€" are 255 bytes; the 86th would end at 258, above the 256 limit.
	long := strings.Repeat("€", 3400)
	atLimit := strings.Repeat("e", 256)
	for _, tc := range []struct {
		name, in, want string
	}{
		{"above the limit", long, strings.Repeat("€", 85)},
		{"at the limit", atLimit, atLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := fake.Engine{
				Verify: func(agentapi.DesiredInstance, agentapi.VerifyRequest) agentapi.VerifyResult {
					return agentapi.VerifyResult{Error: tc.in}
				},
				Qualify: func(agentapi.DesiredInstance, agentapi.QualifyRequest) agentapi.QualifyResult {
					return agentapi.QualifyResult{Checks: []agentapi.CheckOutcome{
						{Name: agentapi.CheckHealth, Passed: true},
						{Name: agentapi.CheckChat, Passed: false, Message: tc.in},
					}}
				},
			}
			r := newRig(t, e)
			r.set(run(instance(withChecks(agentapi.Checks{
				VerificationGeneration: 1,
				Verify:                 &agentapi.VerifyRequest{},
				Qualify:                &agentapi.QualifyRequest{Shapes: shapes},
			}))))
			r.steps(2)
			c := checksOf(t, r)
			eq(t, "VerifyResult.Error", c.Verify.Error, tc.want)
			eq(t, "CheckOutcome.Message", c.Qualify.Checks[1].Message, tc.want)
			eq(t, "a check without a message", c.Qualify.Checks[0].Message, "")
		})
	}
}

func TestAWarmUpWithAnOutOfBoundsTargetSessionsFailsTheStepNamingTheBound(t *testing.T) {
	for _, sessions := range []int32{agentapi.MaxTargetSessions + 1, 0, -1} {
		t.Run(fmt.Sprint(sessions), func(t *testing.T) {
			r := newRig(t, fake.Engine{})
			r.set(run(instance(warmUpOf(sessions, false))))
			err := r.a.Step(bgndCtx)
			if err == nil {
				t.Fatal("Step: no error")
			}
			if want := "1..1024"; !strings.Contains(err.Error(), want) {
				t.Errorf("Step: err = %q, want it to name the bound %s", err, want)
			}
			if want := fmt.Sprint(sessions); !strings.Contains(err.Error(), want) {
				t.Errorf("Step: err = %q, want it to name the value %s", err, want)
			}
			eq(t, "reports sent", r.b.Calls(fake.MethodReport), 0)
			eq(t, "desired states seen", len(r.a.Seen()), 0)
			eq(t, "workers started", len(r.a.Starts()), 0)
		})
	}
	for _, sessions := range []int32{1, agentapi.MaxTargetSessions} {
		r := newRig(t, fake.Engine{})
		r.set(run(instance(warmUpOf(sessions, false))))
		r.steps(2)
		eq(t, fmt.Sprintf("%d sessions: round 1 requests", sessions), len(checksOf(t, r).WarmUp.Rounds[0].Requests), 4+int(sessions))
	}
}
