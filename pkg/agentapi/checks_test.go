package agentapi

import (
	"encoding/json"
	"strings"
	"testing"
)

// The reason codes are CamelCase strings the controller matches exactly, so
// they are written out here by hand. A renamed constant changes the wire value.
func TestReasonCodesAreThePublishedStrings(t *testing.T) {
	for got, want := range map[string]string{
		ReasonRequestFailed:            "RequestFailed",
		ReasonBadResponse:              "BadResponse",
		ReasonEmptyAnswer:              "EmptyAnswer",
		ReasonNoToolCall:               "NoToolCall",
		ReasonWrongTool:                "WrongTool",
		ReasonBadToolArguments:         "BadToolArguments",
		ReasonWrongOutputCount:         "WrongOutputCount",
		ReasonEmptyOutput:              "EmptyOutput",
		ReasonRepeatedToken:            "RepeatedToken",
		ReasonOutputsDiverge:           "OutputsDiverge",
		ReasonLoop:                     "Loop",
		ReasonReasoningBudgetExhausted: "ReasoningBudgetExhausted",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

// Each failure of the corruption check carries its own Reason, and a pass
// carries none. The vectors are the ones of TestCorruptionVerdict, written by
// hand from HBEP-7's rule.
func TestJudgeCorruptionNamesTheFailure(t *testing.T) {
	base := seq(1, 40)
	for _, c := range []struct {
		name   string
		outs   [][]int32
		pass   bool
		reason string
	}{
		{"three identical outputs", [][]int32{base, base, base}, true, ""},
		{"the 33rd token differs (positive control)", [][]int32{base, withAt(base, 32, 99), base}, true, ""},
		{"the 32nd token differs", [][]int32{base, withAt(base, 31, 99), base}, false, ReasonOutputsDiverge},
		{"an empty output", [][]int32{base, {}, base}, false, ReasonEmptyOutput},
		{"one token repeated", [][]int32{rep(7, 40), rep(7, 40), rep(7, 40)}, false, ReasonRepeatedToken},
		{"two outputs", [][]int32{base, base}, false, ReasonWrongOutputCount},
		{"no outputs", nil, false, ReasonWrongOutputCount},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := JudgeCorruption(c.outs)
			if o.Name != CheckCorruption || o.Passed != c.pass || o.Reason != c.reason {
				t.Errorf("JudgeCorruption = {%s passed=%v reason=%q}, want {corruption passed=%v reason=%q}", o.Name, o.Passed, o.Reason, c.pass, c.reason)
			}
			if !c.pass && o.Message == "" {
				t.Error("a failure with no message")
			}
			if c.pass && o.Message != "" {
				t.Errorf("a pass carries the message %q", o.Message)
			}
			// CorruptionVerdict is the same verdict, without the reason.
			pass, msg := CorruptionVerdict(c.outs)
			if pass != o.Passed || msg != o.Message {
				t.Errorf("CorruptionVerdict = %v, %q; JudgeCorruption says %v, %q", pass, msg, o.Passed, o.Message)
			}
		})
	}
}

// A reason and a context length are on the wire only when set, so a passing
// check keeps its old form. The JSON is written by hand.
func TestCheckOutcomeReasonAndContextTokensOnTheWire(t *testing.T) {
	var o CheckOutcome
	const sample = `{"name":"behaviour","passed":false,"reason":"ReasoningBudgetExhausted","message":"no answer","contextTokens":27636}`
	if err := json.Unmarshal([]byte(sample), &o); err != nil {
		t.Fatal(err)
	}
	if o.Name != CheckBehaviour || o.Passed || o.Reason != ReasonReasoningBudgetExhausted || o.Message != "no answer" || o.ContextTokens != 27636 {
		t.Errorf("decoded %+v", o)
	}
	b, err := json.Marshal(CheckOutcome{Name: CheckChat, Passed: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); strings.Contains(got, "reason") || strings.Contains(got, "contextTokens") {
		t.Errorf("a passing check marshals as %s; want no reason and no contextTokens", got)
	}
}

// The behaviour check's context length, worked out by hand from the ruling of
// `@hpc-burst-science-agent`, 31 Oct 2026: with a reasoning parser the prompt is
// max-model-len - overhead - typical output - 4,096, never above contextTokens.
// longest and never below contextTokens.typical.
func TestBehaviourContextTokens(t *testing.T) {
	shapes := Shapes{ContextShort: 512, ContextTypical: 8192, ContextLongest: 30000, OutputTypical: 1024, OutputLongest: 4096}
	for _, c := range []struct {
		name        string
		maxModelLen int64
		overhead    int32
		reasoning   bool
		want        int64
	}{
		// 32768 - 12 - 1024 - 4096 = 27636, between 8192 and 30000.
		{"a reasoning parser shortens the prompt", 32768, 12, true, 27636},
		// 131072 - 12 - 1024 - 4096 = 125940, above 30000.
		{"a long window is capped at the longest context", 131072, 12, true, 30000},
		// 8192 - 12 - 1024 - 4096 = 3060, below 8192.
		{"a short window is raised to the typical context", 8192, 12, true, 8192},
		// 4096 - 12 - 1024 - 4096 is negative.
		{"a window below the headroom is the typical context", 4096, 12, true, 8192},
		// 35132 - 12 - 1024 - 4096 = 30000 exactly.
		{"exactly the longest context", 35132, 12, true, 30000},
		// One token below the upper bound: 35131 - 12 - 1024 - 4096 = 29999.
		{"one below the longest context", 35131, 12, true, 29999},
		{"no reasoning parser: the longest context whatever the window", 32768, 12, false, 30000},
		{"no reasoning parser, a short window", 8192, 12, false, 30000},
	} {
		if got := BehaviourContextTokens(shapes, c.maxModelLen, c.overhead, c.reasoning); got != c.want {
			t.Errorf("%s: BehaviourContextTokens(max-model-len %d, overhead %d, reasoning %v) = %d, want %d", c.name, c.maxModelLen, c.overhead, c.reasoning, got, c.want)
		}
	}
	// A longest context below the typical one: the upper bound wins.
	odd := Shapes{ContextTypical: 8192, ContextLongest: 4096, OutputTypical: 1024}
	if got := BehaviourContextTokens(odd, 32768, 12, true); got != 4096 {
		t.Errorf("a longest below the typical: got %d, want 4096, the longest", got)
	}
	if ReasoningHeadroomTokens != 4096 {
		t.Errorf("ReasoningHeadroomTokens = %d, want 4096", ReasoningHeadroomTokens)
	}
}
