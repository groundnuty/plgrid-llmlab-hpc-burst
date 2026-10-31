package front

import "testing"

// TestJudgeReadsDeltaReasoning (@hpc-burst-science-agent, 31 Oct 2026): vLLM
// v0.32.0 streams reasoning as delta.reasoning, older engines as
// delta.reasoning_content. Either is a content event, so the first one sets
// firstTokenMs and is counted.
func TestJudgeReadsDeltaReasoning(t *testing.T) {
	for _, field := range []string{"reasoning", "reasoning_content"} {
		j := &judge{kind: endpointChat}
		j.event([]byte(`data: {"choices":[{"index":0,"delta":{"` + field + `":"let me think"}}]}` + "\n\n"))
		if !j.sawContent || j.contentEvents != 1 {
			t.Errorf("delta.%s: sawContent %v contentEvents %d, want a content event", field, j.sawContent, j.contentEvents)
		}
	}
	j := &judge{kind: endpointChat} // control: an empty delta is not content
	j.event([]byte(`data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}` + "\n\n"))
	if j.sawContent {
		t.Error("a delta with only a role counted as content")
	}
}
