package agentapi

// Reason codes of a failed check (CheckOutcome.Reason). They are CamelCase
// strings, so the lifecycle controller matches on a code and never on the
// message text (`@hpc-burst-science-agent`, 31 Oct 2026). Each names a kind of
// failure the agent reports or the fakes model: the corruption check's four,
// from JudgeCorruption, and the answer failures of the chat, tool-call and
// behaviour checks.
const (
	// ReasonRequestFailed: the request to the engine, or the health probe,
	// did not get an answer: a connection error, a timeout or an error status.
	ReasonRequestFailed = "RequestFailed"
	// ReasonBadResponse: the engine answered, but the answer lacks what the
	// check reads, such as a choice, or the token IDs that the corruption
	// check asks for.
	ReasonBadResponse = "BadResponse"
	// ReasonEmptyAnswer: the answer to the chat request, or to the behaviour
	// check's request, has no text.
	ReasonEmptyAnswer = "EmptyAnswer"
	// ReasonNoToolCall: a check that wants a tool call got an answer that
	// calls no tool.
	ReasonNoToolCall = "NoToolCall"
	// ReasonWrongTool: the answer calls a tool other than the one offered.
	ReasonWrongTool = "WrongTool"
	// ReasonBadToolArguments: the tool call's arguments are not a JSON object.
	ReasonBadToolArguments = "BadToolArguments"
	// ReasonWrongOutputCount, ReasonEmptyOutput, ReasonRepeatedToken and
	// ReasonOutputsDiverge: the corruption check's failures (JudgeCorruption),
	// in the order it tests them: not CorruptionOutputs outputs, an empty
	// output, an output that is one token repeated, two outputs that differ
	// within their first CorruptionPrefix tokens.
	ReasonWrongOutputCount = "WrongOutputCount"
	ReasonEmptyOutput      = "EmptyOutput"
	ReasonRepeatedToken    = "RepeatedToken"
	ReasonOutputsDiverge   = "OutputsDiverge"
	// ReasonLoop: the behaviour check's answer at the longest context ran to
	// the output limit repeating itself.
	ReasonLoop = "Loop"
	// ReasonReasoningBudgetExhausted: with a reasoning parser declared, the
	// reasoning used up the behaviour check's output budget (the typical
	// output), so the answer has no content and no tool call. The controller
	// tells it from ReasonLoop and ReasonEmptyAnswer by this code.
	ReasonReasoningBudgetExhausted = "ReasoningBudgetExhausted"
)

// ReasoningHeadroomTokens is the room, in tokens, that the agent leaves in
// the behaviour check's context for a reasoning model's thinking when a
// reasoning parser is declared (`@hpc-burst-science-agent`, 31 Oct 2026).
const ReasoningHeadroomTokens = 4096

// BehaviourContextTokens is the length, in tokens, of the prompt that the
// behaviour check runs at; the agent reports it as CheckOutcome.ContextTokens.
//
// Without a reasoning parser it is the workload's longest context,
// s.ContextLongest (HBEP-7, Instance start). With one, the agent shortens the
// prompt to
//
//	maxModelLen - templateOverhead - s.OutputTypical - ReasoningHeadroomTokens
//
// so that the reasoning does not use up the whole output budget, but never to
// less than s.ContextTypical and never to more than s.ContextLongest. Where the
// two bounds contradict (a longest below the typical), the upper bound wins:
// the check never runs longer than the workload's longest context. maxModelLen
// is the max-model-len the engine reported (VerifyResult.Settings) and
// templateOverhead is VerifyResult.TemplateOverhead.
func BehaviourContextTokens(s Shapes, maxModelLen int64, templateOverhead int32, reasoningParser bool) int64 {
	longest := int64(s.ContextLongest)
	if !reasoningParser {
		return longest
	}
	n := maxModelLen - int64(templateOverhead) - int64(s.OutputTypical) - ReasoningHeadroomTokens
	n = max(n, int64(s.ContextTypical))
	return min(n, longest)
}
