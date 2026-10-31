package engine

// Loop detection for the behaviour check (HBEP-7, Instance start: the answer
// at the longest context does not loop). The rule is this agent's, a choice
// for the HBEP's owner to confirm: an answer loops when it ran to its output
// limit (finish reason "length", checked by the caller) and its tail repeats
// one unit of at most LoopMaxUnit tokens at least LoopMinRepeats times in a
// row, over at least LoopMinTokens tokens.
const (
	LoopMaxUnit    = 64
	LoopMinRepeats = 4
	LoopMinTokens  = 16
)

// IsLoop reports whether the output's tail is LoopMinRepeats or more copies
// of one unit of at most LoopMaxUnit tokens, covering LoopMinTokens tokens or
// more.
func IsLoop(tokens []int32) bool {
	n := len(tokens)
	for p := 1; p <= LoopMaxUnit && p*LoopMinRepeats <= n; p++ {
		// run counts the tokens at the end that repeat the token p before.
		run := 0
		for i := n - 1; i >= p && tokens[i] == tokens[i-p]; i-- {
			run++
		}
		periodic := run + p // the unit itself, then its repeats
		if periodic >= p*LoopMinRepeats && periodic >= LoopMinTokens {
			return true
		}
	}
	return false
}
