package enginedouble

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand/v2"
)

// genParams are the parts of a request that decide its output.
type genParams struct {
	promptIDs []int32
	maxTokens int // resolved, at least 1
	minTokens int
	ignoreEOS bool
	greedy    bool
	// repeat is how many earlier requests were identical to this greedy one.
	repeat int
}

// output is what the model says: indices into vocab, and the finish reason.
type output struct {
	tokens []int
	finish string
}

// generate decides an output from the request and the modes.
//
// Length: with ignore_eos, or min_tokens at or above max_tokens, exactly
// max_tokens tokens and "length". Otherwise the model stops of its own accord
// after max(NaturalLen, min_tokens) tokens ("stop"), unless max_tokens comes
// first ("length"). The loop-above mode overrides this for a long prompt.
func (c Config) generate(p genParams) output {
	n, finish := p.maxTokens, "length"
	if !p.ignoreEOS && p.minTokens < p.maxTokens {
		if natural := max(c.NaturalLen, p.minTokens); natural < p.maxTokens {
			n, finish = natural, "stop"
		}
	}
	looping := c.LoopAbove > 0 && len(p.promptIDs) > c.LoopAbove && !p.ignoreEOS
	if looping {
		n, finish = p.maxTokens, "length"
	}

	seed := rand.Uint64()
	if p.greedy {
		seed = promptSeed(p.promptIDs)
	}
	toks := walk(seed, n)
	switch {
	case looping:
		for i := 3; i < n; i++ {
			toks[i] = toks[i%3]
		}
	case p.greedy && c.GreedyRepeat:
		for i := range toks {
			toks[i] = toks[0]
		}
	case p.greedy && c.GreedyDiverge && p.repeat > 0:
		diverge(toks, seed, c.GreedyDivergeAt, p.repeat)
	}
	return output{tokens: toks, finish: finish}
}

// promptSeed is a function of the prompt's token ids alone, so identical
// greedy requests walk the same way. It does not depend on max_tokens: a
// longer output extends a shorter one.
func promptSeed(ids []int32) uint64 {
	h := fnv.New64a()
	var b [4]byte
	for _, id := range ids {
		binary.LittleEndian.PutUint32(b[:], uint32(id))
		_, _ = h.Write(b[:])
	}
	return h.Sum64()
}

func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// stepAt is the walk's step from token i-1 to token i: odd, so it is never
// zero modulo the vocabulary size and no token repeats next to itself, and
// different from token to token, so the walk is not periodic.
func stepAt(seed uint64, i int) int {
	return 2*int(mix64(seed^uint64(i)*0xd1342543de82ef95)%(vocabSize/2)) + 1
}

// walk is n tokens of a walk over the vocabulary.
func walk(seed uint64, n int) []int {
	out := make([]int, n)
	idx := int(mix64(seed) % vocabSize)
	for i := range out {
		if i > 0 {
			idx = (idx + stepAt(seed, i)) % vocabSize
		}
		out[i] = idx
	}
	return out
}

// diverge makes the repeat-th repeat of a greedy request differ from the first
// from token index at on: every token from there on is shifted by a non-zero
// offset, chosen so that it still differs from its neighbour.
func diverge(toks []int, seed uint64, at, repeat int) {
	if at >= len(toks) {
		return
	}
	off := (repeat-1)%(vocabSize-1) + 1
	if at > 0 && (stepAt(seed, at)+off)%vocabSize == 0 {
		off = off%(vocabSize-1) + 1
	}
	for i := at; i < len(toks); i++ {
		toks[i] = (toks[i] + off) % vocabSize
	}
}

// outputWords returns the words and token ids of an output.
func outputWords(o output) (words []string, ids []int32) {
	words = make([]string, len(o.tokens))
	ids = make([]int32, len(o.tokens))
	for i, t := range o.tokens {
		words[i] = vocab[t]
		ids[i] = wordID(vocab[t])
	}
	return words, ids
}

// deltaText is the text a streamed token adds: the word, with a separating
// space unless it is the first. Joined, the deltas are the answer's content,
// and tokenizing that content gives back the token ids.
func deltaText(i int, word string) string {
	if i == 0 {
		return word
	}
	return " " + word
}
