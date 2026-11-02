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
	// seeded says a sampled request carried a seed, seedValue.
	seeded    bool
	seedValue int64
	// repeat is how many earlier requests were identical to this repeatable
	// one.
	repeat int
}

// repeatable says the same request gives the same output: a greedy one, and
// a sampled one with a seed. The greedy-repeat and greedy-diverge modes act
// on these.
func (p genParams) repeatable() bool { return p.greedy || p.seeded }

// output is what the model says: indices into vocab, and the finish reason.
type output struct {
	tokens []int
	finish string
}

// length decides how many tokens the model says and why it stops: with
// min_tokens at or above max_tokens, exactly max_tokens tokens and "length";
// otherwise the model stops of its own accord after max(NaturalLen,
// min_tokens) tokens ("stop"), unless max_tokens comes first ("length").
//
// ignore_eos does not change the length: vLLM merges the server's default
// stop-token ids into every request (to_sampling_params,
// vllm/entrypoints/openai/chat_completion/protocol.py:730-740), so a request that only
// ignores the end-of-sequence token can still stop early, and only min_tokens
// guarantees a length (HBEP-7 review of PR 87, Astra B10). The early-stop mode
// is this stop with a chosen length.
//
// The loop-above mode overrides this for a long prompt, unless ignore_eos: the
// requests that send it, the warm-up and the corruption check, are not the
// behaviour check.
func (c Config) length(p genParams) (n int, finish string) {
	n, finish = p.maxTokens, "length"
	if p.minTokens < p.maxTokens {
		if natural := max(c.NaturalLen, p.minTokens); natural < p.maxTokens {
			n, finish = natural, "stop"
		}
	}
	if c.LoopAbove > 0 && len(p.promptIDs) > c.LoopAbove && !p.ignoreEOS {
		n, finish = p.maxTokens, "length"
	}
	return n, finish
}

// seed is the walk's seed: the prompt's for a greedy request, the prompt's
// and the request's seed for a seeded sampled one, else random.
func (p genParams) seed() uint64 {
	switch {
	case p.greedy:
		return promptSeed(p.promptIDs)
	case p.seeded:
		return mix64(promptSeed(p.promptIDs) ^ mix64(uint64(p.seedValue)))
	}
	return rand.Uint64()
}

// tokens makes n tokens for the request, with the modes applied.
func (c Config) tokens(p genParams, n int) []int {
	seed := p.seed()
	toks := walk(seed, n)
	looping := c.LoopAbove > 0 && len(p.promptIDs) > c.LoopAbove && !p.ignoreEOS
	switch {
	case looping:
		for i := 3; i < n; i++ {
			toks[i] = toks[i%3]
		}
	case p.repeatable() && c.GreedyRepeat:
		for i := range toks {
			toks[i] = toks[0]
		}
	case p.repeatable() && c.GreedyDiverge && p.repeat > 0:
		diverge(toks, seed, c.GreedyDivergeAt, p.repeat)
	}
	return toks
}

// generate decides an output from the request and the modes.
func (c Config) generate(p genParams) output {
	n, finish := c.length(p)
	return output{tokens: c.tokens(p, n), finish: finish}
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
