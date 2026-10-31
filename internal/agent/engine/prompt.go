package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// filler is the synthetic prompts' vocabulary: common short English words,
// each one token in the usual tokenizers, so a prompt's length can be set by
// its word count. The words are drawn in a varied order, not repeated in one
// pattern, so the text itself does not invite a loop.
var filler = strings.Fields(`the of and to in is was for on that with as by at from his her
they this which or had are but not be have one all were we when there can an
your their said each she do how if will up other about out many then them these
so some would make like him into time has look two more write go see number no
way could people my than first water been call who oil its now find long down day
did get come made may part over new sound take only little work know place year
live me back give most very after thing our just name good sentence man think say
great where help through much before line right too mean old any same tell boy
follow came want show also around form three small set put end does another well
large must big even such because turn here why ask went men read need land
different home us move try kind hand picture again change off play spell air away
animal house point page letter mother answer found study still learn should world`)

// fillerText returns n words of filler, in an order drawn from seed.
func fillerText(n int, seed uint64) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(n * 6)
	x := seed | 1
	for i := 0; i < n; i++ {
		// xorshift64: a varied but reproducible order.
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(filler[x%uint64(len(filler))])
	}
	return b.String()
}

// nonce returns a request's unique first word, so no two synthetic requests
// share a prefix the engine could serve from its cache, and the seed of its
// filler.
func nonce() (string, uint64) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("engine: %v", err))
	}
	var seed uint64
	for _, c := range b {
		seed = seed<<8 | uint64(c)
	}
	return "ref-" + hex.EncodeToString(b[:]), seed
}

// promptKey identifies a calibrated prompt length.
type promptKey struct {
	tokens int32
	tail   string
	tools  bool
}

// buildContent is a prompt's user message: the nonce, n filler words and the
// tail.
func buildContent(id string, seed uint64, n int, tail string) string {
	parts := []string{id}
	if f := fillerText(n, seed); f != "" {
		parts = append(parts, f)
	}
	if tail != "" {
		parts = append(parts, tail)
	}
	return strings.Join(parts, " ")
}

// prompt returns a user message whose conversation, rendered through the
// engine's chat template with the tools, takes at most overhead + tokens
// tokens, and as close to it as the vocabulary allows: a shape of N context
// tokens is a request of N tokens beyond the empty conversation's overhead
// (HBEP-7, What the controller reads). The tools' definitions count within
// the N, so a request with tools fits the same context limit as one without.
// A shape the fixed part (the nonce and the tail) alone exceeds is an error,
// never shortened. With exact set, the result is counted with its own nonce
// and trimmed until it fits, for shapes at the context limit.
func (c *Client) prompt(ctx context.Context, tokens int32, tail string, tools []tool, exact bool) (string, error) {
	overhead, err := c.templateOverhead(ctx)
	if err != nil {
		return "", err
	}
	target := overhead + tokens
	id, seed := nonce()
	count := func(n int) (int32, error) {
		return c.countTokens(ctx, []message{{Role: "user", Content: buildContent(id, seed, n, tail)}}, tools)
	}
	key := promptKey{tokens: tokens, tail: tail, tools: len(tools) > 0}
	c.mu.Lock()
	words, cached := c.wordsFor[key]
	c.mu.Unlock()
	if !cached {
		if words, err = c.calibrate(count, target); err != nil {
			return "", fmt.Errorf("a prompt of %d tokens: %w", tokens, err)
		}
		c.mu.Lock()
		c.wordsFor[key] = words
		c.mu.Unlock()
	} else if exact {
		// Another nonce can take a token more: count this one.
		for i := 0; i < 8; i++ {
			n, err := count(words)
			if err != nil {
				return "", err
			}
			if n <= target || words == 0 {
				break
			}
			words -= max(1, int(n-target))
		}
	}
	return buildContent(id, seed, words, tail), nil
}

// calibrate finds the most filler words whose count stays within target. It
// first tries one token per word, which the vocabulary is chosen for, and
// otherwise brackets the answer and bisects: the count rises with the words.
func (c *Client) calibrate(count func(int) (int32, error), target int32) (int, error) {
	n0, err := count(0)
	if err != nil {
		return 0, err
	}
	if n0 > target {
		return 0, fmt.Errorf("the fixed part alone takes %d tokens, above %d", n0, target)
	}
	lo := 0 // count(lo) <= target
	guess := int(target - n0)
	if guess == 0 {
		return 0, nil
	}
	n, err := count(guess)
	if err != nil {
		return 0, err
	}
	if n == target {
		return guess, nil
	}
	hi := guess // count(hi) > target, once found
	if n < target {
		lo = guess
		hi = 2*guess + 1
		for {
			n, err := count(hi)
			if err != nil {
				return 0, err
			}
			if n > target {
				break
			}
			if n == target {
				return hi, nil
			}
			lo, hi = hi, 2*hi+1
			if hi > 1<<24 {
				return lo, nil
			}
		}
	}
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		n, err := count(mid)
		if err != nil {
			return 0, err
		}
		if n <= target {
			lo = mid
		} else {
			hi = mid
		}
	}
	return lo, nil
}
