package enginedouble

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"
)

// engine is the double's state: what vLLM keeps in its scheduler and its
// metrics registry.
type engine struct {
	cfg    Config
	sem    chan struct{} // MaxNumSeqs slots; nil: no cap
	record func(LogEvent)

	mu            sync.Mutex
	running       int
	waiting       int
	kvTokens      int64
	promptTokens  int64
	genTokens     int64
	prefixQueries int64
	prefixHits    int64
	tpot          tpotHistogram
	success       map[string]int64 // finished_reason -> requests
	httpCount     map[httpKey]int64
	shapes        map[shapeKey]int
	repeats       map[repeatKey]int
	prefix        map[uint64]struct{}
}

// shapeKey is a request's shape for slow-first and never-settle: the prompt
// token count and max_tokens. The request nonce varies and the count does not.
type shapeKey struct{ prompt, maxTokens int }

// repeatKey identifies an identical greedy request.
type repeatKey struct {
	prompt               uint64
	maxTokens, minTokens int
	ignoreEOS            bool
}

type httpKey struct{ handler, method, status string }

// tpotBounds are the upper bounds of vllm:request_time_per_output_token_seconds,
// in seconds: vLLM v0.32.0's INTER_TOKEN_LATENCY_BUCKETS
// (vllm/v1/metrics/buckets.py, lines 84-104), which loggers.py, line 953, gives
// this histogram.
var tpotBounds = [...]float64{
	0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 20.0, 40.0, 80.0,
}

// tpotHistogram is the time per output token, one observation per finished
// request: the number of requests, the sum of their means and the requests in
// each bucket (not cumulative; the exposition adds them up).
type tpotHistogram struct {
	count   int64
	sum     float64
	buckets [len(tpotBounds)]int64
}

// observe records one finished request's mean time per output token.
func (t *tpotHistogram) observe(seconds float64) {
	t.count++
	t.sum += seconds
	for i, b := range tpotBounds {
		if seconds <= b {
			t.buckets[i]++
			return
		}
	}
}

func newEngine(cfg Config, record func(LogEvent)) *engine {
	e := &engine{
		cfg:       cfg,
		record:    record,
		success:   map[string]int64{},
		httpCount: map[httpKey]int64{},
		shapes:    map[shapeKey]int{},
		repeats:   map[repeatKey]int{},
		prefix:    map[uint64]struct{}{},
	}
	if cfg.MaxNumSeqs > 0 {
		e.sem = make(chan struct{}, cfg.MaxNumSeqs)
	}
	return e
}

// slot is one admitted request's share of the engine.
type slot struct {
	e         *engine
	kv        int64
	generated int64 // tokens this request has generated
	done      sync.Once
}

// admit waits for a free sequence slot, counting the request as waiting while
// it does, then counts it as running and its prompt as processed (and as KV
// held). It returns false if ctx ends first.
func (e *engine) admit(ctx context.Context, promptIDs []int32) (*slot, bool) {
	if e.sem != nil {
		e.mu.Lock()
		e.waiting++
		e.mu.Unlock()
		select {
		case e.sem <- struct{}{}:
		case <-ctx.Done():
			e.mu.Lock()
			e.waiting--
			e.mu.Unlock()
			return nil, false
		}
	}
	n := int64(len(promptIDs))
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sem != nil {
		e.waiting--
	}
	e.running++
	e.promptTokens += n
	e.kvTokens += n
	e.prefixQueries += n
	e.prefixHits += e.matchPrefix(promptIDs)
	return &slot{e: e, kv: n}, true
}

// matchPrefix is vLLM's prefix cache at block granularity: the tokens of the
// leading full blocks that an earlier prompt already filled. It then remembers
// every full block of this prompt. It returns 0 and remembers nothing when
// prefix caching is off.
func (e *engine) matchPrefix(ids []int32) int64 {
	if !e.cfg.PrefixCaching {
		return 0
	}
	bs := e.cfg.BlockSize
	var hits int64
	matching := true
	h := fnv.New64a()
	var b [4]byte
	for start := 0; start+bs <= len(ids); start += bs {
		for _, id := range ids[start : start+bs] {
			binary.LittleEndian.PutUint32(b[:], uint32(id))
			_, _ = h.Write(b[:])
		}
		sum := h.Sum64()
		if _, ok := e.prefix[sum]; ok && matching {
			hits += int64(bs)
		} else {
			matching = false
		}
		e.prefix[sum] = struct{}{}
	}
	return hits
}

// token counts one generated token.
func (s *slot) token() {
	s.e.mu.Lock()
	defer s.e.mu.Unlock()
	s.kv++
	s.generated++
	s.e.kvTokens++
	s.e.genTokens++
}

// release frees the slot and its KV; finish is the finished_reason to count.
func (s *slot) release(finish string) {
	s.done.Do(func() {
		s.e.mu.Lock()
		s.e.running--
		s.e.kvTokens -= s.kv
		s.e.success[finish]++
		s.e.tpot.observe(s.meanTimePerOutputToken())
		s.e.mu.Unlock()
		if s.e.sem != nil {
			<-s.e.sem
		}
	})
}

// meanTimePerOutputToken is what vLLM observes for a finished request: the
// decode time over the output tokens after the first, and 0 for a request of
// one token or none (vllm/v1/metrics/stats.py, lines 607-612). The double waits
// TokenDelay before each token, so the decode time of n tokens is (n-1) times
// TokenDelay and the mean is TokenDelay itself: it is exact, not measured, so a
// test can compute the histogram's sum.
func (s *slot) meanTimePerOutputToken() float64 {
	if s.generated <= 1 {
		return 0
	}
	return s.e.cfg.TokenDelay.Seconds()
}

// beforeFirstToken returns how long a request waits before its first token
// because of slow-first and never-settle, and how many requests of its shape
// came before it (n, from 0), and counts the request in its shape.
//
// slow-first: the first request of a shape waits SlowFirst. never-settle: the
// n-th request of a shape (counting from 0) waits n * NeverSettle percent of
// TokenDelay * tokens, so each waits that share longer than the one before.
func (e *engine) beforeFirstToken(k shapeKey, tokens int) (wait time.Duration, nth int) {
	e.mu.Lock()
	n := e.shapes[k]
	e.shapes[k]++
	e.mu.Unlock()
	var d time.Duration
	if n == 0 {
		d += e.cfg.SlowFirst
	}
	if e.cfg.NeverSettle > 0 && n > 0 {
		share := float64(e.cfg.NeverSettle) / 100 * float64(e.cfg.TokenDelay) * float64(tokens)
		d += time.Duration(float64(n) * share)
	}
	return d, n
}

// repeatOf counts a greedy request and returns how many identical ones came
// before it.
func (e *engine) repeatOf(k repeatKey) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.repeats[k]
	e.repeats[k]++
	return n
}

func (e *engine) countHTTP(k httpKey) {
	e.mu.Lock()
	e.httpCount[k]++
	e.mu.Unlock()
}

// sleep waits d, or until ctx ends; it returns false in that case.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
