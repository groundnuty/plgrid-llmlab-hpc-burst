package sim

import "container/list"

// Cache models a paged KV cache with automatic prefix caching, after vLLM's
// V1 block pool:
//   - capacity is a fixed number of blocks of B tokens;
//   - a full block is cached under its chained hash and can be shared;
//   - a request pins the blocks it uses; unpinned cached blocks stay
//     resident and are evicted in LRU order only when space is needed;
//   - a finished request's blocks are freed in reverse order (its last block
//     first), so a chain loses its tail before its head;
//   - a request whose blocks do not fit waits; nothing is preempted.
//
// The cache can miss: a block is gone once evicted, and a request only hits
// the longest resident prefix of its own chain.
type Cache struct {
	B        int
	Capacity int // blocks
	table    map[uint64]*entry
	lru      *list.List // unpinned cached blocks; Front is evicted first
	free     int        // empty blocks
	anon     int        // allocated blocks that carry no hash
}

type entry struct {
	hash uint64
	ref  int
	elem *list.Element
}

// Hold is what an admitted request holds in the cache.
type Hold struct {
	pinned []*entry // in chain order
	anon   int
	// HitTokens is the number of prompt tokens served from the cache.
	HitTokens int
	// Blocks is the total number of blocks reserved (prompt and output).
	Blocks int
}

// NewCache returns an empty cache of capacityTokens rounded down to blocks.
func NewCache(b, capacityTokens int) *Cache {
	n := capacityTokens / b
	return &Cache{B: b, Capacity: n, table: make(map[uint64]*entry), lru: list.New(), free: n}
}

func ceilDiv(a, b int) int { return (a + b - 1) / b }

// hitBlocks counts the resident prefix of a prompt. As in vLLM, the last
// prompt token is always computed, so at most (P-1)/B blocks can hit.
func (c *Cache) hitBlocks(prompt []uint64, promptLen int) int {
	maxHit := min((promptLen-1)/c.B, len(prompt))
	h := 0
	for h < maxHit {
		if _, ok := c.table[prompt[h]]; !ok {
			break
		}
		h++
	}
	return h
}

// Fits reports whether a request needing totalBlocks could ever be admitted.
func (c *Cache) Fits(totalBlocks int) bool { return totalBlocks <= c.Capacity }

// Admit tries to reserve blocks for a prompt of promptLen tokens (full-block
// hashes in prompt) plus outLen output tokens. It returns nil when the
// blocks do not fit now.
func (c *Cache) Admit(prompt []uint64, promptLen, outLen int) *Hold {
	total := ceilDiv(promptLen+outLen, c.B)
	h := c.hitBlocks(prompt, promptLen)
	unpinnedHits := 0
	for i := 0; i < h; i++ {
		if c.table[prompt[i]].ref == 0 {
			unpinnedHits++
		}
	}
	need := total - h
	if need > c.free+c.lru.Len()-unpinnedHits {
		return nil
	}
	hold := &Hold{HitTokens: h * c.B, Blocks: total}
	for i := 0; i < h; i++ {
		e := c.table[prompt[i]]
		c.pin(e)
		hold.pinned = append(hold.pinned, e)
	}
	c.allocate(need)
	hold.anon = need
	c.register(hold, prompt[h:min(promptLen/c.B, len(prompt))])
	return hold
}

func (c *Cache) pin(e *entry) {
	if e.ref == 0 {
		c.lru.Remove(e.elem)
		e.elem = nil
	}
	e.ref++
}

// allocate takes n blocks, first from empty ones, then by evicting.
func (c *Cache) allocate(n int) {
	fromFree := min(n, c.free)
	c.free -= fromFree
	for i := fromFree; i < n; i++ {
		front := c.lru.Front()
		e := front.Value.(*entry)
		c.lru.Remove(front)
		delete(c.table, e.hash)
	}
	c.anon += n
}

// register turns anonymous blocks of a hold into cached blocks with the given
// hashes. A hash that is already cached (computed twice by concurrent
// requests) stays anonymous, as in vLLM.
func (c *Cache) register(hold *Hold, hashes []uint64) {
	for _, hs := range hashes {
		if hold.anon == 0 {
			return
		}
		if _, ok := c.table[hs]; ok {
			continue
		}
		e := &entry{hash: hs, ref: 1}
		c.table[hs] = e
		hold.pinned = append(hold.pinned, e)
		hold.anon--
		c.anon--
	}
}

// Release ends a request. full holds the hashes of every full block of the
// request's prompt plus output; blocks not yet cached are registered first,
// so the next turn, whose prompt contains this output, can hit them.
func (c *Cache) Release(hold *Hold, full []uint64) {
	if n := len(hold.pinned); n < len(full) {
		c.register(hold, full[n:])
	}
	c.free += hold.anon
	c.anon -= hold.anon
	hold.anon = 0
	for i := len(hold.pinned) - 1; i >= 0; i-- {
		e := hold.pinned[i]
		e.ref--
		if e.ref == 0 {
			e.elem = c.lru.PushBack(e)
		}
	}
	hold.pinned = nil
}

// Resident reports whether a block hash is cached (for tests).
func (c *Cache) Resident(h uint64) bool { _, ok := c.table[h]; return ok }

// Check verifies the block accounting invariant (for tests).
func (c *Cache) Check() bool {
	return len(c.table)+c.anon+c.free == c.Capacity && c.free >= 0 && c.anon >= 0
}
