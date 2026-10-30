package sim

import "strconv"

// Seg is a run of tokens with one identity. Two contexts that contain the
// same segment at the same position hold the same tokens there. Tokens are
// never materialised: token i of a segment is (ID, i).
type Seg struct {
	ID  uint64
	Len int
}

// mix is a 64-bit finaliser (splitmix64). It makes block and key hashes
// deterministic across runs and processes.
func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func combine(h, v uint64) uint64 { return mix(h ^ mix(v)) }

// SegID derives a segment identity from labelled parts.
func SegID(parts ...uint64) uint64 {
	h := uint64(0x5eed)
	for _, p := range parts {
		h = combine(h, p)
	}
	return h
}

// run is a piece of one segment inside a block.
type run struct {
	seg    uint64
	offset int
	n      int
}

// Context is a conversation's token sequence, kept as the chained hashes of
// its full blocks, the way a paged prefix cache identifies blocks: a block's
// hash covers its parent's hash and its own tokens, so equal hashes mean
// equal prefixes. The trailing partial block has no hash yet.
type Context struct {
	B       int
	Blocks  []uint64
	Segs    []Seg
	Len     int
	partial []run
	pLen    int
	parent  uint64
}

// NewContext returns an empty context with block size b tokens.
func NewContext(b int) *Context {
	return &Context{B: b, parent: 0x0b10c}
}

// Append adds a segment's tokens to the end of the context.
func (c *Context) Append(s Seg) {
	c.Segs = append(c.Segs, s)
	off := 0
	for off < s.Len {
		k := min(s.Len-off, c.B-c.pLen)
		c.partial = append(c.partial, run{seg: s.ID, offset: off, n: k})
		c.pLen += k
		off += k
		c.Len += k
		if c.pLen == c.B {
			c.sealBlock()
		}
	}
}

func (c *Context) sealBlock() {
	h := c.parent
	for _, r := range c.partial {
		h = combine(h, r.seg)
		h = combine(h, uint64(r.offset)<<20|uint64(r.n))
	}
	c.Blocks = append(c.Blocks, h)
	c.parent = h
	c.partial = c.partial[:0]
	c.pLen = 0
}

// PrefixKey hashes the first n tokens of a segment list (all of them when
// the list is shorter). It is how a key over "a fixed token prefix" or "the
// first 100 characters of a message" sees the request.
func PrefixKey(segs []Seg, n int) string {
	h := uint64(0x6b6579)
	left := n
	for _, s := range segs {
		if left <= 0 {
			break
		}
		k := min(s.Len, left)
		h = combine(combine(h, s.ID), uint64(k))
		left -= k
	}
	return strconv.FormatUint(h, 16)
}

// SegsKey hashes whole segments.
func SegsKey(segs ...[]Seg) string {
	h := uint64(0x5e65)
	for _, list := range segs {
		for _, s := range list {
			h = combine(combine(h, s.ID), uint64(s.Len))
		}
		h = combine(h, 0xff)
	}
	return strconv.FormatUint(h, 16)
}

func totalLen(segs []Seg) int {
	n := 0
	for _, s := range segs {
		n += s.Len
	}
	return n
}
