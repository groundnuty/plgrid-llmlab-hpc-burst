package sim

import (
	"container/heap"
	"fmt"
)

// Cluster describes the Instances and the engine timing model.
type Cluster struct {
	Instances   int     // members at time 0
	Spare       int     // extra Instances that can join later
	KVTokens    int     // KV cache per Instance, in tokens
	BlockSize   int     // tokens per cache block
	MaxRunning  int     // concurrent sequences per Instance
	PrefillRate float64 // uncached prompt tokens per second, one prefill at a time
	TPOT        float64 // seconds per output token for a running sequence
}

// DefaultCluster is the S10 cluster. Its numbers are assumptions.
func DefaultCluster(kvTokens int) Cluster {
	return Cluster{Instances: 4, Spare: 1, KVTokens: kvTokens, BlockSize: 64, MaxRunning: 48, PrefillRate: 25000, TPOT: 0.03}
}

// EventKind is a membership change.
type EventKind string

const (
	Drain EventKind = "drain" // leaves the ring; its in-flight requests finish
	Leave EventKind = "leave" // leaves at once; its in-flight requests are cut and re-sent
	Join  EventKind = "join"  // a cold Instance joins the ring
)

// Event changes membership at a time.
type Event struct {
	Time     float64
	Kind     EventKind
	Instance int
}

type convState struct {
	c         *Conversation
	user      *UserStream // nil for an explicit conversation list
	ctx       *Context
	firstUser []Seg
	turn      int
	lastInst  int
	evSeen    int // index of the latest event this conversation was checked for
}

type request struct {
	id        int
	cs        *convState
	turn      int
	promptLen int
	outLen    int
	blocks    []uint64
	inst      int
	release   func()
	hold      *Hold
	dispatchT float64
	aborted   bool
}

type instance struct {
	name        string
	cache       *Cache
	member      bool
	alive       bool
	inflight    int
	waiting     []*request
	running     int
	prefillQ    []*request
	prefillBusy bool
	active      []*request // dispatched here and not finished
}

// Engine runs one simulation.
type Engine struct {
	cl     Cluster
	pol    Policy
	wl     Workload
	convs  []Conversation
	events []Event
	inst   []*instance
	rt     *router
	q      eventQueue
	now    float64
	nextID int
	M      *Metrics
	err    error
	// Conversations started and not finished, and their context tokens.
	activeConvs, activeTokens int
}

// NewEngine prepares a run under pol on cl, with membership events. With
// convs nil it runs the workload's closed loop of users; otherwise it runs
// exactly convs, each starting at its arrival time.
func NewEngine(cl Cluster, pol Policy, wl Workload, convs []Conversation, events []Event) *Engine {
	e := &Engine{cl: cl, pol: pol, wl: wl, convs: convs, events: events}
	names := make([]string, cl.Instances+cl.Spare)
	var initial []int
	for i := range names {
		names[i] = fmt.Sprintf("instance-%d", i)
		e.inst = append(e.inst, &instance{name: names[i], cache: NewCache(cl.BlockSize, cl.KVTokens)})
		if i < cl.Instances {
			initial = append(initial, i)
			e.inst[i].member, e.inst[i].alive = true, true
		}
	}
	e.rt = newRouter(pol, names, initial)
	e.M = newMetrics(pol, len(names))
	return e
}

// Run executes the simulation to the end of the last conversation.
func (e *Engine) Run() error {
	for i := range e.convs {
		c := &e.convs[i]
		e.push(c.Arrival, func() { e.start(c, nil) })
	}
	if e.convs == nil {
		for u := 0; u < e.wl.Users; u++ {
			s := e.wl.Stream(u)
			at := e.wl.Ramp * float64(u) / float64(e.wl.Users)
			e.push(at, func() { e.start(s.Next(at), s) })
		}
	}
	for i, ev := range e.events {
		e.push(ev.Time, func() { e.cluster(i, ev) })
	}
	for e.q.Len() > 0 && e.err == nil {
		it := heap.Pop(&e.q).(*item)
		e.M.advance(e, it.t)
		e.now = it.t
		it.fn()
	}
	if e.err != nil {
		return e.err
	}
	return e.M.finish(e)
}

func (e *Engine) push(t float64, fn func()) {
	e.nextID++
	heap.Push(&e.q, &item{t: t, seq: e.nextID, fn: fn})
}

func (e *Engine) start(c *Conversation, user *UserStream) {
	cs := &convState{c: c, user: user, ctx: NewContext(e.cl.BlockSize), firstUser: c.FirstUser, lastInst: -1, evSeen: -1}
	for _, s := range c.Sys {
		cs.ctx.Append(s)
	}
	for _, s := range c.FirstUser {
		cs.ctx.Append(s)
	}
	e.activeConvs++
	e.activeTokens += cs.ctx.Len
	e.dispatch(cs)
}

// next prepares a conversation's next turn, compacting it first when the
// prompt would grow past the workload's limit.
func (e *Engine) next(cs *convState) {
	cs.turn++
	t := cs.c.Turns[cs.turn]
	e.activeTokens -= cs.ctx.Len
	defer func() { e.activeTokens += cs.ctx.Len }()
	if cs.ctx.Len+t.Append.Len > e.wl.CompactAt {
		sum := cs.c.Summaries[cs.turn]
		cs.ctx = NewContext(e.cl.BlockSize)
		for _, s := range cs.c.Sys {
			cs.ctx.Append(s)
		}
		cs.ctx.Append(sum)
		cs.firstUser = []Seg{sum}
		e.M.Compactions++
	}
	cs.ctx.Append(t.Append)
	e.dispatch(cs)
}

func (e *Engine) dispatch(cs *convState) {
	e.nextID++
	r := &request{id: e.nextID, cs: cs, turn: cs.turn, promptLen: cs.ctx.Len,
		outLen: cs.c.Turns[cs.turn].Out, blocks: cs.ctx.Blocks, dispatchT: e.now}
	if !e.inst[0].cache.Fits(ceilDiv(r.promptLen+r.outLen, e.cl.BlockSize)) {
		e.err = fmt.Errorf("conversation %d turn %d needs %d tokens, more than an Instance's KV cache", cs.c.ID, cs.turn, r.promptLen+r.outLen)
		return
	}
	e.place(r)
}

func (e *Engine) place(r *request) {
	inflight := make([]int, len(e.inst))
	for i, in := range e.inst {
		inflight[i] = in.inflight
	}
	idx, release, err := e.rt.route(r.cs, r.id, inflight)
	if err != nil {
		e.err = err
		return
	}
	in := e.inst[idx]
	if !in.member || !in.alive {
		e.err = fmt.Errorf("request %d placed on non-member %s", r.id, in.name)
		return
	}
	e.M.placement(e, r, idx)
	r.inst, r.release = idx, release
	r.cs.lastInst = idx
	in.inflight++
	in.active = append(in.active, r)
	in.waiting = append(in.waiting, r)
	e.tryAdmit(in)
}

func (e *Engine) tryAdmit(in *instance) {
	for in.running < e.cl.MaxRunning && len(in.waiting) > 0 {
		r := in.waiting[0]
		hold := in.cache.Admit(r.blocks, r.promptLen, r.outLen)
		if hold == nil {
			return
		}
		in.waiting = in.waiting[1:]
		in.running++
		r.hold = hold
		in.prefillQ = append(in.prefillQ, r)
		if !in.prefillBusy {
			e.startPrefill(in)
		}
	}
}

func (e *Engine) startPrefill(in *instance) {
	r := in.prefillQ[0]
	in.prefillQ = in.prefillQ[1:]
	in.prefillBusy = true
	uncached := r.promptLen - r.hold.HitTokens
	e.push(e.now+float64(uncached)/e.cl.PrefillRate, func() { e.prefillDone(r) })
}

func (e *Engine) prefillDone(r *request) {
	if r.aborted {
		return
	}
	in := e.inst[r.inst]
	in.prefillBusy = false
	if len(in.prefillQ) > 0 {
		e.startPrefill(in)
	}
	e.push(e.now+float64(r.outLen)*e.cl.TPOT, func() { e.decodeDone(r) })
}

func (e *Engine) decodeDone(r *request) {
	if r.aborted {
		return
	}
	in := e.inst[r.inst]
	cs := r.cs
	cs.ctx.Append(OutSeg(cs.c.ID, r.turn, r.outLen))
	e.activeTokens += r.outLen
	in.cache.Release(r.hold, cs.ctx.Blocks[:(r.promptLen+r.outLen)/e.cl.BlockSize])
	in.running--
	e.finish(in, r)
	e.M.completed(e, r)
	e.tryAdmit(in)
	if cs.turn+1 < len(cs.c.Turns) {
		e.push(e.now+cs.c.Turns[cs.turn].Think, func() { e.next(cs) })
		return
	}
	e.activeConvs--
	e.activeTokens -= cs.ctx.Len
	if s := cs.user; s != nil {
		at := e.now + cs.c.Turns[cs.turn].Think
		if at < e.wl.ArrivalEnd {
			e.push(at, func() { e.start(s.Next(at), s) })
		}
	}
}

// finish ends a request's life on its Instance and releases its load count.
func (e *Engine) finish(in *instance, r *request) {
	in.inflight--
	r.release()
	for i, a := range in.active {
		if a == r {
			in.active = append(in.active[:i:i], in.active[i+1:]...)
			break
		}
	}
}

func (e *Engine) cluster(k int, ev Event) {
	in := e.inst[ev.Instance]
	e.M.event(e, k, ev)
	switch ev.Kind {
	case Join:
		in.member, in.alive = true, true
		e.rt.add(ev.Instance)
	case Drain:
		in.member = false
		e.rt.remove(ev.Instance)
	case Leave:
		in.member = false
		e.rt.remove(ev.Instance)
		cut := append([]*request(nil), in.active...)
		for _, r := range cut {
			r.aborted = true
			e.finish(in, r)
			e.M.Aborted++
		}
		in.alive = false
		in.waiting, in.prefillQ, in.running, in.prefillBusy = nil, nil, 0, false
		in.cache = NewCache(e.cl.BlockSize, e.cl.KVTokens)
		for _, r := range cut {
			e.dispatch(r.cs)
		}
	}
}

type item struct {
	t   float64
	seq int
	fn  func()
}

type eventQueue []*item

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].t != q[j].t {
		return q[i].t < q[j].t
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(*item)) }
func (q *eventQueue) Pop() any {
	old := *q
	it := old[len(old)-1]
	*q = old[:len(old)-1]
	return it
}
