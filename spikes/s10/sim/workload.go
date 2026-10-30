package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// ClientType describes one kind of client: what its system prompt and first
// user message look like, and whether it sends a session header.
type ClientType struct {
	Name  string
	Share float64
	// SysStatic > 0: every conversation of this type starts with the same
	// static system prompt of this many tokens, followed by a project tail
	// drawn from Projects tails (Zipf-popular) of ProjectTail tokens.
	SysStatic   int
	Projects    int
	ProjectTail int
	// SysStatic == 0: a unique system prompt, uniform in [UniqueMin, UniqueMax].
	UniqueMin, UniqueMax int
	// Preamble is a fixed block the client puts at the start of the first
	// user message (an agent harness's reminder text), in tokens.
	Preamble    int
	SendsHeader bool
}

// Workload holds the generator's parameters. Lengths are in tokens and
// times in seconds; "LN" fields are lognormal (median, sigma).
type Workload struct {
	Seed          uint64
	Clients       []ClientType
	ArrivalRate   float64 // conversations per second (Poisson)
	ArrivalEnd    float64 // no new conversation starts after this time
	ShortShare    float64 // share of short sessions
	ShortTurns    [2]int  // uniform range
	LongTurns     [2]int  // uniform range
	TaskLN        [2]float64
	TaskClip      [2]int
	OutLN         [2]float64
	OutClip       [2]int
	AppendLN      [2]float64
	AppendClip    [2]int
	ThinkLN       [2]float64
	ThinkClip     [2]float64
	CompactAt     int // compact when the next prompt would exceed this
	SummaryTokens int // the summary that replaces a compacted conversation
}

// DefaultWorkload is the S10 agent workload. Its numbers are assumptions
// (see the README), not measurements.
func DefaultWorkload(seed uint64) Workload {
	return Workload{
		Seed: seed,
		Clients: []ClientType{
			{Name: "agent-a", Share: 0.35, SysStatic: 12000, Projects: 12, ProjectTail: 800, Preamble: 400, SendsHeader: true},
			{Name: "agent-b", Share: 0.25, SysStatic: 6000, Projects: 12, ProjectTail: 500, Preamble: 150},
			{Name: "agent-c", Share: 0.15, SysStatic: 2500, Projects: 1, ProjectTail: 0},
			{Name: "script", Share: 0.25, UniqueMin: 300, UniqueMax: 3000},
		},
		ArrivalRate: 0.45, ArrivalEnd: 4800,
		ShortShare: 0.6, ShortTurns: [2]int{2, 6}, LongTurns: [2]int{15, 60},
		TaskLN: [2]float64{300, 1.0}, TaskClip: [2]int{30, 4000},
		OutLN: [2]float64{250, 0.8}, OutClip: [2]int{20, 2000},
		AppendLN: [2]float64{900, 1.0}, AppendClip: [2]int{20, 16000},
		ThinkLN: [2]float64{3, 1.0}, ThinkClip: [2]float64{0.2, 120},
		CompactAt: 100000, SummaryTokens: 3000,
	}
}

// Turn is one request of a conversation.
type Turn struct {
	Append Seg     // tokens appended before this request (zero for turn 0)
	Out    int     // output tokens
	Think  float64 // pause after this turn's reply before the next request
}

// Conversation is a generated agent session. The content is fixed by the
// seed, so every routing policy sees the same workload.
type Conversation struct {
	ID        int
	Client    int
	Arrival   float64
	Header    bool
	Sys       []Seg
	FirstUser []Seg
	Turns     []Turn
	// Summaries[i] is the summary segment used if the conversation is
	// compacted before turn i.
	Summaries []Seg
}

func lognormal(r *rand.Rand, median, sigma float64) float64 {
	return median * math.Exp(sigma*r.NormFloat64())
}

func clipInt(v float64, lo, hi int) int {
	return max(lo, min(hi, int(math.Round(v))))
}

// zipfIndex draws an index in [0, n) with weight 1/(i+1).
func zipfIndex(r *rand.Rand, n int) int {
	var total float64
	for i := 0; i < n; i++ {
		total += 1 / float64(i+1)
	}
	u := r.Float64() * total
	for i := 0; i < n; i++ {
		u -= 1 / float64(i+1)
		if u <= 0 {
			return i
		}
	}
	return n - 1
}

func (w Workload) pickClient(r *rand.Rand) int {
	u := r.Float64()
	for i, c := range w.Clients {
		u -= c.Share
		if u <= 0 {
			return i
		}
	}
	return len(w.Clients) - 1
}

// Generate draws the conversations.
func (w Workload) Generate() ([]Conversation, error) {
	if err := w.validate(); err != nil {
		return nil, err
	}
	r := rand.New(rand.NewPCG(w.Seed, 0x5100))
	var convs []Conversation
	t := 0.0
	for id := 0; ; id++ {
		t += r.ExpFloat64() / w.ArrivalRate
		if t > w.ArrivalEnd {
			break
		}
		convs = append(convs, w.conversation(r, id, t))
	}
	return convs, nil
}

func (w Workload) validate() error {
	var share float64
	for _, c := range w.Clients {
		share += c.Share
	}
	if math.Abs(share-1) > 1e-9 {
		return fmt.Errorf("client shares sum to %v, want 1", share)
	}
	if w.ArrivalRate <= 0 || w.ArrivalEnd <= 0 {
		return fmt.Errorf("arrival rate and end must be positive")
	}
	return nil
}

func (w Workload) conversation(r *rand.Rand, id int, t float64) Conversation {
	ci := w.pickClient(r)
	ct := w.Clients[ci]
	cid := uint64(id)
	c := Conversation{ID: id, Client: ci, Arrival: t, Header: ct.SendsHeader}
	if ct.SysStatic > 0 {
		c.Sys = []Seg{{ID: SegID(1, uint64(ci)), Len: ct.SysStatic}}
		if ct.ProjectTail > 0 {
			p := zipfIndex(r, ct.Projects)
			c.Sys = append(c.Sys, Seg{ID: SegID(2, uint64(ci), uint64(p)), Len: ct.ProjectTail})
		}
	} else {
		n := ct.UniqueMin + r.IntN(ct.UniqueMax-ct.UniqueMin+1)
		c.Sys = []Seg{{ID: SegID(3, cid), Len: n}}
	}
	if ct.Preamble > 0 {
		c.FirstUser = append(c.FirstUser, Seg{ID: SegID(4, uint64(ci)), Len: ct.Preamble})
	}
	task := clipInt(lognormal(r, w.TaskLN[0], w.TaskLN[1]), w.TaskClip[0], w.TaskClip[1])
	c.FirstUser = append(c.FirstUser, Seg{ID: SegID(5, cid), Len: task})
	turns := w.LongTurns
	if r.Float64() < w.ShortShare {
		turns = w.ShortTurns
	}
	n := turns[0] + r.IntN(turns[1]-turns[0]+1)
	for i := 0; i < n; i++ {
		var tr Turn
		if i > 0 {
			a := clipInt(lognormal(r, w.AppendLN[0], w.AppendLN[1]), w.AppendClip[0], w.AppendClip[1])
			tr.Append = Seg{ID: SegID(6, cid, uint64(i)), Len: a}
		}
		tr.Out = clipInt(lognormal(r, w.OutLN[0], w.OutLN[1]), w.OutClip[0], w.OutClip[1])
		tr.Think = max(w.ThinkClip[0], min(w.ThinkClip[1], lognormal(r, w.ThinkLN[0], w.ThinkLN[1])))
		c.Turns = append(c.Turns, tr)
		c.Summaries = append(c.Summaries, Seg{ID: SegID(7, cid, uint64(i)), Len: w.SummaryTokens})
	}
	return c
}

// OutSeg is the identity of a turn's reply.
func OutSeg(conv, turn, n int) Seg { return Seg{ID: SegID(8, uint64(conv), uint64(turn)), Len: n} }
