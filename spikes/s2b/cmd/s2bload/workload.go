package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"
	"time"
)

// Workload is a synthetic multi-turn agent workload. Every arm of the comparison runs the same
// workload (same seed: same agent types, texts, lengths and think times); only Run differs.
// Run is written at the very start of every system prompt, so no prefix can be shared across arms.
//
// Structure, chosen so that neither "every request has a different prefix" nor "every request has
// the same prefix" holds:
//   - a few agent types, each with its own long system prompt shared by all sessions of that type;
//   - each session starts with a unique task, then grows turn by turn: tool output (user) and the
//     generated reply (assistant) are appended, so turn t's prompt extends turn t-1's prompt.
type Workload struct {
	Run          string
	Seed         int64
	Sessions     int
	Turns        int
	AgentWeights []float64 // popularity of each agent type; len = number of types
	SysWords     int
	TaskWords    int
	ToolMinWords int
	ToolMaxWords int
	ThinkMin     time.Duration
	ThinkMax     time.Duration
}

// vocab: common English words; most are a single token for byte-level BPE tokenizers.
var vocab = strings.Fields(`the of and to in is that for it as was with be by on not he this are or his from
at which but have an they you were her she there been one all we their has would when if so no will more can
out up who them some time into only other new could two may then first any like now my over such our man me
even most made after also did many before must through back years where much your way well down should because
each just those people how too little state good very make world still own see men work long get here between
both life being under never day same another know while last might us great old year off come since against go
came right used take three states himself few house use during without again place around however home small
found thought went say part once general high upon school every does got united left number course war until
always away something fact though water less public put think almost hand enough far took head yet government
system better set told nothing night end why called didn find going look asked later knew point next program
city business give group toward young days let room president side social given present several order national
possible rather second face per among form important often things looked early white case john become large big
need four within felt along children saw best church ever least power development light thing seemed family
interest want members others mind country area done although open god service problem certain kind different
thus began door help sense means whole matter perhaps itself york times law human line above name example action
company hands local show whether five history gave today either act feet across taken past quite anything seen
having death week experience body word half really field am car words already themselves information tell
together college shall money period held keep sure free real probably seems behind cannot miss political air
question making office brought whose special heard major problems ago became federal moment study available known
result street economic boy position reason change south board individual job areas society west close turn love
community true court force full seem am front`)

// rng returns a generator seeded by the workload seed and the given labels. Equal labels give equal
// sequences in every arm; Run is deliberately not part of the seed.
func (w *Workload) rng(labels ...any) *rand.Rand {
	h := fnv.New64a()
	fmt.Fprint(h, w.Seed)
	for _, l := range labels {
		fmt.Fprint(h, "|", l)
	}
	return rand.New(rand.NewSource(int64(h.Sum64() >> 1)))
}

func words(r *rand.Rand, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(vocab[r.Intn(len(vocab))])
		if i%17 == 16 {
			b.WriteByte('.')
		}
	}
	return b.String()
}

// AgentOf returns the agent type of session s, drawn by the configured popularity.
func (w *Workload) AgentOf(s int) int {
	var total float64
	for _, x := range w.AgentWeights {
		total += x
	}
	u := w.rng("agent", s).Float64() * total
	for k, x := range w.AgentWeights {
		if u < x {
			return k
		}
		u -= x
	}
	return len(w.AgentWeights) - 1
}

// SystemPrompt is shared by every session of agent type k within one run.
func (w *Workload) SystemPrompt(k int) string {
	return fmt.Sprintf("Deployment %s.\nYou are agent type %d. Follow these instructions.\n", w.Run, k) + words(w.rng("sys", k), w.SysWords)
}

// Tag marks a user message with its run, session and turn. The placement middleware on each worker
// reads the tag of the highest turn in a request body to learn which session turn it served.
func (w *Workload) Tag(s, t int) string { return fmt.Sprintf("[s2b:%s:%d:%d]", w.Run, s, t) }

// UserMessage is turn t's new user content of session s: the task at turn 0, a tool result after.
func (w *Workload) UserMessage(s, t int) string {
	if t == 0 {
		return fmt.Sprintf("Task for session %d: ", s) + words(w.rng("task", s), w.TaskWords) + " " + w.Tag(s, t)
	}
	r := w.rng("tool", s, t)
	n := w.ToolMinWords
	if w.ToolMaxWords > w.ToolMinWords {
		n += r.Intn(w.ToolMaxWords - w.ToolMinWords + 1)
	}
	return "Tool result:\n" + words(r, n) + " " + w.Tag(s, t)
}

// Think is the pause after turn t of session s (the agent running its tool).
func (w *Workload) Think(s, t int) time.Duration {
	if w.ThinkMax <= w.ThinkMin {
		return w.ThinkMin
	}
	return w.ThinkMin + time.Duration(w.rng("think", s, t).Int63n(int64(w.ThinkMax-w.ThinkMin)))
}

// SessionID is the x-session-id header value of session s.
func (w *Workload) SessionID(s int) string { return fmt.Sprintf("%s-s%03d", w.Run, s) }
