package rig

import (
	"fmt"
	"sort"
	"strings"
)

// Begin is one engine's record of a request: it was admitted (begun, the engine started work on it), and
// how it ended, "" when the engine never wrote its end (it died under it).
type Begin struct {
	Instance int    `json:"instance"`
	Admitted bool   `json:"admitted"`
	Outcome  string `json:"outcome,omitempty"`
}

// Begins reads every double's log and returns, per request ID, each engine record of it. An ID's
// admitted record and its end are paired in order on one Instance.
func (r *Rig) Begins() (map[string][]Begin, error) {
	out := map[string][]Begin{}
	for i := 0; i < r.N; i++ {
		evs, err := ReadEngineLog(r.Doubles.Log(i))
		if err != nil {
			return nil, err
		}
		for _, ev := range evs {
			id := IDIn([]byte(ev.Tail))
			if id == "" {
				continue
			}
			switch ev.Event {
			case "admitted":
				out[id] = append(out[id], Begin{Instance: i, Admitted: true})
			case "request":
				bs := out[id]
				matched := false
				for k := range bs {
					if bs[k].Instance == i && bs[k].Admitted && bs[k].Outcome == "" {
						bs[k].Outcome, matched = ev.Outcome, true
						break
					}
				}
				if !matched { // ended without being admitted: refused, or cancelled before a slot
					bs = append(bs, Begin{Instance: i, Outcome: ev.Outcome})
				}
				out[id] = bs
			}
		}
	}
	return out, nil
}

// Admissions counts an ID's admitted records: how many times an engine began it.
func Admissions(bs []Begin) int {
	n := 0
	for _, b := range bs {
		if b.Admitted {
			n++
		}
	}
	return n
}

// Reconcile checks the client's records against the engines': every engine record belongs to a client
// attempt, and every client attempt that got an answer from an Instance (a 200, or an engine's own
// error) was begun or seen by an engine. It returns the problems found, none when the books balance.
func Reconcile(results []Result, begins map[string][]Begin) []string {
	var probs []string
	byID := map[string]Result{}
	for _, r := range results {
		if _, dup := byID[r.ID]; dup {
			probs = append(probs, fmt.Sprintf("attempt %s recorded twice by the client", r.ID))
		}
		byID[r.ID] = r
	}
	ids := make([]string, 0, len(begins))
	for id := range begins {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, ok := byID[id]; !ok {
			probs = append(probs, fmt.Sprintf("an engine recorded %s, which the client never sent", id))
		}
	}
	for _, r := range results {
		if (r.Status == 200 || strings.HasPrefix(r.Code, "Internal")) && len(begins[r.ID]) == 0 {
			probs = append(probs, fmt.Sprintf("%s answered %d by an Instance, with no engine record of it", r.ID, r.Status))
		}
	}
	return probs
}
