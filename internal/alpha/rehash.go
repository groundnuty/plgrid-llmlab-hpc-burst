package alpha

// The checkpoint's rehash as the agent's own log shows it (issue #575): every warm start rehashes the staged checkpoint, so
// the rows that warm-start on Athena (row A, row B's two jobs) measure HBEP-9's open "rehash time on Athena A100" at no
// extra cost. It is information, never a check: a cold start or a log with no line for it is said in words, never a zero,
// and changes no verdict.

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// RehashObs is the first rehash line of the agent's log of a job (AgentRehashMsg): the files found present and rehashed,
// of those listed, their bytes and the rehash's milliseconds. Read says the line was found with all four numbers.
type RehashObs struct {
	Read   bool  `json:"read"`
	Files  int   `json:"files"`
	Listed int   `json:"listed"`
	Bytes  int64 `json:"bytes"`
	MS     int64 `json:"ms"`
}

// rehashKey is one key of the agent's rehash line, a whole number.
func rehashKey(k string) *regexp.Regexp {
	return regexp.MustCompile(`(?:^|\s)` + k + `=(\d+)(?:\s|$)`)
}

var rehashKeys = [4]*regexp.Regexp{rehashKey("files"), rehashKey("listed"), rehashKey("bytes"), rehashKey("ms")}

// RehashOf reads the first rehash line of an agent's log: an INFO line of slog's text form whose msg is AgentRehashMsg,
// with its four numbers. A log without one, or whose line lacks a number, is not read.
func RehashOf(log []byte) RehashObs {
	for _, l := range strings.Split(string(log), "\n") {
		m := slogMsg.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		msg := m[1]
		if u, err := strconv.Unquote(msg); err == nil {
			msg = u
		}
		if lv := slogLevel.FindStringSubmatch(l); msg != AgentRehashMsg || lv == nil || lv[1] != "INFO" {
			continue
		}
		var n [4]int64
		for i, re := range rehashKeys {
			v := re.FindStringSubmatch(l)
			if v == nil {
				return RehashObs{}
			}
			x, err := strconv.ParseInt(v[1], 10, 64)
			if err != nil {
				return RehashObs{}
			}
			n[i] = x
		}
		return RehashObs{Read: true, Files: int(n[0]), Listed: int(n[1]), Bytes: n[2], MS: n[3]}
	}
	return RehashObs{}
}

// rehashLine is one job's rehash in the text to post: the numbers and the rate they give, or in words why there are none.
func rehashLine(job string, w WarmObs) string {
	head := "- the checkpoint's rehash of " + job + ", from its agent's own log (HBEP-9's rehash time; information, not a check): "
	r := w.Rehash
	switch {
	case !w.LogRead:
		return head + "not read: the agent's log was not read\n"
	case !r.Read:
		return head + "not read: the agent's log holds no rehash line\n"
	}
	return head + RehashWords(r) + "\n"
}

// RehashWords is a rehash that was read, as a text to post says it: the numbers and the rate they give, or, for a cold
// start, why there are none, in words. The pre-staging row (internal/providerctl/conformance) says its helper's with it.
func RehashWords(r RehashObs) string {
	if r.Files == 0 {
		return fmt.Sprintf("none: a cold start, no listed file of %d was present to rehash", r.Listed)
	}
	rate := "too short for a rate"
	if r.MS > 0 {
		rate = fmt.Sprintf("%.0f MB/s", float64(r.Bytes)/float64(r.MS)/1e3)
	}
	return fmt.Sprintf("%d of %d listed files, %d bytes (%.2f GB) in %.1f s, %s", r.Files, r.Listed, r.Bytes, float64(r.Bytes)/1e9, float64(r.MS)/1e3, rate)
}
