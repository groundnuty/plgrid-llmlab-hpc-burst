package alpha

// The live tunnel hour's upload direction in alpha (issue #224; the science
// agent's ruling (4) on PR 255: no sender-side timing on the live path): the
// large requests' bodies, the proof that each arrived whole, the head-of-line
// measure, and the verdict's live and dry readings, worked out by hand.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
)

func TestTheArrivalPrompt(t *testing.T) {
	for _, words := range []int{1, 1000, 21_838, 32_700} {
		b := ArrivalPrompt("example-org/example-model", words, 32768)
		var m struct {
			Model     string `json:"model"`
			MaxTokens int    `json:"max_tokens"`
			Messages  []struct {
				Role, Content string
			} `json:"messages"`
		}
		if err := json.Unmarshal([]byte(b), &m); err != nil {
			t.Fatalf("%d words: not JSON: %v", words, err)
		}
		// One user message of exactly that many "hello" words, asking for the whole context.
		if m.Model != "example-org/example-model" || m.MaxTokens != 32768 || len(m.Messages) != 1 || m.Messages[0].Role != "user" ||
			len(strings.Fields(m.Messages[0].Content)) != words || strings.Trim(m.Messages[0].Content, "hello ") != "" {
			t.Errorf("%d words: model %q, max_tokens %d, %d messages, %d words", words, m.Model, m.MaxTokens, len(m.Messages), len(strings.Fields(m.Messages[0].Content)))
		}
	}
	// Model "m", max_tokens 1: 51 bytes of JSON before the prompt and 19 after it, and "hello" then " hello" w-1
	// times is 6w-1 bytes, so the least w with 70+6w-1 >= 400,000 is 66,656 (a body of 400,005 bytes).
	if w := WordsForBytes("m", 400_000, 1); w != 66_656 || len(ArrivalPrompt("m", w, 1)) != 400_005 {
		t.Errorf("WordsForBytes(400,000) = %d (%d bytes), want 66,656 (400,005)", w, len(ArrivalPrompt("m", w, 1)))
	}
	if w := WordsForBytes("m", 400_005, 1); w != 66_656 {
		t.Errorf("WordsForBytes(400,005) = %d, want 66,656", w)
	}
	if w := WordsForBytes("m", 400_006, 1); w != 66_657 {
		t.Errorf("WordsForBytes(400,006) = %d, want 66,657", w)
	}
}

// Three counts give the engine's count as a line in the words; a count that is no line is refused.
func TestCalibrate(t *testing.T) {
	// The engine double's: a token a word, and 4 for the chat template and its roles.
	l, err := Calibrate([2]int{1000, 1004}, [2]int{2000, 2004}, [2]int{4000, 4004})
	if err != nil || l != (TokenLine{Per: 1, Fixed: 4}) {
		t.Fatalf("Calibrate = %+v, %v", l, err)
	}
	// Under a 32,768-token context: 32,763 words make 32,767 tokens, one under it.
	if w := l.MaxWords(32768); w != 32_763 || l.Tokens(w) != 32_767 {
		t.Errorf("MaxWords(32768) = %d (%d tokens), want 32,763 (32,767)", w, l.Tokens(w))
	}
	// Two tokens a word and a 30-token template.
	if l, err := Calibrate([2]int{1000, 2030}, [2]int{2000, 4030}, [2]int{4000, 8030}); err != nil || l != (TokenLine{Per: 2, Fixed: 30}) || l.MaxWords(32768) != 16_368 {
		t.Errorf("Calibrate(2 a word) = %+v (MaxWords %d), %v", l, l.MaxWords(32768), err)
	}
	for name, pts := range map[string][][2]int{
		// A tokenizer that merges two words into one token every 3,000 words: the first two points are on a line
		// of a token a word, the third is one under it.
		"a count that bends after the first two": {{1000, 1004}, {2000, 2004}, {4000, 4003}},
		"a third point one over the line":        {{1000, 1004}, {2000, 2004}, {4000, 4005}},
		"two points only":                        {{1000, 1004}, {2000, 2004}},
		"a fractional count a word":              {{1000, 1004}, {2000, 2500}, {4000, 3500}},
		"the same words twice":                   {{1000, 1004}, {1000, 1004}, {4000, 4004}},
		"a count that falls":                     {{1000, 1004}, {2000, 1004}, {4000, 4004}},
		"fewer tokens than words":                {{1000, 900}, {2000, 1900}, {4000, 3900}},
		"words that fall":                        {{2000, 2004}, {1000, 1004}, {4000, 4004}},
		"a third prompt shorter than the second": {{1000, 1004}, {2000, 2004}, {1500, 1504}},
	} {
		if l, err := Calibrate(pts...); err == nil {
			t.Errorf("%s: Calibrate%v = %+v, want an error", name, pts, l)
		}
	}
}

// engineCount is an engine's count of an ArrivalPrompt of words words, as its usage states it: tokens a word,
// a template's count, and one token less for every merge words (0: none; below 0: one token more for every -merge
// words, a tokenizer that splits).
func engineCount(per, fixed, merge int) func(int) (int, error) {
	return func(words int) (int, error) {
		n := per*words + fixed
		switch {
		case merge > 0:
			n -= words / merge
		case merge < 0:
			n += words / -merge
		}
		return n, nil
	}
}

// The large request is sized from the engine's own count, under the context, and the row fails closed on any
// engine whose count is no line, on a response with no count, and on a context too small for a large body.
func TestArrivalSize(t *testing.T) {
	var asked []int
	count := engineCount(1, 4, 0)
	logged := func(w int) (int, error) { asked = append(asked, w); return count(w) }
	body, expected, err := ArrivalSize(logged, "m", 400_000, 32768)
	if err != nil {
		t.Fatal(err)
	}
	// The fourth prompt is half the large prompt's 32,763 words, rounded up: 16,382 (the delta round of PR 255, B1).
	if !slices.Equal(asked, []int{1000, 2000, 4000, 16_382}) {
		t.Errorf("the calibration asked for %v words, want 1,000, 2,000, 4,000 and 16,382", asked)
	}
	// 32,763 words, 32,767 tokens, one under the context, which leaves room for the one token asked; the body is
	// "hello" then " hello" 32,762 times in the 70 bytes of JSON around it with max_tokens 1: 196,577 + 70 = 196,647.
	if words := len(strings.Fields(strings.Trim(strings.SplitN(strings.SplitN(body, `"content":"`, 2)[1], `"`, 2)[0], " "))); words != 32_763 || expected != 32_767 || len(body) != 196_647 {
		t.Errorf("ArrivalSize = %d words, %d bytes, %d tokens expected; want 32,763, 196,647, 32,767", words, len(body), expected)
	}
	if !strings.HasSuffix(body, `"max_tokens":1}`) {
		t.Errorf("the large body does not ask for one token: %q", body[len(body)-30:])
	}
	// A body that fits whole: its words, not the context, bound it, and the fourth prompt is half of them. Model "m",
	// max_tokens 1: 70 bytes of JSON around the prompt and 6w-1 in it, so the least w with 69+6w >= 131,072 is 21,834
	// (131,073 bytes; 21,833 make 131,067), counted 21,838, and half of it, rounded up, is 10,917.
	asked = nil
	count = engineCount(1, 4, 0)
	if body, expected, err := ArrivalSize(logged, "m", 131_072, 32768); err != nil || len(body) != 131_073 || expected != 21_838 || !slices.Equal(asked, []int{1000, 2000, 4000, 10_917}) {
		t.Errorf("a 128 KiB body: %d bytes, %d expected, asked %v, %v; want 131,073 bytes, 21,838, and 10,917 words last", len(body), expected, asked, err)
	}
	for name, c := range map[string]struct {
		count func(int) (int, error)
		mml   int
		want  string
	}{
		"an engine whose count bends every 3,000 words": {engineCount(1, 4, 3000), 32768, "not linear in the words"},
		"an engine whose count bends every 1,500 words": {engineCount(1, 4, 1500), 32768, "not a whole number of tokens a word"},
		// Astra's engine: words + 4 + floor(words/5000) lies on the line of a token a word and 4 up to 4,000 words; at
		// 16,382 it counts 16,389, three over the line's 16,386.
		"an engine that counts a token more every 5,000 words": {engineCount(1, 4, -5000), 32768, "the calibration prompt of 16382 words, half the large prompt's 32763: a prompt of 16382 words was counted 16389 input tokens, off the line"},
		"no count at the fourth prompt": {func(w int) (int, error) {
			if w > 4000 {
				return 0, errors.New("a prompt of 16382 words was answered 400, not completed with its usage.prompt_tokens")
			}
			return w + 4, nil
		}, 32768, "answered 400, not completed with its usage.prompt_tokens"},
		"no count at the first prompt": {func(w int) (int, error) {
			return 0, errors.New("a prompt of 1000 words was answered 502, not completed with its usage.prompt_tokens")
		}, 32768, "answered 502, not completed"},
		"a context too small for a large body": {engineCount(1, 4, 0), 8192, "under the 100000 bytes of a large body"},
	} {
		if body, n, err := ArrivalSize(c.count, "m", 400_000, c.mml); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %d bytes, %d expected, err %v; want an error naming %q", name, len(body), n, err, c.want)
		}
	}
}

// The prompt's count is read from a completed response's usage.prompt_tokens, and only when every choice finished and
// the usage adds up. Every refusal, whatever its wording (vLLM v0.32.0's "at least" lower bound over the context
// included), an error, a stream's chunk and anything incomplete show no arrival: the row fails closed.
func TestPromptTokensOf(t *testing.T) {
	for _, c := range []struct {
		name, body string
		want       int
		ok         bool
	}{
		{"one token, cut at max_tokens", `{"id":"x","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"a"},"finish_reason":"length"}],"usage":{"prompt_tokens":32767,"completion_tokens":1,"total_tokens":32768}}`, 32767, true},
		{"a stop at once", `{"object":"chat.completion","choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":1009,"completion_tokens":0,"total_tokens":1009}}`, 1009, true},
		// Errors and refusals, of any wording: no count.
		{"vLLM v0.32.0 over the context", `{"object":"error","message":"This model's maximum context length is 32768 tokens. However, your prompt contains at least 32769 input tokens, which exceeds this limit even without any output tokens.","type":"BadRequestError","code":400}`, 0, false},
		{"an error object", `{"error":{"message":"JSON decode error: unexpected end of JSON input","type":"BadRequestError","code":400}}`, 0, false},
		{"not JSON", `not json`, 0, false},
		// A completion, but not one this reads.
		{"a stream's chunk with usage", `{"object":"chat.completion.chunk","choices":[{"finish_reason":"length"}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}`, 0, false},
		{"no choice", `{"object":"chat.completion","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}`, 0, false},
		{"a choice not finished", `{"object":"chat.completion","choices":[{"finish_reason":null}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}`, 0, false},
		{"a choice aborted", `{"object":"chat.completion","choices":[{"finish_reason":"abort"}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}`, 0, false},
		{"one choice of two not finished", `{"object":"chat.completion","choices":[{"finish_reason":"length"},{"finish_reason":null}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`, 0, false},
		{"no usage", `{"object":"chat.completion","choices":[{"finish_reason":"length"}]}`, 0, false},
		{"usage with no prompt_tokens", `{"object":"chat.completion","choices":[{"finish_reason":"length"}],"usage":{"completion_tokens":1,"total_tokens":6}}`, 0, false},
		{"usage with no completion_tokens", `{"object":"chat.completion","choices":[{"finish_reason":"length"}],"usage":{"prompt_tokens":5,"total_tokens":6}}`, 0, false},
		{"usage with no total_tokens", `{"object":"chat.completion","choices":[{"finish_reason":"length"}],"usage":{"prompt_tokens":5,"completion_tokens":1}}`, 0, false},
		{"no prompt token counted", `{"object":"chat.completion","choices":[{"finish_reason":"length"}],"usage":{"prompt_tokens":0,"completion_tokens":1,"total_tokens":1}}`, 0, false},
		{"usage that does not add up", `{"object":"chat.completion","choices":[{"finish_reason":"length"}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":7}}`, 0, false},
	} {
		if got, ok := PromptTokensOf([]byte(c.body)); got != c.want || ok != c.ok {
			t.Errorf("%s: %d %v, want %d %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

func TestArrivalVerified(t *testing.T) {
	for _, c := range []struct {
		expected, arrived int
		want              bool
	}{{21_842, 21_842, true}, {21_842, 21_843, false}, {21_842, 21_841, false}, {21_842, 0, false}, {0, 0, false}} {
		if got := (Stream{ExpectedTokens: c.expected, ArrivedTokens: c.arrived}).ArrivalVerified(); got != c.want {
			t.Errorf("expected %d, arrived %d: %v, want %v", c.expected, c.arrived, got, c.want)
		}
	}
}

// Each gap between two tokens of a stream is classed by the large requests' windows (the science agent's addition):
// one large request sent from 9.9 s to 10.2 s (its send window) and answered at 10.5 s (its prefill window from
// 10.2 s). Stream a has ticks 50 ms apart, then one gap of 1.2 s from 10.0 s, which overlaps the send window and the
// prefill window both, and so is a send; stream c has one gap of 650 ms from 10.25 s, in the prefill window alone;
// stream b's gap of 300 ms from 30.0 s overlaps neither, nor does a gap of 50 ms that ends exactly when the window
// opens.
func TestHeadOfLineOf(t *testing.T) {
	at := func(ms int) time.Time {
		return time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC).Add(time.Duration(ms) * time.Millisecond)
	}
	var a, b []time.Time
	for ms := 9_000; ms <= 10_000; ms += 50 {
		a = append(a, at(ms))
	}
	a = append(a, at(11_200), at(11_250))
	for ms := 29_000; ms <= 30_000; ms += 50 {
		b = append(b, at(ms))
	}
	b = append(b, at(30_300))
	c := []time.Time{at(10_250), at(10_900)}
	edge := []time.Time{at(9_850), at(9_900)}
	reqs := []Stream{{Ticks: a}, {Ticks: b}, {Ticks: c}, {Ticks: edge}, {Ticks: []time.Time{at(9_950)}}}
	w := LargeWindow{Start: at(9_900), Sent: at(10_200), End: at(10_500)}
	// Send: a's 9,900-9,950 and 9,950-10,000 (50 ms each) and 10,000-11,200 (1.2 s); prefill: c's 650 ms.
	h := HeadOfLineOf(reqs, []LargeWindow{w})
	if want := (HeadOfLine{InFlight: 1200 * time.Millisecond, Send: 1200 * time.Millisecond, Prefill: 650 * time.Millisecond, Elsewhere: 300 * time.Millisecond, Gaps: 4}); h != want {
		t.Errorf("HeadOfLineOf = %+v, want %+v", h, want)
	}
	// The longest gap in a prefill window alone is the longest in flight when no send gap is longer.
	if h := HeadOfLineOf([]Stream{{Ticks: c}}, []LargeWindow{w}); h.InFlight != 650*time.Millisecond || h.Prefill != h.InFlight || h.Send != 0 {
		t.Errorf("a prefill gap alone: %+v", h)
	}
	// A send's end not recorded, or outside the window: all of it is a send window, so c's gap is a send.
	for _, sent := range []time.Time{{}, at(10_600), at(9_800)} {
		w := w
		w.Sent = sent
		if h := HeadOfLineOf(reqs, []LargeWindow{w}); h.Send != 1200*time.Millisecond || h.Prefill != 0 || h.Gaps != 4 {
			t.Errorf("sent at %v: %+v; want every in-flight gap a send", sent, h)
		}
	}
	if h := HeadOfLineOf(reqs, nil); h.InFlight != 0 || h.Gaps != 0 || h.Elsewhere != 1200*time.Millisecond {
		t.Errorf("with no window: %+v", h)
	}
}

// scaledLiveHour is scaledHour (timing_test.go) as a live run reports it in alpha: its large request is a prompt
// the engine answered with one token, counting its input tokens in its usage, in flight by the client's window while the 16
// streams were open, with no upload interval and no upload rate.
func scaledLiveHour(r Row) TunnelHourObs {
	o, _ := scaledHour(r)
	o.Requests = append([]Stream(nil), o.Requests...)
	for i, q := range o.Requests {
		if q.BodyBytes >= MinLargeBody {
			o.Requests[i] = Stream{ID: q.ID, Opened: q.Opened, Ended: q.Ended, BodyBytes: 196_400, ExpectedTokens: 32_737, ArrivedTokens: 32_737,
				SendStart: q.Opened, SendEnd: q.Opened.Add(time.Second)}
		}
	}
	o.Rates = []PathRate{o.Rates[1]} // the download alone
	o.UploadNotMeasured = UploadNotMeasuredLive
	o.HeadOfLine = &HeadOfLine{InFlight: 80 * time.Millisecond, Send: 80 * time.Millisecond, Prefill: 60 * time.Millisecond, Elsewhere: 120 * time.Millisecond, Gaps: 16}
	return o
}

// The live reading accepts the upload as not measured, and judges the large requests by their arrival and their
// client windows; the dry reading keeps #244's full check, so the same observation without the live marker fails.
func TestALiveHourIsJudgedWithoutTheUploadTimingAndADryOneWithIt(t *testing.T) {
	r := scaledTunnel()
	_, jobs := scaledHour(r)
	o := scaledLiveHour(r)
	if f := VerifyTunnelHour(r, o, jobs); len(f) > 0 {
		t.Fatalf("a live hour with its arrivals shown failed: %v", f)
	}
	dry := o
	dry.UploadNotMeasured = ""
	f := VerifyTunnelHour(r, dry, jobs)
	for _, want := range []string{
		"the upload path rate was not posted: both directions are needed",
		"no request of 0.1 MB or more was sent while the 16 streams were open",
	} {
		if !slices.Contains(f, want) {
			t.Errorf("the dry reading lacks %q: %v", want, f)
		}
	}
	// Live, but a large body whose usage counted another count than its prompt's own: not shown arrived.
	short := scaledLiveHour(r)
	for i := range short.Requests {
		if short.Requests[i].BodyBytes >= MinLargeBody {
			short.Requests[i].ArrivedTokens = 32_736
		}
	}
	if f := VerifyTunnelHour(r, short, jobs); !slices.Contains(f, "a request of 0.1 MB or more did not show it arrived whole: the engine's usage.prompt_tokens must be its prompt's own input tokens") ||
		!slices.Contains(f, "a stream did not complete") {
		t.Errorf("a large body not shown arrived passed: %v", f)
	}
	// Live, but the large request in flight only after the streams ended.
	late := scaledLiveHour(r)
	for i := range late.Requests {
		if late.Requests[i].BodyBytes >= MinLargeBody {
			late.Requests[i].SendStart, late.Requests[i].SendEnd = late.Requests[i].Opened.Add(20*time.Second), late.Requests[i].Opened.Add(21*time.Second)
		}
	}
	if f := VerifyTunnelHour(r, late, jobs); !slices.Contains(f, "no request of 0.1 MB or more was in flight while the 16 streams were open") {
		t.Errorf("a large request in flight after the streams passed: %v", f)
	}
	// Live, but the client's window not recorded: not counted.
	nowin := scaledLiveHour(r)
	for i := range nowin.Requests {
		if nowin.Requests[i].BodyBytes >= MinLargeBody {
			nowin.Requests[i].SendStart, nowin.Requests[i].SendEnd = time.Time{}, time.Time{}
		}
	}
	if f := VerifyTunnelHour(r, nowin, jobs); !slices.Contains(f, "no request of 0.1 MB or more was in flight while the 16 streams were open") {
		t.Errorf("a large request with no window passed: %v", f)
	}
	// Live, but the client's window ends before it starts, its start inside the burst: not a window, not counted.
	backwards := scaledLiveHour(r)
	for i := range backwards.Requests {
		if backwards.Requests[i].BodyBytes >= MinLargeBody {
			backwards.Requests[i].SendEnd = backwards.Requests[i].SendStart.Add(-time.Second)
		}
	}
	if f := VerifyTunnelHour(r, backwards, jobs); !slices.Contains(f, "no request of 0.1 MB or more was in flight while the 16 streams were open") {
		t.Errorf("a large request whose window ends before it starts passed: %v", f)
	}
	// Live, with an upload rate posted after all: it is judged like any rate.
	posted := scaledLiveHour(r)
	posted.Rates = append(posted.Rates, PathRate{Direction: DirUpload, OK: true, AskedSeconds: 10, TimedSeconds: 10, Bytes: 1, UnderLoad: true})
	if f := VerifyTunnelHour(r, posted, jobs); !slices.Contains(f, "the upload path rate was asked for under 30 s, timed under 29 s, or moved no bytes") {
		t.Errorf("a short upload rate passed on a live hour: %v", f)
	}
}

// The text to post says the upload was not measured and why, how many large bodies were shown arrived, and the
// head-of-line measure; a stall over HeadOfLineStall is flagged.
func TestTheLiveHoursTextToPost(t *testing.T) {
	r := scaledTunnel()
	_, jobs := scaledHour(r)
	o := scaledLiveHour(r)
	post := tunnelHourPost(r, o, VerifyTunnelHour(r, o, jobs))
	for _, want := range []string{
		"- path rate, upload (front to node): not measured in alpha (no sender-side timing on the live path)\n",
		"- large requests (a body of 0.1 MB or more): 1, bodies of 196400 to 196400 bytes; 1 of them in flight while the 16 streams were open (by the client's window); 1 shown arrived whole by the engine's count of their prompt (usage.prompt_tokens)\n",
		"- head-of-line: longest gap between two tokens of a stream while a large body was in flight: 80ms (over 16 gaps; 80ms in a send window, 60ms in a prefill window); elsewhere in the hour: 120ms\n",
	} {
		if !strings.Contains(post, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, post)
		}
	}
	if strings.Contains(post, "head-of-line, in") {
		t.Errorf("gaps of 120 ms at most were flagged:\n%s", post)
	}
	// Each stall FLAG names its window: a large body's send, its prefill, or neither (the science agent's addition).
	// They inform; the verdict is the same.
	labels := map[string]string{
		"send":    "head-of-line, in a send window: a stream waited 1.5s between two tokens while a large body was being sent, over the 1s that marks a stall (a body on its way through the tunnel; another body's prefill may overlap)",
		"prefill": "head-of-line, in a prefill window: a stream waited 1.5s between two tokens after a large body was sent and before its response, with no body being sent, over the 1s that marks a stall (the engine prefilling a body's whole prompt, not the tunnel's upload)",
		"neither": "head-of-line, in neither window: a stream waited 1.5s between two tokens while no large body was in flight, over the 1s that marks a stall",
	}
	set := map[string]func(*HeadOfLine, time.Duration){
		"send":    func(h *HeadOfLine, d time.Duration) { h.Send, h.InFlight = d, d },
		"prefill": func(h *HeadOfLine, d time.Duration) { h.Prefill, h.InFlight = d, d },
		"neither": func(h *HeadOfLine, d time.Duration) { h.Elsewhere = d },
	}
	want := VerifyTunnelHour(r, o, jobs)
	for label, put := range set {
		stalled := o
		h := *o.HeadOfLine
		put(&h, 1500*time.Millisecond)
		stalled.HeadOfLine = &h
		if fl := TunnelHourFlags(stalled); !slices.Equal(fl, []string{labels[label]}) {
			t.Errorf("a 1.5 s gap in the %s class: FLAGs %v; want only %q", label, fl, labels[label])
		}
		if f := VerifyTunnelHour(r, stalled, jobs); !slices.Equal(f, want) {
			t.Errorf("a 1.5 s gap in the %s class changed the verdict: %v, want %v", label, f, want)
		}
		put(&h, HeadOfLineStall)
		if fl := TunnelHourFlags(stalled); len(fl) != 0 {
			t.Errorf("a gap of exactly the threshold in the %s class was flagged: %v", label, fl)
		}
	}
	// All three at once: three FLAGs, in the order send, prefill, neither.
	h := HeadOfLine{InFlight: 2 * time.Second, Send: 2 * time.Second, Prefill: 1500 * time.Millisecond, Elsewhere: 1500 * time.Millisecond}
	all := o
	all.HeadOfLine = &h
	if fl := TunnelHourFlags(all); len(fl) != 3 || !strings.HasPrefix(fl[0], "head-of-line, in a send window: a stream waited 2s") ||
		fl[1] != labels["prefill"] || fl[2] != labels["neither"] {
		t.Errorf("three stalls: %v", fl)
	}
}

// The large requests follow one another a fraction of a second apart; they are no run back to back. A live hour
// whose only chain is its large requests fails (stand-in, before the fix: "longest run back to back: 113").
func TestLargeRequestsAreNoRunBackToBack(t *testing.T) {
	r := scaledTunnel()
	_, jobs := scaledHour(r)
	o := scaledLiveHour(r)
	var reqs []Stream
	for _, q := range o.Requests {
		if q.ID != "a1" { // the chain a0, a1, a2 broken
			reqs = append(reqs, q)
		}
	}
	at := o.Requests[0].Opened.Add(155 * time.Second)
	for i := 0; i < 5; i++ {
		from := at.Add(time.Duration(i) * 300 * time.Millisecond)
		reqs = append(reqs, Stream{ID: "x" + string(rune('0'+i)), Opened: from, Ended: from.Add(200 * time.Millisecond), BodyBytes: 196_400,
			ExpectedTokens: 32_737, ArrivedTokens: 32_737, SendStart: from, SendEnd: from.Add(200 * time.Millisecond)})
	}
	o.Requests = reqs
	if f := VerifyTunnelHour(r, o, jobs); !slices.Contains(f, "the hour held no run of requests back to back") {
		t.Errorf("a chain of large requests stood in for the run back to back: %v", f)
	}
	// a2, then the burst that opens as it ends: 2. The five large requests, 0.1 s apart, would make 5.
	if n := longestChain(reqs, r.Clock().MaxChainGap); n != 2 {
		t.Errorf("longestChain = %d, want 2", n)
	}
}

// A body cut short in the tunnel (the delta round of PR 255, B1). frp is a byte tunnel, so a body cut on the way is no
// longer JSON, and the engine answers it with an error, not a completed response with usage: the row fails. The
// engine is the engine double, vLLM's HTTP subset, served in the test without a listener; the large body is sized
// from its own usage counts, as the live hour sizes it. The whole body is served with exactly its count and passes;
// every cut of it, and a byte of its structure broken, fails with the line that names the response, not the count's.
func TestACutBodyIsAnsweredOtherwiseAndFailsTheRow(t *testing.T) {
	cfg := enginedouble.DefaultConfig()
	h := enginedouble.Handler(cfg)
	send := func(body string) []byte {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		return w.Body.Bytes()
	}
	count := func(words int) (int, error) {
		reply := send(ArrivalPrompt(cfg.ServedName, words, ArrivalMaxTokens))
		n, ok := PromptTokensOf(reply)
		if !ok {
			return 0, fmt.Errorf("a prompt of %d words was not completed with its usage.prompt_tokens: %.300s", words, reply)
		}
		return n, nil
	}
	body, expected, err := ArrivalSize(count, cfg.ServedName, 400_000, cfg.MaxModelLen)
	if err != nil {
		t.Fatal(err)
	}
	r := scaledTunnel()
	_, jobs := scaledHour(r)
	judge := func(reply []byte) []string {
		o := scaledLiveHour(r)
		for i, q := range o.Requests {
			if q.BodyBytes >= MinLargeBody {
				o.Requests[i] = WithArrival(Stream{ID: q.ID, Opened: q.Opened, Ended: q.Ended}, reply, len(body), expected, q.SendStart, q.SendBodyEnd, q.SendEnd)
			}
		}
		return VerifyTunnelHour(r, o, jobs)
	}
	const (
		otherwise  = "a request of 0.1 MB or more was not answered with a completed response counting its prompt (usage.prompt_tokens): a body cut short in the tunnel is no longer JSON"
		miscounted = "a request of 0.1 MB or more did not show it arrived whole: the engine's usage.prompt_tokens must be its prompt's own input tokens"
	)
	whole := send(body)
	if n, ok := PromptTokensOf(whole); !ok || n != expected {
		t.Fatalf("the whole body: the double counted %d (%v), want its own %d: %.300s", n, ok, expected, whole)
	}
	if f := judge(whole); len(f) > 0 {
		t.Fatalf("the whole body, served with its own count, failed the row: %v", f)
	}
	content := strings.Index(body, `"content":"`) + len(`"content":"`)
	maxTokens := strings.LastIndex(body, `:`) + 1 // before max_tokens' one digit
	broken := []byte(body)
	broken[content-1] = 'x' // the quote that opens the prompt
	for name, cut := range map[string]string{
		"cut in the prompt's words":             body[:content+1000],
		"cut at half the body":                  body[:len(body)/2],
		"cut before max_tokens' value":          body[:maxTokens],
		"cut one byte before the closing brace": body[:len(body)-1],
		"a byte of its structure broken":        string(broken),
	} {
		reply := send(cut)
		if n, ok := PromptTokensOf(reply); ok {
			t.Errorf("%s: the engine counted %d input tokens; want another reply", name, n)
		}
		if f := judge(reply); !slices.Contains(f, otherwise) || slices.Contains(f, miscounted) {
			t.Errorf("%s: the row's failures %v; want %q and not the count's", name, f, otherwise)
		}
	}
}

// The request near the deadline must be a completed stream (the delta round of PR 255, B3): a large request that
// came late passes the request checks, but is no stream of about 4 minutes. Here every completed stream is under the
// scaled 48 s, and the only request over it is such a large request, of 50 s.
func TestALateLargeRequestIsNoRequestNearTheDeadline(t *testing.T) {
	r := scaledTunnel()
	_, jobs := scaledHour(r)
	o := scaledLiveHour(r)
	o.Requests = append([]Stream(nil), o.Requests...)
	for i, q := range o.Requests {
		if q.ID == "a0" || q.ID == "a1" || q.ID == "a2" || q.ID == "b0" {
			o.Requests[i].Ended = q.Opened.Add(47 * time.Second) // the chain's pauses grow to 4 s, under its 10 s
		}
	}
	at := o.Requests[0].Opened.Add(100 * time.Second)
	late := Stream{ID: "x0", Opened: at, Ended: at.Add(50 * time.Second), BodyBytes: 196_400, ExpectedTokens: 32_737, ArrivedTokens: 32_737,
		SendStart: at, SendEnd: at.Add(50 * time.Second)}
	o.Requests = append(o.Requests, late)
	if f := VerifyTunnelHour(r, o, jobs); !slices.Equal(f, []string{longStreamFailure}) {
		t.Errorf("an hour whose only request near the deadline is a late large request: failures %v; want the near-deadline one alone", f)
	}
	// The control: the same hour with one completed stream of 50 s passes.
	for i, q := range o.Requests {
		if q.ID == "b0" {
			o.Requests[i].Ended = q.Opened.Add(50 * time.Second)
		}
	}
	if f := VerifyTunnelHour(r, o, jobs); len(f) > 0 {
		t.Errorf("the same hour with a completed stream of 50 s failed: %v", f)
	}
}
