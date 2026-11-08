package alpha

// The live steps' observations (issue #245), built from what the stack and the
// cluster recorded: the API's Instances, ModelDeployment and events, the
// front's usage records and response headers, and frps's proxy list. They are
// pure functions, so that hack/mutate-alpha.sh's plain go test of this package
// kills a change in any of them; the code that drives the stack only collects
// their inputs. Nothing here reads a log line.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
)

// FrpsTimeLayout is how the pinned frps (v0.71.1) writes a proxy's
// lastStartTime: month, day and time in frps's local zone, with no year
// (internal/frpsplugin's reader test holds an entry of that form).
const FrpsTimeLayout = "01-02 15:04:05"

// ParseFrpsTime reads a lastStartTime that was listed at at. frps runs on this
// host, in its zone. The year is at's, or the year before when that would put
// the time after at: a session started on 31 December and looked at on
// 1 January.
func ParseFrpsTime(s string, at time.Time) (time.Time, error) {
	t, err := time.ParseInLocation(FrpsTimeLayout, s, at.Location())
	if err != nil {
		return time.Time{}, fmt.Errorf("frps's lastStartTime is not of the form %s", FrpsTimeLayout)
	}
	y := time.Date(at.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, at.Location())
	if y.After(at) {
		y = y.AddDate(-1, 0, 0)
	}
	return y, nil
}

// TunnelSampleOf is one look at the Instance's tunnel session at at: frps's
// lastStartTime for its proxy, which frps names by the Instance's UID
// (internal/frpsplugin, BuildReport), and whether the Instance's
// TunnelConnected condition was True. A proxy that frps does not list online,
// or whose lastStartTime does not parse, leaves LastStart zero, which the
// verdict fails.
func TunnelSampleOf(at time.Time, proxies []frpsplugin.Proxy, in *v1alpha1.Instance) TunnelSample {
	s := TunnelSample{At: at, Connected: meta.IsStatusConditionTrue(in.Status.Conditions, v1alpha1.ConditionTunnelConnected)}
	for _, p := range proxies {
		if p.Name == string(in.UID) && p.Status == frpsplugin.StatusOnline {
			if t, err := ParseFrpsTime(p.LastStartTime, at); err == nil {
				s.LastStart = t
			}
		}
	}
	return s
}

// DrainWindow is an Instance's drain window from the deployment's and the
// profile's specs alone (HBEP-7, Handover): routing.maxRequestDuration plus the
// profile's drainMargin, or the profile's drainWindow when that is shorter. It
// is worked out here, not with the helper the controller and the front share
// (v1alpha1.InstanceDrainWindow), so that a fault there shows as a stream not in
// flight at the drain start rather than agreeing with itself.
func DrainWindow(maxRequest, drainMargin, drainWindow time.Duration) time.Duration {
	w := maxRequest + drainMargin
	if drainWindow > 0 && drainWindow < w {
		return drainWindow
	}
	return w
}

// DrainStartOf is the Instance's drain start from its recorded lease end:
// status.leaseEnd less the drain window. ok is false while it has no lease end.
func DrainStartOf(in *v1alpha1.Instance, window time.Duration) (t time.Time, ok bool) {
	if in.Status.LeaseEnd == nil {
		return time.Time{}, false
	}
	return in.Status.LeaseEnd.Add(-window), true
}

// SpanOf is one Instance's life as the API recorded it: its readyTime and
// endTime, under its Allocation's job tag.
func SpanOf(in *v1alpha1.Instance, tag string) InstanceSpan {
	s := InstanceSpan{Tag: tag}
	if in.Status.ReadyTime != nil {
		s.ReadyAt = in.Status.ReadyTime.Time
	}
	if in.Status.EndTime != nil {
		s.EndedAt = in.Status.EndTime.Time
	}
	return s
}

// ClientEnd is what the client read at the end of a reply: whether the body
// carried the end the endpoint's completion rule names (a finish reason, and
// [DONE] for a stream), and the code of an error the front put in it (for
// example stream_cut).
type ClientEnd struct {
	Done, Finished bool
	ErrorCode      string
}

// StreamOf is one request as the run saw it: its times and its end from the
// front's usage record (HBEP-5, Usage records), and the end the client read.
// It is complete only when both ends agree: the record has a finish and no
// cutBy, its status is 200, and the client read the finish and [DONE]. A close
// that only looks complete, a body cut short with no error, is not.
func StreamOf(rec usage.Record, end ClientEnd) Stream {
	s := Stream{ID: rec.RequestID, Error: end.ErrorCode}
	s.Opened, _ = time.Parse(time.RFC3339Nano, rec.StartTime)
	s.Ended, _ = time.Parse(time.RFC3339Nano, rec.EndTime)
	if rec.CutBy != nil {
		s.CutBy = *rec.CutBy
	}
	if rec.CompletionTokens != nil {
		s.Tokens = int(*rec.CompletionTokens)
	}
	s.Complete = rec.Status == http.StatusOK && rec.Finish != nil && rec.CutBy == nil && end.Done && end.Finished && end.ErrorCode == ""
	return s
}

// ArrivalPrompt is a chat request whose prompt is words words ("hello", each
// a token or more), asking for maxTokens tokens (ArrivalMaxTokens in the live
// hour). Under the context, the engine serves it whole, and its response's
// usage.prompt_tokens counts every token of the prompt it read
// (PromptTokensOf). The science agent's ruling (4) on PR 255: that count,
// matched against the prompt's own (TokenLine), shows the body arrived whole.
// The parent's reading of vLLM v0.32.0, after the delta round: a refusal is no
// count. Under the context vLLM clamps max_tokens and serves the request
// (api_utils.py, get_max_tokens), and over it the refusal says "at least N
// input tokens", the tokenizer having stopped past the limit (renderers/
// params.py, _token_len_check). usage.prompt_tokens is the OpenAI API's own
// count, and the front passes the response through.
//
// What it shows, and what it does not (the delta round of PR 255, B1). frp is
// a byte tunnel: it carries the body's bytes and never reads them. A body cut
// short on the way is no longer JSON (its closing brace comes last), nor is
// one with a byte of its structure broken, so the engine answers it with an
// error (a decode error), not a completed response, and the row fails on any
// large body answered otherwise. Equal counts are the second check: the
// engine tokenized the whole well-formed body. A path that rewrites a body
// into other well-formed JSON (a letter for a letter, or a word dropped from
// every message) is out of scope; the calibration prompts take the same path,
// so a rewrite that drops a word from each moves the line with it.
func ArrivalPrompt(model string, words, maxTokens int) string {
	m, _ := json.Marshal(model)
	return `{"model":` + string(m) + `,"messages":[{"role":"user","content":"hello` + strings.Repeat(" hello", max(words, 1)-1) +
		`"}],"max_tokens":` + strconv.Itoa(maxTokens) + `}`
}

// ArrivalMaxTokens is what each calibration prompt and large body asks for: one
// token, so that the engine reads the whole prompt (a prefill of all of it) and
// answers at once, with its count in usage.prompt_tokens.
const ArrivalMaxTokens = 1

// WordsForBytes is the fewest words whose ArrivalPrompt is at least n bytes.
func WordsForBytes(model string, n, maxTokens int) int {
	fixed := len(ArrivalPrompt(model, 1, maxTokens)) - len("hello")
	// "hello", then words-1 times " hello": 6 words - 1 bytes.
	return max((n-fixed+1+len(" hello")-1)/len(" hello"), 1)
}

// TokenLine is a prompt's input tokens as its engine counts them, as a
// function of its words: Per tokens a word, and Fixed more, the chat
// template's. Calibrate finds it from the counts of prompts of known words.
type TokenLine struct{ Per, Fixed int }

// Tokens is the input tokens of a prompt of words words.
func (l TokenLine) Tokens(words int) int { return l.Per*words + l.Fixed }

// MaxWords is the most words whose prompt has fewer tokens than maxModelLen,
// leaving room for the one token asked (ArrivalMaxTokens): a prompt the engine
// serves whole, so that usage.prompt_tokens counts every token of it. Over the
// context vLLM v0.32.0 refuses it, stating at best a lower bound.
func (l TokenLine) MaxWords(maxModelLen int) int { return (maxModelLen - 1 - l.Fixed) / l.Per }

// CalibrationWords are the prompts, in words, whose counts give the
// engine's TokenLine: two fix the line, and the third must lie on it. A
// fourth, of half the large prompt's words (NearScaleWords), is sent once the
// line has sized it, and must lie on the line too.
var CalibrationWords = []int{1000, 2000, 4000}

// NearScaleWords is the calibration prompt near the large prompt's scale: at
// least half its words (the delta round of PR 255, B1). A count that is a line
// up to 4,000 words and bends further on (a token more every 5,000 words, say)
// is off the line there, and the row fails closed.
func NearScaleWords(words int) int { return (words + 1) / 2 }

// Calibrate is the TokenLine through prompts of known words (each point is
// {words, input tokens stated}, words rising). The first two fix it: whole
// tokens a word, and a template count not negative. Every further point must
// lie on it exactly: an engine whose count bends (a merge every so many
// words) gives no line, and no count of a large prompt can be expected from
// it. Fewer than three points cannot show a line at all.
func Calibrate(pts ...[2]int) (TokenLine, error) {
	if len(pts) < 3 {
		return TokenLine{}, fmt.Errorf("%d calibration prompts: a line needs a third on it", len(pts))
	}
	w1, n1, w2, n2 := pts[0][0], pts[0][1], pts[1][0], pts[1][1]
	if w2 <= w1 || n2 <= n1 || (n2-n1)%(w2-w1) != 0 {
		return TokenLine{}, fmt.Errorf("prompts of %d and %d words were counted %d and %d input tokens: not a whole number of tokens a word", w1, w2, n1, n2)
	}
	per := (n2 - n1) / (w2 - w1)
	l := TokenLine{Per: per, Fixed: n1 - per*w1}
	if l.Fixed < 0 {
		return TokenLine{}, fmt.Errorf("prompts of %d and %d words were counted %d and %d input tokens: fewer than their words", w1, w2, n1, n2)
	}
	prev := w2
	for _, p := range pts[2:] {
		if p[0] <= prev {
			return TokenLine{}, fmt.Errorf("calibration prompts of %d then %d words: the words must rise", prev, p[0])
		}
		if p[1] != l.Tokens(p[0]) {
			return TokenLine{}, fmt.Errorf("a prompt of %d words was counted %d input tokens, off the line of %d tokens a word and %d more (%d): the engine's count is not linear in the words", p[0], p[1], l.Per, l.Fixed, l.Tokens(p[0]))
		}
		prev = p[0]
	}
	return l, nil
}

// ArrivalSize is the hour's large request and its prompt's own input tokens
// (the science agent's ruling (4) on PR 255). count sends a prompt of the
// given words (ArrivalPrompt, ArrivalMaxTokens) and returns the input tokens
// its completed response counted (PromptTokensOf);
// the CalibrationWords prompts give the engine's TokenLine (Calibrate). The
// large prompt holds as many words as bytes bytes take, but fewer tokens than
// maxModelLen (MaxWords). A prompt of half its words (NearScaleWords) is then
// counted too, and must lie on the line. A response with no count, a count
// off the line, or a context that cannot hold a large body
// (MinLargeBody) under it is an error: the row fails closed.
func ArrivalSize(count func(words int) (int, error), model string, bytes, maxModelLen int) (body string, expected int, err error) {
	var pts [][2]int
	for _, w := range CalibrationWords {
		n, err := count(w)
		if err != nil {
			return "", 0, err
		}
		pts = append(pts, [2]int{w, n})
	}
	line, err := Calibrate(pts...)
	if err != nil {
		return "", 0, err
	}
	words := min(WordsForBytes(model, bytes, ArrivalMaxTokens), line.MaxWords(maxModelLen))
	body = ArrivalPrompt(model, words, ArrivalMaxTokens)
	if len(body) < MinLargeBody {
		return "", 0, fmt.Errorf("the deployment's max-model-len %d holds a prompt of %d bytes at most (%d tokens a word, %d more), under the %d bytes of a large body", maxModelLen, len(body), line.Per, line.Fixed, MinLargeBody)
	}
	near := NearScaleWords(words)
	n, err := count(near)
	if err != nil {
		return "", 0, err
	}
	if _, err := Calibrate(append(pts, [2]int{near, n})...); err != nil {
		return "", 0, fmt.Errorf("the calibration prompt of %d words, half the large prompt's %d: %w", near, words, err)
	}
	return body, line.Tokens(words), nil
}

// PromptTokensOf reads the prompt's input tokens from the engine's completed
// response to an ArrivalPrompt: usage.prompt_tokens of a chat.completion whose
// every choice finished ("stop", or "length" at its one token), and whose
// usage adds up (total_tokens is prompt_tokens and completion_tokens). false
// for anything else: an error (a body cut short in the tunnel is no longer
// JSON, and is answered with a decode error), a refusal of any wording, a
// response that did not finish, or usage that is missing, counts no prompt
// token, or does not add up.
func PromptTokensOf(reply []byte) (int, bool) {
	var r struct {
		Object  string `json:"object"`
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     *int `json:"prompt_tokens"`
			CompletionTokens *int `json:"completion_tokens"`
			TotalTokens      *int `json:"total_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(reply, &r) != nil || r.Object != "chat.completion" || len(r.Choices) == 0 || r.Usage == nil {
		return 0, false
	}
	for _, c := range r.Choices {
		if c.FinishReason == nil || (*c.FinishReason != "stop" && *c.FinishReason != "length") {
			return 0, false
		}
	}
	u := r.Usage
	if u.PromptTokens == nil || u.CompletionTokens == nil || u.TotalTokens == nil {
		return 0, false
	}
	if *u.PromptTokens <= 0 || *u.TotalTokens != *u.PromptTokens+*u.CompletionTokens {
		return 0, false
	}
	return *u.PromptTokens, true
}

// WithArrival is a large request s as the live hour observes it: its body's
// bytes, its prompt's own input tokens (expected), the client's window from
// its first byte sent (start) through its last byte written to the connection
// (sent) to its reply read (end), and the input tokens the engine
// counted (PromptTokensOf), set only from a completed response with usage. A
// body cut short on the way gets another reply, and so no arrived count
// (PromptCounted).
func WithArrival(s Stream, reply []byte, bytes, expected int, start, sent, end time.Time) Stream {
	s.BodyBytes, s.ExpectedTokens, s.SendStart, s.SendBodyEnd, s.SendEnd = bytes, expected, start, sent, end
	s.ArrivedTokens, _ = PromptTokensOf(reply)
	return s
}

// LargeWindow is one large request on the client's clock: Start its first
// byte sent, Sent its last byte written to the connection (stack's
// SendTimed), End its response read. Its send window is Start to Sent, the
// body on its way; its prefill window Sent to End, the engine reading and
// prefilling the whole prompt before its one token (the science agent's
// addition to the switch to usage.prompt_tokens: a stall FLAG names the
// window, so that a reader can tell a tunnel stall from engine work). A Sent
// not recorded, or not inside the window, leaves it all a send window.
type LargeWindow struct{ Start, Sent, End time.Time }

// split is where the window's send ends and its prefill begins.
func (w LargeWindow) split() time.Time {
	if w.Sent.IsZero() || w.Sent.Before(w.Start) || w.Sent.After(w.End) {
		return w.End
	}
	return w.Sent
}

// overlaps reports whether the gap from..to overlaps the window a..b.
func overlaps(from, to, a, b time.Time) bool { return from.Before(b) && to.After(a) }

// HeadOfLineOf is the head-of-line measure (HeadOfLine) of the hour's
// streams: for every two tokens of one stream in a row, the gap between them
// is in a send window when it overlaps any large request's send window, in a
// prefill window when it overlaps a prefill window and no send window (two
// senders interleave, so a gap often overlaps both: the send, the tunnel's
// part, comes first), and elsewhere when it overlaps neither.
func HeadOfLineOf(reqs []Stream, windows []LargeWindow) HeadOfLine {
	var h HeadOfLine
	for _, r := range reqs {
		for i := 1; i < len(r.Ticks); i++ {
			from, to := r.Ticks[i-1], r.Ticks[i]
			gap := to.Sub(from)
			send, prefill := false, false
			for _, w := range windows {
				s := w.split()
				send = send || overlaps(from, to, w.Start, s)
				prefill = prefill || overlaps(from, to, s, w.End)
			}
			switch {
			case send:
				h.Gaps++
				h.Send = max(h.Send, gap)
			case prefill:
				h.Gaps++
				h.Prefill = max(h.Prefill, gap)
			default:
				h.Elsewhere = max(h.Elsewhere, gap)
			}
		}
	}
	h.InFlight = max(h.Send, h.Prefill)
	return h
}

// UploadNotMeasuredLive is why a live row's upload interval and rate are not
// measured in alpha (the science agent's ruling (4) on PR 255; front-side
// sender timing is post-alpha, #273).
const UploadNotMeasuredLive = "no sender-side timing on the live path"

// HeadOfLineStall is the longest gap between two tokens of a stream while a
// large body is in flight that is not flagged as a stall: a second, twenty
// times the 50 ms a token takes at the 20 tokens a second every stream of the
// load must hold (StreamRate). An engine's scheduling hiccup is tens of
// milliseconds; a gap of a second while a body is in flight is the tunnel
// holding the token direction behind the body, the head-of-line blocking
// issue #224 asks to see. A confound: each large body is also a prefill of its
// whole prompt on the same engine (about 16,000 to 32,000 tokens live), which
// can lengthen the streams' gaps by itself; so each FLAG names its window
// (HeadOfLineOf): a body's send, its prefill, or neither.
const HeadOfLineStall = time.Second

// WorkerRestarts counts the restarts at the worker rung that the run's
// Instances recorded (status.restarts whose part is a worker, HBEP-7, Health
// and restarts). Each Instance is given in the last version the run saw.
func WorkerRestarts(ins []v1alpha1.Instance) int {
	n := 0
	for _, in := range ins {
		for _, r := range in.Status.Restarts {
			if strings.HasPrefix(r.Part, "worker-") {
				n++
			}
		}
	}
	return n
}

// deploymentIDRE is the display form of a deployment ID (HBEP-5, Attribution headers): 12 hex characters.
var deploymentIDRE = regexp.MustCompile(`^[0-9a-f]{12}$`)

// Attributed reports whether a response carries HBEP-5's attribution headers
// (hbep/0005-llmlab-compatible-front/README.md, Attribution headers): the
// request ID and the emitter always; and for a reply an Instance emitted, the
// Provider, the deployment ID in its display form, and the Instance, which must
// be one of the run's (uids).
func Attributed(h http.Header, uids map[string]bool) bool {
	if h.Get("Hpcburst-Request-Id") == "" {
		return false
	}
	switch h.Get("Hpcburst-Emitter") {
	case "front":
		return true
	case "instance":
		return h.Get("Hpcburst-Provider") != "" && deploymentIDRE.MatchString(h.Get("Hpcburst-Deployment-Id")) &&
			h.Get("Hpcburst-Instance") != "" && uids[h.Get("Hpcburst-Instance-Uid")]
	case "provider":
		return h.Get("Hpcburst-Provider") != "" && h.Get("Hpcburst-Deployment-Id") != ""
	}
	return false
}

// ErrSelectorDeployment is a deployment that picks its Providers by label: the
// run cannot say which Providers it allowed, so it measures nothing.
var ErrSelectorDeployment = errors.New("the deployment selects its Providers by label, so the run cannot name the Providers it allowed")

// DeploymentSpecOf is what the API held for the run's ModelDeployment: the
// Providers it allows by name, each with the platform it is on (clusterOf, by
// Provider name; a Provider it cannot place has no cluster, which the verdict
// fails), and its timing.
func DeploymentSpecOf(md *v1alpha1.ModelDeployment, clusterOf map[string]string) (DeploymentSpec, error) {
	var d DeploymentSpec
	if md.Spec.Providers.Selector != nil {
		return d, ErrSelectorDeployment
	}
	for _, n := range md.Spec.Providers.Names {
		d.Providers = append(d.Providers, ProviderRef{Name: n, Cluster: clusterOf[n]})
	}
	var err error
	if d.LeaseLimit, err = durationOf(md.Spec.LeaseLimit); err != nil {
		return d, fmt.Errorf("leaseLimit: %w", err)
	}
	if md.Spec.Routing != nil {
		if d.MaxRequestDuration, err = durationOf(md.Spec.Routing.MaxRequestDuration); err != nil {
			return d, fmt.Errorf("routing.maxRequestDuration: %w", err)
		}
	}
	if rp := md.Spec.RetentionPolicy; rp != nil && rp.Mode == v1alpha1.RetentionReleaseWhenIdle {
		d.ReleaseWhenIdle = true
		if d.IdleLimit, err = durationOf(rp.IdleLimit); err != nil {
			return d, fmt.Errorf("retentionPolicy.idleLimit: %w", err)
		}
	}
	return d, nil
}

// durationOf is an API duration; zero when it is not set.
func durationOf(d *v1alpha1.Duration) (time.Duration, error) {
	if d == nil {
		return 0, nil
	}
	return d.AsDuration()
}

// Handovers counts the handovers that completed end to end: a predecessor the
// controller handed over (an event HandedOver regarding it) that then ended
// Drained. ended holds each predecessor in the last version the run saw.
func Handovers(evs []eventsv1.Event, ended []v1alpha1.Instance) int {
	handed := map[string]bool{}
	for _, e := range evs {
		if e.Reason == "HandedOver" {
			handed[string(e.Regarding.UID)] = true
		}
	}
	n := 0
	for _, in := range ended {
		if handed[string(in.UID)] && in.Status.Phase == v1alpha1.InstanceEnded && in.Status.EndReason == v1alpha1.ReasonDrained {
			n++
		}
	}
	return n
}

// SlotSeen reports whether a version of the deployment carried a condition
// with reason NoOverlapSlot (HBEP-7, No-overlap mode).
func SlotSeen(versions []*v1alpha1.ModelDeployment) bool {
	for _, md := range versions {
		for _, c := range md.Status.Conditions {
			if c.Reason == v1alpha1.ReasonNoOverlapSlot {
				return true
			}
		}
	}
	return false
}
