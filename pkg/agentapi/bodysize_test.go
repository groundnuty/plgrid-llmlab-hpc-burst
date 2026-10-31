package agentapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// worstTiming is the longest a RequestTiming's JSON can be: the longest Go
// duration string for both durations, the largest token count, an error of
// MaxTimingErrorBytes characters that Go's JSON encoder writes as six bytes
// each, and a ten-character shape.
func worstTiming() RequestTiming {
	return RequestTiming{
		Shape:            ShapeLongOutput,
		TimeToFirstToken: Duration(time.Duration(math.MinInt64)),
		Total:            Duration(time.Duration(math.MinInt64)),
		OutputTokens:     math.MaxInt32,
		Error:            strings.Repeat("<", 64),
	}
}

// worstWarmUp is the warm-up result at the admission bounds: five rounds of
// 4 + 1 + 1024 worst timings.
func worstWarmUp(rounds int) *WarmUpResult {
	w := &WarmUpResult{Done: true}
	for r := 1; r <= rounds; r++ {
		round := WarmUpRound{Round: int32(r)}
		for range 4 + 1 + 1024 {
			round.Requests = append(round.Requests, worstTiming())
		}
		w.Rounds = append(w.Rounds, round)
	}
	return w
}

// The bounds are the contract's numbers; a change is a decision, so the test
// names them.
func TestSizeBounds(t *testing.T) {
	if MaxTargetSessions != 1024 || MaxTimingErrorBytes != 64 || MaxMessageBytes != 256 ||
		WarmUpBudgetBytes != 3145728 || MaxBodyBytes != 4194304 || MaxWarmUpRounds != 5 {
		t.Errorf("bounds changed: %d %d %d %d %d %d", MaxTargetSessions, MaxTimingErrorBytes, MaxMessageBytes,
			WarmUpBudgetBytes, MaxBodyBytes, MaxWarmUpRounds)
	}
}

// One worst-case timing, counted by hand: 526 bytes.
//
//	{"shape":"longOutput"                              9 + 12  = 21
//	,"timeToFirstToken":"-2562047h47m16.854775808s"   20 + 27  = 47
//	,"total":"-2562047h47m16.854775808s"               9 + 27  = 36
//	,"outputTokens":2147483647                        16 + 10  = 26
//	,"error":"..."                                     9 + 386 = 395
//	}                                                           1
//
// The error is 64 characters '<', which Go's encoder writes as six bytes each
// (64 * 6 = 384, plus two quotes = 386). The duration string is 25 characters
// plus two quotes = 27; no Go duration is longer than the smallest int64.
func TestWorstTimingIs526Bytes(t *testing.T) {
	b, err := json.Marshal(worstTiming())
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 526 {
		t.Fatalf("the worst timing is %d bytes (%s), counted 526 by hand", len(b), b)
	}
	// The longest Go duration, written out: "-2562047h47m16.854775808s" is 25
	// characters, and no other value is longer.
	if !bytes.Contains(b, []byte(`"total":"-2562047h47m16.854775808s"`)) || !bytes.Contains(b, []byte(strings.Repeat(`\u003c`, 64))) {
		t.Errorf("the worst timing is not what the count assumes: %s", b)
	}
}

// The warm-up results of a report at the admission bounds fit WarmUpBudgetBytes
// (3 MiB), and the size is the hand count: per round 1029 timings of 526 bytes,
// 1028 commas and 25 bytes of {"round":R,"requests":[ ... ]}; five rounds, four
// commas, and {"rounds":[ ... ],"done":true} (11 + 14 bytes).
func TestWorstWarmUpFitsItsBudget(t *testing.T) {
	b, err := json.Marshal(worstWarmUp(5))
	if err != nil {
		t.Fatal(err)
	}
	want := 11 + 5*(1029*526+1028+25) + 4 + 14 // 2,711,564
	if want != 2711564 {
		t.Fatalf("the hand count is %d, not 2711564", want)
	}
	if len(b) != want {
		t.Errorf("the worst warm-up is %d bytes, counted %d by hand", len(b), want)
	}
	if len(b) > 3145728 {
		t.Errorf("the worst warm-up is %d bytes, above the 3 MiB budget", len(b))
	}
}

// restOfReport is a realistic report beyond the warm-up: one worker, a
// verification with 200 files, a qualification with 5 checks, and the longest
// messages the agent lets through.
func restOfReport(warm *WarmUpResult) Report {
	msg := strings.Repeat("m", 256)
	var files []FileResult
	for i := range 200 {
		files = append(files, FileResult{
			Path:    "model-" + strconv.Itoa(10000+i) + "-of-00200-with-a-long-checkpoint-name.safetensors",
			Outcome: FileMatch, SHA256: strings.Repeat("0123456789abcdef", 4),
		})
	}
	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	exit := 0
	return Report{
		Time: start,
		Instance: &InstanceReport{
			UID: "11111111-2222-3333-4444-555555555555", Generation: 2,
			Staging: &Staging{Phase: StagingDone, BytesDone: 1 << 40, BytesTotal: 1 << 40, Message: msg},
			Workers: []WorkerReport{{
				Name: "worker-0", Phase: PartRunning, PID: 4242, StartTime: &start, ExitCode: &exit, Generation: 1,
				EngineSocket: "/tmp/hb-run/worker-0.sock",
				Health: &Health{Time: start, ProbeOK: true, MetricsReadAt: &start, RunningRequests: Int64(12), WaitingRequests: Int64(3),
					GeneratedTokens: Int64(1 << 40), KVCacheUsage: Float64(0.5),
					Preemptions: Int64(1 << 40), PrefixCacheQueriedTokens: Int64(1 << 40), PrefixCacheHitTokens: Int64(1 << 40),
					TimePerOutputTokenSecondsSum: Float64(1.2345678901234567e+300), TimePerOutputTokenCount: Int64(1 << 40),
					ResponsesByStatus: map[string]int64{"200": 1200, "429": 3, "500": 2}},
			}},
			ServingEndpoint: "unix:///tmp/hb-run/serve.sock",
			Key:             strings.Repeat("0123456789abcdef", 4),
			Checks: &CheckResults{
				VerificationGeneration: 1,
				Verify: &VerifyResult{Done: true, Files: files, EngineVersion: "0.99.0", ImageDigest: "sha256:" + strings.Repeat("ab", 32),
					Settings: map[string]string{"max-model-len": "32768", "max-num-seqs": "256"}, Fingerprint: "fp_0123456789", TemplateOverhead: 12, Error: msg},
				Qualify: &QualifyResult{Done: true, Checks: []CheckOutcome{
					{Name: CheckHealth, Passed: true, Reason: ReasonReasoningBudgetExhausted, Message: msg},
					{Name: CheckChat, Passed: true, Reason: ReasonReasoningBudgetExhausted, Message: msg},
					{Name: CheckToolCall, Passed: true, Reason: ReasonReasoningBudgetExhausted, Message: msg},
					{Name: CheckCorruption, Passed: true, Reason: ReasonReasoningBudgetExhausted, Message: msg},
					{Name: CheckBehaviour, Passed: true, Reason: ReasonReasoningBudgetExhausted, Message: msg, ContextTokens: 1 << 40},
				}, KVCapacityTokens: 1 << 30, ServedModelNames: []string{"example-org/model-a"}, Fingerprint: "fp_0123456789"},
				WarmUp: warm,
			},
		},
	}
}

// bodyHandler is the server's side of PathReport as #60 will write it: decode
// the body with DecodeBody, answer 400 on its error and 204 otherwise. It
// checks, on every decoded body, that it was whole.
func bodyHandler(t *testing.T, got chan<- Report, size chan<- int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rep Report
		err := DecodeBody(r.Body, &rep)
		// Read the rest of an oversized body, so the client sees the answer and
		// not a reset connection (DecodeBody's doc).
		io.Copy(io.Discard, r.Body)
		if err != nil {
			var e *Error
			if !errors.As(err, &e) {
				t.Errorf("DecodeBody returned %T, not an *Error", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", ContentType)
			w.WriteHeader(e.HTTPStatus())
			json.NewEncoder(w).Encode(e)
			return
		}
		got <- rep
		w.WriteHeader(http.StatusNoContent)
	})
}

func post(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+PathReport, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", ContentType)
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// A report with the worst-case warm-up and a realistic rest goes through an
// HTTP server's DecodeBody; a report above MaxBodyBytes is answered with 400.
func TestReportSizeThroughHTTP(t *testing.T) {
	got := make(chan Report, 2)
	srv := httptest.NewServer(bodyHandler(t, got, nil))
	defer srv.Close()

	// Accepted: the whole report arrives, the last timing's error intact.
	report := restOfReport(worstWarmUp(5))
	body, err := EncodeBody(report)
	if err != nil {
		t.Fatalf("EncodeBody refused the worst-case report: %v", err)
	}
	// The rest of the report is realistic, not tiny: it takes some tens of KiB,
	// and the whole is far above the typical but below the limit.
	rest, err := json.Marshal(restOfReport(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) < 30_000 || len(rest) > 1<<20 {
		t.Errorf("the rest of the report is %d bytes; want at least 30 KB and at most 1 MiB", len(rest))
	}
	if len(body) < 2_700_000 || len(body) > 4194304 {
		t.Errorf("the worst-case report is %d bytes; want between 2.7 MB and the 4 MiB limit", len(body))
	}
	resp := post(t, srv.URL, body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("the worst-case report got status %d, want 204", resp.StatusCode)
	}
	rep := <-got
	w := rep.Instance.Checks.WarmUp
	if len(w.Rounds) != 5 || len(w.Rounds[4].Requests) != 1029 || w.Rounds[4].Requests[1028].Error != strings.Repeat("<", 64) {
		t.Errorf("the decoded report lost part of the warm-up: %d rounds", len(w.Rounds))
	}

	// Refused: ten times the rounds is far above the limit.
	huge, err := json.Marshal(restOfReport(worstWarmUp(50))) // not EncodeBody, which refuses it
	if err != nil {
		t.Fatal(err)
	}
	if len(huge) < 20_000_000 {
		t.Fatalf("the oversized report is only %d bytes", len(huge))
	}
	resp = post(t, srv.URL, huge)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("a %d-byte report got status %d, want 400", len(huge), resp.StatusCode)
	}
	var e Error
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || e.Code != CodeBadRequest {
		t.Errorf("the answer was %+v, %v; want a BadRequest error body", e, err)
	}
	select {
	case <-got:
		t.Error("the handler decoded the oversized report")
	default:
	}
}

func TestEncodeBodyRefusesAboveTheLimit(t *testing.T) {
	huge := restOfReport(worstWarmUp(50))
	b, err := EncodeBody(huge)
	var e *Error
	if err == nil || !errors.As(err, &e) || e.Code != CodeBadRequest || b != nil {
		t.Errorf("EncodeBody of a 27 MB report = %d bytes, %v; want an *Error of BadRequest", len(b), err)
	}
	// An ordinary value is the JSON of json.Marshal.
	small, err := EncodeBody(RegisterRequest{Node: "n", CPUArch: "x86_64"})
	if err != nil || string(small) != `{"node":"n","cpuArch":"x86_64"}` {
		t.Errorf("EncodeBody = %s, %v", small, err)
	}
	// A value JSON cannot hold is an error, not a BadRequest of the caller's.
	if _, err := EncodeBody(math.NaN()); err == nil {
		t.Error("NaN was encoded")
	} else if errors.As(err, &e) {
		t.Errorf("an unencodable value is an *Error: %v", err)
	}
}

// The limit is on the byte: a body of MaxBodyBytes is read, one more is not.
func TestDecodeBodyLimitIsExact(t *testing.T) {
	jsonString := func(total int) []byte { // a JSON string of exactly total bytes
		return []byte(`"` + strings.Repeat("a", total-2) + `"`)
	}
	var s string
	if err := DecodeBody(bytes.NewReader(jsonString(4194304)), &s); err != nil || len(s) != 4194302 {
		t.Errorf("a body of exactly 4 MiB: %d characters, %v", len(s), err)
	}
	err := DecodeBody(bytes.NewReader(jsonString(4194305)), &s)
	var e *Error
	if err == nil || !errors.As(err, &e) || e.Code != CodeBadRequest {
		t.Errorf("a body of 4 MiB + 1: %v; want an *Error of BadRequest", err)
	}
	// The source is not read to its end: an endless reader returns.
	var end endless
	if err := DecodeBody(&end, &s); err == nil || end.n > 4194304+1 {
		t.Errorf("an endless body: %v, %d bytes read; want a refusal after at most 4 MiB + 1", err, end.n)
	}
}

func TestDecodeBodyRefusesWhatIsNotJSON(t *testing.T) {
	var r Report
	for name, body := range map[string]string{
		"empty": "", "not JSON": "hello", "a wrong type": `{"time": 3}`, "trailing data": `{} {}`,
	} {
		err := DecodeBody(strings.NewReader(body), &r)
		var e *Error
		if err == nil || !errors.As(err, &e) || e.Code != CodeBadRequest {
			t.Errorf("%s: %v; want an *Error of BadRequest", name, err)
		}
	}
	// Unknown fields are allowed, as everywhere in the protocol.
	if err := DecodeBody(strings.NewReader(`{"time":"2026-10-31T12:00:00Z","futureField":{"a":1}}`), &r); err != nil {
		t.Errorf("an unknown field was refused: %v", err)
	}
}

func TestTruncateUTF8(t *testing.T) {
	// 1, 2, 3 and 4-byte characters: a, é, €, 😀.
	for _, s := range []string{"", "abc", "éééé", "€€€€", "😀😀😀", "a€é😀b😀é€a", strings.Repeat("a€😀é", 20)} {
		for n := -1; n <= len(s)+1; n++ {
			got := TruncateUTF8(s, n)
			if !utf8.ValidString(got) {
				t.Errorf("TruncateUTF8(%q, %d) = %q is not valid UTF-8", s, n, got)
			}
			if !strings.HasPrefix(s, got) {
				t.Errorf("TruncateUTF8(%q, %d) = %q is not a prefix", s, n, got)
			}
			limit := max(n, 0)
			if len(got) > limit {
				t.Errorf("TruncateUTF8(%q, %d) = %q is %d bytes", s, n, got, len(got))
			}
			// Maximal: the next character would not fit (or the string is whole).
			if len(got) < len(s) {
				_, size := utf8.DecodeRuneInString(s[len(got):])
				if len(got)+size <= limit {
					t.Errorf("TruncateUTF8(%q, %d) = %q stops early: the next character fits", s, n, got)
				}
			}
		}
	}
	// Examples worked by hand: "€" is 3 bytes.
	for _, c := range []struct {
		s    string
		n    int
		want string
	}{
		{"a€b", 0, ""}, {"a€b", 1, "a"}, {"a€b", 2, "a"}, {"a€b", 3, "a"}, {"a€b", 4, "a€"}, {"a€b", 5, "a€b"}, {"a€b", 99, "a€b"},
		{"😀", 3, ""}, {"😀", 4, "😀"},
	} {
		if got := TruncateUTF8(c.s, c.n); got != c.want {
			t.Errorf("TruncateUTF8(%q, %d) = %q, want %q", c.s, c.n, got, c.want)
		}
	}
}
