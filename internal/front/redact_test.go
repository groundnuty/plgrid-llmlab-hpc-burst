package front

import (
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// PR #95 review (Astra B12) and the science agent's ruling (31 Oct 2026):
// before an upstream error passes through, every target credential the front
// holds, the managed keys and every Instance's key, is replaced with
// [redacted], in the body, in any SSE error event and in the headers, and
// Content-Length is framed again. All other bytes pass unchanged.

// rdKeys are the two Instances' keys of a two-Instance scFix.
func rdKeys(t *testing.T, f *scFix) (own, other string) {
	t.Helper()
	read := func(i int) string {
		b, err := os.ReadFile(agentapi.InstanceKeyPath(f.host.CredentialsDir(), string(f.uids[i])))
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(b))
	}
	return read(0), read(1)
}

// TestRedactErrorBodyAndHeaders: a 401 whose body holds the target's own key
// twice and the other Instance's key once, and a header holding the key.
func TestRedactErrorBodyAndHeaders(t *testing.T) {
	f := newScFix(t, scOpts{instances: 2})
	own, other := rdKeys(t, f)
	body := `{"error":{"message":"bad key ` + own + `; again ` + own + `; not ` + other + `","type":"auth","code":401}}`
	want := strings.NewReplacer(own, "[redacted]", other, "[redacted]").Replace(body)
	for i := range f.uids {
		f.retarget(f.uids[i], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Echo", "Bearer "+own)
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, body)
		}))
	}
	resp := f.errPost(scChat, errChatBody(scModel, false, "hi"))
	got := readAll(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d, want the engine's 401", resp.StatusCode)
	}
	if got != want {
		t.Errorf("body\n got %s\nwant %s", got, want)
	}
	if strings.Contains(got, own) || strings.Contains(got, other) {
		t.Errorf("a key reached the client: %s", got)
	}
	if v := resp.Header.Get("X-Echo"); v != "Bearer [redacted]" {
		t.Errorf("header X-Echo %q, want the key redacted", v)
	}
	if cl := resp.Header.Get("Content-Length"); cl != strconv.Itoa(len(want)) {
		t.Errorf("Content-Length %q, want %d, the redacted body's length", cl, len(want))
	}
}

// TestRedactSSEErrorEvent: the engine's mid-stream error event holds the key;
// once in an event written whole, and once split across two writes with a
// flush between, so the front reads it in two chunks. The content event
// before it passes unchanged.
func TestRedactSSEErrorEvent(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "whole", true: "split across chunks"}[split], func(t *testing.T) {
			f := newScFix(t, scOpts{instances: 2})
			own, other := rdKeys(t, f)
			ev := `data: {"error":{"message":"refused Bearer ` + own + ` and ` + other + `","type":"InternalServerError","code":500}}` + "\n\n"
			cut := strings.Index(ev, own) + len(own)/2
			for i := range f.uids {
				f.retarget(f.uids[i], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(200)
					rc := http.NewResponseController(w)
					_, _ = io.WriteString(w, chatToken)
					_ = rc.Flush()
					if split {
						_, _ = io.WriteString(w, ev[:cut])
						_ = rc.Flush()
						_, _ = io.WriteString(w, ev[cut:])
					} else {
						_, _ = io.WriteString(w, ev)
					}
					_, _ = io.WriteString(w, vllmDone)
					_ = rc.Flush()
				}))
			}
			_, got := f.prSend(t, scChat, prChatStream)
			want := chatToken + strings.NewReplacer(own, "[redacted]", other, "[redacted]").Replace(ev)
			if got != want {
				t.Errorf("stream\n got %q\nwant %q", got, want)
			}
		})
	}
}
