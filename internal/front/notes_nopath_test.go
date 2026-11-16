package front

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
)

// No note the log keeps names a path, a user, an account, a grant or a job ID, and none copies another
// component's error text (HBEP-8, Allocation events, No job ID, path or name in a note; the science
// agent's ruling on #640). These tests inject synthetic values into what each emit site copied before
// (the sink's error, the probe's dial or reply), run the real emit path into a MemoryRecorder, and
// check the note as TOKENS: a run of letters, digits, '_', '-', '/' and '.' is one token, and a value
// leaks only as a whole token or as a whole component of one, never as a substring of a longer word.
// Every expected note is a literal written here, never built by the code it checks.

// The synthetic values. None is a real name, path, grant or job.
const (
	synUser    = "example-user-x1"
	synAccount = "example-account-a1"
	synGrant   = "example-grant-g1"
	synJob     = "1234567"
	synHome    = "/home/" + synUser
)

// synAll are the values a note must not hold.
var synAll = []string{synUser, synAccount, synGrant, synJob}

// noteTokens splits s into its tokens: the maximal runs of letters, digits, '_', '-', '/' and '.'
// ('~' too, so that a home path is one token), each without trailing dots (a sentence's end).
func noteTokens(s string) []string {
	var out []string
	start := -1
	flush := func(end int) {
		if start < 0 {
			return
		}
		if tok := strings.TrimRight(s[start:end], "."); tok != "" {
			out = append(out, tok)
		}
		start = -1
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isTok := c == '_' || c == '-' || c == '/' || c == '.' || c == '~' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		if isTok && start < 0 {
			start = i
		}
		if !isTok {
			flush(i)
		}
	}
	flush(len(s))
	return out
}

// pathLooking reports whether a token looks like a path: it starts with '/', '~/', './' or '../', or it
// has two '/' or more. A Kubernetes name, namespace/name, is not one.
func pathLooking(tok string) bool {
	return strings.HasPrefix(tok, "/") || strings.HasPrefix(tok, "~/") || strings.HasPrefix(tok, "./") ||
		strings.HasPrefix(tok, "../") || strings.Count(tok, "/") >= 2
}

// noteLeaks returns what a note holds that it must not: each value that is a whole token or a whole
// component of one (a token split on '/' and '.'), and each path-looking token.
func noteLeaks(note string, values ...string) []string {
	var out []string
	for _, tok := range noteTokens(note) {
		if pathLooking(tok) {
			out = append(out, "path "+tok)
		}
		for _, comp := range strings.FieldsFunc(tok, func(r rune) bool { return r == '/' || r == '.' }) {
			for _, v := range values {
				if comp == v {
					out = append(out, v)
				}
			}
		}
	}
	return out
}

// valuesIn returns the values a text holds as whole tokens or components of one: how a test shows its
// injected text reached the source.
func valuesIn(text string, values ...string) []string {
	var out []string
	for _, v := range values {
		if len(noteLeaks(text, v)) > 0 {
			out = append(out, v)
		}
	}
	return out
}

// TestNoteTokenMatching pins the matcher the tests below rely on, in both directions: a real
// occurrence is found, and a value inside a longer word is not.
func TestNoteTokenMatching(t *testing.T) {
	for _, tc := range []struct {
		name, note string
		want       []string
	}{
		{"a user as a path component", "open /home/example-user-x1/usage.jsonl: denied", []string{"path /home/example-user-x1/usage.jsonl", synUser}},
		{"a bare user", "of example-user-x1, a user", []string{synUser}},
		{"a user ending a sentence", "it was example-user-x1.", []string{synUser}},
		{"a job as a whole token", "job 1234567 ended", []string{synJob}},
		{"a job as a file's stem", "log 1234567.out", []string{synJob}},
		{"a job inside a longer number", "at 11234567 and 12345670", nil},
		{"a job inside a hex word", "request ab1234567cd", nil},
		{"a job with a suffix is another word", "array 1234567_3", nil},
		{"a user inside a longer name", "example-user-x10 and zexample-user-x1", nil},
		{"a Kubernetes name is not a path", "Instance ns/i-71 and Allocation team-a/al-a", nil},
		{"a relative path with two slashes", "see a/b/c now", []string{"path a/b/c"}},
		{"a home path", "under ~/creds now", []string{"path ~/creds"}},
		{"an account and a grant", "example-account-a1, example-grant-g1", []string{synAccount, synGrant}},
	} {
		got := noteLeaks(tc.note, synAll...)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("%s: noteLeaks(%q) = %q, want %q", tc.name, tc.note, got, tc.want)
		}
	}
}

// leakyError is an error whose text names every synthetic value and wraps err, as a failed write of
// the usage log names its file's path.
func leakyError(err error) error {
	return fmt.Errorf("write %s/usage/usage-2026-11-16.jsonl as %s, account %s, grant %s, job %s: %w",
		synHome, synUser, synAccount, synGrant, synJob, err)
}

// TestUsageWriteFailedNoteHoldsNoTextOfTheSinksError: the sink's error names the usage file's path (a
// write that fails is an *os.PathError), and the event it raises goes to a durable log. The note says
// the request and a fixed cause by the error's kind, never its text; the same error is still shown
// by the front's log line and by /healthz (TestHealthAndEventOnAFailingUsageSink).
func TestUsageWriteFailedNoteHoldsNoTextOfTheSinksError(t *testing.T) {
	const tail = "; the error is in the front's log and in the usage member of its health report"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"a full disk", leakyError(&os.PathError{Op: "write", Path: synHome + "/usage/usage-2026-11-16.jsonl", Err: syscall.ENOSPC}),
			"usage records are not being written (request req-ab12): the disk is full" + tail},
		{"a refused write", leakyError(&os.PathError{Op: "open", Path: synHome + "/usage/usage-2026-11-16.jsonl", Err: syscall.EACCES}),
			"usage records are not being written (request req-ab12): the usage directory refused the write" + tail},
		{"another failure", leakyError(errors.New("sync failed")),
			"usage records are not being written (request req-ab12): a write or sync of the usage log failed" + tail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The injected text is in the error the site copied, as tokens, before the emit.
			if got := valuesIn(tc.err.Error(), synAll...); len(got) != len(synAll) || len(noteLeaks(tc.err.Error())) == 0 {
				t.Fatalf("control: the error %q holds %q of %q and a path; the test injects nothing otherwise", tc.err, got, synAll)
			}
			f := newScFix(t, scOpts{})
			f.router.lease.set(true, "lease-uid")
			f.router.usageFailed("req-ab12", tc.err)
			ev := f.rec.Find(eventFailing, reasonUsageWriteFailed)
			if len(ev) != 1 || ev[0].Object != leaseDesc || !ev[0].Warning {
				t.Fatalf("events %+v, want one warning regarding %s", ev, leaseDesc)
			}
			if ev[0].Message != tc.want {
				t.Errorf("note\n got %q\nwant %q", ev[0].Message, tc.want)
			}
			if leaks := noteLeaks(ev[0].Message, synAll...); len(leaks) > 0 {
				t.Errorf("the note %q holds %q", ev[0].Message, leaks)
			}
		})
	}
}

// phraseFrps is a stand-in for frps's tcpmux port, whose replies carry a reason phrase that names every
// synthetic value, as an frps or an Instance's engine could put any text in a status line. In mode
// "ok" it answers CONNECT with 200 and GET /health with 200; "health" answers GET /health with 503
// and the phrase; "connect" answers CONNECT with 502 and the phrase; "hold" reads the CONNECT and
// answers nothing until the test ends.
type phraseFrps struct {
	ln     net.Listener
	phrase string
	mu     sync.Mutex
	mode   string
	done   chan struct{}
}

func startPhraseFrps(t *testing.T) *phraseFrps {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := &phraseFrps{ln: ln, mode: "ok", done: make(chan struct{}),
		phrase: synHome + "/health as " + synUser + " of account " + synAccount + " under grant " + synGrant + " job " + synJob}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go m.serve(c)
		}
	}()
	t.Cleanup(func() { close(m.done); ln.Close() })
	return m
}

func (m *phraseFrps) set(mode string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mode = mode
}

func (m *phraseFrps) serve(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(c)
	if req, err := http.ReadRequest(br); err != nil || req.Method != http.MethodConnect {
		return
	}
	m.mu.Lock()
	mode := m.mode
	m.mu.Unlock()
	switch mode {
	case "hold":
		<-m.done
		return
	case "connect":
		fmt.Fprintf(c, "HTTP/1.1 502 %s\r\nContent-Length: 0\r\n\r\n", m.phrase)
		return
	}
	_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	if _, err := http.ReadRequest(br); err != nil {
		return
	}
	if mode == "health" {
		fmt.Fprintf(c, "HTTP/1.1 503 %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", m.phrase)
		return
	}
	_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
}

// tapPathEvents hands each result to the front's own PathEvents and keeps the failing ones.
type tapPathEvents struct {
	PathEvents
	mu      sync.Mutex
	failing []ProbeResult
}

func (e *tapPathEvents) PathProbeFailing(in *v1alpha1.Instance, r ProbeResult) {
	e.mu.Lock()
	e.failing = append(e.failing, r)
	e.mu.Unlock()
	e.PathEvents.PathProbeFailing(in, r)
}

func (e *tapPathEvents) results() []ProbeResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]ProbeResult(nil), e.failing...)
}

// TestPathProbeFailingNoteHoldsNoTextOfTheProbesError: the probe's error copies what frps or the
// Instance's /health answered, a reason phrase here that names every synthetic value, and it copied a
// dial's text. The note gives the status when /health answered, else a fixed description of the
// cause's kind. The failing text stays in the front's log line, which the test reads.
func TestPathProbeFailingNoteHoldsNoTextOfTheProbesError(t *testing.T) {
	const head = "the front's probe of Instance ns/i-71 through its tunnel failed 2 times in a row while its TunnelConnected is True; " +
		"it is not eligible until a probe passes; the controller restarts nothing on this (last: "
	for _, tc := range []struct {
		name, mode, cause, last string
		status                  int
		injected                bool
	}{
		{"a health answer with a phrase", "health", probeCauseOther, "its health check answered status 503)", 503, true},
		{"frps's refusal of the CONNECT with a phrase", "connect", probeCauseOther, "the probe failed for another reason, which the front's log names)", 0, true},
		{"frps that does not answer", "hold", probeCauseTimeout, "no answer within the probe's deadline)", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := startPhraseFrps(t)
			uid := types.UID("00000000-0000-4000-8000-000000000071")
			in := probeInstance(uid, v1alpha1.InstanceReady, metav1.ConditionTrue)
			insts := []v1alpha1.Instance{in}
			cfg := Config{}.WithDefaults()
			cfg.TunnelSetupTimeout = Duration(300 * time.Millisecond)
			rec := &MemoryRecorder{}
			tap := &tapPathEvents{PathEvents: recorderPathEvents{rec: rec}}
			logs := &syncBuffer{}
			tun := NewTunnel(m.ln.Addr().String(), cfg.TunnelSetupTimeout.D())
			p := NewProber(cfg, tun, clock.Real{}, func() []v1alpha1.Instance { return insts }, tap,
				slog.New(slog.NewTextHandler(logs, nil)))
			// A passing probe registers the Instance; then it fails twice (probeFailures is 2).
			p.Round(t.Context())
			if !tun.Registered(uid) {
				t.Fatal("control: the first probe did not register the Instance")
			}
			m.set(tc.mode)
			p.Round(t.Context())
			p.Round(t.Context())
			res := tap.results()
			if len(res) != 1 || res[0].Cause != tc.cause || res[0].Status != tc.status {
				t.Fatalf("the probe reported %+v, want one result with cause %q and status %d", res, tc.cause, tc.status)
			}
			if tc.injected {
				// The injected text reached the source: the probe's error and the front's log line hold it.
				if got := valuesIn(res[0].Error, synAll...); len(got) != len(synAll) {
					t.Fatalf("control: the probe's error %q holds only %q of %q", res[0].Error, got, synAll)
				}
				if got := valuesIn(logs.String(), synAll...); len(got) != len(synAll) {
					t.Fatalf("control: the front's log holds only %q of %q", got, synAll)
				}
			}
			ev := rec.Find(eventDegraded, reasonPathProbeFailing)
			if len(ev) != 1 || !ev[0].Warning || ev[0].Object != "*v1alpha1.Instance ns/i-71" {
				t.Fatalf("events %+v, want one warning regarding the Instance", ev)
			}
			if want := head + tc.last; ev[0].Message != want {
				t.Errorf("note\n got %q\nwant %q", ev[0].Message, want)
			}
			if leaks := noteLeaks(ev[0].Message, synAll...); len(leaks) > 0 {
				t.Errorf("the note %q holds %q", ev[0].Message, leaks)
			}
		})
	}
}

// TestProbeCauseIsChosenByTheErrorsKind: the classes of a failed probe's error, from the errors a dial
// and a read give, wrapped as the tunnel wraps them (%w), with text that names nothing of the cause.
func TestProbeCauseIsChosenByTheErrorsKind(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"a refused connection", fmt.Errorf("CONNECT u: dial frps: %w", refused), probeCauseRefused},
		{"a deadline of the connection", fmt.Errorf("CONNECT u: read frps's reply: %w", os.ErrDeadlineExceeded), probeCauseTimeout},
		{"a deadline of the context", fmt.Errorf("CONNECT u: %w", context.DeadlineExceeded), probeCauseTimeout},
		{"a net.Error that timed out, joined and wrapped", fmt.Errorf("x: %w", errors.Join(errors.New("a"), fmt.Errorf("b: %w", timeoutErr{}))), probeCauseTimeout},
		{"frps's 404", fmt.Errorf("CONNECT u: %w", errNoProxy), probeCauseNoProxy},
		{"anything else", errors.New("probe: read the /health reply: EOF"), probeCauseOther},
	} {
		if got := probeCauseOf(tc.err); got != tc.want {
			t.Errorf("%s: probeCauseOf(%q) = %q, want %q", tc.name, tc.err, got, tc.want)
		}
	}
	if got := probeCauseOf(nil); got != "" {
		t.Errorf("probeCauseOf(nil) = %q, want none", got)
	}
}

// timeoutErr is a net.Error that says it timed out.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timed out" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return false }
