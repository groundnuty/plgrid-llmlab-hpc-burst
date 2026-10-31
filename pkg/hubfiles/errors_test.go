package hubfiles

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// stallingListing answers 200 and "[" and then sends nothing until the client
// goes away: a Hub that stalls in the middle of a page.
func stallingListing(t *testing.T) *treeServer {
	return newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("["))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(30 * time.Second):
		}
	})
}

func TestFilesKeepsTheContextErrorWhenTheListingBodyStalls(t *testing.T) {
	srv := stallingListing(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	entries, err := srv.client(t, Config{}).Files(ctx, testRepo, testRev)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrInvalidListing) {
		t.Errorf("a stalled body was classified as an invalid listing: %v", err)
	}
	if entries != nil {
		t.Errorf("a failed call returned a list: %+v", entries)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("took %v: the context did not end the read", time.Since(start))
	}
	if !Retryable(err) {
		t.Errorf("a deadline that ended a stalled body should be Retryable: %v", err)
	}

	// Cancellation (not a deadline) in the middle of the body.
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel2() }()
	_, err = srv.client(t, Config{}).Files(ctx2, testRepo, testRev)
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidListing) {
		t.Errorf("err = %v, want context.Canceled and not ErrInvalidListing", err)
	}
	if Retryable(err) {
		t.Errorf("a call the caller cancelled is not Retryable: %v", err)
	}
}

func TestFilesKeepsTheContextErrorWhenADownloadBodyStalls(t *testing.T) {
	const stallPath = "config.json"
	started := make(chan struct{}, 8)
	h := newFakeHub(t, allFiles())
	inner := h.srv.Config.Handler
	h.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/resolve/"+testRev+"/"+stallPath) {
			// The right Content-Length, half of the bytes, then silence.
			w.Header().Set("Content-Length", strconv.Itoa(len(fixtures[1].content)))
			_, _ = w.Write([]byte(fixtures[1].content[:5]))
			w.(http.Flusher).Flush()
			started <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-time.After(30 * time.Second):
			}
			return
		}
		inner.ServeHTTP(w, r)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-started; time.Sleep(100 * time.Millisecond); cancel() }()

	entries, err := h.client(t, Config{}).Files(ctx, testRepo, testRev)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", err)
	}
	for _, bad := range []error{ErrSizeMismatch, ErrBlobMismatch, ErrInvalidListing} {
		if errors.Is(err, bad) {
			t.Errorf("a stalled download body was classified as %v: %v", bad, err)
		}
	}
	if entries != nil {
		t.Errorf("a failed call returned a list: %+v", entries)
	}
}

func TestFilesClassifiesATruncatedListingBodyAsANetworkFailure(t *testing.T) {
	srv := newTreeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "5000")
		_, _ = w.Write([]byte(`[{"type"`))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // closes the connection before the promised length
	})
	_, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if err == nil || errors.Is(err, ErrInvalidListing) {
		t.Fatalf("err = %v, want a transport error and not ErrInvalidListing", err)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want it to wrap io.ErrUnexpectedEOF", err)
	}
	if !Retryable(err) {
		t.Errorf("a body cut short by the connection should be Retryable: %v", err)
	}
}

func TestHTTPErrorCarriesRetryAfterOn429And503(t *testing.T) {
	for _, tc := range []struct {
		status int
		header string
		want   time.Duration
	}{
		{http.StatusTooManyRequests, "7", 7 * time.Second},
		{http.StatusServiceUnavailable, "120", 2 * time.Minute},
		{http.StatusTooManyRequests, "0", 0},
		{http.StatusTooManyRequests, "", 0},
		{http.StatusTooManyRequests, "soon", 0},
		{http.StatusTooManyRequests, "-5", 0},
		{http.StatusTooManyRequests, "999999999999999999999999", 24 * time.Hour},
		{http.StatusInternalServerError, "30", 0}, // only 429 and 503 carry it
		{http.StatusBadGateway, "30", 0},
		{http.StatusNotFound, "30", 0},
	} {
		for _, where := range []string{"listing", "download"} {
			var srv *treeServer
			if where == "listing" {
				srv = newTreeServer(t, func(w http.ResponseWriter, _ *http.Request) {
					if tc.header != "" {
						w.Header().Set("Retry-After", tc.header)
					}
					w.WriteHeader(tc.status)
				})
			} else {
				srv = newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
					if strings.Contains(r.URL.Path, "/resolve/") {
						if tc.header != "" {
							w.Header().Set("Retry-After", tc.header)
						}
						w.WriteHeader(tc.status)
						return
					}
					_, _ = w.Write([]byte(listOf(good("lfs", ``, "path", `"path":"config.json"`, "oid", `"oid":"`+fixtures[1].blob+`"`, "size", `"size":`+strconv.Itoa(len(fixtures[1].content))))))
				})
			}
			_, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
			var he *HTTPError
			if !errors.As(err, &he) || he.Status != tc.status {
				t.Errorf("%s %d: err = %v, want an HTTPError with that status", where, tc.status, err)
				continue
			}
			if he.RetryAfter != tc.want {
				t.Errorf("%s %d with Retry-After %q: RetryAfter = %v, want %v", where, tc.status, tc.header, he.RetryAfter, tc.want)
			}
		}
	}
}

func TestParseRetryAfterReadsAnHTTPDate(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	for header, want := range map[string]time.Duration{
		now.Add(90 * time.Second).Format(http.TimeFormat): 90 * time.Second,
		now.Add(-time.Hour).Format(http.TimeFormat):       0, // in the past
		now.Add(72 * time.Hour).Format(http.TimeFormat):   24 * time.Hour,
		"Sat, 31 Oct 2026 12:00:30 GMT":                   30 * time.Second,
		"31 Oct 2026":                                     0,
	} {
		if got := parseRetryAfter(header, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", header, got, want)
		}
	}
}

func TestRetryable(t *testing.T) {
	timeout := &net.OpError{Op: "read", Err: &timeoutErr{}}
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":                   {nil, false},
		"429":                   {&HTTPError{Status: 429}, true},
		"500":                   {&HTTPError{Status: 500}, true},
		"502":                   {&HTTPError{Status: 502}, true},
		"503":                   {&HTTPError{Status: 503}, true},
		"401":                   {&HTTPError{Status: 401}, false},
		"403":                   {&HTTPError{Status: 403}, false},
		"404":                   {&HTTPError{Status: 404}, false},
		"400":                   {&HTTPError{Status: 400}, false},
		"302 left unfollowed":   {&HTTPError{Status: 302}, false},
		"wrapped 503":           {fmt.Errorf("x: %w", &HTTPError{Status: 503}), true},
		"deadline":              {fmt.Errorf("x: %w", context.DeadlineExceeded), true},
		"canceled":              {fmt.Errorf("x: %w", context.Canceled), false},
		"connection reset":      {&url.Error{Op: "Get", URL: "https://h", Err: &net.OpError{Op: "read", Err: syscall.ECONNRESET}}, true},
		"connection refused":    {&url.Error{Op: "Get", URL: "https://h", Err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}}, true},
		"dns":                   {&url.Error{Op: "Get", URL: "https://h", Err: &net.DNSError{Err: "no such host", Name: "h"}}, true},
		"network timeout":       {&url.Error{Op: "Get", URL: "https://h", Err: timeout}, true},
		"body cut short":        {fmt.Errorf("reading: %w", io.ErrUnexpectedEOF), true},
		"eof":                   {fmt.Errorf("reading: %w", io.EOF), true},
		"unexplained":           {errors.New("something else"), false},
		"redirect refused":      {&url.Error{Op: "Get", URL: "https://h", Err: fmt.Errorf("%w: x", ErrCrossHostLink)}, false},
		"blob mismatch":         {fmt.Errorf("x: %w", ErrBlobMismatch), false},
		"size mismatch":         {fmt.Errorf("x: %w", ErrSizeMismatch), false},
		"file too large":        {fmt.Errorf("x: %w", ErrFileTooLarge), false},
		"invalid listing":       {fmt.Errorf("x: %w", ErrInvalidListing), false},
		"invalid revision":      {fmt.Errorf("x: %w", ErrInvalidRevision), false},
		"invalid repository":    {fmt.Errorf("x: %w", ErrInvalidRepository), false},
		"token file":            {fmt.Errorf("x: %w", ErrTokenFile), false},
		"too many pages":        {fmt.Errorf("x: %w", ErrTooManyPages), false},
		"cross host link":       {fmt.Errorf("x: %w", ErrCrossHostLink), false},
		"invalid listing, EOF":  {fmt.Errorf("%w: %v", ErrInvalidListing, io.ErrUnexpectedEOF), false},
		"permanent beats a 5xx": {fmt.Errorf("%w / %w", ErrBlobMismatch, &HTTPError{Status: 500}), false},
	} {
		if got := Retryable(tc.err); got != tc.want {
			t.Errorf("%s: Retryable(%v) = %v, want %v", name, tc.err, got, tc.want)
		}
	}
}

type timeoutErr struct{}

func (*timeoutErr) Error() string   { return "i/o timeout" }
func (*timeoutErr) Timeout() bool   { return true }
func (*timeoutErr) Temporary() bool { return true }

// TestScrubKeepsTheClassificationWithoutTheText: when the token is blanked out
// of an error, the error still answers errors.Is and Retryable as before.
func TestScrubKeepsTheClassificationWithoutTheText(t *testing.T) {
	tok := token(testToken)
	for name, tc := range map[string]struct {
		cause     error
		is        []error
		isNot     []error
		retryable bool
		status    int
		retry     time.Duration
	}{
		"429 with Retry-After": {&HTTPError{Method: "GET", Path: "/p", Status: 429, RetryAfter: 3 * time.Second}, nil, []error{ErrUnauthorized, ErrNotFound}, true, 429, 3 * time.Second},
		"401":                  {&HTTPError{Method: "GET", Path: "/p", Status: 401, TokenSent: true}, []error{ErrUnauthorized}, []error{ErrForbidden}, false, 401, 0},
		"404":                  {&HTTPError{Method: "GET", Path: "/p", Status: 404}, []error{ErrNotFound}, nil, false, 404, 0},
		"deadline":             {context.DeadlineExceeded, []error{context.DeadlineExceeded}, []error{context.Canceled}, true, 0, 0},
		"canceled":             {context.Canceled, []error{context.Canceled}, nil, false, 0, 0},
		"blob mismatch":        {ErrBlobMismatch, []error{ErrBlobMismatch}, nil, false, 0, 0},
	} {
		err := scrub(fmt.Errorf("lower layer said: Authorization: Bearer %s was refused: %w", testToken, tc.cause), tok)
		leakCheck(t, name, err, testToken)
		if !strings.Contains(err.Error(), "[redacted]") {
			t.Errorf("%s: scrub did not run: %v", name, err)
		}
		for _, want := range tc.is {
			if !errors.Is(err, want) {
				t.Errorf("%s: errors.Is(scrubbed, %v) = false", name, want)
			}
		}
		for _, not := range tc.isNot {
			if errors.Is(err, not) {
				t.Errorf("%s: errors.Is(scrubbed, %v) = true", name, not)
			}
		}
		if got := Retryable(err); got != tc.retryable {
			t.Errorf("%s: Retryable(scrubbed) = %v, want %v", name, got, tc.retryable)
		}
		var he *HTTPError
		switch gotHTTP := errors.As(err, &he); {
		case tc.status == 0 && gotHTTP:
			t.Errorf("%s: errors.As found an HTTPError in an error that had none", name)
		case tc.status != 0 && (!gotHTTP || he.Status != tc.status || he.RetryAfter != tc.retry):
			t.Errorf("%s: errors.As(scrubbed, *HTTPError) = %v, %+v; want status %d, RetryAfter %v", name, gotHTTP, he, tc.status, tc.retry)
		}
	}
}
