package hubfiles

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	// ErrUnauthorized is matched (errors.Is) by an HTTPError with status 401:
	// a gated or private repository, and no token or a rejected one.
	ErrUnauthorized = errors.New("hubfiles: unauthorized")
	// ErrForbidden is matched by status 403: the token is valid but its
	// account may not read the repository, typically a gated repository whose
	// terms the account has not accepted.
	ErrForbidden = errors.New("hubfiles: forbidden")
	// ErrNotFound is matched by status 404: no such repository or revision, or
	// one the caller may not know about.
	ErrNotFound = errors.New("hubfiles: not found")
	// ErrBlobMismatch is returned when a downloaded file's git blob id differs
	// from the listing's oid.
	ErrBlobMismatch = errors.New("hubfiles: file does not match its git blob id")
	// ErrSizeMismatch is returned when a downloaded file's length differs from
	// the size in the listing.
	ErrSizeMismatch = errors.New("hubfiles: file size differs from the listing")
	// ErrFileTooLarge is returned for a file that is not an LFS file but is
	// larger than Config.MaxFileBytes. Such a file would be downloaded to be
	// hashed; a large one is a sign the listing is not what we expect.
	ErrFileTooLarge = errors.New("hubfiles: file too large to download for hashing")
	// ErrTooManyPages is returned when the page chain is longer than
	// Config.MaxPages or loops back to a page already read.
	ErrTooManyPages = errors.New("hubfiles: page chain too long or circular")
	// ErrCrossHostLink is returned when a Link header points to another scheme
	// or host than the configured Hub. It is not followed, so the token never
	// leaves the Hub's host.
	ErrCrossHostLink = errors.New("hubfiles: next-page link points to another host")
)

// errorCodePattern bounds the Hub's X-Error-Code, the only text of a response
// that an error may carry. A response body or X-Error-Message could repeat
// what we sent, so neither is ever copied into an error.
var errorCodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// HTTPError is a response with an unexpected status. It holds no body, no
// header of the request and no query: only what the caller needs to decide.
type HTTPError struct {
	Method string
	// Path is the request path, without query. It names a public repository
	// and file, never a credential.
	Path   string
	Status int
	// Code is the Hub's X-Error-Code (for example "GatedRepo"), when it is a
	// short token of letters, digits, '_' and '-'.
	Code string
	// TokenSent says whether the request carried a token.
	TokenSent bool
	// RetryAfter is the delay the Hub asked for in Retry-After, on a 429 or a
	// 503 (delta-seconds or an HTTP date); zero when it asked for none. It is
	// capped at 24 hours.
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "hubfiles: %s %s: the Hub answered %d %s", e.Method, e.Path, e.Status, http.StatusText(e.Status))
	if e.Code != "" {
		fmt.Fprintf(&b, " (%s)", e.Code)
	}
	switch {
	case e.Status == http.StatusUnauthorized && !e.TokenSent:
		b.WriteString("; no token is configured: a gated or private repository needs a Hub token file")
	case e.Status == http.StatusUnauthorized:
		b.WriteString("; the Hub did not accept the configured token")
	case e.Status == http.StatusForbidden:
		b.WriteString("; the token's account may not read this repository (a gated repository needs its terms accepted)")
	}
	return b.String()
}

// Is lets errors.Is match the status sentinels.
func (e *HTTPError) Is(target error) bool {
	switch target {
	case ErrUnauthorized:
		return e.Status == http.StatusUnauthorized
	case ErrForbidden:
		return e.Status == http.StatusForbidden
	case ErrNotFound:
		return e.Status == http.StatusNotFound
	}
	return false
}

// statusError builds an HTTPError for resp.
func statusError(resp *http.Response, tokenSent bool) *HTTPError {
	code := resp.Header.Get("X-Error-Code")
	if !errorCodePattern.MatchString(code) {
		code = ""
	}
	he := &HTTPError{
		Method:    resp.Request.Method,
		Path:      resp.Request.URL.Path,
		Status:    resp.StatusCode,
		Code:      code,
		TokenSent: tokenSent,
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		he.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	}
	return he
}

const maxRetryAfter = 24 * time.Hour

// parseRetryAfter reads a Retry-After value (RFC 9110: whole seconds, or an
// HTTP date) relative to now. A value that does not parse, or a date in the
// past, gives zero; a very long wait is capped.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if strings.Trim(v, "0123456789") == "" {
		secs, err := strconv.ParseUint(v, 10, 63)
		if err != nil || secs > uint64(maxRetryAfter/time.Second) { // too large to count is a long wait
			return maxRetryAfter
		}
		return time.Duration(secs) * time.Second
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0
	}
	return min(max(t.Sub(now), 0), maxRetryAfter)
}

// permanent are the failures that another try cannot change.
var permanent = []error{
	ErrInvalidRevision, ErrInvalidRepository, ErrTokenFile,
	ErrBlobMismatch, ErrSizeMismatch, ErrFileTooLarge,
	ErrInvalidListing, ErrTooManyPages, ErrCrossHostLink,
}

// Retryable reports whether err from Client.Files is worth another attempt
// later: a 429 or any 5xx answer (see HTTPError.RetryAfter), a network error
// (a reset or refused connection, a body cut short, a DNS failure) and a
// timeout, including a deadline that ended the call. It is false for a
// permanent failure: a 4xx other than 429, a file that does not match its
// blob id or size, an unusable listing, revision, repository or token file,
// a too-long or circular page chain, a link out of the snapshot, and a call
// the caller cancelled.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	var s *scrubbedError
	if errors.As(err, &s) {
		return s.retryable
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	for _, p := range permanent {
		if errors.Is(err, p) {
			return false
		}
	}
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == http.StatusTooManyRequests || he.Status >= 500
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return isNetworkError(err)
}

func isNetworkError(err error) bool {
	var (
		op  *net.OpError
		dns *net.DNSError
		ne  net.Error
	)
	switch {
	case errors.As(err, &op), errors.As(err, &dns):
		return true
	case errors.As(err, &ne) && ne.Timeout():
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// scrubbedError is an error whose text had the token replaced. It does not
// unwrap, because the original's text carries the token and a logger that
// walks the chain would print it. It keeps the original's classification
// without its text: Is answers for the original (so errors.Is still finds
// ErrUnauthorized, context.Canceled and the others), As hands out an
// *HTTPError, which holds no secret, and retryable was computed from the
// original.
type scrubbedError struct {
	msg       string
	orig      error
	retryable bool
}

func (e *scrubbedError) Error() string { return e.msg }

// GoString keeps %#v from printing the struct, whose orig field holds the
// text that was scrubbed.
func (e *scrubbedError) GoString() string { return e.msg }

func (e *scrubbedError) Is(target error) bool { return errors.Is(e.orig, target) }

func (e *scrubbedError) As(target any) bool {
	if t, ok := target.(**HTTPError); ok {
		var he *HTTPError
		if errors.As(e.orig, &he) {
			*t = he
			return true
		}
	}
	return false
}

// scrub is the last line of defence for "the token is in no error message":
// no error is built from a response body, a request header or the token, but
// if a lower layer's text ever held the token, the error is replaced by one
// with the token blanked out. It keeps the classification (see
// scrubbedError) and loses the chain, for the reason given there.
func scrub(err error, tok token) error {
	if err == nil || tok == "" {
		return err
	}
	msg := err.Error()
	if !strings.Contains(msg, tok.reveal()) {
		return err
	}
	return &scrubbedError{msg: strings.ReplaceAll(msg, tok.reveal(), redacted), orig: err, retryable: Retryable(err)}
}
