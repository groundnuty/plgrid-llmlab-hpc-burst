package hubfiles

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
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
	return &HTTPError{
		Method:    resp.Request.Method,
		Path:      resp.Request.URL.Path,
		Status:    resp.StatusCode,
		Code:      code,
		TokenSent: tokenSent,
	}
}

// scrubbedError is an error whose text had the token replaced. It does not
// unwrap: the original error carries the token in its text, and a logger that
// walks the chain would print it.
type scrubbedError struct{ msg string }

func (e *scrubbedError) Error() string { return e.msg }

// scrub is the last line of defence for "the token is in no error message":
// no error is built from a response body, a request header or the token, but
// if a lower layer's text ever held the token, the error is replaced by one
// with the token blanked out (and loses its chain, for the reason above).
func scrub(err error, tok token) error {
	if err == nil || tok == "" {
		return err
	}
	msg := err.Error()
	if !strings.Contains(msg, tok.reveal()) {
		return err
	}
	return &scrubbedError{msg: strings.ReplaceAll(msg, tok.reveal(), redacted)}
}
