package hubfiles

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// DefaultBaseURL is the Hub.
const DefaultBaseURL = "https://huggingface.co"

const (
	defaultMaxPages     = 1000
	defaultMaxFileBytes = 64 << 20 // S7: every non-LFS file of its checkpoints is far below this
	defaultConcurrency  = 4
	defaultTimeout      = 5 * time.Minute
	maxPageSize         = 1000 // the Hub's largest accepted limit
	userAgent           = "hpcburst-hubfiles/1"
)

// Config configures a Client. The zero value reads the public Hub
// anonymously with sensible limits.
type Config struct {
	// BaseURL is the Hub's origin, without a path. Default DefaultBaseURL. It
	// must be https; http is accepted only for a loopback host (tests).
	BaseURL string
	// TokenFile names a file that holds the Hub token, for a gated or private
	// repository. Empty means anonymous. The file must be a regular file with
	// access for the owner only, mode 0600 or 0400, and is read on every call.
	TokenFile string
	// HTTPClient is used for every request. Two copies of it are made, with
	// their own redirect checks, which run after its CheckRedirect (if any)
	// and cannot be undone by it: a listing request may be redirected only
	// within the Hub's origin, never to http or another host; a download may
	// go anywhere (its bytes are verified) but the token is dropped for every
	// origin but the Hub's. Default: a client with a five-minute timeout per
	// request.
	HTTPClient *http.Client
	// PageSize is sent as the listing's limit. Zero sends 1000, the Hub's
	// largest accepted value (spike S7 requires it: the Hub's own default
	// page cut a listing at 50 entries); a smaller value is for tests and for
	// a Hub that pages small. Allowed range 0 to 1000. The Link chain is
	// followed whatever the page size, because the Hub may still cut a page
	// below the limit.
	PageSize int
	// MaxPages bounds the page chain. Default 1000.
	MaxPages int
	// MaxFileBytes bounds the size of a file that is downloaded: one that is
	// hashed by Files (a file that is not in LFS) and any file that Content
	// or TemplateSources reads. Default 64 MiB.
	MaxFileBytes int64
	// Concurrency is the number of files downloaded at once. Default 4.
	Concurrency int
}

// Client reads per-file lists. It is safe for concurrent use.
type Client struct {
	base         *url.URL
	tokenFile    string
	listHTTP     *http.Client // listing requests: same origin only
	dlHTTP       *http.Client // downloads: any origin, token on the Hub's only
	pageSize     int
	maxPages     int
	maxPageBytes int64
	maxFileBytes int64
	concurrency  int
}

// New validates cfg and returns a Client. It makes no request and does not
// read the token file.
func New(cfg Config) (*Client, error) {
	raw := cfg.BaseURL
	if raw == "" {
		raw = DefaultBaseURL
	}
	base, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("hubfiles: base URL: %w", err)
	}
	if base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || strings.Trim(base.Path, "/") != "" {
		return nil, fmt.Errorf("hubfiles: base URL %q must be a scheme and host only", raw)
	}
	switch {
	case base.Scheme == "https":
	case base.Scheme == "http" && isLoopback(base.Hostname()):
	default:
		return nil, fmt.Errorf("hubfiles: base URL %q must be https (http is allowed only for a loopback host)", raw)
	}
	base.Path = ""
	if cfg.PageSize < 0 || cfg.PageSize > maxPageSize {
		return nil, fmt.Errorf("hubfiles: page size %d is outside 0..%d", cfg.PageSize, maxPageSize)
	}
	maxFile := cfg.MaxFileBytes
	if maxFile <= 0 {
		maxFile = defaultMaxFileBytes
	}
	return &Client{
		base:         base,
		tokenFile:    cfg.TokenFile,
		listHTTP:     withRedirectPolicy(cfg.HTTPClient, listingRedirectPolicy(base)),
		dlHTTP:       withRedirectPolicy(cfg.HTTPClient, downloadRedirectPolicy(base)),
		pageSize:     orDefault(cfg.PageSize, maxPageSize),
		maxPages:     orDefault(cfg.MaxPages, defaultMaxPages),
		maxPageBytes: maxPageBytes,
		maxFileBytes: maxFile,
		concurrency:  orDefault(cfg.Concurrency, defaultConcurrency),
	}, nil
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Entry is one file of the checkpoint revision.
type Entry struct {
	// File is the deployment ID's input: the path relative to the checkpoint
	// root and the SHA-256 of the content, in lowercase hex. For an LFS file
	// the SHA-256 is the listing's lfs.oid; otherwise this package computed
	// it from the downloaded content.
	deploymentid.File
	// BlobID is the git blob id (40 lowercase hex, SHA-1) that the listing
	// gives, for the later checks at staging and start. For a file that is not
	// in LFS it is the blob id of the content, and Files verified it. For an
	// LFS file it is the blob id of the small pointer file that git stores,
	// not of the content, and it is not checked against any content.
	BlobID string
	// Size is the content size in bytes (for an LFS file, the size of the real
	// file, not of the pointer).
	Size int64
	// LFS says whether the SHA-256 came from the Hub (true) or was computed
	// here (false).
	LFS bool
}

// Inputs returns the files in the form package deploymentid takes. The order
// of entries is kept; Client.Files returns them sorted bytewise by path.
func Inputs(entries []Entry) []deploymentid.File {
	out := make([]deploymentid.File, len(entries))
	for i, e := range entries {
		out[i] = e.File
	}
	return out
}

// Files returns the per-file list of repository at revision, sorted bytewise
// by path. revision must be 40 lowercase hex characters; anything else is
// refused before any request (ErrInvalidRevision). It fails as a whole: a
// partial list would give a different deployment ID, so no partial result is
// returned.
//
// Errors can be matched with errors.Is: ErrInvalidRevision,
// ErrInvalidRepository, ErrTokenFile, ErrUnauthorized, ErrForbidden,
// ErrNotFound, ErrBlobMismatch, ErrSizeMismatch, ErrFileTooLarge,
// ErrInvalidListing, ErrTooManyPages, ErrCrossHostLink, and the context
// errors, which are kept when a request or a body read is cut short (so
// errors.Is(err, context.DeadlineExceeded) holds when the Hub stalls
// mid-body). ErrInvalidListing is only for a listing that was read and found
// wanting. A response error is an *HTTPError, with the Retry-After of a 429
// or 503. Retryable tells a failure worth another try from a permanent one.
func (c *Client) Files(ctx context.Context, repository, revision string) ([]Entry, error) {
	if err := checkRepository(repository); err != nil {
		return nil, err
	}
	if err := checkRevision(revision); err != nil {
		return nil, err
	}
	tok, err := c.currentToken()
	if err != nil {
		return nil, err
	}
	entries, err := c.files(ctx, repository, revision, tok)
	if err != nil {
		return nil, scrub(err, tok)
	}
	return entries, nil
}

// currentToken reads the token file, if the client has one, afresh: a rotated
// token takes effect on the next call. Without a token file it returns the
// empty token, and requests are anonymous.
func (c *Client) currentToken() (token, error) {
	if c.tokenFile == "" {
		return "", nil
	}
	return readToken(c.tokenFile)
}

func (c *Client) files(ctx context.Context, repo, rev string, tok token) ([]Entry, error) {
	listed, err := c.listTree(ctx, repo, rev, tok)
	if err != nil {
		return nil, err
	}
	entries, err := toEntries(listed)
	if err != nil {
		return nil, err
	}
	if err := c.hashDownloads(ctx, repo, rev, entries, tok); err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(a.Path, b.Path) })
	return entries, nil
}

// hashDownloads fills in the SHA-256 of every entry that is not an LFS file,
// downloading up to c.concurrency files at once. The first failure cancels
// the others and is the one returned.
func (c *Client) hashDownloads(ctx context.Context, repo, rev string, entries []Entry, tok token) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
		sem      = make(chan struct{}, c.concurrency)
	)
	fail := func(err error) {
		once.Do(func() { firstErr = err; cancel() })
	}
	for i := range entries {
		if entries[i].LFS {
			continue
		}
		sem <- struct{}{}
		if ctx.Err() != nil {
			<-sem
			break
		}
		wg.Add(1)
		go func(e *Entry) {
			defer wg.Done()
			defer func() { <-sem }()
			sum, err := c.hashBlob(ctx, repo, rev, *e, tok)
			if err != nil {
				fail(err)
				return
			}
			e.SHA256 = sum
		}(&entries[i])
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}
