package hubfiles

import (
	"context"
	"errors"
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
	// no access for group or others (mode 600), and is read on every call.
	TokenFile string
	// HTTPClient is used for every request (a copy of it: the Client adds a
	// redirect check that keeps the token on the Hub's host). Default: a
	// client with a five-minute timeout per request.
	HTTPClient *http.Client
	// PageSize is sent as the listing's limit. Zero sends none and takes the
	// Hub's default; a value is for tests and for a Hub that pages small.
	// Allowed range 0 to 1000. The page chain is followed either way.
	PageSize int
	// MaxPages bounds the page chain. Default 1000.
	MaxPages int
	// MaxFileBytes bounds the size of a file that is downloaded to be hashed
	// (a file that is not in LFS). Default 64 MiB.
	MaxFileBytes int64
	// Concurrency is the number of files downloaded at once. Default 4.
	Concurrency int
}

// Client reads per-file lists. It is safe for concurrent use.
type Client struct {
	base         *url.URL
	tokenFile    string
	http         *http.Client
	pageSize     int
	maxPages     int
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
	c := &Client{
		base:         base,
		tokenFile:    cfg.TokenFile,
		http:         cfg.HTTPClient,
		pageSize:     cfg.PageSize,
		maxPages:     orDefault(cfg.MaxPages, defaultMaxPages),
		maxFileBytes: maxFile,
		concurrency:  orDefault(cfg.Concurrency, defaultConcurrency),
	}
	c.http = withHostBoundAuth(cfg.HTTPClient, base)
	return c, nil
}

// withHostBoundAuth returns a copy of hc (or of a default client) that drops
// the Authorization header from a redirect to any other scheme, host or port
// than the Hub's. net/http already drops it for another domain, but it keeps
// it for another port of the same host, and the token belongs to the Hub only.
func withHostBoundAuth(hc *http.Client, base *url.URL) *http.Client {
	var out http.Client
	if hc != nil {
		out = *hc
	} else {
		out.Timeout = defaultTimeout
	}
	prev := out.CheckRedirect
	out.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req.URL.Scheme != base.Scheme || !strings.EqualFold(req.URL.Host, base.Host) {
			req.Header.Del("Authorization")
		}
		if prev != nil {
			return prev(req, via)
		}
		if len(via) >= 10 {
			return errors.New("hubfiles: stopped after 10 redirects")
		}
		return nil
	}
	return &out
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
// ErrInvalidListing, ErrTooManyPages, ErrCrossHostLink, and a context error.
// A response error is an *HTTPError.
func (c *Client) Files(ctx context.Context, repository, revision string) ([]Entry, error) {
	if err := checkRepository(repository); err != nil {
		return nil, err
	}
	if err := checkRevision(revision); err != nil {
		return nil, err
	}
	var tok token
	if c.tokenFile != "" {
		var err error
		if tok, err = readToken(c.tokenFile); err != nil {
			return nil, err
		}
	}
	entries, err := c.files(ctx, repository, revision, tok)
	if err != nil {
		return nil, scrub(err, tok)
	}
	return entries, nil
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
