package hubfiles

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// maxPageBytes bounds one page of the listing; 1000 entries are about 0.5 MB.
const maxPageBytes = 32 << 20

// treeEntry is one entry of the Hub's tree listing.
type treeEntry struct {
	Type string `json:"type"`
	OID  string `json:"oid"`
	Size int64  `json:"size"`
	Path string `json:"path"`
	LFS  *struct {
		OID  string `json:"oid"`
		Size int64  `json:"size"`
	} `json:"lfs"`
}

// listTree reads every page of the recursive listing at the full commit id.
// The first URL is built here; each later one is the Link header's rel="next",
// used as given (it holds an escaped cursor) once it is checked to stay on
// the Hub's scheme and host.
func (c *Client) listTree(ctx context.Context, repo, rev string, tok token) ([]treeEntry, error) {
	first := *c.base
	first.Path = "/api/models/" + repo + "/tree/" + rev
	q := url.Values{"recursive": {"true"}}
	if c.pageSize > 0 {
		q.Set("limit", fmt.Sprint(c.pageSize))
	}
	first.RawQuery = q.Encode()

	var (
		all  []treeEntry
		next = first.String()
		seen = map[string]bool{}
	)
	for page := 1; next != ""; page++ {
		if page > c.maxPages {
			return nil, fmt.Errorf("%w: more than %d pages", ErrTooManyPages, c.maxPages)
		}
		if seen[next] {
			return nil, fmt.Errorf("%w: page %s was already read", ErrTooManyPages, next)
		}
		seen[next] = true
		entries, link, err := c.getPage(ctx, next, tok)
		if err != nil {
			return nil, err
		}
		all = append(all, entries...)
		if next, err = c.resolveNext(next, link); err != nil {
			return nil, err
		}
	}
	return all, nil
}

// getPage fetches one page and returns its entries and the raw Link values.
func (c *Client) getPage(ctx context.Context, pageURL string, tok token) ([]treeEntry, []string, error) {
	req, err := c.newRequest(ctx, pageURL, tok)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("hubfiles: listing request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, statusError(resp, tok != "")
	}
	var entries []treeEntry
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxPageBytes))
	if err := dec.Decode(&entries); err != nil {
		return nil, nil, fmt.Errorf("%w: reading %s: %v", ErrInvalidListing, resp.Request.URL.Path, err)
	}
	return entries, resp.Header.Values("Link"), nil
}

// newRequest builds a GET for rawURL, with the token when there is one. The
// token is in the Authorization header only, never in a URL.
func (c *Client) newRequest(ctx context.Context, rawURL string, tok token) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("hubfiles: building request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok.reveal())
	}
	return req, nil
}

// resolveNext returns the URL of the rel="next" link among the Link header
// values, resolved against the page it came with, or "" when there is none.
// A link to another scheme or host is refused.
func (c *Client) resolveNext(current string, links []string) (string, error) {
	target := nextLink(links)
	if target == "" {
		return "", nil
	}
	cur, err := url.Parse(current)
	if err != nil {
		return "", fmt.Errorf("hubfiles: page URL: %w", err)
	}
	ref, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("%w: unparseable next-page link", ErrInvalidListing)
	}
	next := cur.ResolveReference(ref)
	if next.Scheme != c.base.Scheme || !strings.EqualFold(next.Host, c.base.Host) {
		return "", fmt.Errorf("%w: %s://%s, expected %s", ErrCrossHostLink, next.Scheme, next.Host, c.base.Host)
	}
	return next.String(), nil
}

var (
	linkPattern = regexp.MustCompile(`<([^>]*)>((?:\s*;\s*[^;,<]*)*)`)
	relPattern  = regexp.MustCompile(`(?i)(?:^|;)\s*rel\s*=\s*"?([^";]*)"?`)
)

// nextLink returns the target of the first link with rel="next" (RFC 8288)
// among several Link header values, or "".
func nextLink(values []string) string {
	for _, v := range values {
		for _, m := range linkPattern.FindAllStringSubmatch(v, -1) {
			rel := relPattern.FindStringSubmatch(m[2])
			if rel == nil {
				continue
			}
			for _, r := range strings.Fields(rel[1]) {
				if strings.EqualFold(r, "next") {
					return strings.TrimSpace(m[1])
				}
			}
		}
	}
	return ""
}

// toEntries turns the listing into Entries: directories are dropped, every
// file gets its checks. An LFS entry carries its SHA-256 from lfs.oid; the
// SHA-256 of any other entry is filled in by the downloads.
func toEntries(listed []treeEntry) ([]Entry, error) {
	var entries []Entry
	seen := map[string]bool{}
	for _, t := range listed {
		switch t.Type {
		case "directory":
			continue
		case "file":
		default:
			// Fail closed: an entry we do not understand might be a file the
			// deployment ID has to cover.
			return nil, fmt.Errorf("%w: %q has the unknown type %q", ErrInvalidListing, t.Path, t.Type)
		}
		if err := checkListedPath(t.Path); err != nil {
			return nil, err
		}
		if seen[t.Path] {
			return nil, fmt.Errorf("%w: %q is listed twice", ErrInvalidListing, t.Path)
		}
		seen[t.Path] = true
		if !commitPattern.MatchString(t.OID) {
			return nil, fmt.Errorf("%w: %q has no 40-hex git blob id", ErrInvalidListing, t.Path)
		}
		e := Entry{BlobID: t.OID, Size: t.Size}
		e.Path = t.Path
		if t.LFS != nil {
			if !sha256Pattern.MatchString(t.LFS.OID) {
				return nil, fmt.Errorf("%w: %q has an lfs.oid that is not 64 lowercase hex characters", ErrInvalidListing, t.Path)
			}
			e.LFS = true
			e.SHA256 = t.LFS.OID
			e.Size = t.LFS.Size
		}
		if e.Size < 0 {
			return nil, fmt.Errorf("%w: %q has a negative size", ErrInvalidListing, t.Path)
		}
		entries = append(entries, e)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: the revision has no files", ErrInvalidListing)
	}
	return entries, nil
}
