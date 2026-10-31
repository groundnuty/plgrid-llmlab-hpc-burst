package hubfiles

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// maxPageBytes bounds one page of the listing; 1000 entries are about 0.5 MB.
const maxPageBytes = 32 << 20

// page is one answered listing request.
type page struct {
	entries []treeEntry
	links   []string // the values of the Link header fields
	url     *url.URL // the URL that answered, after any redirects
}

// listTree reads every page of the recursive listing at the full commit id.
// The first URL is built here; each later one is the Link header's
// rel="next", used as given (it holds an escaped cursor) once it is checked
// to stay in the same snapshot: see nextPage.
func (c *Client) listTree(ctx context.Context, repo, rev string, tok token) ([]treeEntry, error) {
	first := *c.base
	first.Path = "/api/models/" + repo + "/tree/" + rev
	first.RawQuery = url.Values{
		"recursive": {"true"},
		"limit":     {strconv.Itoa(c.pageSize)},
	}.Encode()

	var (
		all  []treeEntry
		next = first.String()
		seen = map[string]bool{}
	)
	for n := 1; next != ""; n++ {
		if n > c.maxPages {
			return nil, fmt.Errorf("%w: more than %d pages", ErrTooManyPages, c.maxPages)
		}
		if seen[next] {
			return nil, fmt.Errorf("%w: page %s was already read", ErrTooManyPages, next)
		}
		seen[next] = true
		p, err := c.getPage(ctx, next, rev, tok)
		if err != nil {
			return nil, err
		}
		all = append(all, p.entries...)
		if next, err = c.nextPage(p); err != nil {
			return nil, err
		}
	}
	return all, nil
}

// getPage fetches and parses one page. The listing is the one answer that is
// not checked against anything else, so before the body is read the URL that
// answered (after redirects, which stay on the Hub's origin) must be the
// recursive listing of the requested commit.
func (c *Client) getPage(ctx context.Context, pageURL, rev string, tok token) (page, error) {
	req, err := c.newRequest(ctx, pageURL, tok)
	if err != nil {
		return page{}, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.listHTTP.Do(req)
	if err != nil {
		return page{}, fmt.Errorf("hubfiles: listing request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return page{}, statusError(resp, tok != "")
	}
	final := resp.Request.URL
	if err := checkListingURL(final, c.base, rev); err != nil {
		return page{}, err
	}
	body, err := c.readBody(ctx, resp, final.Path)
	if err != nil {
		return page{}, err
	}
	entries, err := parseListing(body)
	if err != nil {
		return page{}, fmt.Errorf("%s: %w", final.Path, err)
	}
	return page{entries: entries, links: resp.Header.Values("Link"), url: final}, nil
}

// readBody reads a page, no more than c.maxPageBytes. A page one byte over the
// limit is an error, not a page cut short: a truncated listing that still
// parses would be a smaller file list. A failure of the read itself (a stall
// until the context ends, a reset connection) is not a verdict on the
// listing, so it keeps its own error for the caller to classify.
func (c *Client) readBody(ctx context.Context, resp *http.Response, path string) ([]byte, error) {
	if resp.ContentLength > c.maxPageBytes {
		return nil, fmt.Errorf("%w: the page %s is %d bytes, the limit is %d", ErrInvalidListing, path, resp.ContentLength, c.maxPageBytes)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxPageBytes+1))
	if err != nil {
		if cerr := ctx.Err(); cerr != nil && !errors.Is(err, cerr) {
			return nil, fmt.Errorf("hubfiles: reading the listing page %s: %w (%w)", path, cerr, err)
		}
		return nil, fmt.Errorf("hubfiles: reading the listing page %s: %w", path, err)
	}
	if int64(len(body)) > c.maxPageBytes {
		return nil, fmt.Errorf("%w: the page %s is longer than %d bytes", ErrInvalidListing, path, c.maxPageBytes)
	}
	return body, nil
}

// recursiveQuery checks that a query string asks for the recursive listing,
// once. A page without recursion lists only the top level.
func recursiveQuery(raw string) error {
	q, err := url.ParseQuery(raw)
	if err != nil {
		return fmt.Errorf("%w: the query string of a listing page does not parse", ErrInvalidListing)
	}
	if v := q["recursive"]; len(v) != 1 || v[0] != "true" {
		return fmt.Errorf("%w: a listing page is not the recursive=true listing (recursive=%q)", ErrInvalidListing, v)
	}
	return nil
}

// checkListingURL checks that u, the URL that answered a listing request, is
// on the Hub's origin and is the recursive listing of commit rev: the path
// /api/models/<repository>/tree/<rev>. The repository is not compared, so a
// Hub redirect to a renamed repository works.
func checkListingURL(u, base *url.URL, rev string) error {
	if originKey(u) != originKey(base) {
		return fmt.Errorf("%w: the listing was answered by %s, expected %s", ErrCrossHostLink, originKey(u), originKey(base))
	}
	if !strings.HasPrefix(u.Path, "/api/models/") || !strings.HasSuffix(u.Path, "/tree/"+rev) {
		return fmt.Errorf("%w: the listing was answered by %s, which is not the tree of commit %s", ErrInvalidListing, u.Path, rev)
	}
	return recursiveQuery(u.RawQuery)
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

// nextPage returns the URL of the rel="next" link of p, resolved against the
// URL that answered, or "" when p is the last page.
//
// The next page must be the same snapshot as this one, or the list would mix
// revisions that no download checks: the same origin (scheme, host and port,
// so a link to http or another host is ErrCrossHostLink and the token never
// goes there), the same path as the page that carried the link (which
// holds the repository and the full commit; comparing with that page and not
// with the first request lets a repository rename redirect through), and
// recursive=true. A link that leaves the snapshot is an error, not the end of
// the list.
func (c *Client) nextPage(p page) (string, error) {
	target, err := nextLinkTarget(p.links)
	if err != nil || target == "" {
		return "", err
	}
	ref, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("%w: unparseable next-page link", ErrInvalidListing)
	}
	next := p.url.ResolveReference(ref)
	if next.User != nil {
		return "", fmt.Errorf("%w: the next-page link carries credentials", ErrInvalidListing)
	}
	if originKey(next) != originKey(c.base) {
		return "", fmt.Errorf("%w: next page at %s, expected %s", ErrCrossHostLink, originKey(next), originKey(c.base))
	}
	if next.Path != p.url.Path {
		return "", fmt.Errorf("%w: the next page %s is not the page %s it continues", ErrInvalidListing, next.Path, p.url.Path)
	}
	if err := recursiveQuery(next.RawQuery); err != nil {
		return "", err
	}
	next.Fragment, next.RawFragment = "", ""
	return next.String(), nil
}
