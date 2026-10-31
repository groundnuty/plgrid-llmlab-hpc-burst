package stage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// maxRedirects is net/http's own limit, which a custom CheckRedirect replaces.
const maxRedirects = 10

// hubRedirectPolicy returns the client's CheckRedirect: it drops the
// Authorization header from a request to any host other than the Hub's. Go
// keeps the header on a redirect to a subdomain of the original host, and
// cdn-lfs.huggingface.co is one, where the Hub sends the content of an LFS
// file; the token is for the Hub alone, and a CDN may refuse a request that
// carries both a signed URL and a bearer token (PR #87 review, Opus nit). next
// is a policy the supplied client had, which still runs.
func hubRedirectPolicy(hubHost string, next func(*http.Request, []*http.Request) error) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return fmt.Errorf("stopped after %d redirects", maxRedirects)
		}
		if !strings.EqualFold(req.URL.Host, hubHost) {
			req.Header.Del("Authorization")
		}
		if next != nil {
			return next(req, via)
		}
		return nil
	}
}

// tally counts the bytes of one Checkpoint call for its Progress function:
// the bytes done and the bytes to download so far.
type tally struct {
	fn          func(done, total int64)
	every       time.Duration
	last        time.Time
	done, total int64
	touched     bool
}

// report calls the Progress function, at most once per interval unless forced;
// the first report always passes.
func (t *tally) report(force bool) {
	if t.fn == nil {
		return
	}
	t.touched = true
	if now := time.Now(); force || t.last.IsZero() || now.Sub(t.last) >= t.every {
		t.last = now
		t.fn(t.done, t.total)
	}
}

// finish reports the final state, which the interval could have swallowed.
func (t *tally) finish() {
	if t.touched {
		t.report(true)
	}
}

// attempt is one try at downloading one file. A try that fails takes back what
// it added, so a retry does not count the file twice (PR #87 review, Opus nit).
type attempt struct {
	t           *tally
	done, total int64
}

func (t *tally) begin() *attempt { return &attempt{t: t} }

func (a *attempt) expect(n int64) {
	a.total += n
	a.t.total += n
	a.t.report(false)
}

func (a *attempt) got(n int64) {
	a.done += n
	a.t.done += n
	a.t.report(false)
}

func (a *attempt) undo() {
	a.t.total -= a.total
	a.t.done -= a.done
	a.total, a.done = 0, 0
}

// download fetches one file from the Hub at the full commit id into a
// temporary name beside its place, checks its SHA-256 against the record and
// renames it into place, trying up to three times. The caller holds the
// checkpoint's lock. A token, when given, goes only into the Authorization
// header of requests to the Hub's own host (hubRedirectPolicy).
func (s *Stager) download(ctx context.Context, dir string, req agent.CheckpointRequest, f agentapi.FileDigest, t *tally) error {
	var last error
	for try := 0; try < 3; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(try) * s.c.RetryBackoff):
			}
		}
		retry, err := s.downloadOnce(ctx, dir, req, f, t.begin())
		if err == nil {
			return nil
		}
		last = err
		if !retry {
			return err
		}
	}
	return last
}

func (s *Stager) downloadOnce(ctx context.Context, dir string, req agent.CheckpointRequest, f agentapi.FileDigest, a *attempt) (retry bool, err error) {
	defer func() {
		if err != nil {
			a.undo()
		}
	}()
	segs := strings.Split(f.Path, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	u := s.c.HubEndpoint + "/" + req.Repository + "/resolve/" + req.Revision + "/" + strings.Join(segs, "/")
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, err
	}
	if req.HubToken != "" {
		hreq.Header.Set("Authorization", "Bearer "+req.HubToken)
	}
	resp, err := s.c.HTTPClient.Do(hreq)
	if err != nil {
		return ctx.Err() == nil, fmt.Errorf("GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		retry := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		return retry, fmt.Errorf("GET %s: HTTP %d", u, resp.StatusCode)
	}
	if resp.ContentLength > 0 {
		a.expect(resp.ContentLength)
	}
	p := filepath.Join(dir, filepath.FromSlash(f.Path))
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), "."+filepath.Base(p)+".part-*")
	if err != nil {
		return false, err
	}
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			if _, werr := tmp.Write(buf[:n]); werr != nil {
				return false, werr
			}
			a.got(int64(n))
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return ctx.Err() == nil, fmt.Errorf("GET %s: %w", u, rerr)
		}
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return false, fmt.Errorf("the download has SHA-256 %s, not the identity record's %s", got, f.SHA256)
	}
	if err := tmp.Sync(); err != nil {
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return false, err
	}
	ok = true
	return false, nil
}
