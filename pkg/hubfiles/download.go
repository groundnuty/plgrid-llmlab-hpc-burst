package hubfiles

import (
	"context"
	"crypto/sha1" // the git blob id is a SHA-1 by definition; it is compared with the listing's oid, not used for security
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// resolveURL is /<repo>/resolve/<rev>/<path> on the Hub, with each path
// segment escaped on its own so a space, '#', '?' or '%' in a file name stays
// part of the name. repo and rev are validated, so they need no escaping, and
// the path was checked by checkListedPath.
func (c *Client) resolveURL(repo, rev, path string) string {
	segs := strings.Split(path, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	u := *c.base
	u.Path = "/" + repo + "/resolve/" + rev + "/" + path
	u.RawPath = "/" + repo + "/resolve/" + rev + "/" + strings.Join(segs, "/")
	return u.String()
}

// hashBlob downloads one file that is not in LFS and returns its SHA-256 in
// lowercase hex. The content is streamed through the SHA-256 and through a
// SHA-1 that starts with the git blob header "blob <size>\0", with the size
// the listing gives, so the SHA-1 is the file's git blob id. That id must
// equal the listing's oid (ErrBlobMismatch), and the number of bytes read must
// equal the listed size (ErrSizeMismatch). Nothing is written to disk.
func (c *Client) hashBlob(ctx context.Context, repo, rev string, e Entry, tok token) (string, error) {
	if e.Size > c.maxFileBytes {
		return "", fmt.Errorf("%w: %q is %d bytes, the limit is %d", ErrFileTooLarge, e.Path, e.Size, c.maxFileBytes)
	}
	req, err := c.newRequest(ctx, c.resolveURL(repo, rev, e.Path), tok)
	if err != nil {
		return "", err
	}
	// Ask for the bytes as stored; net/http would otherwise decompress, and a
	// stored .gz file must be hashed as it is.
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("hubfiles: download of %q: %w", e.Path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", statusError(resp, tok != "")
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && !strings.EqualFold(enc, "identity") {
		return "", fmt.Errorf("hubfiles: %q was served with Content-Encoding %q, so its stored bytes cannot be hashed", e.Path, enc)
	}
	if resp.ContentLength >= 0 && resp.ContentLength != e.Size {
		return "", fmt.Errorf("%w: %q: the listing says %d bytes, the Hub sends %d", ErrSizeMismatch, e.Path, e.Size, resp.ContentLength)
	}

	blob := sha1.New()
	fmt.Fprintf(blob, "blob %d\x00", e.Size)
	sum := sha256.New()
	// One byte more than the listed size, to tell "too long" from "exact".
	n, err := io.Copy(io.MultiWriter(blob, sum), io.LimitReader(resp.Body, e.Size+1))
	if err != nil {
		return "", fmt.Errorf("hubfiles: reading %q: %w", e.Path, err)
	}
	if n != e.Size {
		return "", fmt.Errorf("%w: %q: the listing says %d bytes, the download has %d", ErrSizeMismatch, e.Path, e.Size, n)
	}
	if got := hex.EncodeToString(blob.Sum(nil)); got != e.BlobID {
		return "", fmt.Errorf("%w: %q: the listing's oid is %s, the content hashes to %s", ErrBlobMismatch, e.Path, e.BlobID, got)
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}
