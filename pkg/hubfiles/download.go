package hubfiles

import (
	"bytes"
	"context"
	"crypto/sha1" // the git blob id is a SHA-1 by definition; it is compared with the listing's oid, not used for security
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
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

// checkSize refuses a file the client may not download, before any request.
func (c *Client) checkSize(e Entry) error {
	if e.Size > c.maxFileBytes {
		return fmt.Errorf("%w: %q is %d bytes, the limit is %d", ErrFileTooLarge, e.Path, e.Size, c.maxFileBytes)
	}
	return nil
}

// hashBlob downloads one file that is not in LFS and returns its SHA-256 in
// lowercase hex, after the checks of download. Nothing is written to disk and
// the content is not kept.
func (c *Client) hashBlob(ctx context.Context, repo, rev string, e Entry, tok token) (string, error) {
	return c.download(ctx, repo, rev, e, tok, io.Discard)
}

// fetch downloads one file of the revision and returns its content, which has
// passed every check of download. On any failure it returns no bytes. An empty
// file gives an empty, non-nil slice, because the callers tell "absent" (nil)
// from "present and empty".
func (c *Client) fetch(ctx context.Context, repo, rev string, e Entry, tok token) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := c.download(ctx, repo, rev, e, tok, &buf); err != nil {
		return nil, err
	}
	if buf.Len() == 0 {
		return []byte{}, nil
	}
	return buf.Bytes(), nil
}

// download streams one file of the revision to dst and returns the SHA-256 of
// the content in lowercase hex. It is the one place that decides whether a
// download can be trusted, for both hashBlob and fetch:
//   - a file larger than the client's limit is refused before the request
//     (ErrFileTooLarge), and no more than the listed size plus one byte is ever
//     read, to tell "too long" from "exact";
//   - the number of bytes must equal the listed size (ErrSizeMismatch);
//   - a file that is not in LFS must have the git blob id of the listing,
//     sha1("blob <size>\0" + content) (ErrBlobMismatch);
//   - the content's SHA-256 must equal e.SHA256 (ErrBlobMismatch) when the
//     entry has one: always for an LFS file, whose lfs.oid is that hash, and
//     for another file whose hash was already computed (a caller that holds
//     the entries Files returned). The blob id of an LFS file names its
//     pointer, not its content, and is not checked.
//
// dst receives the bytes as they arrive, before the checks end, so a caller
// that keeps them must drop them when download returns an error.
func (c *Client) download(ctx context.Context, repo, rev string, e Entry, tok token, dst io.Writer) (string, error) {
	if err := c.checkSize(e); err != nil {
		return "", err
	}
	req, err := c.newRequest(ctx, c.resolveURL(repo, rev, e.Path), tok)
	if err != nil {
		return "", err
	}
	// Ask for the bytes as stored; net/http would otherwise decompress, and a
	// stored .gz file must be hashed as it is.
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.dlHTTP.Do(req)
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

	sum := sha256.New()
	sinks := []io.Writer{sum, dst}
	var blob hash.Hash
	if !e.LFS {
		// The SHA-1 starts with the git blob header, with the size the listing
		// gives, so it ends as the file's git blob id.
		blob = sha1.New()
		fmt.Fprintf(blob, "blob %d\x00", e.Size)
		sinks = append(sinks, blob)
	}
	// One byte more than the listed size, to tell "too long" from "exact".
	n, err := io.Copy(io.MultiWriter(sinks...), io.LimitReader(resp.Body, e.Size+1))
	if err != nil {
		return "", fmt.Errorf("hubfiles: reading %q: %w", e.Path, err)
	}
	if n != e.Size {
		return "", fmt.Errorf("%w: %q: the listing says %d bytes, the download has %d", ErrSizeMismatch, e.Path, e.Size, n)
	}
	if blob != nil {
		if got := hex.EncodeToString(blob.Sum(nil)); got != e.BlobID {
			return "", fmt.Errorf("%w: %q: the listing's oid is %s, the content hashes to %s", ErrBlobMismatch, e.Path, e.BlobID, got)
		}
	}
	got := hex.EncodeToString(sum.Sum(nil))
	if (e.LFS || e.SHA256 != "") && got != e.SHA256 {
		return "", fmt.Errorf("%w: %q: the expected SHA-256 is %q, the content hashes to %s", ErrBlobMismatch, e.Path, e.SHA256, got)
	}
	return got, nil
}
