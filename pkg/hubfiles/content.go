package hubfiles

import (
	"context"
	"fmt"
)

// Content returns the content of one file of repository at revision: the
// listing is read as Files reads it (every page, the same checks), the entry
// whose path is exactly path is found, and the file is downloaded from
// /<repo>/resolve/<revision>/<path> and checked before any byte is returned:
//   - a file that is not in LFS has the listed size and the listed git blob
//     id, sha1("blob <size>\0" + content);
//   - an LFS file has the listed size and the listed lfs.oid as its SHA-256.
//
// A file larger than Config.MaxFileBytes is refused before the download
// (ErrFileTooLarge), and the read never takes more than the listed size plus
// one byte. An empty file gives an empty, non-nil slice. On any failure no
// bytes are returned.
//
// path is relative to the checkpoint root and matched exactly (case
// included). A path that is not in the revision, names a directory, or cannot
// be a listed path (empty, absolute, with "." or ".." segments) is an error
// that matches ErrNoSuchFile. The revision and the repository are checked as
// Files checks them, before any request, and the token is handled as Files
// handles it. The errors are those of Files, with ErrNoSuchFile in addition.
func (c *Client) Content(ctx context.Context, repository, revision, path string) ([]byte, error) {
	if err := checkRepository(repository); err != nil {
		return nil, err
	}
	if err := checkRevision(revision); err != nil {
		return nil, err
	}
	if err := checkListedPath(path); err != nil {
		return nil, fmt.Errorf("%w: %q is not a clean relative path", ErrNoSuchFile, path)
	}
	tok, err := c.currentToken()
	if err != nil {
		return nil, err
	}
	data, err := c.content(ctx, repository, revision, path, tok)
	if err != nil {
		return nil, scrub(err, tok)
	}
	return data, nil
}

func (c *Client) content(ctx context.Context, repo, rev, path string, tok token) ([]byte, error) {
	listed, err := c.listTree(ctx, repo, rev, tok)
	if err != nil {
		return nil, err
	}
	entries, err := toEntries(listed)
	if err != nil {
		return nil, err
	}
	e, ok, err := entryAt(entries, path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %q is not a file of revision %s", ErrNoSuchFile, path, rev)
	}
	return c.fetch(ctx, repo, rev, e, tok)
}
