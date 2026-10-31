package hubfiles

import (
	"context"
	"fmt"
	"unicode/utf8"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The checkpoint-root files that can hold a chat template (HBEP-3, Deployment
// ID), as package deploymentid takes them.
const (
	jinjaTemplatePath   = "chat_template.jinja"
	jsonTemplatePath    = "chat_template.json"
	tokenizerConfigPath = "tokenizer_config.json"
)

// TemplateSources reads the chat-template files of a checkpoint for
// deploymentid.ResolveChatTemplate: chat_template.jinja, chat_template.json
// and tokenizer_config.json at the checkpoint root (exact paths, case
// included), each only when entries has an entry with that path. entries are
// those Files returned for repository at revision; the listing is not read
// again, so the blob id, size and LFS flag of each file come from the entries
// and the download is checked against them exactly as Content checks one
// (and, since the entries of Files hold a SHA-256 for every file, against
// that too).
//
// Jinja is set as a string, and content that is not valid UTF-8 is an error
// that matches deploymentid.ErrInvalid. ChatTemplateJSON and TokenizerConfig
// are the raw bytes; the JSON is parsed by ResolveChatTemplate, not here. A
// file that is absent leaves its field nil, and a file that is present but
// empty gives a non-nil empty value, which the resolver treats as present.
// Spec is always nil: the caller sets the serving spec's own template.
//
// Any failure fails the whole call and returns the zero TemplateSources. The
// revision, the repository and the token are handled as in Files, and the
// errors are those of Content.
func (c *Client) TemplateSources(ctx context.Context, repository, revision string, entries []Entry) (deploymentid.TemplateSources, error) {
	if err := checkRepository(repository); err != nil {
		return deploymentid.TemplateSources{}, err
	}
	if err := checkRevision(revision); err != nil {
		return deploymentid.TemplateSources{}, err
	}
	tok, err := c.currentToken()
	if err != nil {
		return deploymentid.TemplateSources{}, err
	}
	src, err := c.templateSources(ctx, repository, revision, entries, tok)
	if err != nil {
		return deploymentid.TemplateSources{}, scrub(err, tok)
	}
	return src, nil
}

func (c *Client) templateSources(ctx context.Context, repo, rev string, entries []Entry, tok token) (deploymentid.TemplateSources, error) {
	// Settle everything the entries can settle before the first download: a
	// path listed twice, a file over the limit.
	var todo []Entry
	for _, path := range []string{jinjaTemplatePath, jsonTemplatePath, tokenizerConfigPath} {
		e, ok, err := entryAt(entries, path)
		if err != nil {
			return deploymentid.TemplateSources{}, err
		}
		if !ok {
			continue
		}
		if err := c.checkSize(e); err != nil {
			return deploymentid.TemplateSources{}, err
		}
		todo = append(todo, e)
	}

	var src deploymentid.TemplateSources
	for _, e := range todo {
		data, err := c.fetch(ctx, repo, rev, e, tok)
		if err != nil {
			return deploymentid.TemplateSources{}, err
		}
		switch e.Path {
		case jinjaTemplatePath:
			if !utf8.Valid(data) {
				return deploymentid.TemplateSources{}, fmt.Errorf("%w: %s is not valid UTF-8", deploymentid.ErrInvalid, e.Path)
			}
			text := string(data)
			src.Jinja = &text
		case jsonTemplatePath:
			src.ChatTemplateJSON = data
		case tokenizerConfigPath:
			src.TokenizerConfig = data
		}
	}
	return src, nil
}

// entryAt finds the entry whose path is exactly path. Two entries with one
// path cannot come from Files, which refuses such a listing, so they are an
// error rather than a choice between two blob ids.
func entryAt(entries []Entry, path string) (Entry, bool, error) {
	var (
		found Entry
		n     int
	)
	for _, e := range entries {
		if e.Path == path {
			found = e
			n++
		}
	}
	if n > 1 {
		return Entry{}, false, fmt.Errorf("%w: the entries list %q %d times", ErrInvalidListing, path, n)
	}
	return found, n == 1, nil
}
