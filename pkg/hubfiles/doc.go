// Package hubfiles reads a checkpoint's per-file list from the Hugging Face
// Hub: the "files" input of the deployment ID (HBEP-3, scheme v1, "The
// per-file list"; package deploymentid takes the result).
//
// For a repository and a full commit id, Client.Files returns one Entry per
// file of that revision, sorted bytewise by path:
//
//   - The listing is GET /api/models/<repo>/tree/<revision>?recursive=true&limit=1000.
//     The Hub's page size depends on the query: on 31 Oct 2026 a request
//     with no limit returned a 77-entry listing in one page, while one with
//     expand=true returned 50 entries and a Link header (spike S7 saw a
//     listing end at 50). So the request always names the largest limit the
//     Hub accepts (Config.PageSize can ask for less). The Hub may still
//     cut a page below the limit, so every Link header with rel="next" is
//     followed, parsed with RFC 8288's quoting rules; a Link header that does
//     not parse is an error, never the end of the list. The next page must
//     stay in the same snapshot (same origin, same path, hence the same full
//     commit, and recursive=true), and a redirect of the listing may only
//     stay on the Hub's origin, because nothing verifies the listing
//     afterwards.
//   - Each page must be exactly one JSON array of objects: invalid UTF-8, an
//     unpaired surrogate escape, trailing data, a key in another case and a
//     repeated key are errors, because a lenient decoder would turn them into
//     a successful list of other paths. The sizes of an LFS entry (size and
//     lfs.size) must agree, and no path may repeat or be both a file and a
//     directory.
//   - An LFS file contributes its lfs.oid, which is the SHA-256 of its
//     content. The file is not downloaded.
//   - Any other file (configuration, tokenizer, template) is downloaded from
//     /<repo>/resolve/<revision>/<path> and streamed through two hashes. Its
//     git blob id, sha1("blob <size>\0" + content), must equal the listing's
//     oid, otherwise the call fails; its SHA-256 is what the Hub does not
//     publish for such a file.
//
// Client.Content returns the bytes of one file of the revision, and
// Client.TemplateSources the chat-template files of a checkpoint, for
// deploymentid.ResolveChatTemplate (HBEP-3, Deployment ID, chat_template_sha256):
//
//   - Content reads the listing as Files does, finds the entry whose path is
//     exactly the one asked for (none: ErrNoSuchFile) and downloads it from
//     /<repo>/resolve/<revision>/<path>. Nothing is returned before the
//     download is checked, with the same code that Files uses on the files it
//     hashes. A file that is not in LFS must have the listed size and the
//     listed git blob id. An LFS file must have the listed size and the listed
//     lfs.oid as its SHA-256 (ErrBlobMismatch otherwise). A file above
//     Config.MaxFileBytes is refused before any request, whether or not it is
//     in LFS, and the read never takes more than the listed size plus one
//     byte. An empty file gives an empty, non-nil slice.
//   - TemplateSources takes the entries Files returned and does not list
//     again: for each of chat_template.jinja, chat_template.json and
//     tokenizer_config.json at the checkpoint root (exact paths, case
//     included) that has an entry, it downloads the file and checks it
//     against that entry's blob id, size, LFS flag and SHA-256, as Content
//     does. Jinja is set as a string (content that is not valid UTF-8 is an
//     error), the other two as raw bytes, and a file with no entry leaves its
//     field nil. A file that is present but empty is not nil, since the
//     resolver tells absent from empty. Spec is left nil for the caller. Any
//     failure fails the whole call and returns nothing.
//
// The revision must be 40 lowercase hex characters. A branch, a tag or a
// short id is refused before any request, because the list of a moving
// reference means nothing (HBEP-3).
//
// A gated or private repository needs a Hub token. The configuration names a
// file that holds it, not the token: the file's mode must be exactly 600 or
// 400, and it is read again for every call, so a rotated token takes effect
// without a restart. The token goes only into the
// Authorization header of requests to the configured Hub host. It is not
// logged, not part of any error, and the type that holds it prints as
// "[redacted]" under every fmt verb. The package does no logging of its own.
//
// Errors keep their kind, for Files, Content and TemplateSources alike: a
// response error is an *HTTPError (with the Retry-After of a 429 or 503), a
// cancelled or stalled call wraps the context error, and Retryable tells a
// failure worth another try (429, 5xx, network and timeout errors) from a
// permanent one (a file that does not match its blob id, a path the revision
// does not have, an unusable listing, a bad revision or token file).
//
// Only the standard library is used.
package hubfiles
