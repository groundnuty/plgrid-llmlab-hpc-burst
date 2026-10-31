// Package hubfiles reads a checkpoint's per-file list from the Hugging Face
// Hub: the "files" input of the deployment ID (HBEP-3, scheme v1, "The
// per-file list"; package deploymentid takes the result).
//
// For a repository and a full commit id, Client.Files returns one Entry per
// file of that revision, sorted bytewise by path:
//
//   - The listing is GET /api/models/<repo>/tree/<revision>?recursive=true&limit=1000.
//     Spike S7 saw the Hub cut a listing at 50 entries with no error when the
//     request named no limit, so the request always names the largest limit
//     the Hub accepts (Config.PageSize can ask for less). The Hub may still
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
// Errors keep their kind: a response error is an *HTTPError (with the
// Retry-After of a 429 or 503), a cancelled or stalled call wraps the context
// error, and Retryable tells a failure worth another try (429, 5xx, network
// and timeout errors) from a permanent one (a file that does not match its
// blob id, an unusable listing, a bad revision or token file).
//
// Only the standard library is used.
package hubfiles
