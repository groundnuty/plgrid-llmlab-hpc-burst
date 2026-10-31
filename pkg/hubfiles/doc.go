// Package hubfiles reads a checkpoint's per-file list from the Hugging Face
// Hub: the "files" input of the deployment ID (HBEP-3, scheme v1, "The
// per-file list"; package deploymentid takes the result).
//
// For a repository and a full commit id, Client.Files returns one Entry per
// file of that revision, sorted bytewise by path:
//
//   - The listing is GET /api/models/<repo>/tree/<revision>?recursive=true.
//     Every Link header with rel="next" is followed. Spike S7 saw the Hub cut
//     a listing at 50 entries with no error, and a probe on 31 Oct 2026 saw
//     all 77 entries of a 74-file repository in one page. The Hub's page size
//     is not ours to rely on, so the chain is always followed, and
//     Config.PageSize can ask for smaller pages.
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
// file that holds it, not the token: the file must not be readable by group
// or others (mode 600), and it is read again for every call, so a rotated
// token takes effect without a restart. The token goes only into the
// Authorization header of requests to the configured Hub host. It is not
// logged, not part of any error, and the type that holds it prints as
// "[redacted]" under every fmt verb. The package does no logging of its own.
//
// Only the standard library is used.
package hubfiles
